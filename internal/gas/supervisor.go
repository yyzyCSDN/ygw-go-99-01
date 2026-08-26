package gas

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"coalminegas/internal/alarm"
	"coalminegas/internal/event"
	"coalminegas/internal/record"
	"coalminegas/internal/sensor"
	"coalminegas/internal/trip"
	"coalminegas/internal/vent"
)

var (
	ErrNotOverLimit = errors.New("concentration not over trip limit")
	ErrManualHold   = errors.New("manual hold active")
)

type Config struct {
	Thresholds     Thresholds
	Bus            *event.Bus
	Trips          *trip.Manager
	Alarms         *alarm.Manager
	Records        *record.Recorder
	Sensors        *sensor.Registry
	Interlock      *vent.Interlock
	HoldTTL        time.Duration
	TripTimeout    time.Duration
	SampleInterval time.Duration
}

type Supervisor struct {
	cfg       Config
	table     *ConcentrationTable
	fans      map[string]*vent.Fan
	mu        sync.Mutex
	hold      map[string]time.Time
	decisions map[string]float64
	decMu     sync.Mutex
}

func NewSupervisor(cfg Config) *Supervisor {
	return &Supervisor{
		cfg:       cfg,
		table:     NewConcentrationTable(),
		fans:      make(map[string]*vent.Fan),
		hold:      make(map[string]time.Time),
		decisions: make(map[string]float64),
	}
}

func (s *Supervisor) Table() *ConcentrationTable {
	return s.table
}

func (s *Supervisor) AddFan(id string, fan *vent.Fan) {
	s.fans[id] = fan
}

func (s *Supervisor) Bus() *event.Bus {
	return s.cfg.Bus
}

func (s *Supervisor) Ingest(reading Reading) (State, error) {
	_ = s.table.UpdatePoint(reading.Point, reading.Zone, reading.Value, reading.At)
	if s.cfg.Sensors != nil {
		_ = s.cfg.Sensors.Update(reading.Point, reading.Value)
	}
	s.decMu.Lock()
	s.decisions[reading.Point] = reading.Value
	s.decMu.Unlock()
	s.cfg.Interlock.Update(reading.Point, reading.Value)
	level := s.cfg.Thresholds.Level(reading.Value)
	switch level {
	case StateAlerted:
		s.table.SetState(reading.Point, StateAlerted)
		_, _ = s.cfg.Alarms.Evaluate(reading.Point, "alerted")
	case StateTripped:
		s.table.SetState(reading.Point, StateTripped)
		_, _ = s.cfg.Alarms.Evaluate(reading.Point, "tripped")
	default:
		s.table.SetState(reading.Point, StateNormal)
		_ = s.cfg.Alarms.Clear(reading.Point)
	}
	if err := s.cfg.Records.Append(record.Entry{
		Point:   reading.Point,
		Kind:    "reading",
		Message: fmt.Sprintf("value %.2f", reading.Value),
		At:      reading.At,
	}); err != nil {
		return level, err
	}
	s.cfg.Bus.Publish("gas.ingest", reading.Point)
	return level, nil
}

func (s *Supervisor) Calibrate(id string, value float64) error {
	if _, ok := s.table.Get(id); !ok {
		return ErrUnknownPoint
	}
	_, err := s.table.UpdateValue(id, value, time.Now())
	if err != nil {
		return err
	}
	return s.refreshDecision(id)
}

func (s *Supervisor) refreshDecision(id string) error {
	state, ok := s.table.Get(id)
	if !ok {
		return ErrUnknownPoint
	}
	s.decMu.Lock()
	s.decisions[id] = state.Value
	s.decMu.Unlock()
	s.cfg.Interlock.Update(id, state.Value)
	s.cfg.Bus.Publish("gas.decision_refreshed", id)
	return nil
}

func (s *Supervisor) StartFan(id string, timeout time.Duration) error {
	fan, ok := s.fans[id]
	if !ok {
		return fmt.Errorf("fan %s not registered", id)
	}
	if !s.cfg.Interlock.Active(id, s.cfg.Thresholds.Alert) {
		return ErrNotOverLimit
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := fan.Start(ctx); err != nil {
		s.cfg.Bus.Publish("gas.fan_start_failed", id)
		_ = s.cfg.Records.Append(record.Entry{
			Point:   id,
			Kind:    "fan_start_failed",
			Message: err.Error(),
			At:      time.Now(),
		})
		return err
	}
	s.cfg.Bus.Publish("gas.fan_started", id)
	return nil
}

func (s *Supervisor) TriggerTrip(id string, timeout time.Duration) error {
	state, ok := s.table.Get(id)
	if !ok {
		return ErrUnknownPoint
	}
	if state.Value < s.cfg.Thresholds.Trip {
		return ErrNotOverLimit
	}
	if state.State == StateTripped {
		s.cfg.Bus.Publish("trip.duplicate_ignored", id)
		_ = s.cfg.Records.Append(record.Entry{
			Point:   id,
			Kind:    "duplicate_trip_ignored",
			Message: "point already tripped",
			At:      time.Now(),
		})
		return nil
	}
	if lock, ok := s.cfg.Trips.State(id); ok && lock.State == trip.StateTripped {
		s.cfg.Bus.Publish("trip.lock_duplicate_ignored", id)
		return nil
	}
	s.mu.Lock()
	until, held := s.hold[id]
	if held {
		if time.Now().Before(until) {
			s.mu.Unlock()
			return ErrManualHold
		}
		delete(s.hold, id)
	}
	s.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	wait, err := s.cfg.Trips.Trigger(context.Background(), id)
	if err != nil {
		return err
	}
	fan, hasFan := s.fans[id]
	fanDone := make(chan error, 1)
	if hasFan {
		go func() {
			fanDone <- fan.Start(ctx)
		}()
	} else {
		fanDone <- nil
	}
	select {
	case <-wait.Done():
	case <-ctx.Done():
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	_ = <-fanDone
	tripped, err := s.table.SetState(id, StateTripped)
	if err != nil {
		return err
	}
	_, err = s.table.SetCounts(id, tripped.AlertCount, tripped.TripCount+1)
	if err != nil {
		return err
	}
	s.cfg.Bus.Publish("trip.commit", id)
	if err := s.cfg.Records.Append(record.Entry{
		Point:   id,
		Kind:    "trip",
		Message: "power trip confirmed",
		At:      time.Now(),
	}); err != nil {
		current, _ := s.table.Get(id)
		_, _ = s.table.SetState(id, StateAlerted)
		_, _ = s.table.SetCounts(id, current.AlertCount, current.TripCount-1)
		if hasFan {
			_ = fan.Abort()
		}
		_ = s.cfg.Trips.Clear(id)
		rollback, _ := s.table.Get(id)
		s.cfg.Bus.Publish("trip.rollback_state", string(rollback.State))
		_ = s.cfg.Records.Append(record.Entry{
			Point:   id,
			Kind:    "trip_rollback",
			Message: err.Error(),
			At:      time.Now(),
		})
		s.cfg.Bus.Publish("trip.rollback", id)
		return err
	}
	return nil
}

func (s *Supervisor) ResetTrip(id string) error {
	return s.cfg.Trips.Reset(id)
}

func (s *Supervisor) ManualRestore(id string) error {
	state, ok := s.table.Get(id)
	if !ok {
		return ErrUnknownPoint
	}
	_, err := s.table.SetState(id, StateRestored)
	if err != nil {
		return err
	}
	_, err = s.table.SetCounts(id, state.AlertCount, state.TripCount)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.hold[id] = time.Now().Add(s.cfg.HoldTTL)
	s.mu.Unlock()
	if fan, ok := s.fans[id]; ok {
		fan.DisableAuto()
	}
	_ = s.cfg.Trips.Clear(id)
	_ = s.cfg.Alarms.Clear(id)
	s.cfg.Bus.Publish("gas.manual_restored", id)
	return nil
}

func (s *Supervisor) ReleaseManualHold(id string) {
	s.mu.Lock()
	delete(s.hold, id)
	s.mu.Unlock()
	if fan, ok := s.fans[id]; ok {
		fan.EnableAuto()
	}
	s.cfg.Bus.Publish("gas.manual_hold_released", id)
}

func (s *Supervisor) Evaluate(id string) error {
	state, ok := s.table.Get(id)
	if !ok {
		return ErrUnknownPoint
	}
	level := s.cfg.Thresholds.Level(state.Value)
	if level != StateTripped {
		return nil
	}
	s.mu.Lock()
	until, held := s.hold[id]
	if held {
		if time.Now().Before(until) {
			s.mu.Unlock()
			return ErrManualHold
		}
		delete(s.hold, id)
	}
	s.mu.Unlock()
	if fan, ok := s.fans[id]; ok && !fan.AutoEnabled() {
		return ErrManualHold
	}
	return s.TriggerTrip(id, s.cfg.TripTimeout)
}

func (s *Supervisor) ConfirmStable(id string, window time.Duration, sample func() float64) error {
	ctx, cancel := context.WithTimeout(context.Background(), window)
	defer cancel()
	err := vent.WaitStable(ctx, s.cfg.Thresholds.Alert, sample, s.cfg.SampleInterval)
	if err != nil {
		s.cfg.Bus.Publish("gas.stabilize_failed", id)
		return err
	}
	s.cfg.Bus.Publish("gas.stabilized", id)
	return nil
}

func (s *Supervisor) RequestFanStop(id string, window time.Duration) error {
	fan, ok := s.fans[id]
	if !ok {
		return fmt.Errorf("fan %s not registered", id)
	}
	if err := s.ConfirmStable(id, window, func() float64 {
		state, ok := s.table.Get(id)
		if !ok {
			return 0
		}
		return state.Value
	}); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), s.cfg.TripTimeout)
	defer cancel()
	return fan.Stop(ctx)
}
