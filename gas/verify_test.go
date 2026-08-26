package gas_test

import (
	"errors"
	"io"
	"testing"
	"time"

	"coalminegas/internal/alarm"
	"coalminegas/internal/event"
	"coalminegas/internal/gas"
	"coalminegas/internal/record"
	"coalminegas/internal/sensor"
	"coalminegas/internal/trip"
	"coalminegas/internal/vent"
)

type instantBreaker struct{}

func (b *instantBreaker) Confirm(id string) <-chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}

func (b *instantBreaker) Open(id string) error {
	return nil
}

type instantActuator struct{}

func (a *instantActuator) Start(id string) error {
	return nil
}

func (a *instantActuator) Stop(id string) error {
	return nil
}

func (a *instantActuator) Confirm(id string) <-chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}

type failOpener struct{}

func (f *failOpener) Open(path string) (io.WriteCloser, error) {
	return nil, errors.New("record store unavailable")
}

func TestTripStateUpdateAtomic(t *testing.T) {
	bus := event.NewBus()
	alarms := alarm.NewManager(bus, alarm.NewDispatcher())
	trips := trip.NewManager(bus, &instantBreaker{})
	registry := sensor.NewRegistry(10 * time.Minute)
	_ = registry.Register("p01", "zone-a")
	recorder := record.NewRecorder(&failOpener{}, bus, nil)
	interlock := vent.NewInterlock()
	sup := gas.NewSupervisor(gas.Config{
		Thresholds:     gas.Thresholds{Alert: 0.8, Trip: 1.2},
		Bus:            bus,
		Trips:          trips,
		Alarms:         alarms,
		Records:        recorder,
		Sensors:        registry,
		Interlock:      interlock,
		HoldTTL:        5 * time.Minute,
		TripTimeout:    2 * time.Second,
		SampleInterval: 10 * time.Millisecond,
	})
	fan := vent.NewFan("p01", bus, &instantActuator{})
	sup.AddFan("p01", fan)
	_, _ = sup.Ingest(gas.Reading{Point: "p01", Zone: "zone-a", Value: 0.3, At: time.Now()})
	_ = sup.Calibrate("p01", 1.5)
	if err := sup.TriggerTrip("p01", time.Second); err == nil {
		t.Fatalf("trip must fail when the record step fails")
	}
	if state, _ := sup.Table().Get("p01"); state.State == gas.StateTripped {
		t.Fatalf("trip state must not be partially committed, got %s", state.State)
	}
	if state, _ := sup.Table().Get("p01"); state.TripCount != 0 {
		t.Fatalf("trip count must roll back on failure, got %d", state.TripCount)
	}
	if state := fan.State(); state.State == vent.StateRunning {
		t.Fatalf("fan must not stay running when the trip commit failed, got %s", state.State)
	}
}
