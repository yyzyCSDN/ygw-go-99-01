package gas_test

import (
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

func TestRecoveryRebuildsFromLiveSensorState(t *testing.T) {
	bus := event.NewBus()
	alarms := alarm.NewManager(bus, alarm.NewDispatcher())
	trips := trip.NewManager(bus, trip.NewSimulatedBreaker(5*time.Millisecond))
	registry := sensor.NewRegistry(10 * time.Minute)
	_ = registry.Register("p01", "zone-a")
	recorder := record.NewRecorder(record.NewMemoryStore(), bus, nil)
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
	_, _ = sup.Ingest(gas.Reading{Point: "p01", Zone: "zone-a", Value: 0.2, At: time.Now()})
	snapshot := map[string]gas.PointState{
		"p01": {ID: "p01", Zone: "zone-a", Value: 1.5, State: gas.StateTripped, TripCount: 1},
	}
	if err := sup.Recover(snapshot); err != nil {
		t.Fatalf("recover must succeed: %v", err)
	}
	if state, _ := sup.Table().Get("p01"); state.State != gas.StateNormal {
		t.Fatalf("recovered point must follow the live reading, got %s", state.State)
	}
	if state := fan.State(); state.State != vent.StateOff {
		t.Fatalf("fan must follow the live gas state after recovery, got %s", state.State)
	}
}
