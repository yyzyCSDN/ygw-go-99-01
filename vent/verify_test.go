package vent_test

import (
	"errors"
	"sync"
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

type flakyActuator struct {
	mu      sync.Mutex
	fails   int
	confirm chan struct{}
}

func (a *flakyActuator) Start(id string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.fails > 0 {
		a.fails--
		return errors.New("motor controller rejected start")
	}
	return nil
}

func (a *flakyActuator) Stop(id string) error {
	return nil
}

func (a *flakyActuator) Confirm(id string) <-chan struct{} {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.confirm
}

func TestFanStartFailureNotShownAsRunning(t *testing.T) {
	bus := event.NewBus()
	alarms := alarm.NewManager(bus, alarm.NewDispatcher())
	trips := trip.NewManager(bus, trip.NewSimulatedBreaker(10*time.Millisecond))
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
	confirm := make(chan struct{})
	close(confirm)
	actuator := &flakyActuator{fails: 1, confirm: confirm}
	fan := vent.NewFan("p01", bus, actuator)
	sup.AddFan("p01", fan)
	_, _ = sup.Ingest(gas.Reading{Point: "p01", Zone: "zone-a", Value: 1.5, At: time.Now()})
	if err := sup.StartFan("p01", 2*time.Second); err == nil {
		t.Fatalf("start fan with failing actuator must return error")
	}
	if state := fan.State(); state.State == vent.StateRunning {
		t.Fatalf("fan must not be reported running after failed start, got %s", state.State)
	}
	if err := sup.StartFan("p01", 2*time.Second); err != nil {
		t.Fatalf("retry start must succeed after actuator recovers: %v", err)
	}
	if state := fan.State(); state.State != vent.StateRunning {
		t.Fatalf("fan must be running after successful retry, got %s", state.State)
	}
	if state := fan.State(); state.Starts != 1 {
		t.Fatalf("fan starts must be exactly one, got %d", state.Starts)
	}
}
