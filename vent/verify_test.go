package vent_test

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

func TestStabilizeTimeoutNotTreatedAsStable(t *testing.T) {
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
		SampleInterval: 20 * time.Millisecond,
	})
	fan := vent.NewFan("p01", bus, vent.NewSimulatedActuator())
	sup.AddFan("p01", fan)
	_, _ = sup.Ingest(gas.Reading{Point: "p01", Zone: "zone-a", Value: 1.5, At: time.Now()})
	if err := sup.StartFan("p01", time.Second); err != nil {
		t.Fatalf("initial fan start must succeed: %v", err)
	}
	err := sup.RequestFanStop("p01", 250*time.Millisecond)
	if err == nil {
		t.Fatalf("stop must be rejected when the concentration never stabilizes")
	}
	if state := fan.State(); state.State != vent.StateRunning {
		t.Fatalf("fan must keep running when stabilization timed out, got %s", state.State)
	}
}
