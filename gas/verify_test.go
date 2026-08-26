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

type neverBreaker struct {
	confirm chan struct{}
}

func (b *neverBreaker) Confirm(id string) <-chan struct{} {
	return b.confirm
}

func (b *neverBreaker) Open(id string) error {
	return nil
}

type blockingActuator struct {
	confirm chan struct{}
}

func (a *blockingActuator) Start(id string) error {
	return nil
}

func (a *blockingActuator) Stop(id string) error {
	return nil
}

func (a *blockingActuator) Confirm(id string) <-chan struct{} {
	return a.confirm
}

func TestLockWaitCancelledOnTimeout(t *testing.T) {
	bus := event.NewBus()
	alarms := alarm.NewManager(bus, alarm.NewDispatcher())
	confirm := make(chan struct{})
	trips := trip.NewManager(bus, &neverBreaker{confirm: confirm})
	registry := sensor.NewRegistry(10 * time.Minute)
	_ = registry.Register("p01", "zone-a")
	recorder := record.NewRecorder(record.NewMemoryStore(), bus, nil)
	interlock := vent.NewInterlock()
	fan := vent.NewFan("p01", bus, &blockingActuator{confirm: confirm})
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
	sup.AddFan("p01", fan)
	_, _ = sup.Ingest(gas.Reading{Point: "p01", Zone: "zone-a", Value: 0.3, At: time.Now()})
	_ = sup.Calibrate("p01", 1.5)
	if err := sup.TriggerTrip("p01", 200*time.Millisecond); err == nil {
		t.Fatalf("trip wait must time out when the breaker never confirms")
	}
	if lock, ok := trips.State("p01"); ok && lock.State == trip.StateExecuting {
		t.Fatalf("lock must not stay executing after timeout, got %s", lock.State)
	}
	if err := sup.ResetTrip("p01"); err != nil {
		t.Fatalf("reset must work after timeout: %v", err)
	}
	close(confirm)
	if err := sup.TriggerTrip("p01", 2*time.Second); err != nil {
		t.Fatalf("re-trigger must work after timeout reset: %v", err)
	}
	if lock, ok := trips.State("p01"); !ok || lock.State != trip.StateTripped {
		t.Fatalf("lock must be tripped after confirmed re-trigger")
	}
}
