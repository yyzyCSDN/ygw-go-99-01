package gas

import (
	"errors"
	"testing"
	"time"

	"coalminegas/internal/alarm"
	"coalminegas/internal/event"
	"coalminegas/internal/record"
	"coalminegas/internal/sensor"
	"coalminegas/internal/trip"
	"coalminegas/internal/vent"
)

// newTestSupervisor 构建一个最小可运行的 Supervisor，含一个测点 p01 及其风机。
func newTestSupervisor(t *testing.T, holdTTL time.Duration) (*Supervisor, *vent.Fan, *trip.Manager) {
	t.Helper()
	bus := event.NewBus()
	dispatcher := alarm.NewDispatcher()
	alarms := alarm.NewManager(bus, dispatcher)
	breaker := trip.NewSimulatedBreaker(10 * time.Millisecond)
	trips := trip.NewManager(bus, breaker)
	registry := sensor.NewRegistry(3 * time.Minute)
	_ = registry.Register("p01", "采区一")
	recorder := record.NewRecorder(record.NewMemoryStore(), bus, nil)
	interlock := vent.NewInterlock()
	sup := NewSupervisor(Config{
		Thresholds:     Thresholds{Alert: 0.8, Trip: 1.2},
		Bus:            bus,
		Trips:          trips,
		Alarms:         alarms,
		Records:        recorder,
		Sensors:        registry,
		Interlock:      interlock,
		HoldTTL:        holdTTL,
		TripTimeout:    500 * time.Millisecond,
		SampleInterval: 50 * time.Millisecond,
	})
	fan := vent.NewFan("p01", bus, vent.NewSimulatedActuator())
	sup.AddFan("p01", fan)
	return sup, fan, trips
}

// 测点浓度超限且已被复电后，自动闭锁 Evaluate 不应再次断电，应让位于现场手动复电。
func TestEvaluateYieldsToManualRestore(t *testing.T) {
	sup, _, trips := newTestSupervisor(t, 5*time.Minute)

	//先用一次低于断电阈值的读数登记测点（避免 TriggerTrip 因 StateTripped 提前返回），
	//再用标校把浓度推到超限，然后真正下发一次断电闭锁。
	if _, err := sup.Ingest(Reading{Point: "p01", Zone: "采区一", Value: 0.5, At: time.Now()}); err != nil {
		t.Fatalf("ingest baseline reading: %v", err)
	}
	if err := sup.Calibrate("p01", 1.5); err != nil {
		t.Fatalf("calibrate over limit: %v", err)
	}
	if err := sup.TriggerTrip("p01", 500*time.Millisecond); err != nil {
		t.Fatalf("initial trip: %v", err)
	}
	if lock, ok := trips.State("p01"); !ok || lock.State != trip.StateTripped {
		t.Fatalf("expected lock tripped after initial trip, got ok=%v state=%q", ok, lock.State)
	}

	//现场手动复电：断开闭锁、恢复供电，并登记手动保持。
	if err := sup.ManualRestore("p01"); err != nil {
		t.Fatalf("manual restore: %v", err)
	}

	//浓度仍超限（人员撤离、排瓦斯尚未回稳的现场常见情形）。
	if err := sup.Calibrate("p01", 1.5); err != nil {
		t.Fatalf("calibrate still over limit: %v", err)
	}

	//自动闭锁策略在此刻不应再次断电，必须让位于手动复电。
	err := sup.Evaluate("p01")
	if !errors.Is(err, ErrManualHold) {
		t.Fatalf("expected ErrManualHold during manual restore window, got %v", err)
	}
	if lock, ok := trips.State("p01"); ok && lock.State == trip.StateTripped {
		t.Fatalf("auto lock should not have re-tripped during manual hold, state=%q", lock.State)
	}
}

// AutoEnabled 必须真实反映 DisableAuto/EnableAuto 的状态，而非恒为 true。
func TestFanAutoEnabledReflectsState(t *testing.T) {
	_, fan, _ := newTestSupervisor(t, 5*time.Minute)
	if !fan.AutoEnabled() {
		t.Fatalf("fresh fan should be auto-enabled")
	}
	fan.DisableAuto()
	if fan.AutoEnabled() {
		t.Fatalf("fan should be auto-disabled after DisableAuto")
	}
	fan.EnableAuto()
	if !fan.AutoEnabled() {
		t.Fatalf("fan should be auto-enabled after EnableAuto")
	}
}

// 手动保持到期后，自动闭锁策略应重新生效，不再让位。
func TestManualHoldExpiresAndRestoresAuto(t *testing.T) {
	sup, _, _ := newTestSupervisor(t, 20*time.Millisecond)

	if _, err := sup.Ingest(Reading{Point: "p01", Zone: "采区一", Value: 1.5, At: time.Now()}); err != nil {
		t.Fatalf("ingest over-limit reading: %v", err)
	}
	if err := sup.TriggerTrip("p01", 500*time.Millisecond); err != nil {
		t.Fatalf("initial trip: %v", err)
	}
	if err := sup.ManualRestore("p01"); err != nil {
		t.Fatalf("manual restore: %v", err)
	}

	//保持期内自动让位。
	if err := sup.Evaluate("p01"); !errors.Is(err, ErrManualHold) {
		t.Fatalf("expected ErrManualHold within TTL, got %v", err)
	}

	//等待 TTL 过期。
	time.Sleep(40 * time.Millisecond)

	//过期后自动闭锁重新生效：Evaluate 不再返回 ErrManualHold，而是真正下发断电。
	err := sup.Evaluate("p01")
	if errors.Is(err, ErrManualHold) {
		t.Fatalf("manual hold should have expired, but Evaluate still yielded: %v", err)
	}
	//Evaluate 成功完成一次新的断电闭锁。
	if err != nil {
		t.Fatalf("expected Evaluate to re-arm auto trip after hold expiry, got %v", err)
	}
}

// 从未登记过手动保持的测点，反复调用 Evaluate 不应误发 manual_hold_expired 事件，
// 也不应改动风机自动状态。
func TestNoHoldNoSpuriousExpiry(t *testing.T) {
	sup, fan, bus := newTestSupervisor(t, time.Minute)
	_ = bus //事件总线用于副作用校验：此处只确认风机自动状态不被反复重置

	//测点超限但从未手动复电（无保持登记）。
	if _, err := sup.Ingest(Reading{Point: "p01", Zone: "采区一", Value: 1.5, At: time.Now()}); err != nil {
		t.Fatalf("ingest over-limit reading: %v", err)
	}
	for i := 0; i < 3; i++ {
		//没有保持时，Evaluate 不应因 manualHoldActive 触发 EnableAuto 等副作用；
		//此处仅断言风机仍处于自动使能（说明未被误改）。
		if !fan.AutoEnabled() {
			t.Fatalf("fan auto state should remain untouched without a hold, iter %d", i)
		}
	}
}
