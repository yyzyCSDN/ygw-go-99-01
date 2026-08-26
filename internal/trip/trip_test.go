package trip

import (
	"context"
	"strings"
	"testing"
	"time"

	"coalminegas/internal/event"
)

// silentBreaker is a Breaker whose power-cut confirmation never arrives,
// modelling a real field failure where the断电动作确认 never comes back.
type silentBreaker struct {
	opened string
}

func (b *silentBreaker) Confirm(id string) <-chan struct{} {
	// Never close: confirmation never arrives. This is the failure mode that
	// triggers the 8s timeout path.
	return make(chan struct{})
}

func (b *silentBreaker) Open(id string) error {
	b.opened = id
	return nil
}

// TestTriggerTimeoutAutoReleases reproduces the reported defect: when the
// breaker never confirms, the trip must time out on its own, leave the
// StateExecuting state, and allow Reset + a fresh Trigger afterwards.
func TestTriggerTimeoutAutoReleases(t *testing.T) {
	bus := event.NewBus()
	br := &silentBreaker{}
	m := NewManager(bus, br)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	wait, err := m.Trigger(ctx, "p01")
	if err != nil {
		t.Fatalf("Trigger returned unexpected error: %v", err)
	}

	// The breaker never confirms, so await must exit via ctx.Done() and settle
	// the lock back to armed. Without the timeout flowing into await, this
	// would block forever.
	select {
	case <-wait.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("await never settled after timeout: lock stuck in executing")
	}

	// Lock must no longer be executing — it reset to armed.
	lock, ok := m.State("p01")
	if !ok {
		t.Fatal("lock vanished after timeout")
	}
	if lock.State == StateExecuting {
		t.Fatalf("lock still executing after timeout: state=%s reason=%s", lock.State, lock.Reason)
	}
	if lock.State != StateArmed {
		t.Fatalf("expected armed after timeout, got state=%s", lock.State)
	}

	// The breaker must NOT have been opened: confirmation never arrived, so
	// there is nothing to断电.
	if br.opened != "" {
		t.Fatalf("breaker should not open on timeout, opened for %s", br.opened)
	}

	// Reset must succeed now (previously refused with "still executing").
	if err := m.Reset("p01"); err != nil {
		t.Fatalf("Reset after timeout failed: %v", err)
	}

	// A fresh re-trigger must be allowed — the lock is no longer stuck.
	ctx2, cancel2 := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel2()
	if _, err := m.Trigger(ctx2, "p01"); err != nil {
		t.Fatalf("re-trigger after timeout rejected: %v", err)
	}
}

// TestTriggerWhileExecutingRejected ensures the "already executing" guard
// still holds while a trip is genuinely in flight.
func TestTriggerWhileExecutingRejected(t *testing.T) {
	bus := event.NewBus()
	m := NewManager(bus, &silentBreaker{})

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	if _, err := m.Trigger(ctx, "p01"); err != nil {
		t.Fatalf("first Trigger failed: %v", err)
	}

	// While still executing (breaker silent), a second trigger must be refused
	// rather than silently overwriting the in-flight lock.
	_, err := m.Trigger(ctx, "p01")
	if err == nil {
		t.Fatal("expected error when triggering an in-flight lock, got nil")
	}
	if !strings.Contains(err.Error(), "already executing") {
		t.Fatalf("unexpected error message: %v", err)
	}

	// Let it time out so the goroutine settles and we don't leak.
	<-time.After(80 * time.Millisecond)
}

// TestTriggerConfirmTripped verifies the happy path: breaker confirms, the
// lock becomes tripped, the breaker opens, and Reset is then refused (a
// tripped lock is a different state from an armed one).
func TestTriggerConfirmTripped(t *testing.T) {
	bus := event.NewBus()
	m := NewManager(bus, &confirmedBreaker{})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	wait, err := m.Trigger(ctx, "p01")
	if err != nil {
		t.Fatalf("Trigger failed: %v", err)
	}
	<-wait.Done()

	lock, ok := m.State("p01")
	if !ok {
		t.Fatal("lock vanished")
	}
	if lock.State != StateTripped {
		t.Fatalf("expected tripped, got %s", lock.State)
	}
}

type confirmedBreaker struct{}

func (b *confirmedBreaker) Confirm(id string) <-chan struct{} {
	ch := make(chan struct{})
	go func() {
		time.Sleep(5 * time.Millisecond)
		close(ch)
	}()
	return ch
}
func (b *confirmedBreaker) Open(id string) error { return nil }
