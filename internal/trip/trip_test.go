package trip

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"coalminegas/internal/event"
)

// countingBreaker records how many times Open is called for a point. The
// safety guarantee under test is that the same over-limit condition closes the
// feeder exactly once: signal bounce after the lock is set must not re-issue
// the breaker command.
type countingBreaker struct {
	opens int32
	delay time.Duration
}

func (b *countingBreaker) Confirm(id string) <-chan struct{} {
	ch := make(chan struct{})
	go func() {
		time.Sleep(b.delay)
		close(ch)
	}()
	return ch
}

func (b *countingBreaker) Open(id string) error {
	atomic.AddInt32(&b.opens, 1)
	return nil
}

func (b *countingBreaker) Opens() int {
	return int(atomic.LoadInt32(&b.opens))
}

// TestTriggerLocksOnceOnBounce simulates an over-limit signal that bounces:
// the first trigger opens the feeder, and subsequent triggers while the point
// is already locked must be rejected and must NOT open the feeder again.
func TestTriggerLocksOnceOnBounce(t *testing.T) {
	breaker := &countingBreaker{delay: 5 * time.Millisecond}
	m := NewManager(event.NewBus(), breaker)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	// First over-limit event: feeder must open exactly once.
	if _, err := m.Trigger(ctx, "p01"); err != nil {
		t.Fatalf("first trigger: unexpected error: %v", err)
	}
	if err := waitForState(m, "p01", StateTripped, time.Second); err != nil {
		t.Fatalf("first trigger did not reach tripped: %v", err)
	}
	if got := breaker.Opens(); got != 1 {
		t.Fatalf("after first trigger, opens = %d, want 1", got)
	}

	// Signal bounce: the same over-limit condition arrives again while the
	// point is already locked. This must not re-issue the breaker command.
	for i := 0; i < 5; i++ {
		_, err := m.Trigger(ctx, "p01")
		if err == nil {
			t.Fatalf("bounce trigger %d: expected error, got nil", i)
		}
	}
	if got := breaker.Opens(); got != 1 {
		t.Fatalf("after bounce triggers, opens = %d, want 1 (feeder must not be re-opened)", got)
	}

	// The lock remains tripped until an explicit Reset re-arms it.
	st, ok := m.State("p01")
	if !ok || st.State != StateTripped {
		t.Fatalf("lock state = %q, want tripped", st.State)
	}
}

// TestResetReArmsForNextOverLimit ensures that after a manual Reset re-arms the
// lock, a fresh over-limit event can trigger again. The "lock once" guarantee
// applies per over-limit episode, not forever.
func TestResetReArmsForNextOverLimit(t *testing.T) {
	breaker := &countingBreaker{delay: 5 * time.Millisecond}
	m := NewManager(event.NewBus(), breaker)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if _, err := m.Trigger(ctx, "p01"); err != nil {
		t.Fatalf("first trigger: %v", err)
	}
	if err := waitForState(m, "p01", StateTripped, time.Second); err != nil {
		t.Fatalf("first trigger did not reach tripped: %v", err)
	}

	if err := m.Reset("p01"); err != nil {
		t.Fatalf("reset: %v", err)
	}

	// A new over-limit episode after reset must open the feeder again.
	if _, err := m.Trigger(ctx, "p01"); err != nil {
		t.Fatalf("trigger after reset: %v", err)
	}
	if err := waitForState(m, "p01", StateTripped, time.Second); err != nil {
		t.Fatalf("trigger after reset did not reach tripped: %v", err)
	}
	if got := breaker.Opens(); got != 2 {
		t.Fatalf("after reset+trigger, opens = %d, want 2", got)
	}
}

func waitForState(m *Manager, id, want string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		st, ok := m.State(id)
		if ok && st.State == want {
			return nil
		}
		if time.Now().After(deadline) {
			got := "<none>"
			if ok {
				got = st.State
			}
			return &stateError{id: id, got: got, want: want}
		}
		time.Sleep(2 * time.Millisecond)
	}
}

type stateError struct {
	id, got, want string
}

func (e *stateError) Error() string {
	return "point " + e.id + " state = " + e.got + ", want " + e.want
}
