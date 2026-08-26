package trip

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"coalminegas/internal/event"
)

// ErrAlreadyTripped is returned when a trip is requested for a point that is
// already locked (StateTripped). The same over-limit condition must only close
// the feeder once; signal bounce after the lock is set must not re-issue the
// breaker command. The lock stays put until an explicit Reset/Clear re-arms it.
var ErrAlreadyTripped = errors.New("trip already locked for point")

const (
	StateArmed     = "armed"
	StateExecuting = "executing"
	StateTripped   = "tripped"
	StateCleared   = "cleared"
)

type LockState struct {
	ID        string
	State     string
	Reason    string
	UpdatedAt time.Time
}

type Wait struct {
	done chan struct{}
}

func (w *Wait) Done() <-chan struct{} {
	return w.done
}

type Manager struct {
	mu      sync.Mutex
	locks   map[string]*LockState
	breaker Breaker
	bus     *event.Bus
}

func NewManager(bus *event.Bus, breaker Breaker) *Manager {
	return &Manager{
		locks:   make(map[string]*LockState),
		breaker: breaker,
		bus:     bus,
	}
}

func (m *Manager) Trigger(ctx context.Context, id string) (*Wait, error) {
	m.mu.Lock()
	lock, ok := m.locks[id]
	if !ok {
		lock = &LockState{ID: id, State: StateArmed}
		m.locks[id] = lock
	}
	// A point that is already locked must not be re-triggered. The same
	// over-limit condition closes the feeder exactly once; a bouncing or
	// still-over-limit signal after the lock is set must not re-issue the
	// breaker command. Only an explicit Reset (re-arm) or Clear (restore)
	// moves the lock out of the tripped state and allows a fresh trigger.
	if lock.State == StateTripped {
		m.mu.Unlock()
		return nil, fmt.Errorf("%w: %s", ErrAlreadyTripped, id)
	}
	if lock.State == StateExecuting {
		m.mu.Unlock()
		return nil, fmt.Errorf("trip for %s is already executing", id)
	}
	lock.State = StateExecuting
	lock.Reason = ""
	lock.UpdatedAt = time.Now()
	m.mu.Unlock()
	m.bus.Publish("trip.executing", id)
	wait := &Wait{done: make(chan struct{})}
	go m.await(ctx, id, wait)
	return wait, nil
}

func (m *Manager) await(ctx context.Context, id string, wait *Wait) {
	select {
	case <-ctx.Done():
		m.finish(id, StateArmed, "cancelled")
	case <-m.breaker.Confirm(id):
		m.finish(id, StateTripped, "confirmed")
	}
	close(wait.done)
}

func (m *Manager) finish(id, state, reason string) {
	m.mu.Lock()
	if lock, ok := m.locks[id]; ok {
		lock.State = state
		lock.Reason = reason
		lock.UpdatedAt = time.Now()
	}
	m.mu.Unlock()
	m.bus.Publish("trip."+state, id)
	if state == StateTripped {
		_ = m.breaker.Open(id)
	}
}

func (m *Manager) Reset(id string) error {
	m.mu.Lock()
	lock, ok := m.locks[id]
	if !ok {
		m.mu.Unlock()
		return fmt.Errorf("trip for %s is unknown", id)
	}
	if lock.State == StateExecuting {
		m.mu.Unlock()
		return fmt.Errorf("trip for %s is still executing", id)
	}
	lock.State = StateArmed
	lock.Reason = ""
	lock.UpdatedAt = time.Now()
	m.mu.Unlock()
	m.bus.Publish("trip.reset", id)
	return nil
}

func (m *Manager) State(id string) (LockState, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	lock, ok := m.locks[id]
	if !ok {
		return LockState{}, false
	}
	return *lock, true
}

func (m *Manager) Clear(id string) error {
	m.mu.Lock()
	lock, ok := m.locks[id]
	if !ok {
		m.mu.Unlock()
		return fmt.Errorf("trip for %s is unknown", id)
	}
	lock.State = StateCleared
	lock.UpdatedAt = time.Now()
	m.mu.Unlock()
	m.bus.Publish("trip.cleared", id)
	return nil
}
