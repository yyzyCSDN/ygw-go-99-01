package gas

import (
	"errors"
	"sort"
	"sync"
	"time"
)

type State string

const (
	StateNormal   State = "normal"
	StateAlerted  State = "alerted"
	StateTripped  State = "tripped"
	StateRestored State = "restored"
)

type Thresholds struct {
	Alert float64
	Trip  float64
}

func (t Thresholds) Level(value float64) State {
	if value >= t.Trip {
		return StateTripped
	}
	if value >= t.Alert {
		return StateAlerted
	}
	return StateNormal
}

type Reading struct {
	Point string
	Zone  string
	Value float64
	At    time.Time
}

type PointState struct {
	ID         string
	Zone       string
	Value      float64
	State      State
	AlertCount int
	TripCount  int
	UpdatedAt  time.Time
}

type ConcentrationTable struct {
	mu    sync.RWMutex
	slots map[string]*PointState
}

func NewConcentrationTable() *ConcentrationTable {
	return &ConcentrationTable{slots: make(map[string]*PointState)}
}

func (t *ConcentrationTable) UpdatePoint(id, zone string, value float64, at time.Time) PointState {
	t.mu.Lock()
	defer t.mu.Unlock()
	state, ok := t.slots[id]
	if !ok {
		state = &PointState{ID: id, Zone: zone, State: StateNormal}
		t.slots[id] = state
	}
	state.Value = value
	state.UpdatedAt = at
	return *state
}

func (t *ConcentrationTable) UpdateValue(id string, value float64, at time.Time) (PointState, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	state, ok := t.slots[id]
	if !ok {
		return PointState{}, ErrUnknownPoint
	}
	state.Value = value
	state.UpdatedAt = at
	return *state, nil
}

func (t *ConcentrationTable) Get(id string) (PointState, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	state, ok := t.slots[id]
	if !ok {
		return PointState{}, false
	}
	return *state, true
}

func (t *ConcentrationTable) SetState(id string, state State) (PointState, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	entry, ok := t.slots[id]
	if !ok {
		return PointState{}, ErrUnknownPoint
	}
	entry.State = state
	entry.UpdatedAt = time.Now()
	return *entry, nil
}

func (t *ConcentrationTable) SetCounts(id string, alert, trip int) (PointState, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	entry, ok := t.slots[id]
	if !ok {
		return PointState{}, ErrUnknownPoint
	}
	entry.AlertCount = alert
	entry.TripCount = trip
	return *entry, nil
}

func (t *ConcentrationTable) Restore(entries map[string]PointState) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for id, entry := range entries {
		copy := entry
		t.slots[id] = &copy
	}
}

func (t *ConcentrationTable) Snapshot() map[string]PointState {
	t.mu.RLock()
	defer t.mu.RUnlock()
	out := make(map[string]PointState, len(t.slots))
	for id, entry := range t.slots {
		out[id] = *entry
	}
	return out
}

func (t *ConcentrationTable) Points() []PointState {
	t.mu.RLock()
	defer t.mu.RUnlock()
	out := make([]PointState, 0, len(t.slots))
	for _, entry := range t.slots {
		out = append(out, *entry)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func (t *ConcentrationTable) Len() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.slots)
}

var ErrUnknownPoint = errors.New("unknown monitoring point")
