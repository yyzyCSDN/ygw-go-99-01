package vent

import "sync"

type Interlock struct {
	mu       sync.Mutex
	readings map[string]float64
}

func NewInterlock() *Interlock {
	return &Interlock{
		readings: make(map[string]float64),
	}
}

func (il *Interlock) Update(id string, value float64) {
	il.mu.Lock()
	defer il.mu.Unlock()
	il.readings[id] = value
}

func (il *Interlock) Active(id string, threshold float64) bool {
	il.mu.Lock()
	defer il.mu.Unlock()
	value, ok := il.readings[id]
	if !ok {
		return false
	}
	return value >= threshold
}

func (il *Interlock) Snapshot() map[string]float64 {
	il.mu.Lock()
	defer il.mu.Unlock()
	out := make(map[string]float64, len(il.readings))
	for id, value := range il.readings {
		out[id] = value
	}
	return out
}
