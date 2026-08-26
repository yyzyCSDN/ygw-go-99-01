package sensor

import (
	"errors"
	"sort"
	"sync"
	"time"
)

var ErrDuplicate = errors.New("probe already registered")

type Registry struct {
	mu      sync.RWMutex
	probes  map[string]*Probe
	offline time.Duration
}

func NewRegistry(offlineAfter time.Duration) *Registry {
	return &Registry{
		probes:  make(map[string]*Probe),
		offline: offlineAfter,
	}
}

func (r *Registry) Register(id, zone string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.probes[id]; ok {
		return ErrDuplicate
	}
	r.probes[id] = &Probe{ID: id, Zone: zone, Online: true, Since: time.Now()}
	return nil
}

func (r *Registry) Update(id string, value float64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	probe, ok := r.probes[id]
	if !ok {
		return ErrUnknown
	}
	probe.Reading = value
	probe.Online = true
	probe.Since = time.Now()
	return nil
}

func (r *Registry) MarkOffline(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if probe, ok := r.probes[id]; ok {
		probe.Online = false
	}
}

func (r *Registry) Probe(id string) (Probe, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	probe, ok := r.probes[id]
	if !ok {
		return Probe{}, false
	}
	return *probe, true
}

func (r *Registry) OnlineCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	count := 0
	for _, probe := range r.probes {
		if probe.Online {
			count++
		}
	}
	return count
}

func (r *Registry) Snapshot() []Probe {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Probe, 0, len(r.probes))
	for _, probe := range r.probes {
		out = append(out, *probe)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func (r *Registry) Count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.probes)
}

var ErrUnknown = errors.New("probe not registered")
