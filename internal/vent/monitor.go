package vent

import (
	"sync"
	"time"
)

type MonitorStats struct {
	Starts       int
	Stops        int
	RunningHours float64
}

type Monitor struct {
	mu          sync.Mutex
	starts      int
	stops       int
	active      bool
	activeSince time.Time
	running     time.Duration
}

func NewMonitor() *Monitor {
	return &Monitor{}
}

func (m *Monitor) RecordStart() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.starts++
	if !m.active {
		m.active = true
		m.activeSince = time.Now()
	}
}

func (m *Monitor) RecordStop() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.stops++
	if m.active {
		m.running += time.Since(m.activeSince)
		m.active = false
	}
}

func (m *Monitor) Stats() MonitorStats {
	m.mu.Lock()
	defer m.mu.Unlock()
	running := m.running
	if m.active {
		running += time.Since(m.activeSince)
	}
	return MonitorStats{
		Starts:       m.starts,
		Stops:        m.stops,
		RunningHours: running.Hours(),
	}
}
