package alarm

import (
	"sort"
	"sync"

	"coalminegas/internal/event"
)

type Manager struct {
	bus        *event.Bus
	dispatcher *Dispatcher
	mu         sync.Mutex
	active     map[string]Severity
	raised     int
	cleared    int
}

func NewManager(bus *event.Bus, dispatcher *Dispatcher) *Manager {
	return &Manager{
		bus:        bus,
		dispatcher: dispatcher,
		active:     make(map[string]Severity),
	}
}

func (m *Manager) Evaluate(point string, state string) (bool, error) {
	sev, ok := severityFor(state)
	if !ok {
		return false, nil
	}
	m.mu.Lock()
	prev, exists := m.active[point]
	m.active[point] = sev
	if !exists || prev != sev {
		m.raised++
	}
	m.mu.Unlock()
	m.dispatcher.Dispatch(sev)
	m.bus.Publish("alarm.raised", point)
	return true, nil
}

func (m *Manager) Clear(point string) error {
	m.mu.Lock()
	if _, ok := m.active[point]; !ok {
		m.mu.Unlock()
		return nil
	}
	delete(m.active, point)
	m.cleared++
	m.mu.Unlock()
	m.bus.Publish("alarm.cleared", point)
	return nil
}

func (m *Manager) ActiveCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.active)
}

func (m *Manager) Raised() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.raised
}

func (m *Manager) Cleared() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.cleared
}

func (m *Manager) DispatchHits() int {
	return m.dispatcher.Hits()
}

func (m *Manager) Active() map[string]Severity {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string]Severity, len(m.active))
	for point, sev := range m.active {
		out[point] = sev
	}
	return out
}

func (m *Manager) ActivePoints() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]string, 0, len(m.active))
	for point := range m.active {
		out = append(out, point)
	}
	sort.Strings(out)
	return out
}

func severityFor(state string) (Severity, bool) {
	switch state {
	case "alerted":
		return SeverityWarning, true
	case "tripped":
		return SeverityFatal, true
	default:
		return "", false
	}
}
