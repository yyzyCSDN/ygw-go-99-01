package trip

import "sort"

func (m *Manager) Locks() []LockState {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]LockState, 0, len(m.locks))
	for _, lock := range m.locks {
		out = append(out, *lock)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func (m *Manager) Executing() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	count := 0
	for _, lock := range m.locks {
		if lock.State == StateExecuting {
			count++
		}
	}
	return count
}

func (m *Manager) Tripped() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	count := 0
	for _, lock := range m.locks {
		if lock.State == StateTripped {
			count++
		}
	}
	return count
}

func (m *Manager) Armed() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	count := 0
	for _, lock := range m.locks {
		if lock.State == StateArmed {
			count++
		}
	}
	return count
}
