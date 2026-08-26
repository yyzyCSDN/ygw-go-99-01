package gas

import "sort"

type Counts struct {
	Normal   int
	Alerted  int
	Tripped  int
	Restored int
	Holds    int
}

func (s *Supervisor) Counts() Counts {
	counts := Counts{}
	for _, state := range s.table.Points() {
		switch state.State {
		case StateNormal:
			counts.Normal++
		case StateAlerted:
			counts.Alerted++
		case StateTripped:
			counts.Tripped++
		case StateRestored:
			counts.Restored++
		}
	}
	s.mu.Lock()
	counts.Holds = len(s.hold)
	s.mu.Unlock()
	return counts
}

func (s *Supervisor) Fans() []FanStatus {
	out := make([]FanStatus, 0, len(s.fans))
	for _, fan := range s.fans {
		state := fan.State()
		monitor := fan.Monitor()
		out = append(out, FanStatus{
			ID:      state.ID,
			State:   state.State,
			Reason:  state.Reason,
			Starts:  state.Starts,
			AutoOff: state.AutoOff,
			Stops:   monitor.Stops,
			Hours:   monitor.RunningHours,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

type FanStatus struct {
	ID      string
	State   string
	Reason  string
	Starts  int
	AutoOff bool
	Stops   int
	Hours   float64
}
