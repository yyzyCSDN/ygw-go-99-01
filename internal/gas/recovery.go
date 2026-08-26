package gas

import (
	"time"

	"coalminegas/internal/record"
)

type RecoveryPlan struct {
	Points map[string]PointState
}

func (s *Supervisor) BuildRecoveryPlan(snapshot map[string]PointState) RecoveryPlan {
	plan := RecoveryPlan{Points: make(map[string]PointState)}
	live := s.table.Snapshot()
	for id, saved := range snapshot {
		current, ok := live[id]
		if !ok {
			plan.Points[id] = saved
			continue
		}
		current.State = s.cfg.Thresholds.Level(current.Value)
		plan.Points[id] = current
	}
	return plan
}

func (s *Supervisor) Recover(snapshot map[string]PointState) error {
	plan := s.BuildRecoveryPlan(snapshot)
	entries := make(map[string]PointState, len(plan.Points))
	for id, state := range plan.Points {
		entries[id] = state
	}
	s.table.Restore(entries)
	for id, state := range plan.Points {
		if fan, ok := s.fans[id]; ok {
			_ = fan.RecoverState(state.State == StateTripped || state.State == StateAlerted, true)
		}
		s.cfg.Bus.Publish("gas.point_recovered", id)
		_ = s.cfg.Records.Append(record.Entry{
			Point:   id,
			Kind:    "recovery",
			Message: string(state.State),
			At:      time.Now(),
		})
	}
	s.cfg.Bus.Publish("gas.recovered", len(plan.Points))
	return nil
}
