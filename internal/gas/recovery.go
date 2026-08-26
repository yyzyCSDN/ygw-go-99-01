package gas

type RecoveryPlan struct {
	Points map[string]PointState
}

func (s *Supervisor) BuildRecoveryPlan(snapshot map[string]PointState) RecoveryPlan {
	plan := RecoveryPlan{Points: make(map[string]PointState)}
	for id, saved := range snapshot {
		plan.Points[id] = saved
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
	}
	s.cfg.Bus.Publish("gas.recovered", len(plan.Points))
	return nil
}
