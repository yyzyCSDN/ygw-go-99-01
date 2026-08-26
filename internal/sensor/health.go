package sensor

import "time"

type Health struct {
	Total   int
	Online  int
	Offline int
	Stale   int
}

func (r *Registry) Health(now time.Time, staleAfter time.Duration) Health {
	items := r.Snapshot()
	health := Health{Total: len(items)}
	for _, probe := range items {
		if !probe.Online {
			health.Offline++
			continue
		}
		health.Online++
		if now.Sub(probe.Since) > staleAfter {
			health.Stale++
		}
	}
	return health
}
