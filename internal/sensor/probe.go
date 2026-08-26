package sensor

import "time"

type Probe struct {
	ID      string
	Zone    string
	Online  bool
	Since   time.Time
	Reading float64
}
