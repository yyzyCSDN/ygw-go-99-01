package gas

import (
	"time"

	"coalminegas/internal/sensor"
)

type Sensor struct {
	registry *sensor.Registry
}

func NewSensor(registry *sensor.Registry) *Sensor {
	return &Sensor{registry: registry}
}

func (s *Sensor) Poll(id string) (Reading, error) {
	probe, ok := s.registry.Probe(id)
	if !ok {
		return Reading{}, ErrUnknownPoint
	}
	return Reading{
		Point: probe.ID,
		Zone:  probe.Zone,
		Value: probe.Reading,
		At:    time.Now(),
	}, nil
}
