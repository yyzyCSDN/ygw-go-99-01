package vent

import (
	"context"
	"errors"
	"fmt"
	"time"
)

var ErrNotStable = errors.New("concentration not stable within window")

type Sampler func() float64

type SampleReport struct {
	Count int
	Limit float64
	Min   float64
	Max   float64
}

func (r SampleReport) String() string {
	return fmt.Sprintf("samples=%d limit=%.2f min=%.2f max=%.2f", r.Count, r.Limit, r.Min, r.Max)
}

func WaitStable(ctx context.Context, limit float64, sample Sampler, interval time.Duration) error {
	history := make([]float64, 0, 8)
	for {
		select {
		case <-ctx.Done():
			report := SampleReport{
				Count: len(history),
				Limit: limit,
				Min:   minOf(history, limit),
				Max:   maxOf(history, limit),
			}
			return fmt.Errorf("%w: %s", ErrNotStable, report.String())
		case <-time.After(interval):
			value := sample()
			history = append(history, value)
			if value < limit {
				return nil
			}
		}
	}
}

func minOf(values []float64, fallback float64) float64 {
	if len(values) == 0 {
		return fallback
	}
	min := values[0]
	for _, value := range values[1:] {
		if value < min {
			min = value
		}
	}
	return min
}

func maxOf(values []float64, fallback float64) float64 {
	if len(values) == 0 {
		return fallback
	}
	max := values[0]
	for _, value := range values[1:] {
		if value > max {
			max = value
		}
	}
	return max
}
