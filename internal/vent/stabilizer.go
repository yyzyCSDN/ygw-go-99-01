package vent

import (
	"context"
	"errors"
	"time"
)

var ErrNotStable = errors.New("concentration not stable within window")

type Sampler func() float64

func WaitStable(ctx context.Context, limit float64, sample Sampler, interval time.Duration) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(interval):
			if sample() < limit {
				return nil
			}
		}
	}
}
