package trip

import "time"

type Breaker interface {
	Confirm(id string) <-chan struct{}
	Open(id string) error
}

type SimulatedBreaker struct {
	delay time.Duration
}

func NewSimulatedBreaker(delay time.Duration) *SimulatedBreaker {
	return &SimulatedBreaker{delay: delay}
}

func (b *SimulatedBreaker) Confirm(id string) <-chan struct{} {
	ch := make(chan struct{})
	go func() {
		time.Sleep(b.delay)
		close(ch)
	}()
	return ch
}

func (b *SimulatedBreaker) Open(id string) error {
	return nil
}
