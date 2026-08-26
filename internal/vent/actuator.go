package vent

import "time"

type Actuator interface {
	Start(id string) error
	Stop(id string) error
	Confirm(id string) <-chan struct{}
}

type SimulatedActuator struct {
	startDelay time.Duration
}

func NewSimulatedActuator() *SimulatedActuator {
	return &SimulatedActuator{
		startDelay: 60 * time.Millisecond,
	}
}

func (a *SimulatedActuator) Start(id string) error {
	return nil
}

func (a *SimulatedActuator) Stop(id string) error {
	return nil
}

func (a *SimulatedActuator) Confirm(id string) <-chan struct{} {
	ch := make(chan struct{})
	go func() {
		time.Sleep(a.startDelay)
		close(ch)
	}()
	return ch
}
