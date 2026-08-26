package vent

import (
	"context"
	"sync"

	"coalminegas/internal/event"
)

const (
	StateOff      = "off"
	StateStarting = "starting"
	StateRunning  = "running"
	StateStopping = "stopping"
	StateFailed   = "failed"
)

type State struct {
	ID      string
	State   string
	Reason  string
	Starts  int
	AutoOff bool
}

type Fan struct {
	id     string
	bus    *event.Bus
	actor  Actuator
	mon    *Monitor
	mu     sync.Mutex
	state  string
	reason string
	starts int
	auto   bool
}

func NewFan(id string, bus *event.Bus, actor Actuator) *Fan {
	return &Fan{
		id:    id,
		bus:   bus,
		actor: actor,
		mon:   NewMonitor(),
		state: StateOff,
		auto:  true,
	}
}

func (f *Fan) Start(ctx context.Context) error {
	f.mu.Lock()
	if f.state == StateRunning || f.state == StateStarting {
		f.mu.Unlock()
		return nil
	}
	f.state = StateStarting
	f.reason = ""
	f.mu.Unlock()
	if err := f.actor.Start(f.id); err != nil {
		f.mu.Lock()
		f.state = StateFailed
		f.reason = err.Error()
		f.mu.Unlock()
		f.bus.Publish("vent.fan_start_rejected", f.id)
		return err
	}
	select {
	case <-f.actor.Confirm(f.id):
		f.mu.Lock()
		f.state = StateRunning
		f.starts++
		f.mu.Unlock()
		f.mon.RecordStart()
		f.bus.Publish("vent.fan_running", f.id)
		return nil
	case <-ctx.Done():
		f.mu.Lock()
		f.state = StateFailed
		f.reason = ctx.Err().Error()
		f.mu.Unlock()
		f.bus.Publish("vent.fan_start_timeout", f.id)
		return ctx.Err()
	}
}

func (f *Fan) Stop(ctx context.Context) error {
	f.mu.Lock()
	if f.state == StateOff {
		f.mu.Unlock()
		return nil
	}
	f.state = StateStopping
	f.mu.Unlock()
	if err := f.actor.Stop(f.id); err != nil {
		f.mu.Lock()
		f.state = StateFailed
		f.reason = err.Error()
		f.mu.Unlock()
		f.bus.Publish("vent.fan_stop_rejected", f.id)
		return err
	}
	select {
	case <-f.actor.Confirm(f.id):
		f.mu.Lock()
		f.state = StateOff
		f.mu.Unlock()
		f.mon.RecordStop()
		f.bus.Publish("vent.fan_stopped", f.id)
		return nil
	case <-ctx.Done():
		f.mu.Lock()
		f.state = StateFailed
		f.reason = ctx.Err().Error()
		f.mu.Unlock()
		return ctx.Err()
	}
}

func (f *Fan) Abort() error {
	f.mu.Lock()
	f.state = StateOff
	f.reason = "aborted"
	f.mu.Unlock()
	f.bus.Publish("vent.fan_aborted", f.id)
	return nil
}

func (f *Fan) DisableAuto() {
	f.mu.Lock()
	f.auto = false
	f.mu.Unlock()
	f.bus.Publish("vent.fan_auto_disabled", f.id)
}

func (f *Fan) EnableAuto() {
	f.mu.Lock()
	f.auto = true
	f.mu.Unlock()
	f.bus.Publish("vent.fan_auto_enabled", f.id)
}

func (f *Fan) AutoEnabled() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.auto
}

func (f *Fan) RecoverState(live bool, auto bool) error {
	f.mu.Lock()
	if live {
		f.state = StateRunning
	} else {
		f.state = StateOff
	}
	f.auto = auto
	f.reason = ""
	f.mu.Unlock()
	f.bus.Publish("vent.fan_recovered", f.id)
	return nil
}

func (f *Fan) State() State {
	f.mu.Lock()
	defer f.mu.Unlock()
	return State{
		ID:      f.id,
		State:   f.state,
		Reason:  f.reason,
		Starts:  f.starts,
		AutoOff: !f.auto,
	}
}

func (f *Fan) Monitor() MonitorStats {
	return f.mon.Stats()
}
