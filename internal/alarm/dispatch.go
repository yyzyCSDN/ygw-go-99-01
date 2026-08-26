package alarm

import (
	"errors"
	"sync"
)

var ErrDuplicateDevice = errors.New("alarm device already attached")

type Device struct {
	ID string
}

type Dispatcher struct {
	mu      sync.Mutex
	devices map[string]Device
	hits    map[string]int
}

func NewDispatcher() *Dispatcher {
	return &Dispatcher{
		devices: make(map[string]Device),
		hits:    make(map[string]int),
	}
}

func (d *Dispatcher) Attach(device Device) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, ok := d.devices[device.ID]; ok {
		return ErrDuplicateDevice
	}
	d.devices[device.ID] = device
	return nil
}

func (d *Dispatcher) Dispatch(sev Severity) int {
	d.mu.Lock()
	defer d.mu.Unlock()
	count := 0
	for id := range d.devices {
		d.hits[id]++
		count++
	}
	return count
}

func (d *Dispatcher) Hits() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	total := 0
	for _, count := range d.hits {
		total += count
	}
	return total
}
