package event

import (
	"reflect"
	"sort"
	"sync"
)

type Handler func(topic string, payload any)

type subscription struct {
	topic string
	hook  Handler
}

type Bus struct {
	mu       sync.RWMutex
	handlers map[string][]Handler
	subs     map[int]subscription
	nextID   int
}

func NewBus() *Bus {
	return &Bus{
		handlers: make(map[string][]Handler),
		subs:     make(map[int]subscription),
	}
}

func (b *Bus) Register(topic string, hook Handler) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.nextID++
	b.subs[b.nextID] = subscription{topic: topic, hook: hook}
	b.handlers[topic] = append(b.handlers[topic], hook)
	return b.nextID
}

func (b *Bus) Deregister(id int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	sub, ok := b.subs[id]
	if !ok {
		return
	}
	delete(b.subs, id)
	list := b.handlers[sub.topic]
	out := list[:0]
	for _, hook := range list {
		if reflect.ValueOf(hook).Pointer() != reflect.ValueOf(sub.hook).Pointer() {
			out = append(out, hook)
		}
	}
	b.handlers[sub.topic] = out
}

func (b *Bus) Publish(topic string, payload any) {
	b.mu.RLock()
	list := append([]Handler(nil), b.handlers[topic]...)
	list = append(list, b.handlers[""]...)
	b.mu.RUnlock()
	for _, hook := range list {
		hook(topic, payload)
	}
}

func (b *Bus) Count() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.subs)
}

func (b *Bus) Topics() []string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	out := make([]string, 0, len(b.handlers))
	for topic := range b.handlers {
		out = append(out, topic)
	}
	sort.Strings(out)
	return out
}
