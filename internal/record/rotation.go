package record

import (
	"fmt"
	"sync"
)

type Rotation struct {
	mu        sync.Mutex
	directory string
	prefix    string
	maxLines  int
	lines     int
	index     int
}

func NewRotation(directory, prefix string, maxLines int) *Rotation {
	return &Rotation{
		directory: directory,
		prefix:    prefix,
		maxLines:  maxLines,
		index:     1,
	}
}

func (r *Rotation) Next() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.lines >= r.maxLines {
		r.lines = 0
		r.index++
	}
	r.lines++
	return fmt.Sprintf("%s/%s-%03d.log", r.directory, r.prefix, r.index)
}
