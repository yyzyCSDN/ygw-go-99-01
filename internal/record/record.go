package record

import (
	"fmt"
	"sync"
	"time"

	"coalminegas/internal/event"
	"github.com/cespare/xxhash/v2"
)

type Entry struct {
	Point   string
	Kind    string
	Message string
	At      time.Time
	Seq     int
}

type Recorder struct {
	opener  FileOpener
	bus     *event.Bus
	rotator *Rotation
	mu      sync.Mutex
	seq     int
	appends int
	fails   int
}

func NewRecorder(opener FileOpener, bus *event.Bus, rotator *Rotation) *Recorder {
	return &Recorder{opener: opener, bus: bus, rotator: rotator}
}

func (r *Recorder) Append(entry Entry) error {
	r.mu.Lock()
	r.seq++
	entry.Seq = r.seq
	r.mu.Unlock()
	line := fingerprint(entry)
	if err := r.write(line); err != nil {
		r.mu.Lock()
		r.fails++
		r.mu.Unlock()
		return err
	}
	r.mu.Lock()
	r.appends++
	r.mu.Unlock()
	r.bus.Publish("record.appended", entry.Point)
	return nil
}

func (r *Recorder) write(line string) error {
	path := r.pathFor()
	handle, err := r.opener.Open(path)
	if err != nil {
		return err
	}
	_, err = handle.Write([]byte(line + "\n"))
	if err != nil {
		return err
	}
	return nil
}

func (r *Recorder) pathFor() string {
	if r.rotator != nil {
		return r.rotator.Next()
	}
	return "records/current.log"
}

func (r *Recorder) Stats() (int, int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.appends, r.fails
}

func fingerprint(entry Entry) string {
	hash := xxhash.New()
	_, _ = fmt.Fprintf(hash, "%s|%s|%s|%d|%d", entry.Point, entry.Kind, entry.Message, entry.At.UnixNano(), entry.Seq)
	return fmt.Sprintf("%016x|%d", hash.Sum64(), entry.Seq)
}
