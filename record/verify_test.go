package record_test

import (
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"coalminegas/internal/event"
	"coalminegas/internal/record"
)

type countingWriter struct {
	opener *countingOpener
}

func (w *countingWriter) Write(p []byte) (int, error) {
	return len(p), nil
}

func (w *countingWriter) Close() error {
	w.opener.mu.Lock()
	defer w.opener.mu.Unlock()
	w.opener.open--
	return nil
}

type countingOpener struct {
	mu    sync.Mutex
	open  int
	limit int
}

func (o *countingOpener) Open(path string) (io.WriteCloser, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.open >= o.limit {
		return nil, errors.New("too many open files")
	}
	o.open++
	return &countingWriter{opener: o}, nil
}

func (o *countingOpener) OpenCount() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.open
}

func TestRecordHandleReleasedAfterWrite(t *testing.T) {
	bus := event.NewBus()
	opener := &countingOpener{limit: 16}
	recorder := record.NewRecorder(opener, bus, nil)
	for i := 0; i < 25; i++ {
		entry := record.Entry{Point: "p01", Kind: "reading", Message: "value", At: time.Now()}
		if err := recorder.Append(entry); err != nil {
			t.Fatalf("append %d must succeed when handles are released: %v", i, err)
		}
		if got := opener.OpenCount(); got != 0 {
			t.Fatalf("append %d must release the file handle, open=%d", i, got)
		}
	}
	appends, fails := recorder.Stats()
	if appends != 25 || fails != 0 {
		t.Fatalf("stats must show 25 appends and 0 fails, got %d/%d", appends, fails)
	}
}
