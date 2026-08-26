package record

import (
	"io"
	"os"
	"path/filepath"
	"sync"
)

type FileOpener interface {
	Open(path string) (io.WriteCloser, error)
}

type DiskStore struct{}

func NewDiskStore() *DiskStore {
	return &DiskStore{}
}

func (d *DiskStore) Open(path string) (io.WriteCloser, error) {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
}

type MemoryStore struct {
	mu   sync.Mutex
	data map[string][]byte
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{data: make(map[string][]byte)}
}

func (m *MemoryStore) Open(path string) (io.WriteCloser, error) {
	return &memoryWriter{store: m, path: path}, nil
}

type memoryWriter struct {
	store *MemoryStore
	path  string
	buf   []byte
}

func (w *memoryWriter) Write(p []byte) (int, error) {
	w.buf = append(w.buf, p...)
	return len(p), nil
}

func (w *memoryWriter) Close() error {
	w.store.mu.Lock()
	defer w.store.mu.Unlock()
	w.store.data[w.path] = append(w.store.data[w.path], w.buf...)
	return nil
}
