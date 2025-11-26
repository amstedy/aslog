package aslog

import (
	"sync"
)

const defaultMaxSize = 1000

type buffer struct {
	mu       sync.Mutex
	data     [][]byte
	bytes    int
	maxBytes int
}

func newBuffer(maxBytes int) *buffer {
	return &buffer{
		data:     make([][]byte, 0, defaultMaxSize),
		maxBytes: maxBytes,
	}
}

func (b *buffer) Write(p []byte) (overflow bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.data = append(b.data, p)
	b.bytes += len(p)

	if b.bytes > b.maxBytes {
		overflow = true
	}

	return
}

func (b *buffer) Flush() [][]byte {
	b.mu.Lock()
	defer b.mu.Unlock()

	if len(b.data) == 0 {
		return nil
	}

	result := b.data
	b.data = make([][]byte, 0, defaultMaxSize)
	b.bytes = 0

	return result
}
