// Package stream provides a thread-safe, append-only byte buffer designed for
// concurrent reading and streaming of the jobmanager process output. Output is
// retained in full: the buffer grows with whatever a job produces.
package stream

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
)

// ErrWriteAfterClose is returned by Write once the buffer is closed. Reaching
// it means the single writer contract was broken: the job manager closes the
// buffer only after cmd.Wait returns, and Wait waits for the copying
// goroutines, so the job's own output cannot arrive late.
var ErrWriteAfterClose = errors.New("stream: write after close")

// ErrInvalidOffset is returned when the read offset exceeds the buffer's
// current size and when the read offset is negative.
var ErrInvalidOffset = errors.New("invalid read offset")

// Buffer is a thread-safe, single-writer, multiple-reader byte array.
// It is append-only as it needs to retain full history for any readers attempting to read late.
type Buffer struct {
	mu   sync.Mutex
	data []byte
	// changed is nil unless a reader is waiting. This is created by the reader that parks.
	// Then closed and cleared by the write or close that releases it.
	changed  chan struct{}
	isClosed bool
}

// NewBuffer initializes a new stream buffer.
func NewBuffer() *Buffer {
	return &Buffer{
		data: make([]byte, 0),
	}
}

// Write appends data to the buffer. It expects to only be used by a single
// writer.
//
// While a job is running Write never returns an error, because os/exec treats
// a write error on a process's output as fatal: it stops copying and closes
// the read end of the pipe, the child takes SIGPIPE on its next write, and the
// job's real exit status is lost.
//
// After Close that reasoning no longer applies, since the process has already
// exited, and reporting the discard is better than claiming to have written
// bytes that were thrown away.
func (b *Buffer) Write(p []byte) (n int, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.isClosed {
		return 0, ErrWriteAfterClose
	}
	if len(p) > 0 {
		// Broadcast only when there are bytes to actually copy:
		b.data = append(b.data, p...)
		b.notifyLocked()
	}
	return len(p), nil
}

// notifyLocked releases every reader currently parked in readAt. Closing a
// channel wakes all of its receivers at once, so notifyLocked closes the current
// 'changed' channel which is the one waiters are holding, then clears it.
//
// The wake carries no information about what changed, so woken readers
// re-acquire b.mu and re-evaluate the buffer from scratch.
//
// Caller must hold b.mu.
func (b *Buffer) notifyLocked() {
	if b.changed == nil {
		return
	}
	close(b.changed)
	b.changed = nil
}

// readAt copies data from the given offset into p, blocking until data is
// available. It returns the number of bytes copied, which may be fewer than
// len(p) even when err is nil.
//
// Cases by offset:
//   - less than the buffer length: copies what fits in p, returns immediately.
//   - equal to the buffer length: blocks until the buffer changes. Returns
//     (0, io.EOF) once the buffer is closed and no further data will arrive.
//   - greater than the buffer length: returns ErrInvalidOffset.
//
// A reader that parks creates the changed channel if it is nil, before the
// mutex is released to guarantee later writes will see the changed channel.
// Parked readers are released when Write or Close closes the 'changed'
// channel. The wake carries no information about what changed, so a woken
// reader re-acquires the lock and re-evaluates from scratch.
//
// If ctx is cancelled while blocked, readAt returns (0, ctx.Err()).
func (b *Buffer) readAt(ctx context.Context, p []byte, offset int) (n int, err error) {
	// Invalid offset
	if offset < 0 {
		return 0, fmt.Errorf("stream: negative offset %d: %w", offset, ErrInvalidOffset)
	}
	// An empty p returns before the offset is bounds checked, so a nonsense
	// offset is reported only when there is somewhere to read into. io.Reader
	// permits both: for len(p) == 0 it says an implementation "may return a
	// non-nil error if some error condition is known". Nothing reaches this
	// today, because a Reader's offset only ever advances by bytes actually
	// read.
	if len(p) == 0 {
		return 0, nil
	}
	for {
		// Need the lock here:
		b.mu.Lock()
		// Sanity check offset:
		if offset > len(b.data) {
			b.mu.Unlock()
			return 0, ErrInvalidOffset
		}
		if offset < len(b.data) {
			n = copy(p, b.data[offset:])
			b.mu.Unlock()
			// A reader that stays behind the writer never reaches the wait
			// below, so cancellation is checked on this path too. The bytes
			// just copied are returned alongside the error: io.Reader requires
			// a caller to process n > 0 before considering err, so the read
			// ends without dropping what it already had. ctx.Err() is nil
			// until cancellation.
			return n, ctx.Err()
		}
		// At this point offset == len(b.data):
		if b.isClosed {
			b.mu.Unlock()
			return 0, io.EOF
		}
		// Create the channel before releasing the mutex. Any write that acquires
		// the mutex after this point sees the non-nil channel and closes it, so
		// a reader is guaranteed to be woken up by a Write or Close.
		if b.changed == nil {
			b.changed = make(chan struct{})
		}
		wait := b.changed
		b.mu.Unlock()
		// Block on select
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-wait:
			// Woken up on a write or close. Go through loop again.
		}
	}
}

// Close marks the buffer as done/terminated. This results in a broadcast to wake up any
// readers currently waiting in readAt. Calling Close on an already closed buffer is
// allowed and is a noop.
func (b *Buffer) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.isClosed {
		return nil
	}
	b.isClosed = true
	b.notifyLocked()
	return nil
}
