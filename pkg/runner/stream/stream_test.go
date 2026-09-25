package stream

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"testing/iotest"
	"testing/synctest"
)

func TestBuffer_HappyWriteRead(t *testing.T) {
	buf := NewBuffer()

	// 1. Write 10 bytes to buf
	payload := []byte("thisistest")
	n, err := buf.Write(payload)
	if err != nil {
		t.Fatalf("unexpected error during write: %v", err)
	}
	if n != len(payload) {
		t.Fatalf("expected to write %d bytes, wrote %d", len(payload), n)
	}
	// 2. Read it back through a Reader, the surface callers use:
	dest := make([]byte, len(payload))
	n, err = NewReader(t.Context(), buf).Read(dest)
	if err != nil {
		t.Fatalf("unexpected error during read: %v", err)
	}
	if n != len(payload) {
		t.Fatalf("expected to read %d bytes, got %d", len(payload), n)
	}

	// 3. Verify string returned:
	if !bytes.Equal(dest[:n], payload) {
		t.Fatalf("expected string %q, got %q", payload, dest[:n])
	}
}

// TestBuffer_WriteAfterCloseIsRejected asserts that Close freezes the buffer.
// Appending would make two readers disagree, since one that already reached
// io.EOF has stopped; erroring rather than discarding is io.Writer's rule that
// n < len(p) needs a non-nil error.
func TestBuffer_WriteAfterCloseIsRejected(t *testing.T) {
	buf := NewBuffer()

	before := []byte("before")
	if _, err := buf.Write(before); err != nil {
		t.Fatalf("unexpected error during write: %v", err)
	}
	if err := buf.Close(); err != nil {
		t.Fatalf("unexpected error during close: %v", err)
	}

	// Rejected, not silently discarded. io.Writer requires a non-nil error
	// whenever n < len(p), and reaching this means the single writer contract
	// was broken.
	after := []byte("after")
	n, err := buf.Write(after)
	if !errors.Is(err, ErrWriteAfterClose) {
		t.Fatalf("write after close: got %v, want ErrWriteAfterClose", err)
	}
	if n != 0 {
		t.Errorf("write after close reported %d bytes written, want 0", n)
	}

	// The contents are the assertion that matters.
	dest := make([]byte, len(before)+len(after))
	got, err := NewReader(t.Context(), buf).Read(dest)
	if err != nil {
		t.Fatalf("Read error: %v", err)
	}
	if !bytes.Equal(dest[:got], before) {
		t.Errorf("buffer contents changed after close: got %q, want %q", dest[:got], before)
	}
}

func TestBuffer_BlockingRead(t *testing.T) {
	synctest.Test(t, testBlockingRead)
}

// testBlockingRead runs inside a synctest bubble so that the reader is known to
// be parked before the write. Without that, the write usually wins the race,
// the reader finds data already present, and it returns without ever blocking.
// The test then passes whether or not a write wakes a parked reader at all.
func testBlockingRead(t *testing.T) {
	buf := NewBuffer()
	payload := []byte("The quick brown fox")

	type readResult struct {
		n   int
		err error
		b   []byte
	}

	resultCh := make(chan readResult)

	// Start a blocking background reader:
	go func() {
		dest := make([]byte, len(payload))
		// This will block because the buffer is empty:
		n, err := NewReader(t.Context(), buf).Read(dest)
		resultCh <- readResult{n, err, dest}
	}()

	// Wait returns only once the reader is durably blocked, which is what
	// orders the park ahead of the write below.
	synctest.Wait()

	// Write data to unblock the reader:
	_, err := buf.Write(payload)
	if err != nil {
		t.Fatalf("unexpected error during write: %v", err)
	}

	// No timeout needed: if the write fails to wake the reader, every
	// goroutine in the bubble is durably blocked and synctest reports a
	// deadlock.
	res := <-resultCh
	if res.err != nil {
		t.Fatalf("unexpected error during read: %v", res.err)
	}
	if res.n != len(payload) {
		t.Fatalf("expected to read %d bytes, got %d", len(payload), res.n)
	}
	if !bytes.Equal(res.b, payload) {
		t.Fatalf("expected string %q, got %q", payload, res.b)
	}
}

func TestBuffer_CloseEmptyBuffer(t *testing.T) {
	synctest.Test(t, testCloseEmptyBuffer)
}

// testCloseEmptyBuffer runs inside a synctest bubble so that synctest.Wait can
// replace a sleep: it returns once the reading goroutine is durably blocked.
// Without waiting for the park, Close could win the race and the test would
// exercise the already-closed path instead of the wake-on-close path. Both
// return io.EOF, so the wrong path would pass silently.
func testCloseEmptyBuffer(t *testing.T) {
	buf := NewBuffer()

	type readResult struct {
		n   int
		err error
		b   []byte
	}

	resultCh := make(chan readResult)

	// Start a blocking background reader:
	go func() {
		dest := make([]byte, 16)
		// This will block because the buffer is empty:
		n, err := NewReader(t.Context(), buf).Read(dest)
		resultCh <- readResult{n, err, dest}
	}()

	// Wait returns only once the reader is durably blocked, which is what
	// orders the park ahead of the close below.
	synctest.Wait()

	// Close the buffer to unblock the reader:
	err := buf.Close()
	if err != nil {
		t.Fatalf("unexpected error during close: %v", err)
	}

	res := <-resultCh
	if res.n != 0 {
		t.Fatalf("expected to read 0 bytes, got %d", res.n)
	}
	if res.err != io.EOF {
		t.Fatalf("expected io.EOF, got %v", res.err)
	}

	// Test idempotent close on already closed buffer
	err = buf.Close()
	if err != nil {
		t.Fatalf("expected nil on second close, got %v", err)
	}
}

func TestBuffer_PartialRead(t *testing.T) {
	buf := NewBuffer()
	payload := []byte("Greetings from bantay")

	// Write the full payload:
	_, err := buf.Write(payload)
	if err != nil {
		t.Fatalf("unexpected error during write: %v", err)
	}

	// Close the buffer before anything else:
	err = buf.Close()
	if err != nil {
		t.Fatalf("unexpected error during close: %v", err)
	}

	const readSize = 5
	dest := make([]byte, readSize)
	n, err := NewReader(t.Context(), buf).Read(dest)
	if n != readSize {
		t.Fatalf("expected to read %d bytes, got %d", readSize, n)
	}
	if string(dest) != string(payload[:readSize]) {
		t.Fatalf("expected %q , got %q", payload[:readSize], dest)
	}

	if err != nil {
		t.Fatalf("partial read should not return an error, got %v", err)
	}
}

func TestBuffer_OffsetBeyondEnd(t *testing.T) {
	buf := NewBuffer()
	payload := []byte("0123456789") // 10 bytes only

	_, err := buf.Write(payload)
	if err != nil {
		t.Fatalf("unexpected error during write: %v", err)
	}

	dest := make([]byte, 8)
	// Try read 8 bytes at offset beyond the 10th byte:
	n, err := buf.readAt(t.Context(), dest, 25)
	if n != 0 {
		t.Fatalf("expected 0 bytes read, got %d", n)
	}
	if !errors.Is(err, ErrInvalidOffset) {
		t.Fatalf("expected ErrInvalidOffset for out-of-bounds offset, got %v", err)
	}
}

func TestReader_IndependentOffsets(t *testing.T) {
	buf := NewBuffer()
	_, err := buf.Write([]byte("hello world"))
	if err != nil {
		t.Fatalf("unexpected error during write: %v", err)
	}
	_ = buf.Close()

	r1 := NewReader(t.Context(), buf)
	r2 := NewReader(t.Context(), buf)

	p := make([]byte, 5)
	n, err := r1.Read(p)
	if err != nil {
		t.Fatalf("unexpected error during read: %v", err)
	}
	if n != 5 {
		t.Fatalf("expected 5 bytes, got %d, %v", n, err)
	}
	if got := string(p[:n]); got != "hello" {
		t.Fatalf("r1 first read: got %q, want %q", got, "hello")
	}

	// r1 continues from where it stopped. Without this the test passes even if
	// Read never advances its offset, which is the whole point of the offset
	// being per reader rather than per buffer.
	n, err = r1.Read(p)
	if err != nil {
		t.Fatalf("unexpected error during r1's second read: %v", err)
	}
	if got := string(p[:n]); got != " worl" {
		t.Errorf("r1 did not continue from its offset: got %q, want %q", got, " worl")
	}

	// r2 should still start from 0
	q := make([]byte, 5)
	n, err = r2.Read(q)
	if err != nil {
		t.Fatalf("unexpected error during read: %v", err)
	}
	if n != 5 {
		t.Fatalf("expected 5 bytes, got %d, %v", n, err)
	}
	if string(q) != "hello" {
		t.Errorf("r2 offset contaminated by r1: got %q", q)
	}
}

// TestBuffer_ReaderReleasedWhenCanceled asserts that a reader parked on an
// empty, still-open buffer returns when its context is cancelled. Without it,
// a disconnected client leaves a goroutine parked until the job itself ends,
// which for a quiet long-running job is effectively never.
func TestBuffer_ReaderReleasedWhenCanceled(t *testing.T) {
	synctest.Test(t, testReaderReleasedWhenCanceled)
}

// Runs in a synctest bubble: synctest.Wait replaces a sleep when waiting for
// every reader to park, and bubble time is fake, so the deadline below costs
// nothing when the test passes.
func testReaderReleasedWhenCanceled(t *testing.T) {
	const (
		readSize = 64
		readers  = 21
	)
	buf := NewBuffer()
	ctx, cancel := context.WithCancel(t.Context())

	exited := make(chan error, readers)
	for range readers {
		go func() {
			r := NewReader(ctx, buf)
			dest := make([]byte, readSize)
			// Read on an empty buffer will park the goroutine:
			_, err := r.Read(dest)
			exited <- err
		}()
	}
	synctest.Wait() // returns once every reader is durably blocked
	cancel()

	// Every reader must return. If one does not, the bubble notices that all
	// its goroutines are durably blocked with no timer pending and panics with
	// a deadlock.
	for range readers {
		if err := <-exited; !errors.Is(err, context.Canceled) {
			t.Errorf("reader returned %v, want context.Canceled", err)
		}
	}
}

// TestBuffer_CancelingOneReaderDoesNotDisturbOthers covers the following
// property: one client going away must not disturb the others.
//
// The first chunk is written before anyone attaches, so the survivors have to
// deliver history as well as the bytes that arrive after the cancellation.
//
// What this does not cover is wake behaviour. A regression that woke every
// parked reader on any cancellation would still pass, because a spurious wake
// is harmless: the woken reader re-acquires the lock, finds no new data, and
// parks again. "Does not disturb" here means the other readers are neither
// terminated nor given wrong bytes.
func TestBuffer_CancelingOneReaderDoesNotDisturbOthers(t *testing.T) {
	synctest.Test(t, testCancelingOneReaderDoesNotDisturbOthers)
}

// Runs in a synctest bubble, so synctest.Wait replaces a sleep when waiting
// for the readers to park and the deadlines below cost nothing on success.
func testCancelingOneReaderDoesNotDisturbOthers(t *testing.T) {
	const survivors = 5
	first := []byte("HEAD written before anyone attached. ")
	second := []byte("TAIL written after the victim was cancelled.")
	want := string(first) + string(second)

	buf := NewBuffer()
	if _, err := buf.Write(first); err != nil {
		t.Fatalf("writing the first chunk: %v", err)
	}

	// The survivors share t.Context(), which nothing cancels during the test.
	// The victim gets its own cancellable context below, and that separation
	// is the property under test: cancellation reaches only the readers
	// holding the cancelled context. Collapsing the two into one shared
	// context would make this test pass while proving nothing.
	got := make(chan string, survivors)
	for range survivors {
		go func() {
			r := NewReader(t.Context(), buf)
			var acc []byte
			p := make([]byte, 8) // small, so several reads are needed
			for {
				n, err := r.Read(p)
				acc = append(acc, p[:n]...)
				if errors.Is(err, io.EOF) {
					got <- string(acc)
					return
				}
				if err != nil {
					got <- "unexpected error: " + err.Error()
					return
				}
			}
		}()
	}

	// The victim is the only reader with a cancellable context.
	victimCtx, cancelVictim := context.WithCancel(t.Context())
	victimErr := make(chan error, 1)
	go func() {
		r := NewReader(victimCtx, buf)
		p := make([]byte, len(first))
		// Drain the history first so the next read parks on an empty buffer.
		if _, err := r.Read(p); err != nil {
			victimErr <- err
			return
		}
		_, err := r.Read(p)
		victimErr <- err
	}()

	synctest.Wait() // returns once every reader is durably blocked

	cancelVictim()

	if err := <-victimErr; !errors.Is(err, context.Canceled) {
		t.Fatalf("victim returned %v, want context.Canceled", err)
	}

	// The survivors must still be parked and must still receive what follows.
	if _, err := buf.Write(second); err != nil {
		t.Fatalf("writing the second chunk: %v", err)
	}
	if err := buf.Close(); err != nil {
		t.Fatalf("closing the buffer: %v", err)
	}

	for i := range survivors {
		if s := <-got; s != want {
			t.Errorf("survivor %d read %q, want %q", i, s, want)
		}
	}
}

// TestReader_CanceledWhileBehindWriter covers a reader that never catches up
// to the writer. It never reaches the wait in readAt, so cancellation has to
// be observed on the path that has data to return. The bytes already copied
// come back with the error rather than being dropped.
func TestReader_CanceledWhileBehindWriter(t *testing.T) {
	buf := NewBuffer()
	ctx, cancel := context.WithCancel(t.Context())
	r := NewReader(ctx, buf)

	// More data than the reader's buffer, so it stays behind throughout.
	if _, err := buf.Write([]byte("0123456789")); err != nil {
		t.Fatalf("unexpected error during write: %v", err)
	}

	p := make([]byte, 4)
	if n, err := r.Read(p); n != 4 || err != nil {
		t.Fatalf("first read: got (%d, %v), want (4, nil)", n, err)
	}

	cancel()

	n, err := r.Read(p)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("read after cancel: got err %v, want context.Canceled", err)
	}
	if n == 0 {
		t.Errorf("read after cancel returned no data, want the buffered bytes")
	}
}

// TestReader_SatisfiesTheIoReaderContract checks that Reader behaves like an
// io.Reader rather than merely having its method.
//
// Satisfaction is structural in Go, so the compiler accepts any Read with the
// right signature. Everything this package is handed to, io.Copy, io.ReadAll,
// bufio.Scanner, the server's streaming loop, assumes the contract on top of
// the signature: that a short read is not an error, that a zero-length read
// does not block, that io.EOF arrives exactly once and with no bytes after it.
//
// iotest.TestReader exercises those: it reads with buffers of many sizes,
// including empty ones, reads past the end, and checks that the reader keeps
// returning io.EOF. A reader that hangs on a zero-length read passes every test
// in this file and fails this one.
func TestReader_SatisfiesTheIoReaderContract(t *testing.T) {
	want := []byte("Fortune favours the well prepared reader.")

	buf := NewBuffer()
	if _, err := buf.Write(want); err != nil {
		t.Fatalf("unexpected error during write: %v", err)
	}
	// TestReader reads to EOF, and an open buffer would block instead, since a
	// live job may always write more.
	if err := buf.Close(); err != nil {
		t.Fatalf("unexpected error during close: %v", err)
	}

	if err := iotest.TestReader(NewReader(t.Context(), buf), want); err != nil {
		t.Error(err)
	}
}

// The same contract has to hold for a reader that starts on an empty stream,
// which is what a job producing no output leaves behind.
func TestReader_SatisfiesTheContractWhenEmpty(t *testing.T) {
	buf := NewBuffer()
	if err := buf.Close(); err != nil {
		t.Fatalf("unexpected error during close: %v", err)
	}

	if err := iotest.TestReader(NewReader(t.Context(), buf), nil); err != nil {
		t.Error(err)
	}
}

// TestReader_ZeroLengthReadDoesNotBlock covers the one part of the io.Reader
// contract that iotest.TestReader cannot reach here.
//
// io.Reader says an implementation "should not block" on a zero-length read and
// may return 0, nil. The dangerous case is an open buffer with the reader
// caught up, where a normal read is supposed to park until there is more. A
// zero-length read must not park there: io.Copy and friends are entitled to
// hand over an empty slice, and a reader that blocks on one hangs a caller that
// asked for nothing.
//
// iotest.TestReader reads to EOF, so it can only run against a closed buffer,
// where a zero-length read takes the EOF path and never reaches the wait. Only
// an open buffer exercises this.
func TestReader_ZeroLengthReadDoesNotBlock(t *testing.T) {
	synctest.Test(t, testZeroLengthReadDoesNotBlock)
}

func testZeroLengthReadDoesNotBlock(t *testing.T) {
	// Open and empty: this is exactly where a read of any real length parks.
	buf := NewBuffer()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	r := NewReader(ctx, buf)

	type outcome struct {
		n   int
		err error
	}
	done := make(chan outcome, 1)
	go func() {
		n, err := r.Read(nil)
		done <- outcome{n, err}
	}()

	// Returns once every other goroutine is durably blocked, so a read still
	// outstanding here is parked rather than merely unscheduled.
	synctest.Wait()

	select {
	case got := <-done:
		if got.n != 0 {
			t.Errorf("read %d bytes into a zero-length buffer, want 0", got.n)
		}
		if got.err != nil {
			t.Errorf("zero-length read returned %v, want nil", got.err)
		}
	default:
		// Release the parked reader so the bubble can end, then report the
		// failure in terms of what went wrong rather than as a deadlock.
		cancel()
		synctest.Wait()
		t.Fatal("a zero-length read blocked on an open, empty buffer")
	}
}
