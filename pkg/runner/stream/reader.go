package stream

import "context"

// Reader is a stateful io.Reader over a Buffer.
// Each Reader maintains its own offset, so multiple Readers
// can independently consume the same Buffer concurrently.
// A single Reader is not safe for concurrent use: Read mutates its offset
// without locking, and each Reader belongs to a single client.
type Reader struct {
	buf    *Buffer
	offset int
	// ctx bounds how long a read will block, and ends reads once it is
	// cancelled. A read with data already buffered returns that data, with
	// the context's error once cancelled, so a reader that never catches up
	// to the writer still stops.
	//
	// This is stored rather than passed because io.Reader.Read doesn't take a
	// ctx parameter. Cancelling it releases this Reader without affecting any
	// other reader.
	ctx context.Context
}

// NewReader returns a Reader starting at the beginning of buf.
func NewReader(ctx context.Context, buf *Buffer) *Reader {
	return &Reader{
		buf: buf,
		ctx: ctx,
	}
}

func (r *Reader) Read(p []byte) (int, error) {
	n, err := r.buf.readAt(r.ctx, p, r.offset)
	r.offset += n
	return n, err
}
