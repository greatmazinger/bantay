package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/greatmazinger/bantay/pkg/runner"
)

// Config is the test harness configuration:
type Config struct {
	Command          string
	Args             []string
	EarlyReaders     int
	DelayedReaders   int
	LateReaders      int
	AbandonedReaders int
	StaggerMax       time.Duration
	// AbandonAfter is how long an abandoned reader reads before its context is
	// cancelled. It has to land while the job is still producing, or the
	// reader reaches the end of a closed buffer and sees io.EOF instead.
	AbandonAfter time.Duration
	// MustEndWith is the last thing the job prints. Without it this harness
	// only proves the readers agree with each other, which a bug that
	// truncates the stream for everyone satisfies just as well as correct
	// delivery. Checking the stream reaches the job's final write is what
	// makes agreement worth something.
	MustEndWith string
}

// result holds a reader's results/outcome
type result struct {
	id       int
	kind     string // "live" | "delayed" | "late" | "abandoned"
	data     []byte
	duration time.Duration
	err      error
}

// abandoned reports whether this result came from a reader that walked away
// partway through. Those are held to a different standard: a partial read and
// a cancellation error are the expected outcome.
func (r result) abandoned() bool { return r.kind == "abandoned" }

func runHarness(mgr *runner.JobManager, cfg Config) error {
	const owner = "harness"

	jobID, err := mgr.Start(owner, cfg.Command, cfg.Args)
	if err != nil {
		return fmt.Errorf("start error: %v", err)
	}
	fmt.Printf("started job %s\n", jobID)

	total := cfg.EarlyReaders + cfg.DelayedReaders + cfg.LateReaders + cfg.AbandonedReaders
	results := make(chan result, total)
	var wg sync.WaitGroup

	// Early readers start early
	for i := range cfg.EarlyReaders {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results <- readOutput(mgr, owner, jobID, i, "live")
		}(i)
	}

	// Delayed readers go here:
	for i := range cfg.DelayedReaders {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			delay := time.Duration(rand.Int64N(int64(cfg.StaggerMax)))
			time.Sleep(delay)
			results <- readOutput(mgr, owner, jobID, i, "delayed")
		}(i)
	}

	// Abandoned readers: the point of the exercise. Each gets its own
	// cancellable context, reads for a while, then goes away. Every other
	// reader must finish with byte-identical output regardless.
	for i := range cfg.AbandonedReaders {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results <- readAbandoned(mgr, owner, jobID, i, cfg.AbandonAfter)
		}(i)
	}

	// Blocking sentinel:
	sentinel, _ := mgr.Output(context.Background(), owner, jobID)
	if _, err := io.Copy(io.Discard, sentinel); err != nil {
		return fmt.Errorf("sentinel error: %w", err)
	}
	snap, _ := mgr.Status(owner, jobID)
	fmt.Printf("job done: state=%s exitCode=%d\n", snap.State, snap.ExitCode)

	// Late readers go here:
	for i := range cfg.LateReaders {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results <- readOutput(mgr, owner, jobID, i, "late")
		}(i)
	}

	wg.Wait()
	close(results)
	return verify(results, cfg.MustEndWith)
}

// readOutput calls Output and reads until EOF. It also measures the duration.
//
// These readers use context.Background deliberately. Reading to EOF is the
// invariant under test, so they must not be cancellable: only the abandoned
// category below gets a context that can be cancelled. Handing every reader the
// same context would silently weaken the harness into proving nothing.
func readOutput(mgr *runner.JobManager, owner string, jobID string, i int, kind string) result {
	start := time.Now()
	r, err := mgr.Output(context.Background(), owner, jobID)
	if err != nil {
		return result{id: i, kind: kind, err: err}
	}
	data, err := io.ReadAll(r)
	return result{id: i, kind: kind, data: data, duration: time.Since(start), err: err}
}

// readAbandoned reads for a while and then walks away, which is what a client
// pressing Ctrl-C or a dropped connection looks like from here.
//
// The reader is expected to come back with context.Canceled and a partial read.
// What matters is not this reader's own outcome but its effect on the others,
// which verify checks: a buffer that released a reader badly would strand or
// corrupt the readers still attached.
func readAbandoned(mgr *runner.JobManager, owner string, jobID string, i int, after time.Duration) result {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	start := time.Now()
	r, err := mgr.Output(ctx, owner, jobID)
	if err != nil {
		return result{id: i, kind: "abandoned", err: err}
	}

	// Walk away partway through, while the job is still producing.
	time.AfterFunc(after, cancel)

	data, err := io.ReadAll(r)
	return result{id: i, kind: "abandoned", data: data, duration: time.Since(start), err: err}
}

// verify holds the two kinds of reader to different standards.
//
// A reader that stayed must have the complete stream, byte-identical to every
// other. A reader that walked away must have come back with context.Canceled,
// and what it did read must be a prefix of that same stream: it was reading the
// real thing from the first byte, it just stopped early. A buffer that released
// a cancelled reader by corrupting shared state would show up as a mismatch in
// the readers that stayed, which is the property this whole exercise exists to
// demonstrate.
func verify(results <-chan result, mustEndWith string) error {
	var stayed, left []result
	for r := range results {
		if r.abandoned() {
			left = append(left, r)
			continue
		}
		stayed = append(stayed, r)
	}

	var reference []byte
	problems := 0

	for _, r := range stayed {
		if r.err != nil {
			fmt.Printf("[%-10s %2d] ERROR: %v\n", r.kind, r.id, r.err)
			problems++
			continue
		}
		fmt.Printf("[%-10s %2d] %5d bytes in %v\n", r.kind, r.id, len(r.data), r.duration.Round(time.Millisecond))
		if reference == nil {
			reference = r.data
		} else if !bytes.Equal(r.data, reference) {
			fmt.Printf("[%-10s %2d] MISMATCH: got %d bytes, want %d\n", r.kind, r.id, len(r.data), len(reference))
			problems++
		}
	}

	for _, r := range left {
		switch {
		case errors.Is(r.err, context.Canceled):
			// Expected. Everything it read must still be the real stream.
			if !bytes.HasPrefix(reference, r.data) {
				fmt.Printf("[%-10s %2d] NOT A PREFIX: %d bytes that are not the start of the stream\n",
					r.kind, r.id, len(r.data))
				problems++
				continue
			}
			fmt.Printf("[%-10s %2d] %5d bytes then released with %v\n",
				r.kind, r.id, len(r.data), r.err)
		case errors.Is(r.err, io.EOF), r.err == nil:
			// The job finished before this reader could be abandoned, so
			// nothing was demonstrated. Not a failure of the buffer.
			fmt.Printf("[%-10s %2d] job ended before abandonment; raise AbandonAfter or use a longer job\n",
				r.kind, r.id)
		default:
			fmt.Printf("[%-10s %2d] UNEXPECTED: %v\n", r.kind, r.id, r.err)
			problems++
		}
	}

	// Agreement is not enough: a bug that truncates the stream for everyone
	// produces perfect agreement. This is the check that the readers agreed on
	// the whole thing.
	if mustEndWith != "" && !bytes.HasSuffix(reference, []byte(mustEndWith)) {
		fmt.Printf("TRUNCATED: the stream does not end with %q, so every reader agreed on an incomplete job\n", mustEndWith)
		problems++
	}

	total := len(stayed) + len(left)
	if problems > 0 {
		return fmt.Errorf("%d/%d readers had problems", problems, total)
	}
	fmt.Printf("all %d readers that stayed received identical output (%d bytes); %d abandoned mid-stream without disturbing them\n",
		len(stayed), len(reference), len(left))
	return nil
}
