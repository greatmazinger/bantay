package runner

import (
	"context"
	"log/slog"
	"os"
	"sync"

	"github.com/greatmazinger/bantay/internal/cgroup"
	"github.com/greatmazinger/bantay/pkg/runner/stream"
)

type (
	JobState string
)

const (
	StateRunning JobState = "RUNNING"
	StateDone    JobState = "DONE"
	StateStopped JobState = "STOPPED"
)

// Job represents the internal state of an executed process
type Job struct {
	// Public fields
	Owner string
	JobID string
	// Private fields
	// mu guards state, exitCode, err, stopRequested
	// All other fields are set at construction and never assigned to.
	mu     sync.Mutex
	cmd    Process
	cancel context.CancelFunc
	output *stream.Buffer

	state         JobState
	exitCode      int
	err           error
	stopRequested bool

	// done is closed once the job has reached a terminal state and its output
	// buffer has been closed. Stop waits on it.
	done chan struct{}

	// group is the job's cgroup, destroyed when the job is reaped. Nil when
	// cgroup isolation is disabled.
	group *cgroup.Group
	// workDir is the job's working directory, removed when the job is reaped.
	// Empty when working directories are disabled.
	workDir string
}

// reap waits for the job's process to exit, records the outcome, releases any
// blocked readers, and removes the resources the job owns. It runs in its own
// goroutine, started by Start after the job is registered.
//
// Defers run last-in-first-out, so done is closed before the cleanup below.
// Stop returns once the job is terminal and its readers are released.
func (j *Job) reap() {
	defer func() {
		j.cancel()
		if j.group != nil {
			// Kill before removing, even though the job's own process has
			// already exited. A descendant that outlived it keeps the cgroup
			// populated, and rmdir on a populated cgroup fails, so without
			// this both the processes and the directory leak. Stop kills on its
			// own path; this covers a job that simply ended.
			if err := j.group.Kill(); err != nil {
				slog.Error("could not kill a finished job's cgroup",
					slog.String("job", j.JobID), slog.String("err", err.Error()))
			}
			if err := j.group.Destroy(); err != nil {
				slog.Error("could not remove a finished job's cgroup",
					slog.String("job", j.JobID), slog.String("err", err.Error()))
			}
		}
		if j.workDir != "" {
			if err := os.RemoveAll(j.workDir); err != nil {
				slog.Error("could not remove a finished job's working directory",
					slog.String("job", j.JobID), slog.String("err", err.Error()))
			}
		}
	}()
	// Deferred so that Stop is released even if anything below panics.
	// Otherwise a Stop blocked on a job whose goroutine died would hang until
	// its caller's context expired.
	defer close(j.done)

	// Deferred for the same reason. Runs before close(j.done).
	defer func() { _ = j.output.Close() }()

	j.setTerminal(j.cmd.Wait())
}

// setTerminal records the process's outcome under the job's lock. The state
// depends on intent rather than on the wait error: a killed process reports an
// ExitError either way, so only stopRequested distinguishes STOPPED from DONE.
//
// The exit code comes from the process, not from waitErr. os/exec reports a
// failure of the wait itself -- an expired WaitDelay, a context cancelled after
// the process had already finished -- only for a process that exited
// successfully, so reading one of those as a signal death would report a clean
// exit as -1. waitErr is still kept, as the reason behind the outcome.
func (j *Job) setTerminal(waitErr error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.err = waitErr
	j.exitCode = j.cmd.ExitCode()
	if j.stopRequested {
		j.state = StateStopped
	} else {
		j.state = StateDone
	}
}

// terminate ends the job's process and the processes it spawned, by the best
// means the manager was configured with.
//
// Stop deliberately does not call this. Its fallback is guarded by the job's
// terminal state, because the pid behind a process group may have been reused
// by then, while its cgroup path is left unguarded so that a reissued Stop
// still reaches cgroup.kill and proves that a redundant one is harmless.
func (j *Job) terminate() error {
	if j.group != nil {
		return j.group.Kill()
	}
	return j.cmd.Terminate()
}

// JobSnapshot is a point-in-time read of a job's observable state. The fields
// are consistent with one another as they are read under a single lock, but any
// of them may be stale by the time the caller sees them.
type JobSnapshot struct {
	State    JobState
	ExitCode int // meaningful only once State is terminal
}
