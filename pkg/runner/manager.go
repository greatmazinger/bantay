// Package runner provides a thread-safe orchestrator for
// managing and getting the output of the spawned process.
package runner

import (
	// "context"

	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/greatmazinger/bantay/internal/cgroup"
	"github.com/greatmazinger/bantay/internal/idgen"
	"github.com/greatmazinger/bantay/pkg/runner/stream"
)

// ErrCommand reports that the command could not be started:
// - it does not exist
// - it is not executable
// - it is not a valid program even though executable bit is set
// These errors are the caller's fault. In the case where the server
// could not create the job's working directory, it is an *fs.PathError
// with the same errno as a command that could not be started.
// The Server can tell which happened and maps the ErrCommand to
// INVALID_ARGUMENT. Everything else is mapped to INTERNAL.
var ErrCommand = errors.New("runner: cannot start command")

// ErrJobNotFound is returned when no job has the given ID. It is the
// registry's answer rather than the policy's: authorization only runs once a
// job has been resolved.
var ErrJobNotFound = errors.New("runner: job not found")

// Authorizer decides whether an actor may act on a job. It is declared here
// rather than in internal/authz so that package returns a concrete type and
// this package states the surface it uses. The tests substitute a fake for
// *authz.Policy.
type Authorizer interface {
	Authorize(actor string, owner string) error
}

// Process interface
type Process interface {
	Start() error
	Wait() error
}

// ProcessSpec describes the process a job is to run.
type ProcessSpec struct {
	// Command and Args are taken verbatim from the request. Nothing is
	// interpreted by a shell.
	Command string
	Args    []string
	// Out receives the merged stdout and stderr.
	Out io.Writer
	// Dir is the working directory. Empty inherits the server's.
	Dir string
	// CgroupFD is a descriptor for the cgroup the process is to be created in.
	// Negative means no cgroup placement, which is how the in-process CLIs and
	// the tests run.
	CgroupFD int
}

// NewProcessFunc defines the function signature for creating a Process
type NewProcessFunc func(ctx context.Context, spec ProcessSpec) Process

// Config holds the isolation settings a manager applies to every job. A zero
// Config disables both, which is how the in-process CLIs and the tests run.
type Config struct {
	// CgroupRoot is the delegated subtree beneath which per-job cgroups are
	// created. Empty disables cgroup isolation.
	CgroupRoot string
	// StateDir is the directory beneath which per-job working directories are
	// created. Empty disables them.
	StateDir string
	// Limits are applied to every job's cgroup.
	Limits cgroup.Limits
}

// JobManager orchestrates the process lifecycle. Consumers that need only
// part of this surface should declare their own interface over it.
type JobManager struct {
	registry   *Registry
	authz      Authorizer
	generateID *idgen.Generator
	newProcess NewProcessFunc
	cfg        Config
}

// NewManager creates a new JobManager
func NewManager(reg *Registry, auth Authorizer, gen *idgen.Generator, newp NewProcessFunc, cfg Config) *JobManager {
	return &JobManager{
		registry:   reg,
		authz:      auth,
		generateID: gen,
		newProcess: newp,
		cfg:        cfg,
	}
}

// Start creates a new job that will run in the background
func (m *JobManager) Start(owner string, command string, args []string) (string, error) {
	jobID := m.generateID.Generate()
	buf := stream.NewBuffer()
	ctx, cancel := context.WithCancel(context.Background())

	// Everything that has to exist before the process does is built first, and
	// unwound together if any of it fails. A job that fails to start leaves no
	// cgroup, no directory, and no registry entry.
	var (
		group   *cgroup.Group
		workDir string
		started bool
	)
	defer func() {
		if started {
			return
		}
		cancel()
		if group != nil {
			_ = group.Destroy()
		}
		if workDir != "" {
			_ = os.RemoveAll(workDir)
		}
	}()

	// The cgroup and its limits, before any process exists. Creating the
	// process first would leave a window in which the job runs unconstrained.
	if m.cfg.CgroupRoot != "" {
		g, err := cgroup.New(m.cfg.CgroupRoot, jobID, m.cfg.Limits)
		if err != nil {
			return "", fmt.Errorf("runner: start %q: %w", command, err)
		}
		group = g
	}

	// The working directory, also before the process. Without one a job
	// inherits the server's, which it has no reason to be able to write to.
	if m.cfg.StateDir != "" {
		workDir = filepath.Join(m.cfg.StateDir, jobID)
		if err := os.Mkdir(workDir, 0o700); err != nil {
			return "", fmt.Errorf("runner: start %q: %w", command, err)
		}
	}

	spec := ProcessSpec{
		Command:  command,
		Args:     args,
		Out:      buf,
		Dir:      workDir,
		CgroupFD: -1,
	}
	if group != nil {
		spec.CgroupFD = group.FD()
	}

	// Create and start the OS process.
	proc := m.newProcess(ctx, spec)
	if err := proc.Start(); err != nil {
		// Only the caller's own mistakes are labelled ErrCommand, which is the
		// one class the server reports back in detail. A fork that failed for
		// the server's reasons stays unlabelled and becomes INTERNAL.
		if callerFault(err) {
			return "", fmt.Errorf("%w %q: %w", ErrCommand, command, err)
		}
		return "", fmt.Errorf("runner: starting %q: %w", command, err)
	}

	job := &Job{
		Owner:   owner,
		JobID:   jobID,
		cmd:     proc,
		cancel:  cancel,
		output:  buf,
		state:   StateRunning,
		done:    make(chan struct{}),
		group:   group,
		workDir: workDir,
	}
	// Register before launching the command goroutine as this prevents
	// a possible race condition.
	if err := m.registry.Add(jobID, job); err != nil {
		// ID collision, unlikely but let's handle it anyway.
		return "", fmt.Errorf("runner: jobID collision %s: %w", jobID, err)
	}
	// Wait for the process in the background, then update state and
	// unblock any Output readers.
	go job.reap()

	started = true
	return jobID, nil
}

func exitCode(err error) int {
	if err == nil {
		return 0
	}
	if exitErr, ok := errors.AsType[*exec.ExitError](err); ok {
		return exitErr.ExitCode()
	}

	return -1 // killed by signal or other OS-level failure
}

// Stop terminates the process. It is synchronous: it does not return until the
// job has reached a terminal state, or until ctx is done. Cancelling ctx ends
// the wait, not the termination, which continues on its own schedule.
func (m *JobManager) Stop(ctx context.Context, actor string, jobID string) error {
	job, found := m.registry.Get(jobID)
	if !found {
		return ErrJobNotFound
	}
	if err := m.authz.Authorize(actor, job.Owner); err != nil {
		return err
	}

	job.mu.Lock()
	job.stopRequested = true
	job.mu.Unlock()

	if job.group != nil {
		if err := job.group.Kill(); err != nil {
			return err
		}
	}
	job.cancel()

	select {
	case <-job.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Status returns some kind of status TODO
func (m *JobManager) Status(actor string, jobID string) (JobSnapshot, error) {
	job, found := m.registry.Get(jobID)
	if !found {
		return JobSnapshot{}, ErrJobNotFound
	}
	if err := m.authz.Authorize(actor, job.Owner); err != nil {
		return JobSnapshot{}, err
	}
	job.mu.Lock()
	defer job.mu.Unlock()
	return JobSnapshot{
		State:    job.state,
		ExitCode: job.exitCode,
	}, nil
}

// Output returns a stream of the job's Output
func (m *JobManager) Output(ctx context.Context, actor string, jobID string) (io.Reader, error) {
	// Look up job:
	job, found := m.registry.Get(jobID)
	if !found {
		return nil, ErrJobNotFound
	}
	// Authorize for actor:
	if err := m.authz.Authorize(actor, job.Owner); err != nil {
		return nil, err
	}
	// Return a reader using the job's output buffer:
	return stream.NewReader(ctx, job.output), nil
}
