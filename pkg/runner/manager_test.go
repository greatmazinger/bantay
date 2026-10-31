package runner

import (
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/greatmazinger/bantay/internal/authz"
	"github.com/greatmazinger/bantay/internal/idgen"
	"github.com/greatmazinger/bantay/pkg/runner/stream"
)

// errKilled stands in for what a real process returns when it is
// killed: *exec.ExitError. Any error can stand in for this as long
// as it's not a context error.
var errKilled = errors.New("signal: killed")

// -----------------------------------------------------------
// Mocks:
// -----------------------------------------------------------
// mockProcess implements Process
type mockProcess struct {
	startErr error
	waitErr  error
	ctx      context.Context
	out      io.Writer
	output   []byte

	// exitCode is what ExitCode reports. Written by Wait and read by the
	// manager only after Wait has returned, which is the same ordering the real
	// process relies on, so it needs no lock.
	exitCode int

	// terminated records whether Stop reached for the process-group fallback,
	// which it only does for a manager configured without a cgroup.
	mu         sync.Mutex
	terminated bool
}

func (m *mockProcess) Terminate() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.terminated = true
	// A real Terminate kills the group, which ends the Wait below. The mock's
	// Wait blocks on its context, so releasing it is the equivalent.
	return nil
}

func (m *mockProcess) wasTerminated() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.terminated
}

func (m *mockProcess) Start() error {
	return m.startErr
}

func (m *mockProcess) ExitCode() int {
	return m.exitCode
}

func (m *mockProcess) Wait() error {
	// If a context was provided, we block until the context is canceled.
	if m.ctx != nil {
		<-m.ctx.Done()
		m.exitCode = -1 // what a real process killed by a signal reports
		return errKilled
	}
	if m.out != nil {
		_, _ = m.out.Write(m.output)
	}
	return m.waitErr
}

// newTestIDGen returns a real generator. No test asserts on a job ID, so there
// is nothing for a fake to be useful for.
func newTestIDGen() *idgen.Generator {
	gen, err := idgen.NewGenerator()
	if err != nil {
		panic(err) // crypto/rand failing is not a recoverable condition
	}
	return gen
}

// The production Authorizer is *authz.Policy. Nothing outside a future PR constructs
// one, so without this a signature drift in Policy.Authorize would not surface
// until the server compiled.
var _ Authorizer = (*authz.Policy)(nil)

// mockAuthz implements Authorizer
type mockAuthz struct {
	err error // To mock authorization errors
}

func (m *mockAuthz) Authorize(actor string, owner string) error {
	if actor != astrid {
		return authz.ErrForbidden
	}
	return m.err
}

// Simple mock Manager:
func setupTestManager(startErr error) (*JobManager, *Registry) {
	reg := NewRegistry()
	authz := &mockAuthz{}

	gen := newTestIDGen()

	// Mock process
	newProc := func(ctx context.Context, spec ProcessSpec) Process {
		// The spec's isolation fields are unused here: a zero Config means
		// no cgroup and no working directory.
		return &mockProcess{
			startErr: startErr,
			waitErr:  nil,
			ctx:      ctx,
		}
	}

	mgr := NewManager(reg, authz, gen, newProc, Config{})
	return mgr, reg
}

func TestManager_StartStop(t *testing.T) {
	mgr, reg := setupTestManager(nil)

	jobID, err := mgr.Start(astrid, "sleep", []string{"128"})
	if err != nil {
		t.Fatalf("Start error: %v", err)
	}
	// Job should be registered and running
	job, found := reg.Get(jobID)
	if !found {
		t.Fatal("Start: job not found in registry")
	}

	if err := mgr.Stop(t.Context(), astrid, jobID); err != nil {
		t.Fatalf("Stop error: %v", err)
	}

	// Get output to block and allow the goroutine to finish updating state
	// and close the buffer
	if _, err := io.Copy(io.Discard, stream.NewReader(t.Context(), job.output)); err != nil {
		t.Fatalf("Error copying output: %v", err)
	}

	job.mu.Lock()
	defer job.mu.Unlock()
	if job.state != StateStopped {
		t.Errorf("expected StateStopped, got %v", job.state)
	}
}

func TestManager_Stop_WrongActor(t *testing.T) {
	mgr, reg := setupTestManager(nil)

	jobID, err := mgr.Start(astrid, "sleep", []string{"128"})
	if err != nil {
		t.Fatalf("Start error: %v", err)
	}
	// Job should be registered and running
	_, found := reg.Get(jobID)
	if !found {
		t.Fatal("Start: job not found in registry")
	}

	// A refused Stop leaves the job running, so stop it as its owner before
	// the test ends.
	t.Cleanup(func() {
		if err := mgr.Stop(context.Background(), astrid, jobID); err != nil {
			t.Errorf("cleanup Stop: %v", err)
		}
	})

	// Wrong actor used:
	err = mgr.Stop(t.Context(), bheng, jobID)
	if err != authz.ErrForbidden {
		t.Fatalf("expected ErrForbidden, got: %v", err)
	}
}

func TestManager_OutputSimpleHappyPath(t *testing.T) {
	mgr, reg := setupTestManager(nil)

	jobID, err := mgr.Start(astrid, "sleep", []string{"128"})
	if err != nil {
		t.Fatalf("Start error: %v", err)
	}
	// Job should be registered and running
	job, found := reg.Get(jobID)
	if !found {
		t.Fatal("Start: job not found in registry")
	}

	if err := mgr.Stop(t.Context(), astrid, jobID); err != nil {
		t.Fatalf("Stop error: %v", err)
	}

	// Get output to block and allow the goroutine to finish updating state
	// and close the buffer
	if _, err := io.Copy(io.Discard, stream.NewReader(t.Context(), job.output)); err != nil {
		t.Fatalf("Error copying output: %v", err)
	}

	job.mu.Lock()
	defer job.mu.Unlock()
	if job.state != StateStopped {
		t.Errorf("expected StateStopped, got %v", job.state)
	}
}

func setupTestManagerWithOutput(output []byte) (*JobManager, *Registry) {
	reg := NewRegistry()
	authz := &mockAuthz{}

	gen := newTestIDGen()

	// Mock process
	newProc := func(ctx context.Context, spec ProcessSpec) Process {
		// Ignore out in these tests:
		return &mockProcess{
			out:    spec.Out,
			output: output,
		}
	}

	mgr := NewManager(reg, authz, gen, newProc, Config{})
	return mgr, reg
}

func TestManager_OutputIsReturned(t *testing.T) {
	want := []byte("correct output TestManager_OutputIsReturned")
	mgr, _ := setupTestManagerWithOutput(want)

	jobID, err := mgr.Start(astrid, "echo", nil)
	if err != nil {
		t.Fatalf("Start error: %v", err)
	}

	r, err := mgr.Output(t.Context(), astrid, jobID)
	if err != nil {
		t.Fatalf("Output error: %v", err)
	}
	result, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("Readall error: %v", err)
	}

	if string(result) != string(want) {
		t.Errorf("expected %q, got %q", want, result)
	}
}

func TestManager_Output_LateReaderGetsAllOutput(t *testing.T) {
	want := []byte("Lorem ipsum dolor sit amet, consectetur adipiscing elit")
	mgr, _ := setupTestManagerWithOutput(want)

	jobID, err := mgr.Start(astrid, "echo", nil)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Drain a reader to make sure the job has fully completed
	r1, err := mgr.Output(t.Context(), astrid, jobID)
	if err != nil {
		t.Fatalf("Output first reader: %v", err)
	}
	if _, err = io.Copy(io.Discard, r1); err != nil {
		t.Fatalf("first sync drain: %v", err)
	}

	// Job is now done. Late reader should still get all bytes from offset 0.
	r2, err := mgr.Output(t.Context(), astrid, jobID)
	if err != nil {
		t.Fatalf("Output late reader: %v", err)
	}
	got, err := io.ReadAll(r2)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}

	if string(got) != string(want) {
		t.Errorf("got %q, want %q", got, want)
	}
}

// Concurrent output readers testing:
func TestManager_Output_ConcurrentReaders(t *testing.T) {
	want := []byte(" sed do eiusmod tempor incididunt ut labore et dolore magna aliqua")
	mgr, _ := setupTestManagerWithOutput(want)
	jobID, _ := mgr.Start(astrid, "echo", nil)

	const numReaders = 1000
	results := make([][]byte, numReaders)
	var wg sync.WaitGroup

	for i := range numReaders {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r, err := mgr.Output(t.Context(), astrid, jobID)
			if err != nil {
				t.Errorf("Output reader [%d] error: %v", i, err)
				return
			}
			results[i], _ = io.ReadAll(r)
		}(i)
	}
	wg.Wait()

	// Everyone is here, let's check:
	for i, got := range results {
		if string(got) != string(want) {
			t.Errorf("reader %d: expected %q, got %q", i, want, got)
		}
	}
}

// TestManager_StopKillsTheCgroup covers the wiring between Stop and the cgroup.
// Like the cgroup package's own tests, this runs against an ordinary directory.
// It asserts that Stop wrote what it says it writes, not that the kernel acted
// on it.
// TestManager_StopDoesNotWaitOnCleanup asserts the defer order in Job.reap.
// The cleanup defer is registered first so that LIFO runs it last, after
// close(j.done), which is what lets Stop return once the job is terminal and
// its readers are released rather than waiting on bookkeeping no client can
// observe.
//
// The bookkeeping is not cheap. Destroy retries os.Remove for destroyTimeout,
// and against an ordinary directory that still holds cgroup.kill every retry
// fails, so reversing the two defers puts a two second grind in front of every
// Stop. Nothing else in the suite notices: with the defers swapped the whole
// package still passes, two seconds slower.
//
// Inside a bubble the clock is fake, so Destroy's sleeps advance it without
// anyone waiting and the cost becomes an assertion rather than a delay.
func TestManager_StopDoesNotWaitOnCleanup(t *testing.T) {
	synctest.Test(t, testStopDoesNotWaitOnCleanup)
}

func testStopDoesNotWaitOnCleanup(t *testing.T) {
	root := t.TempDir()

	newProc := func(ctx context.Context, spec ProcessSpec) Process {
		return &mockProcess{ctx: ctx}
	}
	mgr := NewManager(NewRegistry(), &mockAuthz{}, newTestIDGen(),
		newProc, Config{CgroupRoot: root})

	jobID, err := mgr.Start(astrid, "sleep", []string{"128"})
	if err != nil {
		t.Fatalf("Start error: %v", err)
	}

	start := time.Now()
	if err := mgr.Stop(t.Context(), astrid, jobID); err != nil {
		t.Fatalf("Stop error: %v", err)
	}
	if elapsed := time.Since(start); elapsed != 0 {
		t.Errorf("Stop consumed %v of bubble time, want 0: it is waiting on cleanup", elapsed)
	}

	// The reaping goroutine is still in Destroy, retrying os.Remove against a
	// directory holding the cgroup.kill that Stop just wrote. It gives up
	// after destroyTimeout, so the bubble's clock has to run past that or the
	// test ends with a goroutine still sleeping on a timer. None of this costs
	// real time: advancing a fake clock is free, which is the point.
	time.Sleep(3 * time.Second)
	synctest.Wait()
}

func TestManager_StopKillsTheCgroup(t *testing.T) {
	root := t.TempDir()

	newProc := func(ctx context.Context, spec ProcessSpec) Process {
		return &mockProcess{ctx: ctx}
	}
	mgr := NewManager(NewRegistry(), &mockAuthz{}, newTestIDGen(),
		newProc, Config{CgroupRoot: root})

	jobID, err := mgr.Start(astrid, "sleep", []string{"128"})
	if err != nil {
		t.Fatalf("Start error: %v", err)
	}

	if err = mgr.Stop(t.Context(), astrid, jobID); err != nil {
		t.Fatalf("Stop error: %v", err)
	}

	got, err := os.ReadFile(filepath.Join(root, jobID, "cgroup.kill"))
	if err != nil {
		t.Fatalf("Stop should have written cgroup.kill: %v", err)
	}

	if string(got) != "1" {
		t.Errorf("cgroup.kill = %q, expected %q", got, "1")
	}
}

// TestManager_StopIsIdempotentAfterTheGroupIsReaped covers the retry the design
// doc tells clients to make: an ambiguous Stop can be reissued, because a
// redundant one succeeds and does nothing.
//
// The reap has to be simulated. In a real hierarchy the kernel removes a
// cgroup's interface files along with its directory, so Destroy leaves nothing
// behind. Against a temp directory cgroup.kill is an ordinary file that blocks
// the rmdir, so the group survives its own Destroy and the second Stop would
// pass for the wrong reason.
func TestManager_StopIsIdempotentAfterTheGroupIsReaped(t *testing.T) {
	root := t.TempDir()

	newProc := func(ctx context.Context, spec ProcessSpec) Process {
		return &mockProcess{ctx: ctx}
	}
	mgr := NewManager(NewRegistry(), &mockAuthz{}, newTestIDGen(),
		newProc, Config{CgroupRoot: root})

	jobID, err := mgr.Start(astrid, "sleep", []string{"128"})
	if err != nil {
		t.Fatalf("Start error: %v", err)
	}
	if err := mgr.Stop(t.Context(), astrid, jobID); err != nil {
		t.Fatalf("first Stop: %v", err)
	}

	if err := os.RemoveAll(filepath.Join(root, jobID)); err != nil {
		t.Fatal(err)
	}

	if err := mgr.Stop(t.Context(), astrid, jobID); err != nil {
		t.Errorf("Stop on a job whose group was reaped should succeed, got: %v", err)
	}
}

func TestManager_StartRejectsNonexistentCommand(t *testing.T) {
	mgr := NewManager(NewRegistry(), &mockAuthz{}, newTestIDGen(), NewOSProcess, Config{})
	jobID, err := mgr.Start(astrid, "definitely-not-a-real-cmd-12345", nil)
	if err == nil {
		t.Fatalf("Start should have failed for non-existent command, job got %q", jobID)
	}
	if !errors.Is(err, ErrCommand) {
		t.Errorf("Expected ErrCommand, got: %v", err)
	}
	if jobID != "" {
		t.Error("Expected no job created, got a real jobID")
	}
}

func TestManager_ServerStartFailIsNotCommandError(t *testing.T) {
	root := t.TempDir()

	if err := os.Chmod(root, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(root, 0o700) })

	mgr := NewManager(NewRegistry(), &mockAuthz{}, newTestIDGen(), NewOSProcess, Config{StateDir: root})
	_, err := mgr.Start(astrid, "true", nil)
	if err == nil {
		t.Fatalf("Start should have failed as the state directory isn't writable.")
	}
	if errors.Is(err, ErrCommand) {
		t.Fatal("Error shouldn't be Errcommand")
	}
}

// TestManager_WorkingDirectory covers the following claims about a job's
// working directory: the job is given its own working directory, and that it
// is removed when the job is reaped.
//
// It runs in a synctest bubble for the second claim. close(job.done) releases
// Stop before the cleanup defers run, so checking immediately after Stop would
// race the removal. synctest.Wait returns once every other goroutine in the
// bubble is durably blocked or gone, and the wait goroutine exits after its
// defers, so by the time Wait returns the removal has happened.
//
// Whether the operating system honours cmd.Dir is up to os/exec, so the first
// claim is asserted by checking the path handed to the process rather than by
// running one.
func TestManager_WorkingDirectory(t *testing.T) {
	synctest.Test(t, testWorkingDirectory)
}

func testWorkingDirectory(t *testing.T) {
	root := t.TempDir()

	var gotDir string
	newProc := func(ctx context.Context, spec ProcessSpec) Process {
		gotDir = spec.Dir
		return &mockProcess{ctx: ctx}
	}
	mgr := NewManager(NewRegistry(), &mockAuthz{}, newTestIDGen(),
		newProc, Config{StateDir: root})

	jobID, err := mgr.Start(astrid, "sleep", []string{"128"})
	if err != nil {
		t.Fatalf("Start error: %v", err)
	}

	wantDir := filepath.Join(root, jobID)
	if gotDir != wantDir {
		t.Errorf("process was given Dir %q, want %q", gotDir, wantDir)
	}

	info, err := os.Stat(wantDir)
	if err != nil {
		t.Fatalf("working directory should exist while the job runs: %v", err)
	}
	if !info.IsDir() {
		t.Errorf("%s is not a directory", wantDir)
	}
	// 0700: the job writes here, and nothing else on the host should read it.
	if perm := info.Mode().Perm(); perm != 0o700 {
		t.Errorf("working directory mode = %v, want %v", perm, os.FileMode(0o700))
	}

	if err := mgr.Stop(t.Context(), astrid, jobID); err != nil {
		t.Fatalf("Stop error: %v", err)
	}

	// Stop returns when job.done closes, which happens before the cleanup
	// defers. Wait for the goroutine to finish them and exit.
	synctest.Wait()

	if _, err := os.Stat(wantDir); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("working directory should be gone once the job is reaped, got %v", err)
	}
}

// steppedProcess writes its output in stages, waiting to be told to continue
// between them. It exists so a test can attach readers at chosen points in a
// job's life rather than only before it starts or after it has finished.
type steppedProcess struct {
	out   io.Writer
	steps [][]byte
	next  chan struct{} // one receive per step after the first
}

func (p *steppedProcess) Start() error     { return nil }
func (p *steppedProcess) Terminate() error { return nil }
func (p *steppedProcess) ExitCode() int    { return 0 }

func (p *steppedProcess) Wait() error {
	for i, step := range p.steps {
		if i > 0 {
			<-p.next
		}
		if _, err := p.out.Write(step); err != nil {
			return err
		}
	}
	return nil
}

// TestManager_Output_ReadersAttachAtDifferentPoints is the property the design
// calls the one most easily broken by a change that looks harmless: every
// client receives the same complete stream from the process's first write, no
// matter when it attached.
//
// The existing concurrent-reader test attaches everyone after the job has
// finished, so it never exercises a reader that has to block and be woken. This
// one attaches before the job writes anything, midway through, and after it has
// exited, and requires all three to end up byte-identical.
//
// Runs in a synctest bubble so that "the readers are parked" is a fact rather
// than a hope: synctest.Wait returns only once every other goroutine is durably
// blocked.
//
// What this does not cover is the wake mechanism itself. A reader woken by the
// last write may find the buffer already closed and take the EOF path instead
// of parking again, so removing either wake still leaves this test passing.
// testBlockingRead and testCloseEmptyBuffer in the stream package are what
// catch that; both were checked against exactly those two mutations.
func TestManager_Output_ReadersAttachAtDifferentPoints(t *testing.T) {
	synctest.Test(t, testReadersAttachAtDifferentPoints)
}

func testReadersAttachAtDifferentPoints(t *testing.T) {
	steps := [][]byte{[]byte("first "), []byte("second "), []byte("third")}
	want := "first second third"

	var proc *steppedProcess
	newProc := func(ctx context.Context, spec ProcessSpec) Process {
		proc = &steppedProcess{out: spec.Out, steps: steps, next: make(chan struct{})}
		return proc
	}
	mgr := NewManager(NewRegistry(), &mockAuthz{}, newTestIDGen(), newProc, Config{})

	// Attached before the process has written anything: this reader must block
	// and be woken three times.
	early := readAll(t, mgr, "early")

	jobID, err := mgr.Start(astrid, "stepped", nil)
	if err != nil {
		t.Fatalf("Start error: %v", err)
	}
	early.start(t, mgr, jobID)

	// Wait for the first step to land and the early reader to park again.
	synctest.Wait()

	// Attached midway, with one step already buffered and two to come.
	midway := readAll(t, mgr, "midway")
	midway.start(t, mgr, jobID)
	synctest.Wait()

	proc.next <- struct{}{} // second
	synctest.Wait()
	proc.next <- struct{}{} // third
	synctest.Wait()

	// The job is finished and its buffer closed, so this reader never blocks.
	late := readAll(t, mgr, "late")
	late.start(t, mgr, jobID)

	for _, r := range []*reader{early, midway, late} {
		got, err := r.result()
		if err != nil {
			t.Errorf("%s reader: %v", r.name, err)
			continue
		}
		if got != want {
			t.Errorf("%s reader read %q, want %q", r.name, got, want)
		}
	}
}

// reader drains an Output stream on its own goroutine.
type reader struct {
	name string
	done chan struct{}
	got  string
	err  error
}

func readAll(t *testing.T, mgr *JobManager, name string) *reader {
	t.Helper()
	return &reader{name: name, done: make(chan struct{})}
}

func (r *reader) start(t *testing.T, mgr *JobManager, jobID string) {
	t.Helper()
	src, err := mgr.Output(t.Context(), astrid, jobID)
	if err != nil {
		t.Fatalf("%s reader: Output error: %v", r.name, err)
	}
	go func() {
		defer close(r.done)
		b, err := io.ReadAll(src)
		r.got, r.err = string(b), err
	}()
}

func (r *reader) result() (string, error) {
	<-r.done
	return r.got, r.err
}

// blockingProcess stays in Wait until the test releases it, ignoring context
// cancellation. mockProcess returns as soon as its context is done, which Stop
// cancels, so it cannot show whether Stop waits for anything.
type blockingProcess struct {
	release chan struct{}
}

func (p *blockingProcess) Start() error { return nil }

// Deliberately does not release: this mock exists so a test can hold the job
// running while it checks that Stop has not returned. The test releases it.
func (p *blockingProcess) Terminate() error { return nil }

// Reports what a real process killed by a signal does, to match the errKilled
// its Wait returns.
func (p *blockingProcess) ExitCode() int { return -1 }

func (p *blockingProcess) Wait() error {
	<-p.release
	return errKilled
}

// TestManager_StopIsSynchronous asserts the property the design's termination
// section is built on: Stop does not return until the job has reached a
// terminal state. That is what lets JobState have no intermediate stopping
// value, and what makes a Status immediately after a Stop trustworthy.
//
// Runs in a synctest bubble because the assertion is about something not
// having happened yet, which is only meaningful once everything that could
// happen has: synctest.Wait returns when every other goroutine is durably
// blocked, so a Stop still outstanding at that point is genuinely waiting
// rather than merely unscheduled.
func TestManager_StopIsSynchronous(t *testing.T) {
	synctest.Test(t, testStopIsSynchronous)
}

func testStopIsSynchronous(t *testing.T) {
	var proc *blockingProcess
	newProc := func(ctx context.Context, spec ProcessSpec) Process {
		proc = &blockingProcess{release: make(chan struct{})}
		return proc
	}
	mgr := NewManager(NewRegistry(), &mockAuthz{}, newTestIDGen(), newProc, Config{})

	jobID, err := mgr.Start(astrid, "blocker", nil)
	if err != nil {
		t.Fatalf("Start error: %v", err)
	}

	returned := make(chan error, 1)
	go func() { returned <- mgr.Stop(t.Context(), astrid, jobID) }()

	synctest.Wait()

	select {
	case err := <-returned:
		t.Fatalf("Stop returned %v while the process was still running", err)
	default:
	}

	// And the job is still running, so Stop has not declared an outcome early.
	snap, err := mgr.Status(astrid, jobID)
	if err != nil {
		t.Fatalf("Status error: %v", err)
	}
	if snap.State != StateRunning {
		t.Errorf("state = %q while Stop is still waiting, want %q", snap.State, StateRunning)
	}

	close(proc.release)
	synctest.Wait()

	select {
	case err := <-returned:
		if err != nil {
			t.Fatalf("Stop error: %v", err)
		}
	default:
		t.Fatal("Stop did not return once the process exited")
	}

	// Terminal by the time Stop returned, not merely soon afterwards.
	snap, err = mgr.Status(astrid, jobID)
	if err != nil {
		t.Fatalf("Status error: %v", err)
	}
	if snap.State != StateStopped {
		t.Errorf("state = %q after Stop returned, want %q", snap.State, StateStopped)
	}
}

// TestManager_ReapKillsTheCgroup covers the job that nobody stopped. Stop
// writes cgroup.kill on its own path, but a job that simply exited leaves
// whatever its descendants became, and rmdir on a populated cgroup fails, so
// without a kill here both the processes and the directory survive the job.
//
// Like the other cgroup tests this runs against an ordinary directory and
// asserts what the service wrote, not what the kernel did with it.
func TestManager_ReapKillsTheCgroup(t *testing.T) {
	synctest.Test(t, testReapKillsTheCgroup)
}

func testReapKillsTheCgroup(t *testing.T) {
	root := t.TempDir()

	// No ctx field, so Wait returns immediately: the job ends on its own and
	// nothing calls Stop.
	newProc := func(ctx context.Context, spec ProcessSpec) Process {
		return &mockProcess{}
	}
	mgr := NewManager(NewRegistry(), &mockAuthz{}, newTestIDGen(),
		newProc, Config{CgroupRoot: root})

	jobID, err := mgr.Start(astrid, "true", nil)
	if err != nil {
		t.Fatalf("Start error: %v", err)
	}

	// Let the reaping goroutine run to completion, including Destroy's retries.
	synctest.Wait()
	time.Sleep(3 * time.Second)
	synctest.Wait()

	got, err := os.ReadFile(filepath.Join(root, jobID, "cgroup.kill"))
	if err != nil {
		t.Fatalf("reap should have written cgroup.kill for a job nobody stopped: %v", err)
	}
	if string(got) != "1" {
		t.Errorf("cgroup.kill = %q, want %q", got, "1")
	}
}

// TestManager_StopWithoutACgroupKillsTheProcessGroup covers the default
// deployment, where --cgroup-root is empty.
//
// There Stop has no cgroup.kill to write, and cancelling the context signals
// the leader alone, leaving its children reparented and running. The proto
// promises Stop terminates "a job and every process it spawned" without
// qualification, so the process group is the fallback.
func TestManager_StopWithoutACgroupKillsTheProcessGroup(t *testing.T) {
	var proc *mockProcess
	newProc := func(ctx context.Context, spec ProcessSpec) Process {
		proc = &mockProcess{ctx: ctx}
		return proc
	}
	// No CgroupRoot: this is the case under test.
	mgr := NewManager(NewRegistry(), &mockAuthz{}, newTestIDGen(), newProc, Config{})

	jobID, err := mgr.Start(astrid, "sleep", []string{"300"})
	if err != nil {
		t.Fatalf("Start error: %v", err)
	}
	if err := mgr.Stop(t.Context(), astrid, jobID); err != nil {
		t.Fatalf("Stop error: %v", err)
	}
	if !proc.wasTerminated() {
		t.Error("Stop did not reach for the process group, so a job's children outlive it")
	}
}

// And the other half: with a cgroup, cgroup.kill is the complete answer and
// the weaker fallback must not be used. It reaches only the process group,
// which anything that called setsid has already left.
func TestManager_StopWithACgroupDoesNotUseTheFallback(t *testing.T) {
	root := t.TempDir()

	var proc *mockProcess
	newProc := func(ctx context.Context, spec ProcessSpec) Process {
		proc = &mockProcess{ctx: ctx}
		return proc
	}
	mgr := NewManager(NewRegistry(), &mockAuthz{}, newTestIDGen(),
		newProc, Config{CgroupRoot: root})

	jobID, err := mgr.Start(astrid, "sleep", []string{"300"})
	if err != nil {
		t.Fatalf("Start error: %v", err)
	}
	if err := mgr.Stop(t.Context(), astrid, jobID); err != nil {
		t.Fatalf("Stop error: %v", err)
	}
	if proc.wasTerminated() {
		t.Error("Stop used the process-group fallback despite having a cgroup")
	}
	if _, err := os.ReadFile(filepath.Join(root, jobID, "cgroup.kill")); err != nil {
		t.Errorf("Stop should have written cgroup.kill: %v", err)
	}
}

// A job that exited cleanly reports 0, even when the wait around it failed.
//
// os/exec substitutes an error of its own -- an expired WaitDelay, a context
// cancelled after the process had already finished -- only when the process
// itself exited successfully, so such an error says nothing about the exit
// status. Deriving the code from it reported a clean exit as -1, which the
// schema documents as a signal death.
//
// Verified against a real process too: `sh -c 'sleep 30 & exit 0'` leaves the
// backgrounded sleep holding the output pipe, so the pipe never reports EOF,
// the 5s WaitDelay ends the wait, and Wait returns ErrWaitDelay for a process
// whose ProcessState says exit status 0. That takes five seconds of wall clock
// to reproduce, so the contract is pinned here instead.
func TestManager_CleanExitWithAFailedWaitReportsZero(t *testing.T) {
	synctest.Test(t, testCleanExitWithAFailedWaitReportsZero)
}

func testCleanExitWithAFailedWaitReportsZero(t *testing.T) {
	newProc := func(ctx context.Context, spec ProcessSpec) Process {
		// No ctx, so Wait returns at once and the job reaps itself with nobody
		// stopping it. exitCode is left at zero: the process exited 0, and only
		// the wait around it went wrong.
		return &mockProcess{waitErr: exec.ErrWaitDelay}
	}
	mgr := NewManager(NewRegistry(), &mockAuthz{}, newTestIDGen(), newProc, Config{})

	jobID, err := mgr.Start(astrid, "sh", []string{"-c", "sleep 30 & exit 0"})
	if err != nil {
		t.Fatalf("Start error: %v", err)
	}
	// Nothing to wait on: no Stop is issued, so wait for the reaper to record
	// the outcome and go away.
	synctest.Wait()

	snap, err := mgr.Status(astrid, jobID)
	if err != nil {
		t.Fatalf("Status error: %v", err)
	}
	if snap.State != StateDone {
		t.Errorf("state = %q, want %q", snap.State, StateDone)
	}
	if snap.ExitCode != 0 {
		t.Errorf("exit code = %d, want 0: the process exited cleanly and only the wait failed", snap.ExitCode)
	}
}
