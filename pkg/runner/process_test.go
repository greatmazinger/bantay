package runner

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

// TestStart_ClassifiesFailures drives the real os/exec path rather than a mock,
// because what is being tested is which error os/exec actually produces for
// each way a command can fail to start. A mock returning a hand-built error
// would only assert that the classifier agrees with the test's own guess.
func TestStart_ClassifiesFailures(t *testing.T) {
	dir := t.TempDir()

	notExecutable := filepath.Join(dir, "noexec.txt")
	if err := os.WriteFile(notExecutable, []byte("not a program\n"), 0o644); err != nil {
		t.Fatalf("writing the fixture: %v", err)
	}
	noShebang := filepath.Join(dir, "notaprogram")
	if err := os.WriteFile(noShebang, []byte("this is not a program\n"), 0o755); err != nil {
		t.Fatalf("writing the fixture: %v", err)
	}
	aDirectory := filepath.Join(dir, "adirectory")
	if err := os.Mkdir(aDirectory, 0o755); err != nil {
		t.Fatalf("creating the fixture: %v", err)
	}

	tests := []struct {
		name    string
		command string
		args    []string
	}{
		{"a name that is not on PATH", "nosuchbinary-xyz-quux", nil},
		{"an absolute path that does not exist", "/nonexistent/path/prog", nil},
		{"a file that is not executable", notExecutable, nil},
		{"a directory", aDirectory, nil},
		{"a text file with no shebang", noShebang, nil},
		{"a null byte in an argument", "echo", []string{"a\x00b"}},
		// Past ARG_MAX, which on Linux bounds argv and env together.
		{"an argv the kernel will not accept", "echo", []string{strings.Repeat("x", 4<<20)}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mgr := NewManager(NewRegistry(), &mockAuthz{}, newTestIDGen(), NewOSProcess, Config{})

			_, err := mgr.Start(astrid, tt.command, tt.args)
			if err == nil {
				t.Fatal("expected Start to fail")
			}
			// ErrCommand is what the server reports back in detail, so every
			// one of these has to carry it.
			if !errors.Is(err, ErrCommand) {
				t.Errorf("err = %v, want it to wrap ErrCommand", err)
			}
		})
	}
}

// The server's own failures must not be labelled as the caller's, or a client
// is told to fix something it did not cause and is shown the server's state.
func TestCallerFault_ServerFailuresAreNotTheCallers(t *testing.T) {
	for _, tt := range []struct {
		name string
		err  error
	}{
		{"out of processes", &fs.PathError{Op: "fork/exec", Path: "/bin/echo", Err: syscall.EAGAIN}},
		{"out of memory", &fs.PathError{Op: "fork/exec", Path: "/bin/echo", Err: syscall.ENOMEM}},
		{"too many open files", &fs.PathError{Op: "fork/exec", Path: "/bin/echo", Err: syscall.EMFILE}},
		{"a cgroup write failed", errors.New("cgroup: writing cpu.max: permission denied")},
		{"context cancelled", context.Canceled},
		// clone3 refuses a UseCgroupFD it cannot honour with EINVAL, on a kernel
		// older than 5.7 or for a destination that will not take processes. That
		// is the server's misconfiguration and it fails every Start, so a client
		// must not be told it sent a bad request. A null byte in argv arrives as
		// the same errno and is caught by validateArgv before the fork.
		{"a cgroup descriptor the kernel refused", &fs.PathError{Op: "fork/exec", Path: "/bin/echo", Err: syscall.EINVAL}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if callerFault(tt.err) {
				t.Errorf("callerFault(%v) = true, want false", tt.err)
			}
		})
	}
}

func TestCallerFault_RecognisesTheCallersMistakes(t *testing.T) {
	for _, tt := range []struct {
		name string
		err  error
	}{
		{"PATH lookup failed", &exec.Error{Name: "nosuchbinary", Err: exec.ErrNotFound}},
		{"no such file", &fs.PathError{Op: "fork/exec", Err: syscall.ENOENT}},
		{"permission denied", &fs.PathError{Op: "fork/exec", Err: syscall.EACCES}},
		{"not an executable format", &fs.PathError{Op: "fork/exec", Err: syscall.ENOEXEC}},
		{"argv larger than the kernel accepts", &fs.PathError{Op: "fork/exec", Err: syscall.E2BIG}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if !callerFault(tt.err) {
				t.Errorf("callerFault(%v) = false, want true", tt.err)
			}
		})
	}

	// Wrapped, as it arrives once Start has added its own context.
	t.Run("through a wrapper", func(t *testing.T) {
		wrapped := errors.Join(errors.New("outer"), &exec.Error{Name: "x", Err: exec.ErrNotFound})
		if !callerFault(wrapped) {
			t.Error("callerFault should see through a wrapper")
		}
	})
}

// A server failure must reach the boundary unlabelled, so it maps to INTERNAL
// and its detail stays on the server.
func TestStart_ServerFailureIsNotErrCommand(t *testing.T) {
	newProc := func(ctx context.Context, spec ProcessSpec) Process {
		return &mockProcess{startErr: &fs.PathError{Op: "fork/exec", Path: "/bin/echo", Err: syscall.EAGAIN}}
	}
	mgr := NewManager(NewRegistry(), &mockAuthz{}, newTestIDGen(), newProc, Config{})

	_, err := mgr.Start(astrid, "echo", nil)
	if err == nil {
		t.Fatal("expected Start to fail")
	}
	if errors.Is(err, ErrCommand) {
		t.Errorf("err = %v, should not be labelled as the caller's mistake", err)
	}
}

// The null byte used to be inferred from the EINVAL that fork/exec returns for
// it, which the kernel also uses to refuse a cgroup descriptor. It is named
// before the fork now, so the caller is told which argument is at fault and a
// server that cannot place processes in cgroups no longer blames its clients.
func TestStart_RejectsANullByteInArgv(t *testing.T) {
	for _, tt := range []struct {
		name    string
		command string
		args    []string
	}{
		{"in the command", "ec\x00ho", nil},
		{"in an argument", "echo", []string{"fine", "not\x00fine"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			mgr := NewManager(NewRegistry(), &mockAuthz{}, newTestIDGen(), NewOSProcess, Config{})
			jobID, err := mgr.Start(astrid, tt.command, tt.args)
			if !errors.Is(err, ErrCommand) {
				t.Errorf("err = %v, want it to wrap ErrCommand", err)
			}
			if jobID != "" {
				t.Errorf("job ID = %q, want none: no job should have been created", jobID)
			}
		})
	}
}

// Nothing is created before argv is checked, so a rejected one leaves no
// cgroup and no working directory behind.
func TestStart_ANullByteLeavesNothingBehind(t *testing.T) {
	root, state := t.TempDir(), t.TempDir()
	mgr := NewManager(NewRegistry(), &mockAuthz{}, newTestIDGen(), NewOSProcess,
		Config{CgroupRoot: root, StateDir: state})

	if _, err := mgr.Start(astrid, "echo", []string{"not\x00fine"}); !errors.Is(err, ErrCommand) {
		t.Fatalf("err = %v, want it to wrap ErrCommand", err)
	}
	for _, dir := range []string{root, state} {
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) != 0 {
			t.Errorf("%s holds %d entries, want none", dir, len(entries))
		}
	}
}
