package runner

import (
	"context"
	"errors"
	"io/fs"
	"os/exec"
	"syscall"
	"time"
)

// NewOSProcess is the default NewProcessFunc. It builds an exec.Cmd whose
// stdout and stderr are merged into spec.Out, that runs in spec.Dir, that
// leads its own process group, and that is created inside the cgroup named by
// spec.CgroupFD when that is non-negative.
func NewOSProcess(ctx context.Context, spec ProcessSpec) Process {
	cmd := exec.CommandContext(ctx, spec.Command, spec.Args...)
	// stdout and stderr are merged into a single io.Writer
	cmd.Stdout = spec.Out
	cmd.Stderr = spec.Out
	cmd.Dir = spec.Dir

	// Bounds one specific hang: the job's own process has exited but a
	// descendant that escaped the cgroup still holds the output pipe, so its
	// read end never reports end-of-file and Wait blocks on the copier
	// goroutines forever. The job stays RUNNING with every reader parked on it.
	// After the delay os/exec closes the pipes and Wait returns.
	//
	// It does not bound a process that cannot be reaped at all, such as one in
	// uninterruptible sleep. There the delay escalates to Process.Kill, which
	// such a task cannot receive, and Wait goes on waiting for the child.
	cmd.WaitDelay = 5 * time.Second

	attr := &syscall.SysProcAttr{
		// The job leads its own process group, detached from the server's,
		// so a Ctrl-C at the server's terminal reaches the server alone
		// rather than every running job.
		Setpgid: true,
	}
	if spec.CgroupFD >= 0 {
		// The kernel resolves this descriptor in this process's descriptor
		// table during the clone and creates the child directly inside that
		// cgroup. Nothing is handed to the child, and there is no window in
		// which the job runs outside its limits.
		attr.UseCgroupFD = true
		attr.CgroupFD = spec.CgroupFD
	}
	cmd.SysProcAttr = attr

	return cmd
}

// callerFault reports whether a failure from Process.Start is the caller's
// doing rather than the server's.
//
// Both arrive as one error from one call, so telling them apart is an errno
// check rather than a clean split: os/exec reports a failed PATH lookup as
// *exec.Error, and anything the kernel refused at fork or exec as *fs.PathError
// carrying the errno. ENOENT, EACCES and ENOEXEC mean the program named cannot
// be run. EINVAL means the argv could not be passed, which is how a null byte
// in an argument arrives, and E2BIG means the caller sent more of it than the
// kernel accepts. Everything else, EAGAIN and ENOMEM among them, is the server
// failing to do its part.
//
// The distinction matters because it decides what the client is told. A caller
// who misspelled a binary can fix that; a caller who hit a process limit on the
// server cannot, and the detail would only leak the server's condition.
func callerFault(err error) bool {
	var execErr *exec.Error
	if errors.As(err, &execErr) {
		return true
	}
	var pathErr *fs.PathError
	if errors.As(err, &pathErr) {
		return errors.Is(pathErr.Err, fs.ErrNotExist) ||
			errors.Is(pathErr.Err, fs.ErrPermission) ||
			errors.Is(pathErr.Err, syscall.ENOEXEC) ||
			errors.Is(pathErr.Err, syscall.EINVAL) ||
			errors.Is(pathErr.Err, syscall.E2BIG)
	}
	return false
}
