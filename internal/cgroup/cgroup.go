// Package cgroup manages one cgroup v2 group per job.
//
// The server is expected to be running inside a cgroup subtree that has been
// delegated to its user, so nothing here requires privilege: creating a group,
// writing its limits, placing a process into it, and killing it are all done by
// an ordinary user who owns the subtree.
//
// A group is created before the process that will live in it exists. Its limits
// are therefore in force from the moment the process is created, rather than
// from some point shortly afterwards.
package cgroup

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// Limits are the resource controls applied to a group. An empty field is not
// written, which is what makes a zero Limits useful in tests.
type Limits struct {
	// CPUMax is written to cpu.max, as "quota period" in microseconds.
	CPUMax string
	// MemoryMax is written to memory.max, in bytes.
	MemoryMax string
	// MemorySwapMax is written to memory.swap.max, in bytes.
	//
	// Set it to "0" to make MemoryMax an allocation ceiling. memory.max on its
	// own caps resident memory, so on a host with swap a job that exceeds it is
	// paged out rather than killed, drawing on a pool the cgroup does not bound.
	// The file exists wherever the memory controller is enabled, which is the
	// same precondition memory.max has.
	MemorySwapMax string
	// IOMax is written to io.max, as "major:minor rbps=N wbps=N".
	IOMax string
	// PidsMax is written to pids.max, as a count.
	//
	// It bounds how many processes the job's cgroup may hold, which is the
	// only thing that stops a fork bomb: a process that spends its life
	// forking uses little memory and no CPU quota worth speaking of, so
	// neither of the other limits notices it, while the host's global pid
	// space is exhausted for everyone.
	PidsMax string
}

// Group is one job's cgroup. It holds an open descriptor for the group's
// directory, which is what the kernel is given at process creation so that the
// process is placed in the group as it is made.
type Group struct {
	path string
	dir  *os.File
}

// New creates a group named name beneath root and writes its limits.
//
// The descriptor is opened last, after the limits are in place, so that a
// caller holding it cannot create a process in a group that is not yet
// constrained.
func New(root, name string, l Limits) (*Group, error) {
	path := filepath.Join(root, name)
	if err := os.Mkdir(path, 0o755); err != nil {
		return nil, fmt.Errorf("cgroup: creating %s: %w", path, err)
	}

	for _, w := range []struct{ file, value string }{
		{"cpu.max", l.CPUMax},
		{"memory.max", l.MemoryMax},
		{"memory.swap.max", l.MemorySwapMax},
		{"io.max", l.IOMax},
		{"pids.max", l.PidsMax},
	} {
		if w.value == "" {
			continue
		}
		if err := os.WriteFile(filepath.Join(path, w.file), []byte(w.value), 0o644); err != nil {
			// Roll back rather than leave a job running under partial limits.
			_ = os.Remove(path)
			return nil, fmt.Errorf("cgroup: writing %s: %w", w.file, err)
		}
	}

	dir, err := os.Open(path)
	if err != nil {
		_ = os.Remove(path)
		return nil, fmt.Errorf("cgroup: opening %s: %w", path, err)
	}

	return &Group{path: path, dir: dir}, nil
}

// FD returns the descriptor for the group's directory, for use as
// SysProcAttr.CgroupFD. The descriptor belongs to this Group: the kernel reads
// it out of the caller's descriptor table during the clone, so the Group must
// outlive the call that creates the process.
func (g *Group) FD() int { return int(g.dir.Fd()) }

// Path returns the group's directory.
func (g *Group) Path() string { return g.path }

// Kill terminates every process in the group, atomically, including any that
// left their process group. It is the authoritative mechanism of termination: a
// process group is advisory and a program can leave one incidentally, whereas
// leaving a cgroup takes a deliberate write to a specific file.
//
// A group that no longer exists is not a failure. A group is removed once it is
// empty, so its absence already satisfies what Kill is asking for. Reporting an
// error would break Stop's idempotency, which the API contract promises: a
// client reissuing Stop on a job that has since been reaped would get an error
// rather than the documented success.
func (g *Group) Kill() error {
	err := os.WriteFile(filepath.Join(g.path, "cgroup.kill"), []byte("1"), 0o644)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("cgroup: killing %s: %w", g.path, err)
	}
	return nil
}

// destroyTimeout bounds how long Destroy waits for the group to empty. It is a
// variable so that tests exercising the timeout need not wait it out.
var destroyTimeout = 2 * time.Second

// Destroy closes the descriptor and removes the group.
//
// Removal fails while any process remains, and processes exit asynchronously:
// a wait returning tells you about one process, not about the descendants the
// kernel has been asked to kill. So this retries rather than assuming. A group
// that will not empty is reported, but it holds no resources once empty of
// processes, so a caller can reasonably log and continue.
func (g *Group) Destroy() error {
	if err := g.dir.Close(); err != nil {
		return fmt.Errorf("cgroup: closing %s: %w", g.path, err)
	}

	deadline := time.Now().Add(destroyTimeout)
	for {
		err := os.Remove(g.path)
		if err == nil || errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("cgroup: removing %s: %w", g.path, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
