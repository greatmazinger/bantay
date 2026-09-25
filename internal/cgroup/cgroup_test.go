package cgroup

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// These tests run against an ordinary directory rather than a real cgroup
// hierarchy. This verifies that this package writes the values we expect it to
// write, to the appropriate files. We don't test kernel-level enforcement
// here, as that is slow, possibly flaky, and hard to debug. Enforcement is
// verified by manually using the delegated launcher instead.

func TestNew_WritesLimits(t *testing.T) {
	root := t.TempDir()
	limits := Limits{
		CPUMax:        "50000 100000",
		MemoryMax:     "268435456",
		MemorySwapMax: "0",
		PidsMax:       "128",
		IOMax:         "8:0 rbps=10485760 wbps=10485760",
	}

	g, err := New(root, "job-1", limits)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { removeGroup(t, g) })

	for _, want := range []struct{ file, value string }{
		{"cpu.max", limits.CPUMax},
		{"memory.max", limits.MemoryMax},
		{"memory.swap.max", limits.MemorySwapMax},
		{"pids.max", limits.PidsMax},
		{"io.max", limits.IOMax},
	} {
		got, err := os.ReadFile(filepath.Join(g.Path(), want.file))
		if err != nil {
			t.Errorf("reading %s: %v", want.file, err)
			continue
		}
		if string(got) != want.value {
			t.Errorf("%s = %q, want %q", want.file, got, want.value)
		}
	}
}

func TestNew_SkipsEmptyLimits(t *testing.T) {
	root := t.TempDir()

	g, err := New(root, "job-1", Limits{MemoryMax: "1024"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { removeGroup(t, g) })

	if _, err := os.Stat(filepath.Join(g.Path(), "memory.max")); err != nil {
		t.Errorf("memory.max should have been written: %v", err)
	}
	// An unset limit must not be written at all. Writing an empty value to a
	// real cgroup file is an error, not a no-op.
	for _, file := range []string{"cpu.max", "io.max"} {
		if _, err := os.Stat(filepath.Join(g.Path(), file)); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("%s should not have been written, got %v", file, err)
		}
	}
}

// TestNew_LeavesNothingBehindWhenItCannotCreate covers the first failure path:
// the group directory cannot be made at all.
//
// The second path, where the directory is made but a limit write fails, cannot
// be reached against a temp directory, because WriteFile creates the file
// rather than failing. In a real hierarchy it is reached when a controller is
// not enabled in the parent's subtree_control, so cpu.max simply does not
// exist. That path is verified by hand under the delegated launcher.
func TestNew_LeavesNothingBehindWhenItCannotCreate(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "job-1")

	if err := os.Chmod(root, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(root, 0o700) })

	if _, err := New(root, "job-1", Limits{MemoryMax: "1024"}); err == nil {
		t.Fatal("New should have failed when the root is not writable")
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("a failed New must leave nothing behind, got %v", err)
	}
}

func TestNew_RefusesDuplicate(t *testing.T) {
	root := t.TempDir()

	g, err := New(root, "job-1", Limits{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { removeGroup(t, g) })

	// Two jobs must never share a cgroup: the second would inherit the first's
	// limits and be caught by its termination.
	if _, err := New(root, "job-1", Limits{}); err == nil {
		t.Error("New should refuse to reuse an existing group")
	}
}

func TestKill_WritesCgroupKill(t *testing.T) {
	root := t.TempDir()

	g, err := New(root, "job-1", Limits{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { removeGroup(t, g) })

	if err := g.Kill(); err != nil {
		t.Fatalf("Kill: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(g.Path(), "cgroup.kill"))
	if err != nil {
		t.Fatalf("reading cgroup.kill: %v", err)
	}
	if string(got) != "1" {
		t.Errorf("cgroup.kill = %q, want %q", got, "1")
	}
}

func TestDestroy_RemovesTheGroup(t *testing.T) {
	root := t.TempDir()

	g, err := New(root, "job-1", Limits{MemoryMax: "1024"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	path := g.Path()

	clearFiles(t, path)

	if err := g.Destroy(); err != nil {
		t.Fatalf("Destroy: %v", err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("group should be gone, got %v", err)
	}
}

func TestDestroy_ReportsAGroupThatWillNotEmpty(t *testing.T) {
	root := t.TempDir()

	g, err := New(root, "job-1", Limits{MemoryMax: "1024"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(g.Path()) })

	// memory.max is still present, so the directory is not empty and rmdir
	// fails. In a real hierarchy this stands in for a group that still holds a
	// process. Destroy should give up after its deadline rather than block.
	defer func(d time.Duration) { destroyTimeout = d }(destroyTimeout)
	destroyTimeout = 20 * time.Millisecond

	err = g.Destroy()
	if err == nil {
		t.Fatal("Destroy should report a group it could not remove")
	}
	if !strings.Contains(err.Error(), g.Path()) {
		t.Errorf("error should name the group, got: %v", err)
	}
}

// clearFiles removes a group's interface files. In a real hierarchy the kernel
// removes them along with the directory; against a temp directory they are
// ordinary files, so rmdir would never succeed while they exist.
func clearFiles(t *testing.T, path string) {
	t.Helper()
	entries, err := os.ReadDir(path)
	if err != nil {
		return
	}
	for _, e := range entries {
		if err := os.Remove(filepath.Join(path, e.Name())); err != nil {
			t.Errorf("clearing %s: %v", e.Name(), err)
		}
	}
}

// removeGroup is the cleanup counterpart to New.
func removeGroup(t *testing.T, g *Group) {
	t.Helper()
	clearFiles(t, g.Path())
	if err := g.Destroy(); err != nil {
		t.Errorf("Destroy: %v", err)
	}
}

func TestKill_ToleratesAGroupThatIsAlreadyGone(t *testing.T) {
	root := t.TempDir()

	g, err := New(root, "job-1", Limits{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	removeGroup(t, g)

	// A group that no longer exists holds no processes, which is the
	// postcondition Kill asks for. Reporting an error here would break the
	// idempotency the API contract promises for Stop.
	if err := g.Kill(); err != nil {
		t.Errorf("Kill on a group that is already gone: %v", err)
	}
}
