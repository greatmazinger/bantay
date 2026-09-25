package main

import (
	"os"
	"path/filepath"
	"testing"
)

// mustWrite creates a file, making its parent directories as needed.
func mustWrite(t *testing.T, path string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatalf("creating %s: %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte("x"), 0o600); err != nil {
		t.Fatalf("writing %s: %v", path, err)
	}
}

func entries(t *testing.T, dir string) []string {
	t.Helper()
	des, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("reading %s: %v", dir, err)
	}
	var names []string
	for _, de := range des {
		names = append(names, de.Name())
	}
	return names
}

func TestPrepareStateDir(t *testing.T) {
	t.Run("creates the directory when it does not exist", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "state")

		if err := prepareStateDir(dir); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		info, err := os.Stat(dir)
		if err != nil {
			t.Fatalf("the directory was not created: %v", err)
		}
		if !info.IsDir() {
			t.Fatal("the path exists but is not a directory")
		}
		// Job directories hold a job's working files and nothing else needs
		// to read them.
		if perm := info.Mode().Perm(); perm != 0o700 {
			t.Errorf("mode = %o, want 700", perm)
		}
	})

	t.Run("removes job directories a previous run left behind", func(t *testing.T) {
		dir := t.TempDir()
		// Non-empty, because a crashed server leaves whatever the job wrote.
		mustWrite(t, filepath.Join(dir, "job-1-3f2a", "output.log"))
		mustWrite(t, filepath.Join(dir, "job-2-3f2a", "nested", "deep.txt"))
		if err := os.Mkdir(filepath.Join(dir, "job-7-b91c"), 0o700); err != nil {
			t.Fatalf("creating an empty job directory: %v", err)
		}

		if err := prepareStateDir(dir); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got := entries(t, dir); len(got) != 0 {
			t.Errorf("state directory still holds %v, want it empty", got)
		}
	})

	// --state-dir is a flag. Pointing it at the wrong place should cost a
	// refusal to start, not the contents of that place.
	t.Run("leaves anything it did not create", func(t *testing.T) {
		dir := t.TempDir()
		mustWrite(t, filepath.Join(dir, "job-1-3f2a", "output.log")) // removed
		mustWrite(t, filepath.Join(dir, "important.txt"))            // kept
		mustWrite(t, filepath.Join(dir, "Documents", "taxes.pdf"))   // kept
		mustWrite(t, filepath.Join(dir, "job-0-3f2a", "x"))          // kept: counter starts at 1
		mustWrite(t, filepath.Join(dir, "jobs", "x"))                // kept

		if err := prepareStateDir(dir); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		got := entries(t, dir)
		want := map[string]bool{"Documents": true, "important.txt": true, "job-0-3f2a": true, "jobs": true}
		if len(got) != len(want) {
			t.Fatalf("kept %v, want %v", got, want)
		}
		for _, name := range got {
			if !want[name] {
				t.Errorf("%q should have been kept but is gone, or was created unexpectedly", name)
			}
		}
		// The one it should have taken.
		if _, err := os.Stat(filepath.Join(dir, "job-1-3f2a")); !os.IsNotExist(err) {
			t.Error("the job directory was not removed")
		}
	})

	t.Run("an empty directory is left alone", func(t *testing.T) {
		dir := t.TempDir()
		if err := prepareStateDir(dir); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got := entries(t, dir); len(got) != 0 {
			t.Errorf("state directory holds %v, want it empty", got)
		}
	})

	t.Run("a path that is a file is an error, not a panic", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "notadir")
		mustWrite(t, path)

		if err := prepareStateDir(path); err == nil {
			t.Error("expected an error for a state directory that is a file")
		}
	})
}
