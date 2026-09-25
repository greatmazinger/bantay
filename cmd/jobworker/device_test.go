package main

import (
	"os"
	"path/filepath"
	"testing"
)

// fakeSysfs builds the shape /sys/dev/block actually has: numbered symlinks
// into a device tree. The partition walk depends on that indirection, because
// ".." has to be taken relative to the link's target, so a flat directory of
// real files would not exercise it.
//
//	<root>/devices/sda/dev          "8:0"
//	<root>/devices/sda/sda1/partition   "1"
//	<root>/dev/block/8:0 -> ../../devices/sda
//	<root>/dev/block/8:1 -> ../../devices/sda/sda1
func fakeSysfs(t *testing.T) string {
	t.Helper()
	root := t.TempDir()

	disk := filepath.Join(root, "devices", "sda")
	part := filepath.Join(disk, "sda1")
	if err := os.MkdirAll(part, 0o755); err != nil {
		t.Fatalf("creating the device tree: %v", err)
	}
	if err := os.WriteFile(filepath.Join(disk, "dev"), []byte("8:0\n"), 0o644); err != nil {
		t.Fatalf("writing the disk's dev: %v", err)
	}
	// Only partitions have this file. Its contents are the partition number,
	// which nothing reads: existence is the signal.
	if err := os.WriteFile(filepath.Join(part, "partition"), []byte("1\n"), 0o644); err != nil {
		t.Fatalf("writing the partition marker: %v", err)
	}

	block := filepath.Join(root, "dev", "block")
	if err := os.MkdirAll(block, 0o755); err != nil {
		t.Fatalf("creating the block directory: %v", err)
	}
	for name, target := range map[string]string{
		"8:0": filepath.Join("..", "..", "devices", "sda"),
		"8:1": filepath.Join("..", "..", "devices", "sda", "sda1"),
	} {
		if err := os.Symlink(target, filepath.Join(block, name)); err != nil {
			t.Fatalf("linking %s: %v", name, err)
		}
	}
	return block
}

func TestDiskFor(t *testing.T) {
	block := fakeSysfs(t)

	t.Run("a partition resolves to its disk", func(t *testing.T) {
		major, minor, err := diskFor(block, 8, 1)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if major != 8 || minor != 0 {
			t.Errorf("got %d:%d, want 8:0", major, minor)
		}
	})

	t.Run("a whole disk is returned unchanged", func(t *testing.T) {
		major, minor, err := diskFor(block, 8, 0)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if major != 8 || minor != 0 {
			t.Errorf("got %d:%d, want 8:0", major, minor)
		}
	})

	// A device sysfs does not know about is not a partition, so it is treated
	// as a whole disk rather than failing. The probe in main is what catches a
	// number the io controller will not accept.
	t.Run("an unknown device is treated as a whole disk", func(t *testing.T) {
		major, minor, err := diskFor(block, 254, 7)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if major != 254 || minor != 7 {
			t.Errorf("got %d:%d, want 254:7", major, minor)
		}
	})
}

func TestParseDevPair(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		major, minor, err := parseDevPair("259:3")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if major != 259 || minor != 3 {
			t.Errorf("got %d:%d, want 259:3", major, minor)
		}
	})

	for _, s := range []string{"", "8", "8:", ":0", "8:x", "x:0", "8:0:1"} {
		t.Run("rejects "+s, func(t *testing.T) {
			if _, _, err := parseDevPair(s); err == nil {
				t.Errorf("parseDevPair(%q) should have failed", s)
			}
		})
	}
}

// resolveIODevice needs a real filesystem, so this only checks that it produces
// the major:minor shape for a directory that certainly exists. Whether the
// number is throttleable is a property of the host and is verified by hand.
func TestResolveIODevice(t *testing.T) {
	got, err := resolveIODevice(sysfsBlock, t.TempDir())
	if err != nil {
		t.Skipf("cannot resolve a device on this host: %v", err)
	}
	if _, _, err := parseDevPair(got); err != nil {
		t.Errorf("resolveIODevice returned %q: %v", got, err)
	}
	t.Logf("state directory is backed by device %s", got)
}
