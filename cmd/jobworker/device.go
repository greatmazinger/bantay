package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"
)

// sysfsBlock is where the kernel exposes block devices by number. It is a
// parameter of diskFor rather than a constant so the partition walk can be
// tested against a fake tree.
const sysfsBlock = "/sys/dev/block"

// resolveIODevice returns the "major:minor" pair io.max is keyed by for the
// disk backing path.
//
// io.max applies per device, and those numbers vary by machine: a SATA disk, an
// NVMe namespace and a virtio disk all carry different majors, and virtio's is
// allocated dynamically. So the value cannot be a constant and is resolved at
// startup instead.
func resolveIODevice(sysfsRoot, path string) (string, error) {
	var st syscall.Stat_t
	if err := syscall.Stat(path, &st); err != nil {
		return "", fmt.Errorf("stat %s: %w", path, err)
	}
	// Dev, not Rdev: Dev is the device holding this file, which is what we
	// want. Rdev is only meaningful for device nodes such as /dev/sda.
	dev := uint64(st.Dev)
	major, minor, err := diskFor(sysfsRoot, unix.Major(dev), unix.Minor(dev))
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%d:%d", major, minor), nil
}

// diskFor maps a device number to the whole disk containing it.
//
// st_dev usually names a partition, and the io controller throttles at the
// disk's request queue, which a partition does not have. A partition's numbers
// written to io.max are accepted and enforce nothing, so the silent failure is
// worth two file operations to avoid.
//
// Sysfs answers both questions: a "partition" file exists only for partitions,
// and the parent directory's "dev" holds the disk's pair.
func diskFor(sysfsRoot string, major, minor uint32) (uint32, uint32, error) {
	dir := filepath.Join(sysfsRoot, fmt.Sprintf("%d:%d", major, minor))
	if _, err := os.Stat(filepath.Join(dir, "partition")); err != nil {
		if os.IsNotExist(err) {
			// Already a whole disk.
			return major, minor, nil
		}
		return 0, 0, fmt.Errorf("checking %s: %w", dir, err)
	}

	// Resolve the symlink first: the entries under /sys/dev/block are links
	// into /sys/devices, and ".." has to be taken relative to the target for
	// the parent to be the disk rather than /sys/dev/block itself.
	target, err := filepath.EvalSymlinks(dir)
	if err != nil {
		return 0, 0, fmt.Errorf("resolving %s: %w", dir, err)
	}

	raw, err := os.ReadFile(filepath.Join(target, "..", "dev"))
	if err != nil {
		return 0, 0, fmt.Errorf("reading the parent device of %d:%d: %w", major, minor, err)
	}
	return parseDevPair(strings.TrimSpace(string(raw)))
}

// parseDevPair reads the "major:minor" form sysfs writes into a dev file.
func parseDevPair(s string) (uint32, uint32, error) {
	majStr, minStr, ok := strings.Cut(s, ":")
	if !ok {
		return 0, 0, fmt.Errorf("device pair %q is not major:minor", s)
	}
	major, err := strconv.ParseUint(majStr, 10, 32)
	if err != nil {
		return 0, 0, fmt.Errorf("device pair %q: %w", s, err)
	}
	minor, err := strconv.ParseUint(minStr, 10, 32)
	if err != nil {
		return 0, 0, fmt.Errorf("device pair %q: %w", s, err)
	}
	return uint32(major), uint32(minor), nil
}
