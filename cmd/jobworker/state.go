package main

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/greatmazinger/bantay/internal/idgen"
)

// prepareStateDir makes sure the state directory exists and clears what a
// previous run left in it.
//
// Job directories are removed when their job is reaped, so anything still
// present belongs to a server that died without reaping. Nothing in it is
// live: the registry is in memory and did not survive either, so those jobs
// are unreachable by any client.
//
// Only job-shaped names are removed. The server creates nothing else here, and
// --state-dir is a flag: pointing it somewhere unintended should cost a
// refusal to start, not the contents of a home directory. Anything else found
// is left alone and logged, since it means the directory is not what the
// operator thinks it is.
func prepareStateDir(dir string) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("creating %s: %w", dir, err)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("reading %s: %w", dir, err)
	}

	var removed, foreign int
	for _, e := range entries {
		if !idgen.IsID(e.Name()) {
			foreign++
			slog.Warn("leaving an unrecognised entry in the state directory",
				slog.String("dir", dir), slog.String("entry", e.Name()))
			continue
		}
		if err := os.RemoveAll(filepath.Join(dir, e.Name())); err != nil {
			return fmt.Errorf("removing %s from %s: %w", e.Name(), dir, err)
		}
		removed++
	}

	if removed > 0 {
		slog.Info("cleared job directories left by a previous run",
			slog.String("dir", dir), slog.Int("removed", removed))
	}
	if foreign > 0 {
		slog.Warn("the state directory holds entries this server did not create",
			slog.String("dir", dir), slog.Int("count", foreign))
	}
	return nil
}
