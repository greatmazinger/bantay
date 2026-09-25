package idgen

import (
	"sync"
	"testing"
)

func TestGenerator_SimpleSequence(t *testing.T) {
	gen, err := NewGenerator()
	if err != nil {
		t.Fatalf("unexpected error creating generator: %v", err)
	}

	// This check is retained so that the case where empty suffixes are used will be marked
	// as an error. This is to be a simple guardrail against job ID reuse across server runs.
	if gen.suffix == "" {
		t.Fatal("generator has an empty suffix")
	}

	id1 := gen.Generate()
	id2 := gen.Generate()
	if want1 := "job-1-" + gen.suffix; id1 != want1 {
		t.Errorf("expected job %q, got ID %q", want1, id1)
	}
	if want2 := "job-2-" + gen.suffix; id2 != want2 {
		t.Errorf("expected job %q, got ID %q", want2, id2)
	}
}

func TestGenerator_ConcurrentIdGen(t *testing.T) {
	gen, err := NewGenerator()
	if err != nil {
		t.Fatalf("unexpected error creating generator: %v", err)
	}

	const numIds = 10000

	var wg sync.WaitGroup
	var mu sync.Mutex

	seenIDs := make(map[string]bool)

	for range numIds {
		wg.Go(func() {
			// Action tested for concurrency:
			id := gen.Generate()

			// Need to atomically check and save our new id:
			mu.Lock()
			defer mu.Unlock()
			if seenIDs[id] {
				t.Errorf("duplicate ID found: %s", id)
			}
			seenIDs[id] = true
		})
	}

	// Wait here for all running goroutines:
	wg.Wait()

	if len(seenIDs) != numIds {
		t.Errorf("expected %d unique IDs, got %d", numIds, len(seenIDs))
	}
}

func TestIsID(t *testing.T) {
	gen, err := NewGenerator()
	if err != nil {
		t.Fatalf("unexpected error creating generator: %v", err)
	}

	// Whatever Generate produces must be recognised, or the two have drifted.
	for range 3 {
		if id := gen.Generate(); !IsID(id) {
			t.Errorf("IsID(%q) = false for an ID Generate just produced", id)
		}
	}

	for _, s := range []string{
		"",
		"job",
		"job-1",
		"job-1-",
		"job--3f2a",
		"job-0-3f2a",  // the counter starts at 1
		"job-1-3f2",   // suffix too short
		"job-1-3f2ab", // suffix too long
		"job-1-3F2A",  // hex is written lowercase
		"job-1-zzzz",  // not hex
		"job-x-3f2a",  // counter not a number
		"../../etc/passwd",
		"job-1-3f2a/../..",
		"notajob",
	} {
		if IsID(s) {
			t.Errorf("IsID(%q) = true, want false", s)
		}
	}
}
