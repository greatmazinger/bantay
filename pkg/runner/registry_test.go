package runner

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/greatmazinger/bantay/internal/idgen"
)

const (
	astrid = "astrid"
	bheng  = "bheng"
	admin  = "admin"

	JobID1 string = "job-astrid-1"
	JobID2 string = "job-bheng-2"
)

func TestMemoryRegistry_Add(t *testing.T) {
	t.Run("add happy path", func(t *testing.T) {
		reg := NewRegistry()
		job1 := Job{
			Owner: astrid,
			JobID: JobID1,
		}
		err := reg.Add(JobID1, &job1)
		if err != nil {
			t.Errorf("expected nil (succes), but got %v", err)
		}
	})

	t.Run("existing id", func(t *testing.T) {
		reg := NewRegistry()
		job2 := Job{
			Owner: bheng,
			JobID: JobID2,
		}
		// Add JobID2 once:
		err := reg.Add(JobID2, &job2)
		if err != nil {
			t.Errorf("expected nil (succes), but got %v", err)
		}
		// Add JobID2 again:
		err = reg.Add(JobID2, &job2)
		if !errors.Is(err, ErrJobExists) {
			t.Errorf("expected '%v' , but got '%v'", ErrJobExists, err)
		}
	})
}

func TestMemoryRegistry_Get(t *testing.T) {
	t.Run("get happy path", func(t *testing.T) {
		reg := NewRegistry()
		jobAstrid := Job{
			Owner: astrid,
			JobID: JobID1,
		}
		err := reg.Add(JobID1, &jobAstrid)
		if err != nil {
			t.Errorf("expected nil (succes), but got %v", err)
		}
		job, found := reg.Get(JobID1)
		if !found {
			t.Errorf("expected get miss, but got '%+v'", job)
		}
		if job.Owner != jobAstrid.Owner || job.JobID != jobAstrid.JobID {
			t.Errorf("expected owner %q id=%q, but got owner %q id=%q",
				jobAstrid.Owner, jobAstrid.JobID, job.Owner, job.JobID)
		}
	})

	t.Run("id that doesn't exist", func(t *testing.T) {
		reg := NewRegistry()
		jobAstrid := Job{
			Owner: astrid,
			JobID: JobID1,
		}
		err := reg.Add(JobID1, &jobAstrid)
		if err != nil {
			t.Errorf("expected nil (succes), but got %v", err)
		}
		job, found := reg.Get(JobID2)
		if found {
			t.Errorf("expected get miss, but got '%+v'", job)
		}
	})
}

func TestMemoryRegistry_Concurrency(t *testing.T) {
	t.Run("Concurrent Adds and Gets", func(t *testing.T) {
		reg := NewRegistry()
		gen, err := idgen.NewGenerator()
		if err != nil {
			t.Fatalf("unexpected error creating generator: %v", err)
		}
		var wg sync.WaitGroup

		const numIds = 10000

		// Each goroutine writes its own index, so no lock is needed here.
		ids := make([]string, numIds)
		for i := range numIds {
			wg.Go(func() {
				// Action tested for concurrency:
				id := gen.Generate()
				job := Job{
					Owner: fmt.Sprintf("owner-%d", i),
					JobID: id,
				}
				if err := reg.Add(id, &job); err != nil {
					t.Errorf("Add(%q): %v", id, err)
					return
				}
				ids[i] = id
			})
		}
		wg.Wait()

		// Read every job back. Without this the test passes even if Add
		// stores nothing.
		for i, id := range ids {
			job, found := reg.Get(id)
			if !found {
				t.Errorf("job %d (%q) missing after concurrent adds", i, id)
				continue
			}
			if want := fmt.Sprintf("owner-%d", i); job.Owner != want {
				t.Errorf("job %q: owner %q, want %q", id, job.Owner, want)
			}
		}
	})
}
