package runner

import (
	"errors"
	"sync"
)

var ErrJobExists = errors.New("jobID already exists in registry")

// Registry holds the jobs a manager has started. It is in memory only: jobs do
// not survive a restart, which the design scopes out deliberately.
type Registry struct {
	mu   sync.Mutex
	jobs map[string]*Job
}

func NewRegistry() *Registry {
	return &Registry{
		jobs: make(map[string]*Job),
	}
}

// Add atomically stores jobID -> job in the registry
func (r *Registry) Add(jobID string, job *Job) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.jobs[jobID]; exists {
		return ErrJobExists
	}

	r.jobs[jobID] = job
	return nil
}

// Get safely retrieves a job for 'jobID' from the registry
func (r *Registry) Get(jobID string) (*Job, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	job, exists := r.jobs[jobID]
	return job, exists
}
