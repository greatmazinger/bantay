package idgen

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"regexp"
	"sync/atomic"
)

// Generator holds the state for creating unique job IDs
type Generator struct {
	counter atomic.Uint64
	suffix  string
}

// NewGenerator initializes a new ID generator.
// The random 2 byte suffix is drawn once at creationof the generator.
func NewGenerator() (*Generator, error) {
	b := make([]byte, 2)
	_, err := rand.Read(b)
	if err != nil {
		return nil, fmt.Errorf("failed to generate random job suffix: %w", err)
	}
	return &Generator{
		suffix: hex.EncodeToString(b),
	}, nil
}

func (g *Generator) Generate() string {
	n := g.counter.Add(1)
	return fmt.Sprintf("job-%d-%s", n, g.suffix)
}

// idPattern matches what Generate produces. It lives here so that the format
// is described in one place: a caller that has to recognise an ID, rather than
// merely hold one, would otherwise copy the shape and drift from it.
var idPattern = regexp.MustCompile(`^job-[1-9][0-9]*-[0-9a-f]{4}$`)

// IsID reports whether s has the shape Generate produces. It says nothing
// about whether such a job exists or ever did.
func IsID(s string) bool {
	return idPattern.MatchString(s)
}
