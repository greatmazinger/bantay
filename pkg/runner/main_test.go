package runner

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain fails the package if any test leaves a goroutine running.
//
// The design document calls a blocked reader that nothing can release "the
// failure this design most needs to rule out". synctest bubbles catch it for
// the tests that run inside one, but most of this package's tests do not, and
// every Start launches a goroutine that waits on the process. goleak covers
// those: it retries with a backoff before reporting, so it does not need the
// sleep a hand-rolled goroutine count would.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
