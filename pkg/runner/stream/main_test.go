package stream

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain fails the package if any test leaves a goroutine running. A reader
// parked with nothing to release it is the failure this package most needs to
// rule out, and the tests that are not inside a synctest bubble have no other
// check for it.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
