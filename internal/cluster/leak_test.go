package cluster

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain fails the package if any test leaks a goroutine. Servers,
// pools, and fan-outs must shut down cleanly; background ticks that outlive
// a test are bugs, not noise. Run the suite with -count=1; flakes here
// mean a real lifecycle race.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
