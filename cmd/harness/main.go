package main

import (
	"log"
	"time"

	"github.com/greatmazinger/bantay/internal/authz"
	"github.com/greatmazinger/bantay/internal/idgen"
	"github.com/greatmazinger/bantay/pkg/runner"
)

func main() {
	cfg := Config{
		Command:          "bash",
		Args:             []string{"-c", `for i in $(seq 1 20); do echo "burst $i $(date +%T.%N)"; sleep 0.1; done; echo DONE-MARKER`},
		EarlyReaders:     500,
		DelayedReaders:   500,
		LateReaders:      500,
		AbandonedReaders: 100,
		StaggerMax:       500 * time.Millisecond,
		// The job runs for about two seconds, so this lands mid-stream.
		AbandonAfter: 700 * time.Millisecond,
		MustEndWith:  "DONE-MARKER\n",
	}

	reg := runner.NewRegistry()
	gen, _ := idgen.NewGenerator()
	// No admin role here: this binary runs the manager in-process, so
	// identities are plain strings rather than certificate subjects.
	policy := authz.NewPolicy("")
	mgr := runner.NewManager(reg, policy, gen, runner.NewOSProcess,
		// No cgroup or working-directory isolation: this binary runs the
		// manager in-process, outside any delegated cgroup subtree.
		runner.Config{})
	if err := runHarness(mgr, cfg); err != nil {
		log.Fatalf("harness error %v", err)
	}
}
