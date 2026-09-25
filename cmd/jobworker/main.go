// Command jobworker serves the job manager over gRPC with mutual TLS.
//
// It is the only place that decides policy: which limits every job runs under,
// which identities are administrators, and where the cgroup subtree and state
// directory live. The library beneath it takes those as configuration and has
// no defaults of its own.
package main

import (
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/greatmazinger/bantay/internal/authz"
	"github.com/greatmazinger/bantay/internal/cgroup"
	"github.com/greatmazinger/bantay/internal/idgen"
	"github.com/greatmazinger/bantay/internal/server"
	"github.com/greatmazinger/bantay/internal/tlsconfig"
	"github.com/greatmazinger/bantay/pkg/runner"
	jobmanagerv1 "github.com/greatmazinger/bantay/proto/jobmanager/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// The limits every job runs under. Hardcoded deliberately: per-job limits are
// out of scope, and the design document records the reasoning for each value.
//
// cpu.max is "quota period" in microseconds, so half of one core. memory.max is
// in bytes. memory.swap.max is zero so that memory.max is an allocation ceiling
// rather than a resident-set cap, which on a host with swap would page a job out
// instead of killing it. io.max gets the device prefix at startup.
//
// pids.max bounds the processes a job may hold. It is the only limit that stops
// a fork bomb: forking costs almost no memory and no meaningful CPU quota, so
// the other three do not notice one, while the host's global pid space is
// exhausted for every process on the machine. 128 is generous for a build or a
// test run and far below anything that threatens the host.
const (
	cpuMax        = "50000 100000"
	memoryMax     = "268435456" // 256 MiB
	memorySwapMax = "0"
	ioBandwidth   = "rbps=10485760 wbps=10485760" // 10 MiB/s each way
	pidsMax       = "128"
)

// requiredControllers must appear in the parent cgroup's subtree_control. A
// controller compiled into the kernel but not delegated down the hierarchy
// leaves no file to write, so a job would run unconstrained in exactly the
// dimension the operator thought they had bounded.
var requiredControllers = []string{"cpu", "memory", "io", "pids"}

type config struct {
	listen      string
	cert        string
	key         string
	ca          string
	cgroupRoot  string
	stateDir    string
	adminPrefix string
	ioDevice    string
}

func main() {
	var c config
	flag.StringVar(&c.listen, "listen", ":8443", "address to listen on")
	flag.StringVar(&c.cert, "cert", "certs/server.crt", "server certificate")
	flag.StringVar(&c.key, "key", "certs/server.key", "server private key")
	flag.StringVar(&c.ca, "ca", "certs/ca.crt", "certificate authority for both peers")
	flag.StringVar(&c.cgroupRoot, "cgroup-root", "", "delegated cgroup v2 subtree for per-job cgroups")
	flag.StringVar(&c.stateDir, "state-dir", "state", "directory for per-job working directories, empty to disable")
	flag.StringVar(&c.adminPrefix, "admin-prefix", "", "SPIFFE path prefix granting the administrative role")
	flag.StringVar(&c.ioDevice, "io-device", "", "major:minor for io.max, skipping device resolution")
	flag.Parse()

	if err := run(c); err != nil {
		slog.Error("jobworker failed to start", "err", err)
		os.Exit(1)
	}
}

func run(c config) error {
	limits, err := resolveLimits(c)
	if err != nil {
		return err
	}

	// Before the manager exists, so no job directory can be created and then
	// swept away by the cleanup meant for the previous run.
	if c.stateDir != "" {
		if err := prepareStateDir(c.stateDir); err != nil {
			return err
		}
	}

	tlsCfg, err := tlsconfig.Server(tlsconfig.Files{Cert: c.cert, Key: c.key, CA: c.ca})
	if err != nil {
		return fmt.Errorf("tls: %w", err)
	}

	gen, err := idgen.NewGenerator()
	if err != nil {
		return fmt.Errorf("idgen: %w", err)
	}

	mgr := runner.NewManager(
		runner.NewRegistry(),
		authz.NewPolicy(c.adminPrefix),
		gen,
		runner.NewOSProcess,
		runner.Config{CgroupRoot: c.cgroupRoot, StateDir: c.stateDir, Limits: limits},
	)

	// Recovery is listed first, which makes it outermost, so it also covers a
	// panic inside authentication rather than only inside a handler.
	srv := grpc.NewServer(
		grpc.Creds(credentials.NewTLS(tlsCfg)),
		grpc.ChainUnaryInterceptor(server.UnaryRecoveryInterceptor, server.UnaryAuthInterceptor),
		grpc.ChainStreamInterceptor(server.StreamRecoveryInterceptor, server.StreamAuthInterceptor),
	)
	jobmanagerv1.RegisterJobServiceServer(srv, server.New(mgr))

	lis, err := net.Listen("tcp", c.listen)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", c.listen, err)
	}

	// SIGINT and SIGTERM stop accepting new connections and let in-flight RPCs
	// finish. Running jobs are unaffected: they live in their own cgroups and
	// process groups, and nothing here reaps them on the way out.
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-stop
		slog.Info("shutting down", "signal", sig.String())
		srv.GracefulStop()
	}()

	slog.Info("serving",
		"address", lis.Addr().String(),
		"cgroupRoot", orNone(c.cgroupRoot),
		"stateDir", orNone(c.stateDir),
		"adminPrefix", orNone(c.adminPrefix))

	if err := srv.Serve(lis); err != nil {
		return fmt.Errorf("serve: %w", err)
	}
	return nil
}

// resolveLimits builds the per-job limits, checking the two preconditions the
// io limit depends on: the controllers are delegated, and the device the state
// directory sits on can actually be throttled.
//
// With no cgroup root the server runs without isolation, which is how the
// in-process tools and tests run, so there is nothing to check and no limit to
// apply.
func resolveLimits(c config) (cgroup.Limits, error) {
	if c.cgroupRoot == "" {
		slog.Warn("no cgroup root given: jobs will run without resource limits")
		return cgroup.Limits{}, nil
	}

	if err := checkControllers(c.cgroupRoot); err != nil {
		return cgroup.Limits{}, err
	}

	limits := cgroup.Limits{
		CPUMax:        cpuMax,
		MemoryMax:     memoryMax,
		MemorySwapMax: memorySwapMax,
		PidsMax:       pidsMax,
	}

	device := c.ioDevice
	if device == "" {
		// The state directory is the thing jobs write to, so its backing disk
		// is the one worth throttling. Without one there is nothing to resolve.
		if c.stateDir == "" {
			slog.Warn("no state directory given: io.max will not be set")
			return limits, nil
		}
		resolved, err := resolveIODevice(sysfsBlock, c.stateDir)
		if err != nil {
			return cgroup.Limits{}, fmt.Errorf("resolving the io device: %w", err)
		}
		device = resolved
		slog.Info("resolved the io device", "device", device, "from", c.stateDir)
	}

	limits.IOMax = device + " " + ioBandwidth
	if err := probeIOMax(c.cgroupRoot, limits.IOMax); err != nil {
		return cgroup.Limits{}, err
	}
	return limits, nil
}

// checkControllers requires every controller this server writes to be enabled
// in the parent's subtree_control.
func checkControllers(root string) error {
	path := filepath.Join(root, "cgroup.subtree_control")
	raw, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("reading %s: %w", path, err)
	}
	enabled := make(map[string]bool)
	for _, name := range strings.Fields(string(raw)) {
		enabled[name] = true
	}
	var missing []string
	for _, name := range requiredControllers {
		if !enabled[name] {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("%s does not delegate %s: enable them with 'echo +%s > %s'",
			path, strings.Join(missing, ", "), strings.Join(missing, " +"), path)
	}
	return nil
}

// probeIOMax writes the io limit to a throwaway cgroup and removes it again.
//
// Device mapper, loop and some virtio configurations reject an io.max write for
// a device that has no request queue to throttle, and the kernel reports that as
// an error on the write rather than by ignoring it. Failing here names the
// device; failing per job would mean every Start returning INTERNAL with no clue
// why.
func probeIOMax(root, ioMax string) error {
	probe, err := cgroup.New(root, "io-probe", cgroup.Limits{IOMax: ioMax})
	if err != nil {
		return fmt.Errorf("the io limit %q was rejected, pass --io-device to override: %w", ioMax, err)
	}

	// The write is what was being tested, and it succeeded. Removing the probe
	// is tidiness, so a failure there is logged rather than fatal: on a real
	// cgroup filesystem the control files are synthetic and the directory goes
	// away with them, but against an ordinary directory, which is how this gets
	// exercised without a delegated subtree, the file just written keeps it
	// from being empty.
	if err := probe.Destroy(); err != nil {
		slog.Warn("could not remove the probe cgroup", slog.String("err", err.Error()))
	}
	return nil
}

func orNone(s string) string {
	if s == "" {
		return "(none)"
	}
	return s
}
