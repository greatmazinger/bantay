// Command jobctl is the client for the job worker API.
//
// One subcommand per RPC, under the same name, so anything the API can do the
// CLI can do. It holds no job logic: every subcommand is a single call whose
// result is printed and whose error becomes the exit status.
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	jobmanagerv1 "github.com/greatmazinger/bantay/proto/jobmanager/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

// stopDeadline bounds how long stop waits. The server's own wait carries a
// five-second delay for the case where a descendant escaped the cgroup and
// holds the output pipe, so thirty sits comfortably above it: a client never
// abandons a job the server would have resolved on its own. What it does bound
// is the case nothing can fix, a child in uninterruptible sleep that cannot be
// reaped however often it is killed.
const stopDeadline = 30 * time.Second

const usage = `jobctl controls jobs on a job worker server.

usage:
  jobctl [flags] start -- COMMAND [ARGS...]
  jobctl [flags] status JOB_ID
  jobctl [flags] output JOB_ID
  jobctl [flags] stop JOB_ID

flags:
  --identity NAME    certificate to present, without extension (default $JOBCTL_IDENTITY)
  --certs-dir DIR    directory holding NAME.crt, NAME.key and ca.crt (default "certs")
  --server ADDR      server address (default "localhost:8443")

The -- before a command is required, so that a job's own flags are never
mistaken for jobctl's:
  jobctl start -- ls -l /tmp
`

type options struct {
	identity string
	certsDir string
	server   string
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "jobctl:", err)
		os.Exit(1)
	}
}

// run returns an error for anything that should exit non-zero. It never adopts
// the job's own exit code: that would make a successful query of a failed job
// indistinguishable from a failed query. The job's status is what reports it.
func run(argv []string) error {
	opts := options{
		identity: os.Getenv("JOBCTL_IDENTITY"),
		certsDir: "certs",
		server:   "localhost:8443",
	}

	// Hand-rolled rather than the flag package, because everything after the
	// subcommand belongs to the job, and flag would try to interpret it.
	args, err := parseGlobalFlags(argv, &opts)
	if err != nil {
		return err
	}
	if len(args) == 0 {
		fmt.Fprint(os.Stderr, usage)
		return errors.New("no subcommand given")
	}

	cmd, rest := args[0], args[1:]

	// Help needs no credentials and no server, so it is answered before either
	// is required.
	switch cmd {
	case "help", "-h", "--help":
		fmt.Print(usage)
		return nil
	case "start", "status", "output", "stop":
		// Handled below, once there is a connection.
	default:
		fmt.Fprint(os.Stderr, usage)
		return fmt.Errorf("unknown subcommand %q", cmd)
	}

	if opts.identity == "" {
		return errors.New("no identity: pass --identity or set JOBCTL_IDENTITY")
	}

	// Ctrl-C cancels the RPC. For output that ends the stream and leaves the
	// job running: the client is a viewer, not an owner, and detaching is not
	// the same as stopping.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	client, closeConn, err := dial(opts)
	if err != nil {
		return err
	}
	defer closeConn()

	switch cmd {
	case "start":
		return startJob(ctx, client, rest)
	case "status":
		return statusJob(ctx, client, rest)
	case "output":
		return outputJob(ctx, client, rest)
	case "stop":
		return stopJob(ctx, client, rest)
	}
	// Unreachable: the switch above admits nothing else.
	return fmt.Errorf("unhandled subcommand %q", cmd)
}

// parseGlobalFlags consumes the flags that precede the subcommand and returns
// what is left. It stops at the first argument that is not one of them, so a
// job's flags are never seen.
func parseGlobalFlags(argv []string, opts *options) ([]string, error) {
	for len(argv) > 0 {
		var target *string
		switch argv[0] {
		case "--identity":
			target = &opts.identity
		case "--certs-dir":
			target = &opts.certsDir
		case "--server":
			target = &opts.server
		default:
			return argv, nil
		}
		if len(argv) < 2 {
			return nil, fmt.Errorf("%s needs a value", argv[0])
		}
		*target = argv[1]
		argv = argv[2:]
	}
	return argv, nil
}

// dial builds a mutually authenticated connection. The identity selects which
// certificate to present; it asserts nothing. The server derives the caller
// from the SPIFFE ID inside that certificate, so renaming a file changes
// nothing.
func dial(opts options) (jobmanagerv1.JobServiceClient, func(), error) {
	pair, err := tls.LoadX509KeyPair(
		filepath.Join(opts.certsDir, opts.identity+".crt"),
		filepath.Join(opts.certsDir, opts.identity+".key"),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("loading the %q certificate: %w", opts.identity, err)
	}
	caPEM, err := os.ReadFile(filepath.Join(opts.certsDir, "ca.crt"))
	if err != nil {
		return nil, nil, fmt.Errorf("reading the CA certificate: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, nil, fmt.Errorf("no certificate found in %s", filepath.Join(opts.certsDir, "ca.crt"))
	}

	conn, err := grpc.NewClient(opts.server, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{pair},
		RootCAs:      pool,
	})))
	if err != nil {
		return nil, nil, fmt.Errorf("connecting to %s: %w", opts.server, err)
	}
	return jobmanagerv1.NewJobServiceClient(conn), func() { _ = conn.Close() }, nil
}

// startJob prints the job ID and nothing else, so that it composes:
//
//	jobctl output "$(jobctl start -- ./build.sh)"
func startJob(ctx context.Context, c jobmanagerv1.JobServiceClient, args []string) error {
	// Requiring -- rather than guessing is the safer behaviour: without it a
	// job's flags and jobctl's own are indistinguishable.
	if len(args) == 0 || args[0] != "--" {
		return errors.New("usage: jobctl start -- COMMAND [ARGS...]")
	}
	args = args[1:]
	if len(args) == 0 {
		return errors.New("no command given after --")
	}

	resp, err := c.Start(ctx, &jobmanagerv1.StartRequest{Command: args[0], Args: args[1:]})
	if err != nil {
		return rpcErr("start", err)
	}
	fmt.Println(resp.JobId)
	return nil
}

func statusJob(ctx context.Context, c jobmanagerv1.JobServiceClient, args []string) error {
	id, err := oneJobID("status", args)
	if err != nil {
		return err
	}
	resp, err := c.Status(ctx, &jobmanagerv1.StatusRequest{JobId: id})
	if err != nil {
		return rpcErr("status", err)
	}
	// exit is absent while the job runs, so it is printed only once there is
	// something to print rather than as a misleading zero.
	if resp.ExitCode == nil {
		fmt.Printf("state=%s\n", shortState(resp.State))
		return nil
	}
	fmt.Printf("state=%s  exit=%d\n", shortState(resp.State), resp.GetExitCode())
	return nil
}

// outputJob copies the stream to stdout byte for byte, with no framing,
// prefixes or line buffering, because a job's output may be binary.
func outputJob(ctx context.Context, c jobmanagerv1.JobServiceClient, args []string) error {
	id, err := oneJobID("output", args)
	if err != nil {
		return err
	}
	stream, err := c.Output(ctx, &jobmanagerv1.OutputRequest{JobId: id})
	if err != nil {
		return rpcErr("output", err)
	}
	for {
		chunk, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			// Ctrl-C is a clean detach, not a failure.
			if ctx.Err() != nil {
				return nil
			}
			return rpcErr("output", err)
		}
		if _, err := os.Stdout.Write(chunk.Data); err != nil {
			return fmt.Errorf("writing to stdout: %w", err)
		}
	}
}

func stopJob(ctx context.Context, c jobmanagerv1.JobServiceClient, args []string) error {
	id, err := oneJobID("stop", args)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, stopDeadline)
	defer cancel()

	if _, err := c.Stop(ctx, &jobmanagerv1.StopRequest{JobId: id}); err != nil {
		return rpcErr("stop", err)
	}
	return nil
}

func oneJobID(cmd string, args []string) (string, error) {
	if len(args) != 1 {
		return "", fmt.Errorf("usage: jobctl %s JOB_ID", cmd)
	}
	return args[0], nil
}

// rpcErr strips the gRPC wrapping so that the message reads as a sentence
// rather than as "rpc error: code = NotFound desc = job not found".
func rpcErr(cmd string, err error) error {
	if s, ok := status.FromError(err); ok {
		return fmt.Errorf("%s: %s (%s)", cmd, s.Message(), s.Code())
	}
	return fmt.Errorf("%s: %w", cmd, err)
}

// shortState trims the wire enum's prefix: JOB_STATE_DONE reads as DONE.
func shortState(s jobmanagerv1.JobState) string {
	const prefix = "JOB_STATE_"
	name := s.String()
	if len(name) > len(prefix) && name[:len(prefix)] == prefix {
		return name[len(prefix):]
	}
	return name
}
