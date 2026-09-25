package server

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"testing"

	"github.com/greatmazinger/bantay/internal/authz"
	"github.com/greatmazinger/bantay/pkg/runner"
	jobmanagerv1 "github.com/greatmazinger/bantay/proto/jobmanager/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// fakeManager returns whatever a test tells it to. The library has its own
// tests; what these check is the translation either side of it.
type fakeManager struct {
	jobID     string
	startErr  error
	stopErr   error
	snap      runner.JobSnapshot
	statusErr error
	output    io.Reader
	outputErr error

	// Recorded so a test can assert what crossed the boundary.
	gotActor   string
	gotCommand string
	gotArgs    []string
	gotJobID   string
}

func (f *fakeManager) Start(owner, command string, args []string) (string, error) {
	f.gotActor, f.gotCommand, f.gotArgs = owner, command, args
	return f.jobID, f.startErr
}

func (f *fakeManager) Stop(ctx context.Context, actor, jobID string) error {
	f.gotActor, f.gotJobID = actor, jobID
	return f.stopErr
}

func (f *fakeManager) Status(actor, jobID string) (runner.JobSnapshot, error) {
	f.gotActor, f.gotJobID = actor, jobID
	return f.snap, f.statusErr
}

func (f *fakeManager) Output(ctx context.Context, actor, jobID string) (io.Reader, error) {
	f.gotActor, f.gotJobID = actor, jobID
	return f.output, f.outputErr
}

const testCaller = "spiffe://jobworker.local/client/astrid"

// newClient serves mgr over an in-memory connection and returns a client for
// it. Authentication is replaced by an interceptor that injects a fixed actor:
// the real ones are tested in interceptor_test.go, and bufconn carries no TLS
// peer for them to read. Pass an empty actor to serve with no identity at all,
// which is the wiring bug each handler guards against.
func newClient(t *testing.T, mgr Manager, actor string) jobmanagerv1.JobServiceClient {
	t.Helper()

	var opts []grpc.ServerOption
	if actor != "" {
		opts = append(opts,
			grpc.ChainUnaryInterceptor(UnaryRecoveryInterceptor),
			grpc.ChainStreamInterceptor(StreamRecoveryInterceptor),
			grpc.ChainUnaryInterceptor(func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
				return h(withActor(ctx, actor), req)
			}),
			grpc.ChainStreamInterceptor(func(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, h grpc.StreamHandler) error {
				return h(srv, &authStream{ServerStream: ss, ctx: withActor(ss.Context(), actor)})
			}),
		)
	}

	lis := bufconn.Listen(1024 * 1024)
	srv := grpc.NewServer(opts...)
	jobmanagerv1.RegisterJobServiceServer(srv, New(mgr))

	go func() {
		// Serve returns when the listener closes, which Cleanup does below.
		_ = srv.Serve(lis)
	}()

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dialling the in-memory server: %v", err)
	}
	t.Cleanup(func() {
		_ = conn.Close()
		srv.Stop()
	})
	return jobmanagerv1.NewJobServiceClient(conn)
}

func TestStart(t *testing.T) {
	t.Run("returns the job ID and passes the caller through as owner", func(t *testing.T) {
		mgr := &fakeManager{jobID: "job-1-abcd"}
		c := newClient(t, mgr, testCaller)

		resp, err := c.Start(t.Context(), &jobmanagerv1.StartRequest{
			Command: "echo", Args: []string{"hello", "world"},
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if resp.JobId != "job-1-abcd" {
			t.Errorf("job ID = %q, want %q", resp.JobId, "job-1-abcd")
		}
		// The owner is the identity from the context, never anything the
		// client put in the request: no message carries an owner field.
		if mgr.gotActor != testCaller {
			t.Errorf("owner = %q, want %q", mgr.gotActor, testCaller)
		}
		if mgr.gotCommand != "echo" {
			t.Errorf("command = %q, want %q", mgr.gotCommand, "echo")
		}
		if strings.Join(mgr.gotArgs, ",") != "hello,world" {
			t.Errorf("args = %v, want [hello world]", mgr.gotArgs)
		}
	})

	// The library does not validate the command, so the boundary does.
	t.Run("an empty command is INVALID_ARGUMENT", func(t *testing.T) {
		mgr := &fakeManager{jobID: "unused"}
		c := newClient(t, mgr, testCaller)

		_, err := c.Start(t.Context(), &jobmanagerv1.StartRequest{Command: ""})
		if status.Code(err) != codes.InvalidArgument {
			t.Errorf("code = %v, want %v", status.Code(err), codes.InvalidArgument)
		}
		if mgr.gotCommand != "" {
			t.Error("the manager was called for an empty command")
		}
	})

	// ErrCommand is the one class of failure the caller caused and can fix, so
	// it is the one case where the detail crosses back.
	t.Run("a bad command carries its detail back", func(t *testing.T) {
		c := newClient(t, &fakeManager{startErr: runner.ErrCommand}, testCaller)

		_, err := c.Start(t.Context(), &jobmanagerv1.StartRequest{Command: "nosuchbinary"})
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("code = %v, want %v", status.Code(err), codes.InvalidArgument)
		}
		if msg := status.Convert(err).Message(); !strings.Contains(msg, "cannot start command") {
			t.Errorf("message = %q, want it to carry the reason", msg)
		}
	})

	// Anything else may name cgroup paths or other internals.
	t.Run("an unexpected failure leaks nothing", func(t *testing.T) {
		c := newClient(t, &fakeManager{startErr: errors.New("/sys/fs/cgroup/jobworker/job-1: permission denied")}, testCaller)

		_, err := c.Start(t.Context(), &jobmanagerv1.StartRequest{Command: "echo"})
		if status.Code(err) != codes.Internal {
			t.Fatalf("code = %v, want %v", status.Code(err), codes.Internal)
		}
		if msg := status.Convert(err).Message(); msg != "internal error" {
			t.Errorf("message = %q, want %q", msg, "internal error")
		}
	})

	// Serving without the authentication interceptor is a wiring bug, not a
	// client error, so it must not run a job owned by the empty string.
	t.Run("no identity in the context is INTERNAL", func(t *testing.T) {
		mgr := &fakeManager{jobID: "unused"}
		c := newClient(t, mgr, "")

		_, err := c.Start(t.Context(), &jobmanagerv1.StartRequest{Command: "echo"})
		if status.Code(err) != codes.Internal {
			t.Errorf("code = %v, want %v", status.Code(err), codes.Internal)
		}
		if mgr.gotCommand != "" {
			t.Error("a job was started with no caller identity")
		}
	})
}

// A job that belongs to someone else and a job that never existed must be the
// same answer, or the API becomes an oracle for which IDs are real.
func TestNotFoundAndForbiddenAreIndistinguishable(t *testing.T) {
	notFound := newClient(t, &fakeManager{statusErr: runner.ErrJobNotFound}, testCaller)
	forbidden := newClient(t, &fakeManager{statusErr: authz.ErrForbidden}, testCaller)

	_, errNF := notFound.Status(t.Context(), &jobmanagerv1.StatusRequest{JobId: "job-1-abcd"})
	_, errFB := forbidden.Status(t.Context(), &jobmanagerv1.StatusRequest{JobId: "job-1-abcd"})

	if status.Code(errNF) != codes.NotFound || status.Code(errFB) != codes.NotFound {
		t.Fatalf("codes = %v and %v, want both %v", status.Code(errNF), status.Code(errFB), codes.NotFound)
	}
	if a, b := status.Convert(errNF).Message(), status.Convert(errFB).Message(); a != b {
		t.Errorf("messages differ: %q vs %q", a, b)
	}
}

func TestStatus(t *testing.T) {
	// Explicit presence: absent means running, not an exit code of zero.
	t.Run("a running job reports no exit code", func(t *testing.T) {
		c := newClient(t, &fakeManager{snap: runner.JobSnapshot{State: runner.StateRunning}}, testCaller)

		resp, err := c.Status(t.Context(), &jobmanagerv1.StatusRequest{JobId: "job-1-abcd"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if resp.State != jobmanagerv1.JobState_JOB_STATE_RUNNING {
			t.Errorf("state = %v, want RUNNING", resp.State)
		}
		if resp.ExitCode != nil {
			t.Errorf("exit code = %d, want absent while running", resp.GetExitCode())
		}
	})

	t.Run("terminal states report one", func(t *testing.T) {
		for _, tt := range []struct {
			name  string
			snap  runner.JobSnapshot
			state jobmanagerv1.JobState
		}{
			{"done", runner.JobSnapshot{State: runner.StateDone, ExitCode: 0}, jobmanagerv1.JobState_JOB_STATE_DONE},
			{"done non-zero", runner.JobSnapshot{State: runner.StateDone, ExitCode: 3}, jobmanagerv1.JobState_JOB_STATE_DONE},
			{"stopped", runner.JobSnapshot{State: runner.StateStopped, ExitCode: -1}, jobmanagerv1.JobState_JOB_STATE_STOPPED},
		} {
			t.Run(tt.name, func(t *testing.T) {
				c := newClient(t, &fakeManager{snap: tt.snap}, testCaller)

				resp, err := c.Status(t.Context(), &jobmanagerv1.StatusRequest{JobId: "job-1-abcd"})
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if resp.State != tt.state {
					t.Errorf("state = %v, want %v", resp.State, tt.state)
				}
				if resp.ExitCode == nil {
					t.Fatal("exit code absent for a terminal state")
				}
				if got := resp.GetExitCode(); got != int32(tt.snap.ExitCode) {
					t.Errorf("exit code = %d, want %d", got, tt.snap.ExitCode)
				}
			})
		}
	})

	// A state this package has no mapping for is a bug here, not a client error.
	t.Run("an unknown state is INTERNAL", func(t *testing.T) {
		c := newClient(t, &fakeManager{snap: runner.JobSnapshot{State: runner.JobState("ASCENDED")}}, testCaller)

		_, err := c.Status(t.Context(), &jobmanagerv1.StatusRequest{JobId: "job-1-abcd"})
		if status.Code(err) != codes.Internal {
			t.Errorf("code = %v, want %v", status.Code(err), codes.Internal)
		}
	})
}

func TestStop(t *testing.T) {
	t.Run("passes the caller and job through", func(t *testing.T) {
		mgr := &fakeManager{}
		c := newClient(t, mgr, testCaller)

		if _, err := c.Stop(t.Context(), &jobmanagerv1.StopRequest{JobId: "job-1-abcd"}); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if mgr.gotActor != testCaller || mgr.gotJobID != "job-1-abcd" {
			t.Errorf("manager saw (%q, %q), want (%q, %q)", mgr.gotActor, mgr.gotJobID, testCaller, "job-1-abcd")
		}
	})

	// Stop waits, so its context can expire. The library's error maps to the
	// matching status code rather than to INTERNAL.
	t.Run("a deadline maps to DEADLINE_EXCEEDED", func(t *testing.T) {
		c := newClient(t, &fakeManager{stopErr: context.DeadlineExceeded}, testCaller)

		_, err := c.Stop(t.Context(), &jobmanagerv1.StopRequest{JobId: "job-1-abcd"})
		if status.Code(err) != codes.DeadlineExceeded {
			t.Errorf("code = %v, want %v", status.Code(err), codes.DeadlineExceeded)
		}
	})
}

func TestOutput(t *testing.T) {
	t.Run("streams everything the reader produces", func(t *testing.T) {
		want := strings.Repeat("lorem ipsum ", 10_000) // larger than one chunk
		c := newClient(t, &fakeManager{output: strings.NewReader(want)}, testCaller)

		stream, err := c.Output(t.Context(), &jobmanagerv1.OutputRequest{JobId: "job-1-abcd"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		var got []byte
		for {
			chunk, err := stream.Recv()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				t.Fatalf("receiving: %v", err)
			}
			got = append(got, chunk.Data...)
		}
		if string(got) != want {
			t.Errorf("streamed %d bytes, want %d", len(got), len(want))
		}
	})

	t.Run("a job that cannot be read is NOT_FOUND", func(t *testing.T) {
		c := newClient(t, &fakeManager{outputErr: runner.ErrJobNotFound}, testCaller)

		stream, err := c.Output(t.Context(), &jobmanagerv1.OutputRequest{JobId: "job-1-abcd"})
		if err != nil {
			t.Fatalf("opening the stream: %v", err)
		}
		_, err = stream.Recv()
		if status.Code(err) != codes.NotFound {
			t.Errorf("code = %v, want %v", status.Code(err), codes.NotFound)
		}
	})

	// A read that fails partway must not look like a clean end of stream.
	t.Run("a mid-stream read failure is reported, not swallowed", func(t *testing.T) {
		c := newClient(t, &fakeManager{output: io.MultiReader(
			strings.NewReader("partial output"),
			errReader{errors.New("buffer exploded")},
		)}, testCaller)

		stream, err := c.Output(t.Context(), &jobmanagerv1.OutputRequest{JobId: "job-1-abcd"})
		if err != nil {
			t.Fatalf("opening the stream: %v", err)
		}
		var got []byte
		var recvErr error
		for {
			chunk, err := stream.Recv()
			if err != nil {
				recvErr = err
				break
			}
			got = append(got, chunk.Data...)
		}
		if errors.Is(recvErr, io.EOF) {
			t.Fatal("the stream ended cleanly despite a read failure")
		}
		if status.Code(recvErr) != codes.Internal {
			t.Errorf("code = %v, want %v", status.Code(recvErr), codes.Internal)
		}
		// Whatever was read before the failure still reached the client.
		if string(got) != "partial output" {
			t.Errorf("received %q, want %q", got, "partial output")
		}
	})
}

// errReader fails on every read.
type errReader struct{ err error }

func (e errReader) Read([]byte) (int, error) { return 0, e.err }

// panicReader panics partway through, standing in for a bug in anything the
// stream handler touches.
type panicReader struct{ before string }

func (p *panicReader) Read(b []byte) (int, error) {
	if p.before != "" {
		n := copy(b, p.before)
		p.before = ""
		return n, nil
	}
	panic("reader blew up")
}

// TestFailureContainment covers the second half of what the design promises: a
// panicking handler costs its own caller an INTERNAL, and costs everyone else
// nothing. The first half, that the process survives at all, is asserted by
// this test binary still running, since an unrecovered panic in a handler
// goroutine would take it down.
func TestFailureContainment(t *testing.T) {
	const healthy = "this stream is fine"

	doomed := newClient(t, &fakeManager{output: &panicReader{before: "partial"}}, testCaller)
	fine := newClient(t, &fakeManager{output: strings.NewReader(healthy)}, testCaller)

	// Open the healthy stream first so it is in flight when the other panics.
	fineStream, err := fine.Output(t.Context(), &jobmanagerv1.OutputRequest{JobId: "job-2-abcd"})
	if err != nil {
		t.Fatalf("opening the healthy stream: %v", err)
	}

	doomedStream, err := doomed.Output(t.Context(), &jobmanagerv1.OutputRequest{JobId: "job-1-abcd"})
	if err != nil {
		t.Fatalf("opening the doomed stream: %v", err)
	}
	var panicErr error
	for {
		if _, err := doomedStream.Recv(); err != nil {
			panicErr = err
			break
		}
	}
	if status.Code(panicErr) != codes.Internal {
		t.Errorf("the panicking stream returned %v, want %v", status.Code(panicErr), codes.Internal)
	}
	if msg := status.Convert(panicErr).Message(); msg != "internal error" {
		t.Errorf("message = %q, want %q: the panic value must not reach the client", msg, "internal error")
	}

	// The other stream, opened before the panic, must still deliver in full.
	var got []byte
	for {
		chunk, err := fineStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("the healthy stream failed after another panicked: %v", err)
		}
		got = append(got, chunk.Data...)
	}
	if string(got) != healthy {
		t.Errorf("healthy stream read %q, want %q", got, healthy)
	}
}
