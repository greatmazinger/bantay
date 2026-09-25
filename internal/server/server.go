// Package server is the translation layer between the network and the job library.
// It is the only component that knows both network transport (gRPC) and the job
// library API, most notably the job manager. This package turns network messages
// into library calls. It also translates results and errors from the library
// into network equivalents.
//
// The caller's identity comes from the request context, where the authentication
// interceptor places it after deriving it from the verified client certificate.
// It is never taken from the request body. No message carries an owner field, so
// a client cannot assert an identity on the wire.
package server

import (
	"context"
	"errors"
	"io"
	"log/slog"

	"github.com/greatmazinger/bantay/pkg/runner"
	jobmanagerv1 "github.com/greatmazinger/bantay/proto/jobmanager/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Manager is the part of the job library this package uses. It is declared
// here rather than in pkg/runner so that the library returns a concrete type
// and each consumer states the surface it needs. The server's tests supply a
// fake in place of *runner.JobManager.
type Manager interface {
	Start(owner string, command string, args []string) (string, error)
	Stop(ctx context.Context, actor string, jobID string) error
	Status(actor string, jobID string) (runner.JobSnapshot, error)
	Output(ctx context.Context, actor string, jobID string) (io.Reader, error)
}

// Server implements the generated JobServiceServer by delegating to a Manager.
// Server holds no job state of its own: every RPC translates its request into a
// library call. The library call's result or error is translated back into a
// wire response or status code.
type Server struct {
	jobmanagerv1.UnimplementedJobServiceServer
	mgr Manager
}

// Compile-time check that *Server satisfies the generated interface. Without
// it, a handler whose signature doesn't match the proto shadows the embedded
// stub and the failure surfaces at RegisterJobServiceServer call rather than
// here.
var _ jobmanagerv1.JobServiceServer = (*Server)(nil)

// New returns a Server backed by mgr. This takes the interface so tests can supply a fake manager.
func New(mgr Manager) *Server {
	return &Server{mgr: mgr}
}

// actorKey is a struct and not a key so that no other package can construct and
// actorKey. This means that an identity can only be placed into a context from inside
// server. This also means that nothing can collide with this key, unlike a string key.
// A string cannot guarantee either requirement. The empty struct also costs nothing
// at runtime.
type actorKey struct{}

// withActor returns a copy of ctx carrying actor as the caller's identity. The
// function wraps ctx with an extra key-value pair, keeping the original
// context unchanged.
func withActor(ctx context.Context, actor string) context.Context {
	return context.WithValue(ctx, actorKey{}, actor)
}

// actorFrom extracts the actor from the context.
// The boolean reports whether one was present. False means the authentication
// did not run, which is a wiring bug rather than a client error, so handlers
// should return INTERNAL error. A value of the wrong type under the actorKey
// returns false rather than panicking.
func actorFrom(ctx context.Context) (string, bool) {
	actor, ok := ctx.Value(actorKey{}).(string)
	return actor, ok
}

// Start launchers a job owned by the calling identity and retuns its job ID.
//
// This is the only RPC that records an identity rather than checking one: the
// actor becomes the job's owner. Every later call is authorized against it.
// An empty command is rejected here because the runner library does not
// validate it.
func (s *Server) Start(ctx context.Context, req *jobmanagerv1.StartRequest) (*jobmanagerv1.StartResponse, error) {
	actor, ok := actorFrom(ctx)
	if !ok {
		slog.Error("no caller identity in context", slog.String("rpc", "Start"))
		return nil, status.Error(codes.Internal, "internal error")
	}

	cmd := req.GetCommand()
	if cmd == "" {
		return nil, status.Error(codes.InvalidArgument, "command must be not empty")
	}
	args := req.GetArgs()
	jobID, err := s.mgr.Start(actor, cmd, args)
	if err != nil {
		return nil, rpcError(err)
	}
	return &jobmanagerv1.StartResponse{JobId: jobID}, nil
}

// Stop terminates job and waits for the job to reach a terminal state.
//
// The RPC context is passed to the library, so it bounds how long the caller
// waits. A client that disconnects or times out leaves the job terminating on
// its own schedule. Stopping a job that has already exited always succeeds.
func (s *Server) Stop(ctx context.Context, req *jobmanagerv1.StopRequest) (*jobmanagerv1.StopResponse, error) {
	actor, ok := actorFrom(ctx)
	if !ok {
		slog.Error("no caller identity in context", slog.String("rpc", "Stop"))
		return nil, status.Error(codes.Internal, "internal error")
	}

	jobID := req.GetJobId()
	if err := s.mgr.Stop(ctx, actor, jobID); err != nil {
		return nil, rpcError(err)
	}

	return &jobmanagerv1.StopResponse{}, nil
}

// Status reports a job's current state, and once it has terminated, its
// exit code.
//
// Like Start, it takes no context from the caller as reading a snapshot does
// not wait for anything. The exit code is set only for a terminal state, so an
// absent field means the job is still running rather than it exited with zero.
func (s *Server) Status(ctx context.Context, req *jobmanagerv1.StatusRequest) (*jobmanagerv1.StatusResponse, error) {
	actor, ok := actorFrom(ctx)
	if !ok {
		slog.Error("no caller identity in context", slog.String("rpc", "Status"))
		return nil, status.Error(codes.Internal, "internal error")
	}

	jobID := req.GetJobId()
	snap, err := s.mgr.Status(actor, jobID)
	if err != nil {
		return nil, rpcError(err)
	}
	state, err := protoState(snap.State)
	if err != nil {
		return nil, rpcError(err)
	}

	resp := &jobmanagerv1.StatusResponse{State: state}
	switch snap.State {
	case runner.StateDone, runner.StateStopped:
		resp.ExitCode = new(int32(snap.ExitCode))
	case runner.StateRunning:
		// No exit code while the job is running. The field uses explicit presence,
		// so absent is indistinguishable from an exit code of zero.
	}

	return resp, nil
}

// Output streams a job's combined stdout and stderr, beginning at the first
// byte the process wrote regardless of when the caller attached.
//
// The stream's context bounds the read, so a client that disconnects releases
// its reader without disturbing any other. Each call is independent: a client
// that reconnects receives the stream from the beginning again.
//
// A read that returns bytes and an error together sends the bytes first.
// io.Reader requires a caller to process n > 0 before considering the error,
// and a client that saw a chunk followed by a failure is better off than one
// that saw a clean end of stream it should not have trusted.
func (s *Server) Output(req *jobmanagerv1.OutputRequest, stream grpc.ServerStreamingServer[jobmanagerv1.OutputChunk]) error {
	ctx := stream.Context()
	actor, ok := actorFrom(ctx)
	if !ok {
		slog.Error("no caller identity in context", slog.String("rpc", "Output"))
		return status.Error(codes.Internal, "internal error")
	}

	jobID := req.GetJobId()
	r, err := s.mgr.Output(ctx, actor, jobID)
	if err != nil {
		return rpcError(err)
	}

	chunk := make([]byte, 32*1024)
	for {
		n, err := r.Read(chunk)
		if n > 0 {
			if sendErr := stream.Send(&jobmanagerv1.OutputChunk{Data: chunk[:n]}); sendErr != nil {
				return sendErr
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return rpcError(err)
		}
	}
}
