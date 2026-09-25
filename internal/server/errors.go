package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/greatmazinger/bantay/internal/authz"
	"github.com/greatmazinger/bantay/pkg/runner"
	jobmanagerv1 "github.com/greatmazinger/bantay/proto/jobmanager/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// stateToProto translates the library's job states to their wire equivalents
//
// Our schema documents that the server never sends JOB_STATE_UNSPECIFIED , and
// proto3 omits zero values, so it will serialize to the same empty message as
// a state that was never set. Should be read through 'protoState' rather than
// indexing directly.
var stateToProto = map[runner.JobState]jobmanagerv1.JobState{
	runner.StateRunning: jobmanagerv1.JobState_JOB_STATE_RUNNING,
	runner.StateDone:    jobmanagerv1.JobState_JOB_STATE_DONE,
	runner.StateStopped: jobmanagerv1.JobState_JOB_STATE_STOPPED,
}

// protoState returns the wire equivalent of a library job state.
//
// An error means the library has a state this package does not know about,
// which is a bug here, so the handler returns INTERNAL. Returning an error
// rather than a bare value makes that case impossible to ignore: a one-value
// call does not compile.
func protoState(s runner.JobState) (jobmanagerv1.JobState, error) {
	p, ok := stateToProto[s]
	if !ok {
		return 0, fmt.Errorf("server: unknown job state %q", s)
	}
	return p, nil
}

// rpcError takes domain errors and maps these to RPC errors for
// clients.
func rpcError(err error) error {
	switch {
	// Both collapse to one error externally. A caller should not be able to
	// tell a job that belongs to someone else from one that does not exist.
	case errors.Is(err, runner.ErrJobNotFound), errors.Is(err, authz.ErrForbidden):
		return status.Error(codes.NotFound, "job not found")

	// The one case that returns the error detail to the client. This makes
	// for better ergonomics for the user. This is the one class of failure
	// the caller caused, and can fix.
	case errors.Is(err, runner.ErrCommand):
		return status.Error(codes.InvalidArgument, err.Error())

	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return status.FromContextError(err).Err()

	// The error detail stays on the server side. These messages carry internal
	// implementation details such as cgroup paths, which should not be seen by
	// the client.
	default:
		slog.Error("unexpected error from the job library", "err", err)
		return status.Error(codes.Internal, "internal error")
	}
}
