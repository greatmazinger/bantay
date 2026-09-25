package server

import (
	"context"
	"errors"
	"log/slog"
	"runtime/debug"

	"github.com/greatmazinger/bantay/internal/authn"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// Compile-time check that each interceptor matches grpc-go's function type.
// Without these, a wrong signature is only caught when the server registers
// them, which is a runtime failure in main rather than a build failure here.
var (
	_ grpc.UnaryServerInterceptor  = UnaryAuthInterceptor
	_ grpc.StreamServerInterceptor = StreamAuthInterceptor
	_ grpc.UnaryServerInterceptor  = UnaryRecoveryInterceptor
	_ grpc.StreamServerInterceptor = StreamRecoveryInterceptor
)

// These errors are never returned to the client and should only be used by the
// authentication interceptor. They separate the ways a caller can fail to be
// identified, so that the server can log which check failed. All these errors
// will map to UNAUTHENTICATED when the error is returned to the caller.
var (
	errNoPeer       = errors.New("server: no peer information in context")
	errNotTLS       = errors.New("server: connection is not TLS")
	errNoClientCert = errors.New("server: no client certificate presented")
)

// actorFromPeer returns the SPIFFE ID of the client in the context ctx.
//
// This function verifies nothing. The TLS handshake has already checked the
// certificate's signature and chain up to the CA. This function only establishes
// that a certificate is available. It then relies on authn.Identity to inspect
// the certificate and enforce the trust domain.
//
// Every error means the caller could not be identified.
func actorFromPeer(ctx context.Context) (string, error) {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return "", errNoPeer
	}

	tlsInfo, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok {
		return "", errNotTLS
	}

	if len(tlsInfo.State.PeerCertificates) == 0 {
		// If this error is returned, then our jobservice server has been
		// misconfigured. RequireAndVerifyClientCert is set by the
		// tlsconfig.Server and will prevent this.
		return "", errNoClientCert
	}
	return authn.Identity(tlsInfo.State.PeerCertificates[0])
}

// UnaryAuthInterceptor authenticates every unary RPC from the peer's
// certificate and puts the caller's identity in the context the handler sees.
// A handler therefore never reaches for the peer itself, and an RPC added later
// is covered without anyone remembering to cover it.
//
// The client is told only that it was not authenticated. Which of the three
// checks failed goes to the log: two of them describe this server's state, and
// the third means its own TLS config is wrong.
func UnaryAuthInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	actor, err := actorFromPeer(ctx)
	if err != nil {
		slog.Warn("authentication failed",
			slog.String("rpc", info.FullMethod),
			slog.String("error", err.Error()))
		return nil, status.Error(codes.Unauthenticated, "unauthenticated")
	}
	return handler(withActor(ctx, actor), req)
}

// authStream carries a context of our own choosing. grpc.ServerStream exposes
// its context through a method and offers no way to replace it, so wrapping is
// the only way a stream interceptor can pass anything down to the handler.
type authStream struct {
	grpc.ServerStream
	ctx context.Context
}

// Context shadows the embedded stream's method. Everything else on
// grpc.ServerStream is promoted unchanged.
func (s *authStream) Context() context.Context { return s.ctx }

// StreamAuthInterceptor is UnaryAuthInterceptor for streaming RPCs. The logic
// is identical; only the plumbing differs, because grpc-go splits unary and
// streaming into separate interceptor types.
func StreamAuthInterceptor(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	actor, err := actorFromPeer(ss.Context())
	if err != nil {
		slog.Warn("authentication failed",
			slog.String("rpc", info.FullMethod),
			slog.String("error", err.Error()))
		return status.Error(codes.Unauthenticated, "unauthenticated")
	}
	return handler(srv, &authStream{ServerStream: ss, ctx: withActor(ss.Context(), actor)})
}

// recovered turns a panic into an INTERNAL status and a logged stack. It is
// deferred by both recovery interceptors, so the decision about what a panic
// means lives in one place even though grpc-go needs two interceptors to
// deliver it.
//
// A panic is still a bug, and the log is the record of one. What this buys is
// that the bug costs one RPC rather than the process: an unrecovered panic in a
// handler would take down every running job's wait goroutine and drop every
// attached output stream.
func recovered(method string, err *error) {
	r := recover()
	if r == nil {
		return
	}
	slog.Error("handler panicked",
		slog.String("rpc", method),
		slog.Any("panic", r),
		slog.String("stack", string(debug.Stack())))
	*err = status.Error(codes.Internal, "internal error")
}

// UnaryRecoveryInterceptor keeps a panicking unary handler from taking the
// server with it. Register it outermost, so it also covers a panic in the
// authentication interceptor.
func UnaryRecoveryInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
	defer recovered(info.FullMethod, &err)
	return handler(ctx, req)
}

// StreamRecoveryInterceptor is UnaryRecoveryInterceptor for streaming RPCs.
func StreamRecoveryInterceptor(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) (err error) {
	defer recovered(info.FullMethod, &err)
	return handler(srv, ss)
}
