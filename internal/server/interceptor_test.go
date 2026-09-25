package server

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net/url"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

const (
	testActor  = "spiffe://jobworker.local/client/astrid"
	testMethod = "/jobmanager.v1.JobService/Status"
)

// The interceptors never verify a certificate, so these tests do not sign one.
// They build the x509.Certificate that a completed handshake would have left in
// the peer's TLSInfo, which is all actorFromPeer reads.
func certWithURI(t *testing.T, uri string) *x509.Certificate {
	t.Helper()
	u, err := url.Parse(uri)
	if err != nil {
		t.Fatalf("parsing %q: %v", uri, err)
	}
	return &x509.Certificate{URIs: []*url.URL{u}}
}

// ctxWithCert returns a context carrying the peer a mutually authenticated
// connection would produce.
func ctxWithCert(t *testing.T, cert *x509.Certificate) context.Context {
	t.Helper()
	return peer.NewContext(t.Context(), &peer.Peer{
		AuthInfo: credentials.TLSInfo{
			State: tls.ConnectionState{PeerCertificates: []*x509.Certificate{cert}},
		},
	})
}

// notTLS is an AuthInfo that is not credentials.TLSInfo, which is what an
// insecure connection produces.
type notTLS struct{}

func (notTLS) AuthType() string { return "insecure" }

func unaryInfo() *grpc.UnaryServerInfo {
	return &grpc.UnaryServerInfo{FullMethod: testMethod}
}

func streamInfo() *grpc.StreamServerInfo {
	return &grpc.StreamServerInfo{FullMethod: testMethod}
}

// fakeStream is the minimum grpc.ServerStream needed to drive the stream
// interceptors: they only ever call Context.
type fakeStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *fakeStream) Context() context.Context { return s.ctx }

func TestUnaryAuthInterceptor_RejectsUnidentifiableCallers(t *testing.T) {
	tests := []struct {
		name string
		ctx  func(t *testing.T) context.Context
	}{
		{
			name: "no peer in context",
			ctx:  func(t *testing.T) context.Context { return t.Context() },
		},
		{
			name: "peer is not TLS",
			ctx: func(t *testing.T) context.Context {
				return peer.NewContext(t.Context(), &peer.Peer{AuthInfo: notTLS{}})
			},
		},
		{
			name: "no client certificate",
			ctx: func(t *testing.T) context.Context {
				return peer.NewContext(t.Context(), &peer.Peer{
					AuthInfo: credentials.TLSInfo{State: tls.ConnectionState{}},
				})
			},
		},
		{
			name: "certificate from another trust domain",
			ctx: func(t *testing.T) context.Context {
				return ctxWithCert(t, certWithURI(t, "spiffe://elsewhere.example/client/mallory"))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			called := false
			handler := func(ctx context.Context, req any) (any, error) {
				called = true
				return nil, nil
			}

			_, err := UnaryAuthInterceptor(tt.ctx(t), nil, unaryInfo(), handler)

			if status.Code(err) != codes.Unauthenticated {
				t.Errorf("code = %v, want %v (err: %v)", status.Code(err), codes.Unauthenticated, err)
			}
			// The reason belongs in the log, not on the wire.
			if msg := status.Convert(err).Message(); msg != "unauthenticated" {
				t.Errorf("message = %q, want %q", msg, "unauthenticated")
			}
			if called {
				t.Error("handler ran for a caller that could not be identified")
			}
		})
	}
}

func TestUnaryAuthInterceptor_PutsActorInContext(t *testing.T) {
	var got string
	var found bool
	handler := func(ctx context.Context, req any) (any, error) {
		got, found = actorFrom(ctx)
		return "response", nil
	}

	resp, err := UnaryAuthInterceptor(ctxWithCert(t, certWithURI(t, testActor)), nil, unaryInfo(), handler)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp != "response" {
		t.Errorf("response = %v, want %q", resp, "response")
	}
	if !found {
		t.Fatal("handler saw no actor in its context")
	}
	if got != testActor {
		t.Errorf("actor = %q, want %q", got, testActor)
	}
}

func TestStreamAuthInterceptor_RejectsUnidentifiableCallers(t *testing.T) {
	called := false
	handler := func(srv any, ss grpc.ServerStream) error {
		called = true
		return nil
	}

	err := StreamAuthInterceptor(nil, &fakeStream{ctx: t.Context()}, streamInfo(), handler)

	if status.Code(err) != codes.Unauthenticated {
		t.Errorf("code = %v, want %v (err: %v)", status.Code(err), codes.Unauthenticated, err)
	}
	if called {
		t.Error("handler ran for a caller that could not be identified")
	}
}

// The stream's context is reached through a method with no setter, so the
// interceptor has to wrap. This asserts the wrapper is what the handler sees.
func TestStreamAuthInterceptor_WrappedStreamCarriesActor(t *testing.T) {
	var got string
	var found bool
	handler := func(srv any, ss grpc.ServerStream) error {
		got, found = actorFrom(ss.Context())
		return nil
	}

	ctx := ctxWithCert(t, certWithURI(t, testActor))
	if err := StreamAuthInterceptor(nil, &fakeStream{ctx: ctx}, streamInfo(), handler); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !found {
		t.Fatal("handler saw no actor on its stream's context")
	}
	if got != testActor {
		t.Errorf("actor = %q, want %q", got, testActor)
	}
}

func TestUnaryRecoveryInterceptor_TurnsPanicIntoInternal(t *testing.T) {
	handler := func(ctx context.Context, req any) (any, error) {
		panic("handler blew up")
	}

	// The assertion is partly that this call returns at all.
	resp, err := UnaryRecoveryInterceptor(t.Context(), nil, unaryInfo(), handler)

	if status.Code(err) != codes.Internal {
		t.Errorf("code = %v, want %v (err: %v)", status.Code(err), codes.Internal, err)
	}
	// The panic value could name anything about the server's internals.
	if msg := status.Convert(err).Message(); msg != "internal error" {
		t.Errorf("message = %q, want %q", msg, "internal error")
	}
	if resp != nil {
		t.Errorf("response = %v, want nil", resp)
	}
}

func TestStreamRecoveryInterceptor_TurnsPanicIntoInternal(t *testing.T) {
	handler := func(srv any, ss grpc.ServerStream) error {
		panic("handler blew up")
	}

	err := StreamRecoveryInterceptor(nil, &fakeStream{ctx: t.Context()}, streamInfo(), handler)

	if status.Code(err) != codes.Internal {
		t.Errorf("code = %v, want %v (err: %v)", status.Code(err), codes.Internal, err)
	}
}

// A handler that returns normally must pass through untouched, error and all.
func TestRecoveryInterceptors_PassThroughWhenNothingPanics(t *testing.T) {
	sentinel := errors.New("ordinary failure")

	t.Run("unary", func(t *testing.T) {
		handler := func(ctx context.Context, req any) (any, error) { return "ok", sentinel }
		resp, err := UnaryRecoveryInterceptor(t.Context(), nil, unaryInfo(), handler)
		if !errors.Is(err, sentinel) {
			t.Errorf("err = %v, want %v", err, sentinel)
		}
		if resp != "ok" {
			t.Errorf("response = %v, want %q", resp, "ok")
		}
	})

	t.Run("stream", func(t *testing.T) {
		handler := func(srv any, ss grpc.ServerStream) error { return sentinel }
		err := StreamRecoveryInterceptor(nil, &fakeStream{ctx: t.Context()}, streamInfo(), handler)
		if !errors.Is(err, sentinel) {
			t.Errorf("err = %v, want %v", err, sentinel)
		}
	})
}
