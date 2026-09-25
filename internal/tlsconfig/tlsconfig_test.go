package tlsconfig

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// These tests assert the server's configuration, not the correctness of Go's
// TLS implementation. What is being checked is that the handshake refuses the
// things this service says it refuses.

func TestHandshake_Succeeds(t *testing.T) {
	authority := newCA(t)
	serverFiles := authority.issue(t, "server", x509.ExtKeyUsageServerAuth, "localhost")
	clientFiles := authority.issue(t, "alice", x509.ExtKeyUsageClientAuth)

	serverCfg := mustServer(t, serverFiles)
	clientCfg := mustClient(t, clientFiles)

	state, err := handshake(t, serverCfg, clientCfg)
	if err != nil {
		t.Fatalf("handshake failed: %v", err)
	}
	if state.Version != tls.VersionTLS13 {
		t.Errorf("negotiated version 0x%04x, want TLS 1.3 (0x%04x)", state.Version, tls.VersionTLS13)
	}
	if len(state.PeerCertificates) == 0 {
		t.Error("client received no server certificate")
	}
}

func TestHandshake_RejectsMissingClientCertificate(t *testing.T) {
	authority := newCA(t)
	serverCfg := mustServer(t, authority.issue(t, "server", x509.ExtKeyUsageServerAuth, "localhost"))

	// A client that verifies the server but presents nothing of its own.
	clientCfg := mustClient(t, authority.issue(t, "alice", x509.ExtKeyUsageClientAuth))
	clientCfg.Certificates = nil

	if _, err := handshake(t, serverCfg, clientCfg); err == nil {
		t.Fatal("handshake succeeded without a client certificate")
	}
}

func TestHandshake_RejectsForeignCA(t *testing.T) {
	authority := newCA(t)
	impostor := newCA(t)

	serverCfg := mustServer(t, authority.issue(t, "server", x509.ExtKeyUsageServerAuth, "localhost"))

	// A well-formed client certificate, signed by the wrong authority.
	clientCfg := mustClient(t, impostor.issue(t, "mallory", x509.ExtKeyUsageClientAuth))
	// Trust the real server so the failure is the server rejecting the client,
	// not the client rejecting the server.
	clientCfg.RootCAs = authority.pool(t)

	if _, err := handshake(t, serverCfg, clientCfg); err == nil {
		t.Fatal("handshake succeeded with a certificate from an untrusted authority")
	}
}

func TestHandshake_RejectsTLS12(t *testing.T) {
	authority := newCA(t)
	serverCfg := mustServer(t, authority.issue(t, "server", x509.ExtKeyUsageServerAuth, "localhost"))

	clientCfg := mustClient(t, authority.issue(t, "alice", x509.ExtKeyUsageClientAuth))
	clientCfg.MinVersion = tls.VersionTLS12
	clientCfg.MaxVersion = tls.VersionTLS12

	_, err := handshake(t, serverCfg, clientCfg)
	if err == nil {
		t.Fatal("handshake succeeded at TLS 1.2")
	}
	if !strings.Contains(err.Error(), "version") && !strings.Contains(err.Error(), "protocol") {
		t.Logf("rejected, though not obviously on version grounds: %v", err)
	}
}

func TestLoad_ReportsMissingFiles(t *testing.T) {
	_, err := Server(Files{Cert: "nope.crt", Key: "nope.key", CA: "nope.crt"})
	if err == nil {
		t.Fatal("expected an error for missing files")
	}
	if !strings.Contains(err.Error(), "nope.crt") {
		t.Errorf("error should name the offending path, got: %v", err)
	}
}

// --- helpers ---

func mustServer(t *testing.T, f Files) *tls.Config {
	t.Helper()
	cfg, err := Server(f)
	if err != nil {
		t.Fatalf("Server: %v", err)
	}
	return cfg
}

func mustClient(t *testing.T, f Files) *tls.Config {
	t.Helper()
	cfg, err := Client(f)
	if err != nil {
		t.Fatalf("Client: %v", err)
	}
	// The test listener is dialled by IP, but its certificate carries only
	// DNS:localhost, so the name has to be supplied rather than inferred.
	cfg.ServerName = "localhost"
	return cfg
}

// handshake connects a client to a server over a real loopback listener and
// completes a one-byte round trip, returning the client's connection state.
//
// A round trip rather than just a handshake, because in TLS 1.3 the client
// finishes before the server has validated its certificate: a client-auth
// rejection surfaces on the client's first read, not from Handshake.
func handshake(t *testing.T, serverCfg, clientCfg *tls.Config) (tls.ConnectionState, error) {
	t.Helper()

	ln, err := tls.Listen("tcp", "127.0.0.1:0", serverCfg)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
		buf := make([]byte, 1)
		if _, err := conn.Read(buf); err != nil {
			return // rejected client; the alert is already on its way
		}
		_, _ = conn.Write(buf)
	}()

	conn, err := tls.Dial("tcp", ln.Addr().String(), clientCfg)
	if err != nil {
		return tls.ConnectionState{}, err // version mismatch lands here
	}
	defer conn.Close()

	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := conn.Write([]byte("x")); err != nil {
		return tls.ConnectionState{}, err
	}
	if _, err := conn.Read(make([]byte, 1)); err != nil {
		return tls.ConnectionState{}, err // client-auth rejection lands here
	}

	return conn.ConnectionState(), nil
}

// testCA issues certificates into a temp directory.
type testCA struct {
	cert    *x509.Certificate
	key     *ecdsa.PrivateKey
	certPEM []byte
	dir     string
}

func newCA(t *testing.T) *testCA {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}

	ca := &testCA{
		cert:    cert,
		key:     key,
		certPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		dir:     t.TempDir(),
	}
	if err := os.WriteFile(filepath.Join(ca.dir, "ca.crt"), ca.certPEM, 0o644); err != nil {
		t.Fatal(err)
	}
	return ca
}

func (c *testCA) pool(t *testing.T) *x509.CertPool {
	t.Helper()
	p := x509.NewCertPool()
	if !p.AppendCertsFromPEM(c.certPEM) {
		t.Fatal("could not build pool from CA PEM")
	}
	return p
}

// issue writes a certificate and key for name into the CA's directory and
// returns the Files needed to load them.
func (c *testCA) issue(t *testing.T, name string, eku x509.ExtKeyUsage, dnsNames ...string) Files {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(time.Now().UnixNano()),
		Subject:               pkix.Name{CommonName: name},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{eku},
		BasicConstraintsValid: true,
		DNSNames:              dnsNames,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, c.cert, &key.PublicKey, c.key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}

	certPath := filepath.Join(c.dir, name+".crt")
	keyPath := filepath.Join(c.dir, name+".key")
	write(t, certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
	write(t, keyPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}))

	return Files{Cert: certPath, Key: keyPath, CA: filepath.Join(c.dir, "ca.crt")}
}

func write(t *testing.T, path string, b []byte) {
	t.Helper()
	if err := os.WriteFile(path, b, 0o600); err != nil {
		t.Fatal(err)
	}
}
