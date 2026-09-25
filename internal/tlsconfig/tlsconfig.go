// Package tlsconfig builds the TLS configurations for the job worker's server
// and clients.
//
// Both sides require TLS 1.3 and mutual authentication against a single
// certificate authority. Cipher suites are not configured because TLS 1.3 does
// not allow it: Go fixes the three permitted suites and ignores any list
// supplied to it. Allowing a fallback to 1.2 would make cipher selection a
// decision again, and buys nothing in a closed system where both ends are
// issued by the same authority.
package tlsconfig

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

// Files names the certificate material a peer needs.
type Files struct {
	Cert string // PEM certificate for this peer
	Key  string // PEM private key for this peer
	CA   string // PEM certificate of the authority that signed both peers
}

// Server returns a configuration that requires every client to present a
// certificate issued by the configured authority, and verifies it.
//
// Verification here establishes only that the peer holds a certificate this CA
// issued. Deciding *which* identity that is, and whether it is acceptable, is
// the caller's job: see the identity interceptor.
func Server(f Files) (*tls.Config, error) {
	cert, pool, err := load(f)
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    pool,
	}, nil
}

// Client returns a configuration that presents this client's certificate and
// verifies the server against the same authority.
//
// Hostname verification is left on. The server's certificate carries a DNS SAN,
// so the standard path works and there is no need to disable verification and
// reimplement it in a callback.
func Client(f Files) (*tls.Config, error) {
	cert, pool, err := load(f)
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{cert},
		RootCAs:      pool,
	}, nil
}

func load(f Files) (tls.Certificate, *x509.CertPool, error) {
	cert, err := tls.LoadX509KeyPair(f.Cert, f.Key)
	if err != nil {
		return tls.Certificate{}, nil, fmt.Errorf("tlsconfig: loading %s and %s: %w", f.Cert, f.Key, err)
	}

	pem, err := os.ReadFile(f.CA)
	if err != nil {
		return tls.Certificate{}, nil, fmt.Errorf("tlsconfig: reading %s: %w", f.CA, err)
	}

	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return tls.Certificate{}, nil, fmt.Errorf("tlsconfig: %s contains no usable certificate", f.CA)
	}

	return cert, pool, nil
}
