// Package authn extracts a caller's identity from its client certificate.
//
// It does not verify the certificate. The package assumes that the layer
// above has already checked the signature and the chain to the CA. As an
// authentication layer, this package answers "who is this". The layers
// above are tasked with the question "should this identity be trusted".
package authn

import (
	"crypto/x509"
	"errors"
	"fmt"
)

// trustDomain is the bare host, compared against url.URL.host
// gencerts holds the same value as a URI. Any changes here or there
// must be done together.
const trustDomain = "jobworker.local"

// ErrNoIdentity is the umbrella error for every way a certificate can
// fail to carry a valid SPIFFE ID. This ensures that calls to Authenticate
// fail securely. The error message carries the reason for the server layer's
// log.
var ErrNoIdentity = errors.New("authn: no valid SPIFFE ID for this trust domain")

// Identity returns the SPIFFE ID in cert's URI SAN.
// Exactly one URI SAN is required. Anything else is rejected.
// No URI SAN is no identity. More than one is ambiguous.
func Identity(cert *x509.Certificate) (string, error) {
	if n := len(cert.URIs); n != 1 {
		return "", fmt.Errorf("authn: %d URI SANs, want 1: %w", n, ErrNoIdentity)
	}
	u := cert.URIs[0]
	if u.Scheme != "spiffe" {
		return "", fmt.Errorf("authn: scheme %q: %w", u.Scheme, ErrNoIdentity)
	}
	if u.Host != trustDomain {
		return "", fmt.Errorf("authn: trust domain %q: %w", u.Host, ErrNoIdentity)
	}
	return u.String(), nil
}
