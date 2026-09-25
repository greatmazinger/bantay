package authn

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"errors"
	"math/big"
	"net/url"
	"testing"
	"time"
)

// certWithURIs returns a self-signed certificate with the given URI SANs.
// Identity doesn't verify the certificate, so there's no need for the CA chain.
func certWithURIs(t *testing.T, uris ...string) *x509.Certificate {
	t.Helper()
	var parsed []*url.URL
	for _, s := range uris {
		u, err := url.Parse(s)
		if err != nil {
			t.Fatalf("test URI %q, %v", s, err)
		}
		parsed = append(parsed, u)
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		URIs:         parsed,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}

	return cert
}

func TestIdentity(t *testing.T) {
	const astrid = "spiffe://jobworker.local/client/astrid"

	tests := []struct {
		name string
		uris []string
		want string // empty means ErrNoIdentity
	}{
		{"client identity", []string{astrid}, astrid},
		{"admin identity", []string{"spiffe://jobworker.local/admin/astrid"}, "spiffe://jobworker.local/admin/astrid"},
		{"empty path identity", []string{"spiffe://jobworker.local"}, "spiffe://jobworker.local"},
		{"no URI SAN", nil, ""},
		{"two URI SANs", []string{astrid, "spiffe://jobworker.local/client/bheng"}, ""},
		{"wrong scheme", []string{"https://jobworker.local/client/bheng"}, ""},
		{"wrong trust domain", []string{"spiffe://otherservice.local/client/bheng"}, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Identity(certWithURIs(t, tt.uris...))
			if tt.want == "" {
				if !errors.Is(err, ErrNoIdentity) {
					t.Fatalf("err = %v, expected ErrNoIdentity", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Identity: %v", err)
			}
			if got != tt.want {
				t.Errorf("Identity = %q, expected %q", got, tt.want)
			}
		})
	}
}
