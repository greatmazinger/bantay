// Command gencerts issues the certificates the job worker needs: one
// self-signed CA, one server certificate, and one certificate per client
// identity.
//
// Certificates are generated rather than committed so that no private key ever
// enters version control. Run it with `make certs`.
package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"flag"
	"fmt"
	"math/big"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"time"
)

const (
	trustDomain = "spiffe://jobworker.local"
	validFor    = 365 * 24 * time.Hour
)

// Identities issued by default.
//
// bob exists so that authorization can be tested: a caller acting on a job it
// does not own needs a second identity. alice-admin exists so the
// administrative role can be, and deliberately belongs to the same person as
// alice: holding both is the intended way to use it, reaching for the
// administrative certificate only when acting on someone else's job.
var identities = []struct {
	file string // certificate file stem
	path string // SPIFFE path beneath the trust domain
}{
	{"alice", "/client/alice"},
	{"bob", "/client/bob"},
	{"alice-admin", "/admin/alice"},
}

func main() {
	outDir := flag.String("out", "certs", "directory to write certificates into")
	force := flag.Bool("force", false, "regenerate even if certificates already exist")
	flag.Parse()

	if err := run(*outDir, *force); err != nil {
		fmt.Fprintf(os.Stderr, "gencerts: %v\n", err)
		os.Exit(1)
	}
}

func run(outDir string, force bool) error {
	caCertPath := filepath.Join(outDir, "ca.crt")
	if _, err := os.Stat(caCertPath); err == nil && !force {
		fmt.Printf("%s already exists, nothing to do (use -force to regenerate)\n", caCertPath)
		return nil
	}

	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return err
	}

	// The CA. Self-signed, and the only thing that signs anything else.
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return err
	}
	caTmpl := &x509.Certificate{
		SerialNumber:          serial(),
		Subject:               pkix.Name{CommonName: "jobworker-ca"},
		NotBefore:             time.Now().Add(-time.Hour), // tolerate clock skew
		NotAfter:              time.Now().Add(validFor),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
		MaxPathLenZero:        true, // issues leaves only, never intermediates
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		return err
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		return err
	}
	if err := writePair(outDir, "ca", caDER, caKey); err != nil {
		return err
	}
	fmt.Printf("  ca.crt          self-signed, valid %s\n", validFor)

	// The server. Carries a DNS and IP SAN so that ordinary hostname
	// verification works, plus its own SPIFFE ID. ExtKeyUsageServerAuth is what
	// stops this certificate being presented as a client credential, which is
	// what makes a single CA for both sides safe.
	serverURI, err := url.Parse(trustDomain + "/server")
	if err != nil {
		return err
	}
	serverTmpl := &x509.Certificate{
		SerialNumber:          serial(),
		Subject:               pkix.Name{CommonName: "jobworker-server"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(validFor),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		DNSNames:              []string{"localhost"},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")},
		URIs:                  []*url.URL{serverURI},
	}
	if err := issue(outDir, "server", serverTmpl, caCert, caKey); err != nil {
		return err
	}
	fmt.Printf("  server.crt      %s, DNS:localhost\n", serverURI)

	// Clients. No DNS or IP SANs: they are never dialled, only presented.
	// Exactly one URI SAN, which the server validates. Whether an identity is
	// administrative is carried by its path, so nothing else distinguishes
	// these certificates from one another.
	for _, id := range identities {
		uri, err := url.Parse(trustDomain + id.path)
		if err != nil {
			return err
		}
		tmpl := &x509.Certificate{
			SerialNumber:          serial(),
			Subject:               pkix.Name{CommonName: id.file},
			NotBefore:             time.Now().Add(-time.Hour),
			NotAfter:              time.Now().Add(validFor),
			KeyUsage:              x509.KeyUsageDigitalSignature,
			ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
			BasicConstraintsValid: true,
			URIs:                  []*url.URL{uri},
		}
		if err := issue(outDir, id.file, tmpl, caCert, caKey); err != nil {
			return err
		}
		fmt.Printf("  %-15s %s\n", id.file+".crt", uri)
	}

	return nil
}

func issue(dir, name string, tmpl, parent *x509.Certificate, parentKey *ecdsa.PrivateKey) error {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return err
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, parent, &key.PublicKey, parentKey)
	if err != nil {
		return err
	}
	return writePair(dir, name, der, key)
}

func writePair(dir, name string, der []byte, key *ecdsa.PrivateKey) error {
	certPath := filepath.Join(dir, name+".crt")
	if err := writePEM(certPath, "CERTIFICATE", der, 0o644); err != nil {
		return err
	}

	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return err
	}
	return writePEM(filepath.Join(dir, name+".key"), "PRIVATE KEY", keyDER, 0o600)
}

func writePEM(path, blockType string, der []byte, mode os.FileMode) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, mode)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := pem.Encode(f, &pem.Block{Type: blockType, Bytes: der}); err != nil {
		return err
	}
	return f.Chmod(mode) // O_CREATE respects umask; be explicit for the key
}

func serial() *big.Int {
	max := new(big.Int).Lsh(big.NewInt(1), 128)
	n, err := rand.Int(rand.Reader, max)
	if err != nil {
		panic(err) // crypto/rand failing is not a recoverable condition
	}
	return n
}
