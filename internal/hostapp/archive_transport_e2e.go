//go:build (darwin || linux) && e2e

package hostapp

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"
	"os"
)

// Only the E2E executable trusts the local archive fixture. Release builds use
// the platform trust store and contain no environment override.
func configureArchiveTransport() error {
	path := os.Getenv("AGENT_VMM_E2E_ARCHIVE_CA")
	if path == "" {
		return nil
	}
	pem, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	roots, err := x509.SystemCertPool()
	if err != nil {
		return err
	}
	if !roots.AppendCertsFromPEM(pem) {
		return fmt.Errorf("invalid E2E archive CA")
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = &tls.Config{RootCAs: roots}
	http.DefaultTransport = transport
	return nil
}
