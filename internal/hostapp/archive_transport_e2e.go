//go:build (darwin || linux) && e2e

package hostapp

import (
	"crypto/tls"
	"crypto/x509"
	"log"
	"net/http"
	"os"
)

// Only the E2E executable trusts the local archive fixture. Release builds use
// the platform trust store and contain no environment override.
func init() {
	path := os.Getenv("AGENT_VMM_E2E_ARCHIVE_CA")
	if path == "" {
		return
	}
	pem, err := os.ReadFile(path)
	if err != nil {
		log.Fatal(err)
	}
	roots, err := x509.SystemCertPool()
	if err != nil {
		log.Fatal(err)
	}
	if !roots.AppendCertsFromPEM(pem) {
		log.Fatal("invalid E2E archive CA")
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = &tls.Config{RootCAs: roots}
	http.DefaultTransport = transport
}
