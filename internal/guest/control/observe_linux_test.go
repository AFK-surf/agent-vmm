//go:build linux

package control

import (
	"os"
	"path/filepath"
	"testing"
)

func TestReadNetworkUsage(t *testing.T) {
	directory := t.TempDir()
	if err := os.WriteFile(filepath.Join(directory, "rx_bytes"), []byte("1234\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, "tx_bytes"), []byte("5678\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	usage, err := readNetworkUsage(directory)
	if err != nil {
		t.Fatal(err)
	}
	if usage.GetNetworkRxBytes() != 1234 || usage.GetNetworkTxBytes() != 5678 || usage.GetSampledAtUnixNano() == 0 {
		t.Fatalf("readNetworkUsage() = %+v", usage)
	}
}
