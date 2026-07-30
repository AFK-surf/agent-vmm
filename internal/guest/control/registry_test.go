package control

import (
	"net/netip"
	"testing"
)

func TestPublicAddress(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		address string
		public  bool
	}{
		{address: "8.8.8.8", public: true},
		{address: "2606:4700:4700::1111", public: true},
		{address: "::ffff:8.8.8.8", public: true},
		{address: "127.0.0.1"},
		{address: "169.254.169.254"},
		{address: "10.0.0.1"},
		{address: "::ffff:10.0.0.1"},
		{address: "172.16.0.1"},
		{address: "192.168.1.1"},
		{address: "192.0.2.1"},
		{address: "100.64.0.1"},
		{address: "198.18.5.216", public: true},
		{address: "2001:db8::1"},
		{address: "64:ff9b::808:808"},
		{address: "::1"},
	} {
		t.Run(test.address, func(t *testing.T) {
			if got := publicAddress(netip.MustParseAddr(test.address)); got != test.public {
				t.Fatalf("publicAddress(%s) = %v", test.address, got)
			}
		})
	}
}
