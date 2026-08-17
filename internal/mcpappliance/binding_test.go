package mcpappliance

import (
	"context"
	"strings"
	"testing"
)

func TestHTTPBindingUsesSameAllocationIdentity(t *testing.T) {
	token := strings.Repeat("0a", bindingTokenLength/2)
	identity, err := validateHTTPBinding(context.Background(), token, nil)
	if err != nil {
		t.Fatal(err)
	}
	if identity.UserID != allocationForBinding(token) {
		t.Fatalf("HTTP allocation = %q", identity.UserID)
	}
	for _, invalid := range []string{strings.ToUpper(token), token[:len(token)-1], strings.Repeat("z", bindingTokenLength)} {
		if _, err := validateHTTPBinding(context.Background(), invalid, nil); err == nil {
			t.Fatalf("accepted HTTP binding %q", invalid)
		}
	}
}
