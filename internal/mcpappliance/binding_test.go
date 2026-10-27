package mcpappliance

import (
	"bytes"
	"context"
	"github.com/AFK-surf/agent-vmm/internal/mcptoken"
	"strings"
	"testing"
)

func TestHTTPBindingUsesSameAllocationIdentity(t *testing.T) {
	controller := NewController(&Identity{tokenKey: testMCPKey}, nil)
	token := issueHTTPTestToken(t)
	identity, err := controller.validateHTTPBinding(context.Background(), token, nil)
	if err != nil {
		t.Fatal(err)
	}
	if identity.UserID != allocationForBinding(token) {
		t.Fatalf("HTTP allocation = %q", identity.UserID)
	}
	for _, invalid := range []string{strings.ToUpper(token), token[:len(token)-1], strings.Repeat("z", bindingTokenLength)} {
		if _, err := controller.validateHTTPBinding(context.Background(), invalid, nil); err == nil {
			t.Fatalf("accepted HTTP binding %q", invalid)
		}
	}
}

var testMCPKey = bytes.Repeat([]byte{7}, mcptoken.KeySize)

func issueHTTPTestToken(t *testing.T) string {
	t.Helper()
	token, err := mcptoken.Create(testMCPKey)
	if err != nil {
		t.Fatal(err)
	}
	return token
}
