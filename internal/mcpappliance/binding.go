package mcpappliance

import (
	"crypto/sha256"
	"encoding/hex"
)

const bindingTokenLength = 64

func allocationForBinding(token string) string {
	digest := sha256.Sum256([]byte(token))
	return "mcp-" + hex.EncodeToString(digest[:])
}
