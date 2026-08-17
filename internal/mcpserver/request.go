package mcpserver

import (
	"crypto/rand"
	"encoding/hex"
)

func newRequestID() (string, error) {
	value := make([]byte, 16)
	if _, err := rand.Read(value); err != nil {
		return "", err
	}
	return "mcp-" + hex.EncodeToString(value), nil
}
