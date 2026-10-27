package mcptoken

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
)

const KeySize = 32
const InstallationID = "agent-vmm-mcp"
const FrontendName = "mcp-http"

func Create(key []byte) (string, error) {
	if len(key) != KeySize {
		return "", errors.New("MCP signing key must contain 32 bytes")
	}
	value := make([]byte, 32)
	if _, err := rand.Read(value[:16]); err != nil {
		return "", err
	}
	mac := hmac.New(sha256.New, key)
	mac.Write(value[:16])
	copy(value[16:], mac.Sum(nil)[:16])
	return hex.EncodeToString(value), nil
}

func Verify(key []byte, token string) bool {
	if len(key) != KeySize || len(token) != 64 || token != strings.ToLower(token) {
		return false
	}
	value, err := hex.DecodeString(token)
	if err != nil {
		return false
	}
	mac := hmac.New(sha256.New, key)
	mac.Write(value[:16])
	return hmac.Equal(value[16:], mac.Sum(nil)[:16])
}
