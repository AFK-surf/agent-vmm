//go:build darwin || linux

package operation

import (
	"crypto/sha256"
	"encoding/hex"
)

func StableID(kind, requestID string) string {
	sum := sha256.Sum256([]byte(kind + "\x00" + requestID))
	return hex.EncodeToString(sum[:16])
}
