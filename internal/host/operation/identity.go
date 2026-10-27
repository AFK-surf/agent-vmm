package operation

import (
	"crypto/sha256"
	"encoding/hex"
)

func DerivedRequestID(value string) string {
	digest := sha256.Sum256([]byte(value))
	return "reconcile-" + hex.EncodeToString(digest[:26])
}
