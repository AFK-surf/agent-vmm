//go:build darwin && !development && !e2e

package main

import (
	"crypto/ed25519"
	"encoding/hex"
)

var trustRoot = mustPublicKey("3d4017c3e843895a92b70aa74d1b7ebc9c982ccf2ec4968cc0cd55f12af4660c")

func mustPublicKey(value string) ed25519.PublicKey {
	key, err := hex.DecodeString(value)
	if err != nil || len(key) != ed25519.PublicKeySize {
		panic("invalid embedded trust root")
	}
	return key
}
