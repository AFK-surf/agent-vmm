//go:build darwin && (development || e2e)

package main

import (
	"crypto/ed25519"
	"encoding/hex"
)

var trustRoot = mustPublicKey("d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a")

func mustPublicKey(value string) ed25519.PublicKey {
	key, err := hex.DecodeString(value)
	if err != nil || len(key) != ed25519.PublicKeySize {
		panic("invalid embedded trust root")
	}
	return key
}
