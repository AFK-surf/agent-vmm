//go:build linux || (darwin && (!keychain || !cgo))

package hostapp

import "context"

func isGUIHost(context.Context) bool { return false }
