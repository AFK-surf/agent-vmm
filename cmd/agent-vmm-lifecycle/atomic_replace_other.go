//go:build !darwin && !linux

package main

import "errors"

func atomicSwapApp(_, _ string) error {
	return errors.New("atomic application bundle replacement is unsupported on this platform")
}
