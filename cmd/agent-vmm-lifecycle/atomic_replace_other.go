//go:build !darwin && !linux

package main

import "errors"

func atomicSwapApp(_, _ string) error {
	return errors.New("atomic application bundle replacement is unsupported on this platform")
}

func atomicPublishApp(_, _ string) error {
	return errors.New("atomic application publication is unsupported on this platform")
}
