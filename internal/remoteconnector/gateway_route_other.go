//go:build !darwin || !cgo

package remoteconnector

import (
	"context"
	"errors"
	"syscall"
)

func gatewayRoutes(context.Context, string) []gatewayRoute { return nil }
func gatewaySocketControl(int) func(string, string, syscall.RawConn) error {
	return func(string, string, syscall.RawConn) error { return errors.New("scoped gateway route is unavailable") }
}
