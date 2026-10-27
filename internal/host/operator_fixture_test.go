//go:build darwin || linux

package host

import (
	"context"
	"testing"

	"github.com/AFK-surf/agent-vmm/internal/host/auth"
	"google.golang.org/grpc"
)

func callAsOperator[T any](t *testing.T, call func(context.Context) (T, error)) (T, error) {
	t.Helper()
	var zero T
	value, err := auth.UnaryInterceptor(auth.Identity{Role: auth.Operator})(context.Background(), nil, &grpc.UnaryServerInfo{}, func(ctx context.Context, _ any) (any, error) {
		return call(ctx)
	})
	if err != nil {
		return zero, err
	}
	return value.(T), nil
}
