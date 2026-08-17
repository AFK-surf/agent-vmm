//go:build darwin

package host

import (
	"testing"

	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRequireLocalOperatorEnvironment(t *testing.T) {
	for _, test := range []struct {
		name        string
		environment store.Environment
		wantCode    codes.Code
	}{
		{name: "local", environment: store.Environment{ID: "local", OwnerKind: store.OwnerLocal}},
		{name: "remote", environment: store.Environment{ID: "remote", OwnerKind: store.OwnerRemoteRegistration}, wantCode: codes.FailedPrecondition},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := requireLocalOperatorEnvironment(test.environment)
			if test.wantCode == codes.OK {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			if status.Code(err) != test.wantCode {
				t.Fatalf("error=%v code=%s, want %s", err, status.Code(err), test.wantCode)
			}
		})
	}
}
