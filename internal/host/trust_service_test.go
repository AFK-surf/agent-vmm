//go:build darwin || linux

package host

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"io"
	"math/big"
	"net"
	"path/filepath"
	"testing"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	servicev1 "github.com/AFK-surf/agent-vmm/api/service/v1"
	trustv1 "github.com/AFK-surf/agent-vmm/api/trust/v1"
	hosttrust "github.com/AFK-surf/agent-vmm/internal/host/trust"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type trustTestKeys struct{ key *ecdsa.PrivateKey }

func (provider *trustTestKeys) Ensure(context.Context, uint64) ([]byte, string, error) {
	if provider.key == nil {
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			return nil, "", err
		}
		provider.key = key
	}
	return elliptic.MarshalCompressed(elliptic.P256(), provider.key.X, provider.key.Y), "test", nil
}

func (provider *trustTestKeys) Sign(_ context.Context, _ uint64, digest []byte) ([]byte, error) {
	r, s, err := ecdsa.Sign(rand.Reader, provider.key, digest)
	if err != nil {
		return nil, err
	}
	half := new(big.Int).Rsh(new(big.Int).Set(elliptic.P256().Params().N), 1)
	if s.Cmp(half) > 0 {
		s.Sub(elliptic.P256().Params().N, s)
	}
	result := make([]byte, sha256.Size*2)
	r.FillBytes(result[:sha256.Size])
	s.FillBytes(result[sha256.Size:])
	return result, nil
}

func (*trustTestKeys) Delete(context.Context, uint64) error { return nil }

func TestTrustServiceRejectsSameProcessWithoutConnectorCapability(t *testing.T) {
	database, err := openTestStore(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	manager, err := hosttrust.New(context.Background(), database, &trustTestKeys{})
	if err != nil {
		t.Fatal(err)
	}
	controller := &Controller{trust: manager}
	capability := []byte("0123456789abcdef0123456789abcdef")
	server, err := controller.NewTrustServer(capability)
	if err != nil {
		t.Fatal(err)
	}
	listener := bufconn.Listen(1 << 20)
	go server.Serve(listener)
	defer server.Stop()
	connection, err := grpc.NewClient("passthrough:///trust", grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	client := trustv1.NewTrustServiceClient(connection)
	if _, err := client.GetDeviceIdentity(context.Background(), &trustv1.GetDeviceIdentityRequest{}); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("same-process client without capability code=%s err=%v", status.Code(err), err)
	}
	ctx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs(connectorAuthorizationHeader, string(capability)))
	if identity, err := client.GetDeviceIdentity(ctx, &trustv1.GetDeviceIdentityRequest{}); err != nil || identity.GetDeviceId() == "" {
		t.Fatalf("authorized connector identity=%+v err=%v", identity, err)
	}
	if _, err := hostv1.NewOperatorServiceClient(connection).GetDeviceIdentity(ctx, &hostv1.GetDeviceIdentityRequest{}); status.Code(err) != codes.Unimplemented {
		t.Fatalf("operator method crossed trust socket code=%s err=%v", status.Code(err), err)
	}
	serviceClient := servicev1.NewServiceRouteServiceClient(connection)
	forward, err := serviceClient.OpenServiceForward(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := forward.Send(&servicev1.OpenServiceForwardRequest{Payload: &servicev1.OpenServiceForwardRequest_Header{Header: &servicev1.OpenServiceForwardHeader{RouteId: "route"}}}); err != nil && !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
	if _, err := forward.Recv(); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("stream bypassed connector capability code=%s err=%v", status.Code(err), err)
	}
	if _, err := serviceClient.ListServiceExports(ctx, &servicev1.ListServiceExportsRequest{}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("connector enumerated exports code=%s err=%v", status.Code(err), err)
	}
}
