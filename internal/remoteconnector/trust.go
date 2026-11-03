package remoteconnector

import (
	"context"
	"errors"
	"fmt"
	connectorv1 "github.com/AFK-surf/agent-vmm/internal/api/connector/v1"
	"net"
	"time"

	trustv1 "github.com/AFK-surf/agent-vmm/api/trust/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func probeTrust(ctx context.Context, socketPath string) error {
	client, requestContext, closeClient, err := trustClient(ctx, socketPath)
	if err != nil {
		return err
	}
	defer closeClient()
	identity, err := client.GetDeviceIdentity(requestContext, &trustv1.GetDeviceIdentityRequest{})
	if err != nil {
		return err
	}
	if identity.GetDeviceId() == "" || len(identity.GetRootPublicKey()) == 0 {
		return fmt.Errorf("host returned an incomplete device identity")
	}
	return nil
}

func signEndpointObservation(ctx context.Context, socketPath, endpointID string, generation uint64) (*trustv1.EndpointObservation, error) {
	if endpointID == "" || generation == 0 {
		return nil, errors.New("complete data endpoint identity is required")
	}
	client, requestContext, closeClient, err := trustClient(ctx, socketPath)
	if err != nil {
		return nil, err
	}
	defer closeClient()
	identity, err := client.GetDeviceIdentity(requestContext, &trustv1.GetDeviceIdentityRequest{})
	if err != nil {
		return nil, err
	}
	return client.SignEndpointObservation(requestContext, &trustv1.SignEndpointObservationRequest{UnsignedObservation: &trustv1.EndpointObservation{
		DeviceId: identity.GetDeviceId(), RootKeyRevision: identity.GetRootKeyRevision(), EndpointNodeId: []byte(endpointID), EndpointGeneration: generation,
		SupportedAlpns: []string{"agent-vmm/mesh-control/1", "agent-vmm/pairing/1", "agent-vmm/service-tunnel/1"},
		FeatureSet:     []string{"direct", "relay"}, ExpiresAt: timestamppb.New(time.Now().Add(10 * time.Minute)),
	}})
}

func trustClient(ctx context.Context, socketPath string) (connectorv1.TrustServiceClient, context.Context, func(), error) {
	return trustClientWithTimeout(ctx, socketPath, 5*time.Second)
}

func trustClientWithTimeout(ctx context.Context, socketPath string, timeout time.Duration) (connectorv1.TrustServiceClient, context.Context, func(), error) {
	dialer := func(ctx context.Context, _ string) (net.Conn, error) {
		var dialer net.Dialer
		return dialer.DialContext(ctx, "unix", socketPath)
	}
	connection, err := grpc.NewClient("passthrough:///agent-vmm-trust", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(dialer))
	if err != nil {
		return nil, nil, nil, err
	}
	requestContext, cancel := context.WithTimeout(ctx, timeout)
	closeClient := func() { cancel(); _ = connection.Close() }
	return connectorv1.NewTrustServiceClient(connection), requestContext, closeClient, nil
}
