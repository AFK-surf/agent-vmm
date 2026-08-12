package remoteconnector

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"time"

	trustv1 "github.com/AFK-surf/agent-vmm/api/trust/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const maxTrustCapabilityBytes = 256

func probeTrust(ctx context.Context, socketPath, capabilityPath string) error {
	client, requestContext, closeClient, err := trustClient(ctx, socketPath, capabilityPath)
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

func enrollmentIdentity(ctx context.Context, socketPath, capabilityPath string) (*trustv1.DeviceIdentity, error) {
	client, requestContext, closeClient, err := trustClient(ctx, socketPath, capabilityPath)
	if err != nil {
		return nil, err
	}
	defer closeClient()
	identity, err := client.GetDeviceIdentity(requestContext, &trustv1.GetDeviceIdentityRequest{})
	if err != nil || identity.GetDeviceId() == "" || len(identity.GetRootPublicKey()) != 33 || identity.GetRootKeyRevision() == 0 {
		return nil, errors.New("host device identity is unavailable")
	}
	return identity, nil
}

func installManagedTrust(ctx context.Context, socketPath, capabilityPath string, anchor *trustv1.TrustAnchor, credential *trustv1.MembershipCredential) error {
	if anchor == nil || credential == nil {
		return errors.New("controller managed trust bundle is incomplete")
	}
	client, requestContext, closeClient, err := trustClient(ctx, socketPath, capabilityPath)
	if err != nil {
		return err
	}
	defer closeClient()
	_, err = client.InstallManagedTrust(requestContext, &trustv1.InstallManagedTrustRequest{TrustAnchor: anchor, MembershipCredential: credential})
	return err
}

func signEndpointObservation(ctx context.Context, socketPath, capabilityPath, endpointID string, generation uint64) (*trustv1.EndpointObservation, error) {
	if endpointID == "" || generation == 0 {
		return nil, errors.New("complete data endpoint identity is required")
	}
	client, requestContext, closeClient, err := trustClient(ctx, socketPath, capabilityPath)
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

func trustClient(ctx context.Context, socketPath, capabilityPath string) (trustv1.TrustServiceClient, context.Context, func(), error) {
	return trustClientWithTimeout(ctx, socketPath, capabilityPath, 5*time.Second)
}

func trustClientWithTimeout(ctx context.Context, socketPath, capabilityPath string, timeout time.Duration) (trustv1.TrustServiceClient, context.Context, func(), error) {
	capability, err := os.ReadFile(capabilityPath)
	if err != nil {
		return nil, nil, nil, err
	}
	if len(capability) < 32 || len(capability) > maxTrustCapabilityBytes {
		return nil, nil, nil, errors.New("connector trust capability is invalid")
	}
	dialer := func(ctx context.Context, _ string) (net.Conn, error) {
		var dialer net.Dialer
		return dialer.DialContext(ctx, "unix", socketPath)
	}
	connection, err := grpc.NewClient("passthrough:///agent-vmm-trust", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(dialer))
	if err != nil {
		return nil, nil, nil, err
	}
	requestContext, cancel := context.WithTimeout(ctx, timeout)
	requestContext = metadata.NewOutgoingContext(requestContext, metadata.Pairs("authorization", string(capability)))
	closeClient := func() { cancel(); _ = connection.Close() }
	return trustv1.NewTrustServiceClient(connection), requestContext, closeClient, nil
}
