package auth

import (
	"context"
	"errors"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type Role uint8

const (
	Agent Role = iota + 1
	Operator
	RemoteConnector
)

type Identity struct {
	Role                 Role
	SessionID            string
	EnvironmentID        string
	NamespaceID          string
	RegistrationID       string
	AllocationID         string
	AllocationGeneration uint64
	ExecutionOwnerID     string
}

type identityKey struct{}

func UnaryInterceptor(identity Identity) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, request any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		return handler(context.WithValue(ctx, identityKey{}, identity), request)
	}
}

func StreamInterceptor(identity Identity) grpc.StreamServerInterceptor {
	return func(server any, stream grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		return handler(server, &authenticatedStream{ServerStream: stream, ctx: context.WithValue(stream.Context(), identityKey{}, identity)})
	}
}

func RemoteConnectorUnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, request any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		identity, err := remoteConnectorMetadataIdentity(ctx)
		if err != nil {
			return nil, err
		}
		if err := authorizeRequestRegistration(identity, request); err != nil {
			return nil, err
		}
		return handler(context.WithValue(ctx, identityKey{}, identity), request)
	}
}

func authorizeRequestRegistration(identity Identity, request any) error {
	if identity.Role == RemoteConnector {
		if scoped, ok := request.(interface{ GetRegistrationId() string }); ok && scoped.GetRegistrationId() != identity.RegistrationID {
			return status.Error(codes.PermissionDenied, "remote registration does not match transport identity")
		}
	}
	return nil
}

func remoteConnectorMetadataIdentity(ctx context.Context) (Identity, error) {
	values := metadata.ValueFromIncomingContext(ctx, "x-agent-vmm-registration")
	if len(values) != 1 || values[0] == "" || len(values[0]) > 128 {
		return Identity{}, status.Error(codes.Unauthenticated, "remote registration transport identity is required")
	}
	return Identity{Role: RemoteConnector, RegistrationID: values[0]}, nil
}

func AgentIdentity(ctx context.Context) (Identity, error) {
	identity, ok := ctx.Value(identityKey{}).(Identity)
	if !ok || identity.Role != Agent || identity.SessionID == "" || identity.EnvironmentID == "" || identity.NamespaceID == "" {
		return Identity{}, status.Error(codes.Unauthenticated, "agent session is not authenticated")
	}
	return identity, nil
}

func IdentityFromContext(ctx context.Context) (Identity, error) {
	identity, ok := ctx.Value(identityKey{}).(Identity)
	if !ok {
		return Identity{}, errors.New("missing authenticated identity")
	}
	return identity, nil
}

type authenticatedStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (stream *authenticatedStream) Context() context.Context { return stream.ctx }
