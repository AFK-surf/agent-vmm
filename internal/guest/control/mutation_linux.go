//go:build linux

package control

import (
	"context"
	"crypto/sha256"
	"fmt"
	"time"

	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func (s *Server) mutate(ctx context.Context, operation string, request proto.Message, function func() (any, error)) (any, error) {
	if operation != "prepare" {
		s.operationMu.Lock()
		if s.shuttingDown.Load() {
			s.operationMu.Unlock()
			return nil, status.Error(codes.Unavailable, "vmmd is shutting down")
		}
		s.operations.Add(1)
		s.operationMu.Unlock()
		defer s.operations.Done()
	}
	requestID := mutationRequestID(request)
	if err := validateRequestID(requestID); err != nil {
		return nil, invalid(err)
	}
	bytes, err := proto.MarshalOptions{Deterministic: true}.Marshal(request)
	if err != nil {
		return nil, err
	}
	fingerprint := fmt.Sprintf("%x", sha256.Sum256(bytes))
	return s.dedupe.do(ctx, operation+"/"+mutationScope(request)+"/"+requestID, fingerprint, func() (any, error) {
		value, err := function()
		if err == nil {
			s.events.publish(&vmmdv1.RuntimeEvent{Type: operation, GuestBootId: s.health.BootID, RequestId: requestID, TimestampUnixNano: time.Now().UnixNano()})
		}
		return value, err
	})
}

func mutationRequestID(request proto.Message) string {
	switch request := request.(type) {
	case *vmmdv1.ReconcileSystemImagesRequest:
		return request.GetRequestId()
	case *vmmdv1.PrepareShutdownRequest:
		return request.GetRequestId()
	case *vmmdv1.EnsureSystemApplianceRequest:
		return request.GetRequestId()
	case *vmmdv1.StopSystemApplianceRequest:
		return request.GetRequestId()
	case *vmmdv1.DeleteSystemApplianceDataRequest:
		return request.GetRequestId()
	case *vmmdv1.EnsureNamespaceRequest:
		return request.GetRequestId()
	case *vmmdv1.DeleteNamespaceRequest:
		return request.GetRequestId()
	case *vmmdv1.PullImageRequest:
		return request.GetRequestId()
	case *vmmdv1.ImportImageHeader:
		return request.GetRequestId()
	case *vmmdv1.DeleteImageRequest:
		return request.GetRequestId()
	case *vmmdv1.CreateVolumeRequest:
		return request.GetRequestId()
	case *vmmdv1.DeleteVolumeRequest:
		return request.GetRequestId()
	case *vmmdv1.CreateContainerRequest:
		return request.GetRequestId()
	case *vmmdv1.StartContainerRequest:
		return request.GetRequestId()
	case *vmmdv1.StopContainerRequest:
		return request.GetRequestId()
	case *vmmdv1.DeleteContainerRequest:
		return request.GetRequestId()
	case *vmmdv1.SignalContainerRequest:
		return request.GetRequestId()
	case *vmmdv1.ApplyNetworkLinkRequest:
		return request.GetRequestId()
	case *vmmdv1.DeleteNetworkLinkRequest:
		return request.GetRequestId()
	case *vmmdv1.ApplyServiceProjectionRequest:
		return request.GetRequestId()
	case *vmmdv1.DeleteServiceProjectionRequest:
		return request.GetRequestId()
	default:
		return ""
	}
}

func mutationScope(request proto.Message) string {
	if scoped, ok := request.(interface{ GetActorNamespaceId() string }); ok {
		return scoped.GetActorNamespaceId()
	}
	if scoped, ok := request.(interface{ GetNamespaceId() string }); ok {
		return scoped.GetNamespaceId()
	}
	if scoped, ok := request.(interface {
		GetIdentity() *vmmdv1.SystemApplianceIdentity
	}); ok {
		return scoped.GetIdentity().GetInstallationId()
	}
	if scoped, ok := request.(interface{ GetLink() *vmmdv1.NetworkLink }); ok {
		return scoped.GetLink().GetSourceNamespaceId()
	}
	if scoped, ok := request.(interface {
		GetProjection() *vmmdv1.ServiceProjection
	}); ok {
		return scoped.GetProjection().GetSourceNamespaceId()
	}
	return ""
}
