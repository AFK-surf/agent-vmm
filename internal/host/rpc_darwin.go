//go:build darwin

package host

import (
	"context"
	"path"
	"strings"
	"sync"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/auth"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/durationpb"
)

type rpcRateLimiter struct {
	mu        sync.Mutex
	tokens    float64
	capacity  float64
	perSecond float64
	updated   time.Time
}

func auditUnary(controller *Controller) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		response, err := handler(ctx, request)
		if isMutation(info.FullMethod) {
			auditRPC(controller, ctx, info.FullMethod, request, response, err)
		}
		return response, err
	}
}

func remoteRuntimeUnary(ownerKind int32) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if ownerKind == store.OwnerRemoteRegistration && remoteRuntimeDenied(info.FullMethod) {
			return nil, status.Error(codes.PermissionDenied, "remote runtime session cannot manage environment lifecycle")
		}
		return handler(ctx, request)
	}
}

func remoteRuntimeStream(ownerKind int32) grpc.StreamServerInterceptor {
	return func(server any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if ownerKind == store.OwnerRemoteRegistration && remoteRuntimeDenied(info.FullMethod) {
			return status.Error(codes.PermissionDenied, "remote runtime session cannot manage environment lifecycle")
		}
		return handler(server, stream)
	}
}

func remoteRuntimeDenied(method string) bool {
	switch method {
	case hostv1.AgentRuntimeService_GetEnvironment_FullMethodName,
		hostv1.AgentRuntimeService_PullImage_FullMethodName,
		hostv1.AgentRuntimeService_CreateContainer_FullMethodName,
		hostv1.AgentRuntimeService_StartContainer_FullMethodName,
		hostv1.AgentRuntimeService_StopContainer_FullMethodName,
		hostv1.AgentRuntimeService_WaitContainer_FullMethodName,
		hostv1.AgentRuntimeService_DeleteContainer_FullMethodName,
		hostv1.AgentRuntimeService_GetContainer_FullMethodName,
		hostv1.AgentRuntimeService_ListContainers_FullMethodName,
		hostv1.AgentRuntimeService_Logs_FullMethodName,
		hostv1.AgentRuntimeService_Exec_FullMethodName,
		hostv1.AgentRuntimeService_SignalContainer_FullMethodName,
		hostv1.AgentRuntimeService_CreateVolume_FullMethodName,
		hostv1.AgentRuntimeService_DeleteVolume_FullMethodName,
		hostv1.AgentRuntimeService_GetVolume_FullMethodName,
		hostv1.AgentRuntimeService_ListVolumes_FullMethodName,
		hostv1.AgentRuntimeService_ForwardTCP_FullMethodName,
		hostv1.AgentRuntimeService_BuildImage_FullMethodName,
		hostv1.AgentRuntimeService_ReleaseSession_FullMethodName:
		return false
	default:
		return true
	}
}

func auditStream(controller *Controller) grpc.StreamServerInterceptor {
	return func(server any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		capture := &auditServerStream{ServerStream: stream}
		err := handler(server, capture)
		if isMutation(info.FullMethod) {
			auditRPC(controller, stream.Context(), info.FullMethod, capture.request, nil, err)
		}
		return err
	}
}

type auditServerStream struct {
	grpc.ServerStream
	request any
}

func (stream *auditServerStream) RecvMsg(message any) error {
	err := stream.ServerStream.RecvMsg(message)
	if err == nil && stream.request == nil {
		stream.request = proto.Clone(message.(proto.Message))
	}
	return err
}

func auditRPC(controller *Controller, ctx context.Context, fullMethod string, request, response any, rpcErr error) {
	identity, _ := auth.IdentityFromContext(ctx)
	actor := "operator"
	target := identity.EnvironmentID
	if identity.Role == auth.Agent {
		actor = identity.SessionID
		if identity.RegistrationID != "" {
			actor = "remote:" + identity.RegistrationID + ":" + identity.LeaseID
			target = identity.AllocationID
		}
	}
	requestID := ""
	if message, ok := request.(proto.Message); ok {
		requestID = protoStringField(message.ProtoReflect(), "request_id")
		for _, field := range []protoreflect.Name{"environment_id", "container_id", "volume_id", "link_id", "session_id"} {
			if value := protoStringField(message.ProtoReflect(), field); value != "" {
				if target == "" {
					target = value
				} else {
					target += "/" + value
				}
			}
		}
	}
	code, reason := auditOutcome(response, rpcErr)
	_ = controller.store.Audit(context.Background(), actor, target, path.Base(fullMethod), "", requestID, int32(code), reason)
}

func auditOutcome(response any, rpcErr error) (codes.Code, string) {
	if rpcErr != nil {
		code := status.Code(rpcErr)
		return code, "RPC_" + strings.ToUpper(code.String())
	}
	if result, ok := response.(*hostv1.OperationResponse); ok && result.GetOperation().GetState() == hostv1.OperationState_OPERATION_STATE_FAILED {
		reason := result.GetOperation().GetError().GetReason()
		if reason == "" {
			reason = "OPERATION_FAILED"
		}
		return codes.Internal, reason
	}
	return codes.OK, ""
}

func protoStringField(message protoreflect.Message, name protoreflect.Name) string {
	if field := message.Descriptor().Fields().ByName(name); field != nil && field.Kind() == protoreflect.StringKind && message.Has(field) {
		return message.Get(field).String()
	}
	var result string
	message.Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		if field.Kind() != protoreflect.MessageKind || !message.Has(field) {
			return true
		}
		switch {
		case field.IsList():
			list := value.List()
			for index := 0; index < list.Len() && result == ""; index++ {
				result = protoStringField(list.Get(index).Message(), name)
			}
		case field.IsMap():
			if field.MapValue().Kind() == protoreflect.MessageKind {
				value.Map().Range(func(_ protoreflect.MapKey, entry protoreflect.Value) bool {
					result = protoStringField(entry.Message(), name)
					return result == ""
				})
			}
		default:
			result = protoStringField(value.Message(), name)
		}
		return result == ""
	})
	return result
}

func isMutation(method string) bool {
	name := path.Base(method)
	switch name {
	case "GetEnvironment", "GetContainer", "ListContainers", "WaitContainer", "GetVolume", "ListVolumes", "Logs",
		"GetVM", "GetSystemSnapshot", "ListEnvironments", "WatchEvents", "WatchMetrics", "GetOperation",
		"PlanResetEnvironment", "PlanDiscardEnvironment", "PreviewSupportBundle":
		return false
	default:
		return true
	}
}

func newRPCRateLimiter(perSecond, burst int) *rpcRateLimiter {
	return &rpcRateLimiter{tokens: float64(burst), capacity: float64(burst), perSecond: float64(perSecond), updated: time.Now()}
}

func (limiter *rpcRateLimiter) allow() bool {
	limiter.mu.Lock()
	defer limiter.mu.Unlock()
	now := time.Now()
	limiter.tokens = min(limiter.capacity, limiter.tokens+now.Sub(limiter.updated).Seconds()*limiter.perSecond)
	limiter.updated = now
	if limiter.tokens < 1 {
		return false
	}
	limiter.tokens--
	return true
}

func (limiter *rpcRateLimiter) unary(ctx context.Context, request any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	if !limiter.allow() {
		return nil, retryable("RPC_RATE_LIMITED", "too many control requests", time.Second)
	}
	return handler(ctx, request)
}

func (limiter *rpcRateLimiter) stream(server any, stream grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	if !limiter.allow() {
		return retryable("RPC_RATE_LIMITED", "too many control streams", time.Second)
	}
	return handler(server, stream)
}

func deadlineUnary(ctx context.Context, request any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	if _, ok := ctx.Deadline(); ok {
		return handler(ctx, request)
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	return handler(ctx, request)
}

func errorDetailsUnary(ctx context.Context, request any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	response, err := handler(ctx, request)
	return response, addErrorDetails(ctx, err)
}

func errorDetailsStream(server any, stream grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	return addErrorDetails(stream.Context(), handler(server, stream))
}

func addErrorDetails(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	value, ok := status.FromError(err)
	if !ok {
		return err
	}
	identity, identityErr := auth.IdentityFromContext(ctx)
	remote := identityErr == nil && identity.RegistrationID != ""
	if len(value.Details()) != 0 {
		if !remote {
			return err
		}
		clean := status.New(value.Code(), value.Message())
		for _, detail := range value.Details() {
			var detailErr error
			switch safe := detail.(type) {
			case *errdetails.ErrorInfo:
				clean, detailErr = clean.WithDetails(safe)
			case *errdetails.RetryInfo:
				clean, detailErr = clean.WithDetails(safe)
			}
			if detailErr != nil {
				return status.Error(value.Code(), value.Message())
			}
		}
		return clean.Err()
	}
	reason := "RPC_" + strings.ToUpper(value.Code().String())
	withDetails, detailErr := value.WithDetails(&errdetails.ErrorInfo{Reason: reason, Domain: "agentvmm.afk.surf"})
	if detailErr != nil {
		return err
	}
	if identityErr == nil && identity.EnvironmentID != "" && !remote {
		withDetails, detailErr = withDetails.WithDetails(&errdetails.ResourceInfo{ResourceType: "environment", ResourceName: identity.EnvironmentID})
		if detailErr != nil {
			return err
		}
	}
	if value.Code() == codes.Unavailable || value.Code() == codes.ResourceExhausted {
		withDetails, detailErr = withDetails.WithDetails(&errdetails.RetryInfo{RetryDelay: durationpb.New(time.Second)})
		if detailErr != nil {
			return err
		}
	}
	return withDetails.Err()
}

func deadlineStream(server any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	if _, ok := stream.Context().Deadline(); ok {
		return handler(server, stream)
	}
	duration := 30 * time.Minute
	switch info.FullMethod {
	case hostv1.AgentRuntimeService_Exec_FullMethodName,
		hostv1.AgentRuntimeService_ForwardTCP_FullMethodName,
		hostv1.AgentRuntimeService_BuildImage_FullMethodName,
		hostv1.AgentRuntimeService_Logs_FullMethodName,
		hostv1.OperatorService_WatchEvents_FullMethodName,
		hostv1.OperatorService_WatchMetrics_FullMethodName,
		hostv1.OperatorService_Logs_FullMethodName:
		duration = 8 * time.Hour
	}
	ctx, cancel := context.WithTimeout(stream.Context(), duration)
	defer cancel()
	return handler(server, &hostContextStream{ServerStream: stream, context: ctx})
}

type hostContextStream struct {
	grpc.ServerStream
	context context.Context
}

func (stream *hostContextStream) Context() context.Context { return stream.context }
