//go:build darwin || linux

package host

import (
	"context"
	"fmt"
	"log/slog"
	"path"
	"strings"
	"sync"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/auth"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
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
		if !rpcPolicies[info.FullMethod].readOnly {
			auditRPC(controller, ctx, info.FullMethod, request, err)
		}
		return response, err
	}
}

func auditStream(controller *Controller) grpc.StreamServerInterceptor {
	return func(server any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		capture := &auditServerStream{ServerStream: stream}
		err := handler(server, capture)
		if !rpcPolicies[info.FullMethod].readOnly {
			auditRPC(controller, stream.Context(), info.FullMethod, capture.request, err)
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

func auditRPC(controller *Controller, ctx context.Context, fullMethod string, request any, rpcErr error) {
	identity, _ := auth.IdentityFromContext(ctx)
	actor := "operator"
	target := identity.EnvironmentID
	if identity.Role == auth.Agent {
		actor = identity.SessionID
		if identity.RegistrationID != "" {
			actor = fmt.Sprintf("remote:%s:%s:%d", identity.RegistrationID, identity.AllocationID, identity.AllocationGeneration)
			target = identity.AllocationID
		}
	}
	requestID := ""
	if message, ok := request.(proto.Message); ok {
		requestID = protoStringField(message.ProtoReflect(), "request_id")
		for _, field := range []protoreflect.Name{"environment_id", "container_id", "volume_id", "share_id", "link_id", "session_id"} {
			if value := protoStringField(message.ProtoReflect(), field); value != "" {
				if target == "" {
					target = value
				} else {
					target += "/" + value
				}
			}
		}
	}
	code, reason := auditOutcome(rpcErr)
	_ = controller.store.Audit(context.Background(), actor, target, path.Base(fullMethod), "", requestID, int32(code), reason)
}

func auditOutcome(rpcErr error) (codes.Code, string) {
	if rpcErr != nil {
		code := status.Code(rpcErr)
		reason, ok := hostv1.ErrorReasonOf(rpcErr)
		if ok {
			return code, strings.TrimPrefix(reason.String(), "ERROR_REASON_")
		}
		return code, ""
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
		return nil, status.Error(codes.Unavailable, "too many control requests")
	}
	return handler(ctx, request)
}

func (limiter *rpcRateLimiter) stream(server any, stream grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	if !limiter.allow() {
		return status.Error(codes.Unavailable, "too many control streams")
	}
	return handler(server, stream)
}

func deadlineUnary(ctx context.Context, request any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	if _, ok := ctx.Deadline(); ok {
		return handler(ctx, request)
	}
	timeout := 30 * time.Second
	if mutation, ok := request.(interface{ GetDeadlineUnixMillis() int64 }); ok {
		requested := time.Until(time.UnixMilli(mutation.GetDeadlineUnixMillis()))
		if requested > timeout {
			timeout = min(requested, 15*time.Minute)
		}
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
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
		slog.ErrorContext(ctx, "RPC failed", "error", err)
		return status.Error(codes.Internal, "internal operation failed")
	}
	identity, identityErr := auth.IdentityFromContext(ctx)
	remote := identityErr == nil && identity.RegistrationID != ""
	hasRetry := false
	if remote {
		clean := status.New(value.Code(), value.Message())
		for _, detail := range value.Details() {
			var detailErr error
			switch safe := detail.(type) {
			case *errdetails.ErrorInfo:
				clean, detailErr = clean.WithDetails(safe)
			case *errdetails.RetryInfo:
				hasRetry = true
				clean, detailErr = clean.WithDetails(safe)
			case *hostv1.RemoteAllocation:
				allocation := proto.Clone(safe).(*hostv1.RemoteAllocation)
				allocation.EnvironmentId = ""
				clean, detailErr = clean.WithDetails(allocation)
			}
			if detailErr != nil {
				return status.Error(value.Code(), value.Message())
			}
		}
		value = clean
	} else {
		for _, detail := range value.Details() {
			if _, ok := detail.(*errdetails.RetryInfo); ok {
				hasRetry = true
			}
		}
	}
	if !hasRetry && (value.Code() == codes.Unavailable || value.Code() == codes.ResourceExhausted) {
		withRetry, detailErr := value.WithDetails(&errdetails.RetryInfo{RetryDelay: durationpb.New(time.Second)})
		if detailErr == nil {
			value = withRetry
		}
	}
	return value.Err()
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
		hostv1.AgentRuntimeService_CopyImage_FullMethodName,
		hostv1.AgentRuntimeService_ImportImage_FullMethodName,
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

type rpcPolicy struct {
	readOnly          bool
	sessionLockExempt bool
}

var rpcPolicies = collectRPCPolicies()

func collectRPCPolicies() map[string]rpcPolicy {
	policies := make(map[string]rpcPolicy)
	protoregistry.GlobalFiles.RangeFiles(func(file protoreflect.FileDescriptor) bool {
		services := file.Services()
		for i := 0; i < services.Len(); i++ {
			service := services.Get(i)
			for j := 0; j < service.Methods().Len(); j++ {
				method := service.Methods().Get(j)
				options := method.Options().(*descriptorpb.MethodOptions)
				policy := rpcPolicy{readOnly: options.GetIdempotencyLevel() == descriptorpb.MethodOptions_NO_SIDE_EFFECTS}
				if proto.HasExtension(options, hostv1.E_MethodPolicy) {
					policy.sessionLockExempt = proto.GetExtension(options, hostv1.E_MethodPolicy).(*hostv1.MethodPolicy).GetSessionLockExempt()
				}
				policies["/"+string(service.FullName())+"/"+string(method.Name())] = policy
			}
		}
		return true
	})
	return policies
}
