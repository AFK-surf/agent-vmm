package mcpserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Adapter struct {
	runtime   hostv1.AgentRuntimeServiceClient
	cleanup   CleanupOwner
	logger    *slog.Logger
	active    chan struct{}
	mutations chan struct{}
	lifetime  context.Context
	cancel    context.CancelFunc
	draining  atomic.Bool
}

func New(runtime hostv1.AgentRuntimeServiceClient, cleanup CleanupOwner, logger *slog.Logger) *Adapter {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(discardWriter{}, nil))
	}
	lifetime, cancel := context.WithCancel(context.Background())
	return &Adapter{runtime: runtime, cleanup: cleanup, logger: logger, active: make(chan struct{}, 16), mutations: make(chan struct{}, 4), lifetime: lifetime, cancel: cancel}
}

func (adapter *Adapter) Server() *mcp.Server {
	server := mcp.NewServer(&mcp.Implementation{Name: "agent-vmm-mcp", Version: "1"}, &mcp.ServerOptions{
		Logger:       adapter.logger,
		Capabilities: &mcp.ServerCapabilities{Tools: &mcp.ToolCapabilities{}},
	})
	adapter.registerReadTools(server)
	adapter.registerMutationTools(server)
	adapter.registerCleanupTools(server)
	adapter.registerServiceTools(server)
	return server
}

func (adapter *Adapter) Run(ctx context.Context, transport mcp.Transport) error {
	runContext, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		select {
		case <-runContext.Done():
			adapter.cancel()
		case <-adapter.lifetime.Done():
			cancel()
		}
	}()
	defer adapter.Close()
	return adapter.Server().Run(runContext, transport)
}

func addTool[In any](server *mcp.Server, definition toolDefinition, handler mcp.ToolHandlerFor[In, Envelope]) {
	readOnly, destructive, openWorld := definition.readOnly, definition.destructive, definition.openWorld
	tool := &mcp.Tool{
		Name: definition.name, Description: definition.description,
		InputSchema: json.RawMessage(definition.inputSchema), OutputSchema: json.RawMessage(envelopeSchema),
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: readOnly, DestructiveHint: &destructive, IdempotentHint: definition.idempotent, OpenWorldHint: &openWorld},
	}
	mcp.AddTool(server, tool, func(ctx context.Context, request *mcp.CallToolRequest, input In) (*mcp.CallToolResult, Envelope, error) {
		callContext, cancel := context.WithTimeout(ctx, definition.timeout)
		defer cancel()
		return handler(callContext, request, input)
	})
}

type toolDefinition struct {
	name, description, inputSchema               string
	readOnly, destructive, idempotent, openWorld bool
	timeout                                      time.Duration
}

const envelopeSchema = `{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","additionalProperties":false,"required":["ok"],"properties":{"ok":{"type":"boolean"},"data":{},"error":{"type":"object","additionalProperties":false,"required":["code","message","retryable"],"properties":{"code":{"type":"string"},"message":{"type":"string"},"retryable":{"type":"boolean"},"resource":{"type":"object","additionalProperties":false,"required":["kind","id"],"properties":{"kind":{"type":"string"},"id":{"type":"string"}}}}}}}`

func (adapter *Adapter) call(ctx context.Context, timeout time.Duration, mutation bool, operation func(context.Context) (any, error)) (*mcp.CallToolResult, Envelope, error) {
	if adapter.draining.Load() {
		return adapter.failure(status.Error(codes.FailedPrecondition, "server is draining"))
	}
	select {
	case adapter.active <- struct{}{}:
		defer func() { <-adapter.active }()
	case <-ctx.Done():
		return adapter.failure(ctx.Err())
	}
	if mutation {
		select {
		case adapter.mutations <- struct{}{}:
			defer func() { <-adapter.mutations }()
		case <-ctx.Done():
			return adapter.failure(ctx.Err())
		}
	}
	callContext, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	data, err := operation(callContext)
	if err != nil {
		var partial *toolFailure
		if errors.As(err, &partial) {
			mapped := mapError(partial.err)
			envelope := Envelope{OK: false, Data: partial.data, Error: &mapped}
			return &mcp.CallToolResult{IsError: true}, envelope, nil
		}
		return adapter.failure(err)
	}
	envelope := Envelope{OK: true, Data: data}
	if payload, err := json.Marshal(envelope); err != nil || len(payload) > maxResultBytes {
		return adapter.failure(status.Error(codes.ResourceExhausted, "serialized tool result exceeds limit"))
	}
	return nil, envelope, nil
}

func (adapter *Adapter) Close() {
	adapter.cancel()
}

type toolFailure struct {
	data any
	err  error
}

func (failure *toolFailure) Error() string { return failure.err.Error() }
func (failure *toolFailure) Unwrap() error { return failure.err }

func (adapter *Adapter) failure(err error) (*mcp.CallToolResult, Envelope, error) {
	mapped := mapError(err)
	envelope := Envelope{OK: false, Error: &mapped}
	return &mcp.CallToolResult{IsError: true}, envelope, nil
}

func mapError(err error) ToolError {
	code := status.Code(err)
	grpcStatus := status.Convert(err)
	result := ToolError{Code: "INTERNAL", Message: "runtime request failed"}
	switch {
	case errors.Is(err, context.Canceled), code == codes.Canceled:
		result.Code, result.Message = "CANCELED", "request was canceled"
	case errors.Is(err, context.DeadlineExceeded), code == codes.DeadlineExceeded:
		result.Code, result.Message, result.Retryable = "TIMEOUT", "request timed out", true
	case code == codes.InvalidArgument:
		result.Code, result.Message = "INVALID_ARGUMENT", "request is invalid"
	case code == codes.NotFound:
		result.Code, result.Message = "NOT_FOUND", "resource was not found"
	case code == codes.AlreadyExists:
		result.Code, result.Message = "ALREADY_EXISTS", "resource already exists"
	case code == codes.FailedPrecondition:
		result.Code, result.Message = reasonFromStatus(err, "FAILED_PRECONDITION"), "runtime precondition failed"
	case code == codes.PermissionDenied || code == codes.Unauthenticated:
		result.Code, result.Message = "PERMISSION_DENIED", "operation is not permitted"
	case code == codes.ResourceExhausted:
		result.Code, result.Message = "RESOURCE_EXHAUSTED", "resource limit exceeded"
	case code == codes.Aborted:
		result.Code, result.Message, result.Retryable = "CONFLICT", "resource changed concurrently", true
	case code == codes.Unavailable:
		result.Code, result.Message, result.Retryable = "UNAVAILABLE", "runtime is unavailable", true
	}
	for _, detail := range grpcStatus.Details() {
		switch value := detail.(type) {
		case *errdetails.ErrorInfo:
			if allowedRuntimeReason(value.GetReason()) {
				result.Code = value.GetReason()
				result.Message = grpcStatus.Message()
				result.Retryable = retryableRuntimeReason(value.GetReason()) || result.Retryable
			}
		case *errdetails.ResourceInfo:
			result.Resource = &Resource{Kind: value.GetResourceType(), ID: value.GetResourceName()}
		}
	}
	return result
}

func allowedRuntimeReason(value string) bool {
	switch value {
	case "IMAGE_REFERENCE_INVALID", "IMAGE_RESOLVE_FAILED", "IMAGE_REGISTRY_UNAVAILABLE", "IMAGE_MANIFEST_FETCH_FAILED", "IMAGE_PLATFORM_UNAVAILABLE", "IMAGE_BLOB_FETCH_FAILED", "IMAGE_AUTH_UNAVAILABLE", "IMAGE_DOWNLOAD_INTERRUPTED", "IMAGE_DOWNLOAD_TIMEOUT", "IMAGE_REGISTRY_RATE_LIMITED", "IMAGE_UNPACK_FAILED", "IMAGE_METADATA_FAILED", "IMAGE_IN_USE", "IMAGE_GENERATION_CONFLICT", "IMAGE_DIGEST_CONFLICT", "CONTAINER_NOT_RUNNING", "CONTAINER_RUNNING", "CONTAINER_GENERATION_CONFLICT", "EXEC_START_FAILED", "LOG_CURSOR_STALE", "IMAGE_NOT_FOUND", "VOLUME_MOUNTED", "PLAN_STALE", "LEASE_REVOKED", "ALLOCATION_RETAINED":
		return true
	default:
		return false
	}
}

func retryableRuntimeReason(value string) bool {
	switch value {
	case "IMAGE_REGISTRY_UNAVAILABLE", "IMAGE_MANIFEST_FETCH_FAILED", "IMAGE_BLOB_FETCH_FAILED", "IMAGE_AUTH_UNAVAILABLE", "IMAGE_DOWNLOAD_INTERRUPTED", "IMAGE_DOWNLOAD_TIMEOUT", "IMAGE_REGISTRY_RATE_LIMITED":
		return true
	default:
		return false
	}
}

func reasonFromStatus(err error, defaultReason string) string {
	for _, detail := range status.Convert(err).Details() {
		text := fmt.Sprint(detail)
		for _, reason := range []string{"CONTAINER_NOT_RUNNING", "CONTAINER_RUNNING", "CONTAINER_GENERATION_CONFLICT", "EXEC_START_FAILED", "LOG_CURSOR_STALE", "IMAGE_NOT_FOUND", "IMAGE_IN_USE", "IMAGE_GENERATION_CONFLICT", "IMAGE_DIGEST_CONFLICT", "VOLUME_MOUNTED", "PLAN_STALE", "LEASE_REVOKED", "ALLOCATION_RETAINED"} {
			if strings.Contains(text, reason) {
				return reason
			}
		}
	}
	return defaultReason
}

func pageBounds(total, requested int, cursor string) (int, int, string, error) {
	if requested == 0 {
		requested = 50
	}
	if requested < 1 || requested > 100 || len(cursor) > 1024 {
		return 0, 0, "", status.Error(codes.InvalidArgument, "invalid pagination")
	}
	start := 0
	if cursor != "" {
		value, err := strconv.Atoi(cursor)
		if err != nil || value < 0 || value > total {
			return 0, 0, "", status.Error(codes.InvalidArgument, "invalid cursor")
		}
		start = value
	}
	end := min(start+requested, total)
	next := ""
	if end < total {
		next = strconv.Itoa(end)
	}
	return start, end, next, nil
}

type discardWriter struct{}

func (discardWriter) Write(bytes []byte) (int, error) { return len(bytes), nil }
