//go:build linux

package control

import (
	"context"

	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Server) beginFilesystemOperation(ctx context.Context) (context.Context, func(), error) {
	s.operationMu.Lock()
	defer s.operationMu.Unlock()
	if s.shuttingDown.Load() {
		return nil, nil, status.Error(codes.Unavailable, "vmmd is shutting down")
	}
	if s.streamContext == nil {
		s.streamContext, s.cancelStreams = context.WithCancel(context.Background())
	}
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(s.streamContext, cancel)
	s.operations.Add(1)
	return ctx, func() { stop(); cancel(); s.operations.Done() }, nil
}

func shutdownUnaryInterceptor(serverContext context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	switch info.FullMethod {
	case vmmdv1.VMMDService_CopyImage_FullMethodName, vmmdv1.VMMDService_StatWorkspace_FullMethodName,
		vmmdv1.VMMDService_ListWorkspace_FullMethodName:
	default:
		return handler(serverContext, request)
	}
	server := info.Server.(*Server)
	ctx, end, err := server.beginFilesystemOperation(serverContext)
	if err != nil {
		return nil, err
	}
	defer end()
	return handler(ctx, request)
}

func shutdownStreamInterceptor(server any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	switch info.FullMethod {
	case vmmdv1.VMMDService_BuildImage_FullMethodName, vmmdv1.VMMDService_BuildAppliance_FullMethodName,
		vmmdv1.VMMDService_ImportImage_FullMethodName, vmmdv1.VMMDService_StageApplianceContent_FullMethodName,
		vmmdv1.VMMDService_WriteWorkspace_FullMethodName, vmmdv1.VMMDService_ExtractWorkspace_FullMethodName,
		vmmdv1.VMMDService_ReadWorkspace_FullMethodName, vmmdv1.VMMDService_ArchiveWorkspace_FullMethodName,
		vmmdv1.VMMDService_Logs_FullMethodName, vmmdv1.VMMDService_PullImage_FullMethodName,
		vmmdv1.VMMDService_Exec_FullMethodName:
	default:
		return handler(server, stream)
	}
	ctx, end, err := server.(*Server).beginFilesystemOperation(stream.Context())
	if err != nil {
		return err
	}
	defer end()
	return handler(server, &shutdownStream{ServerStream: stream, ctx: ctx})
}

// A derived context does not interrupt transport I/O. Returning the handler
// after cancellation also closes the transport calls that remain blocked.
type shutdownStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *shutdownStream) Context() context.Context { return s.ctx }
func (s *shutdownStream) RecvMsg(message any) error {
	done := make(chan error, 1)
	go func() { done <- s.ServerStream.RecvMsg(message) }()
	select {
	case err := <-done:
		return err
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}
func (s *shutdownStream) SendMsg(message any) error {
	done := make(chan error, 1)
	go func() { done <- s.ServerStream.SendMsg(message) }()
	select {
	case err := <-done:
		return err
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}
