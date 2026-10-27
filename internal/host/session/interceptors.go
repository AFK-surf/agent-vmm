//go:build darwin || linux

package session

import (
	"context"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"google.golang.org/grpc"
)

func (owner *Owner) unary(sessionID string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		ctx, cancel := owner.sessionContext(ctx, sessionID)
		defer cancel()
		end := &sessionResponseEnd{}
		ctx = context.WithValue(ctx, sessionResponseEndKey{}, end)
		unlock, err := owner.lockSessionMutation(ctx, sessionID, info.FullMethod)
		if err != nil {
			return nil, err
		}
		defer unlock()
		if info.FullMethod != hostv1.AgentRuntimeService_ReleaseSession_FullMethodName {
			if err := owner.authorizeSession(ctx, sessionID); err != nil {
				return nil, err
			}
		}
		owner.markSessionActivity(sessionID)
		response, err := handler(ctx, request)
		if end.requested {
			owner.mu.Lock()
			if tracked := owner.servers[sessionID]; tracked != nil {
				tracked.revoked = true
			}
			owner.mu.Unlock()
			go func() {
				if err := owner.Revoke(context.Background(), sessionID); err != nil {
					owner.logger.Warn("revoke session", "session_id", sessionID, "error", err)
				}
			}()
		}
		return response, err
	}
}

func (owner *Owner) stream(sessionID string) grpc.StreamServerInterceptor {
	return func(server any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx, cancel := owner.sessionContext(stream.Context(), sessionID)
		defer cancel()
		stream = &sessionContextStream{ServerStream: stream, context: ctx}
		unlock, err := owner.lockSessionMutation(stream.Context(), sessionID, info.FullMethod)
		if err != nil {
			return err
		}
		defer unlock()
		if err := owner.authorizeSession(stream.Context(), sessionID); err != nil {
			return err
		}
		owner.markSessionActivity(sessionID)
		return handler(server, stream)
	}
}

type sessionContextStream struct {
	grpc.ServerStream
	context context.Context
}

func (stream *sessionContextStream) Context() context.Context { return stream.context }

func (stream *sessionContextStream) RecvMsg(message any) error {
	result := make(chan error, 1)
	go func() { result <- stream.ServerStream.RecvMsg(message) }()
	select {
	case err := <-result:
		return err
	case <-stream.context.Done():
		return stream.context.Err()
	}
}
