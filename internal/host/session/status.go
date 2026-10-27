//go:build darwin || linux

package session

import (
	"context"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/stats"
)

func (owner *Owner) markSessionActivity(sessionID string) {
	value, _ := owner.sessionActivity.LoadOrStore(sessionID, &atomic.Int64{})
	last := value.(*atomic.Int64)
	now := time.Now().UnixNano()
	for {
		previous := last.Load()
		if previous != 0 && now-previous < int64(5*time.Second) {
			return
		}
		if last.CompareAndSwap(previous, now) {
			_ = owner.store.MarkSessionActivity(context.Background(), sessionID)
			return
		}
	}
}

type server struct {
	integrationID        string
	integrationRevision  uint64
	server               *grpc.Server
	listener             net.Listener
	path                 string
	registrationID       string
	allocationID         string
	allocationGeneration uint64
	connections          atomic.Uint32
	lifetime             context.Context
	cancel               context.CancelFunc
	expiresAt            time.Time
	environmentID        string
	revoked              bool
	connected            atomic.Bool
	timer                *time.Timer
	stopOnce             sync.Once
	stopDone             chan struct{}
}

type sessionStats struct {
	owner       *Owner
	sessionID   string
	connections *atomic.Uint32
}

func (handler *sessionStats) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context {
	return ctx
}

func (handler *sessionStats) HandleRPC(context.Context, stats.RPCStats) {}

func (handler *sessionStats) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	return ctx
}

func (handler *sessionStats) HandleConn(_ context.Context, event stats.ConnStats) {
	switch event.(type) {
	case *stats.ConnBegin:
		handler.connections.Add(1)
		handler.owner.mu.Lock()
		tracked := handler.owner.servers[handler.sessionID]
		handler.owner.mu.Unlock()
		if tracked != nil && tracked.connected.CompareAndSwap(false, true) {
			if err := handler.owner.store.MarkSessionConnected(context.Background(), handler.sessionID, tracked.integrationID, tracked.integrationRevision); err != nil {
				handler.owner.logger.Warn("mark session connected", "session_id", handler.sessionID, "error", err)
			}
		}
	case *stats.ConnEnd:
		if handler.connections.Add(^uint32(0)) == 0 {
			go handler.owner.closeSession(handler.sessionID, "CLIENT_DISCONNECTED")
		}
	}
}

func (owner *Owner) stopSession(ctx context.Context, id string) error {
	owner.mu.Lock()
	session := owner.servers[id]
	owner.mu.Unlock()
	if session != nil {
		session.timer.Stop()
		session.cancel()
	}
	return owner.drainSession(ctx, id)
}

func (owner *Owner) drainSession(ctx context.Context, id string) error {
	owner.mu.Lock()
	session := owner.servers[id]
	if session != nil {
		session.revoked = true
	}
	owner.mu.Unlock()
	if session == nil {
		return nil
	}
	session.stopOnce.Do(func() {
		go func() {
			session.server.GracefulStop()
			session.listener.Close()
			_ = os.Remove(session.path)
			owner.mu.Lock()
			if owner.servers[id] == session {
				delete(owner.servers, id)
			}
			owner.mu.Unlock()
			close(session.stopDone)
		}()
	})
	select {
	case <-session.stopDone:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
