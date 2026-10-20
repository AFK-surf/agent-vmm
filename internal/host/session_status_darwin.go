//go:build darwin

package host

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/stats"
)

func randomID() (string, error) {
	bytes := make([]byte, 16)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes), nil
}

func stableID(kind, requestID string) string {
	sum := sha256.Sum256([]byte(kind + "\x00" + requestID))
	return hex.EncodeToString(sum[:16])
}

func listenUnix(path string) (net.Listener, error) {
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	listener, err := net.Listen("unix", path)
	if err != nil {
		return nil, err
	}
	if err := os.Chmod(path, 0o600); err != nil {
		listener.Close()
		return nil, err
	}
	return listener, nil
}

func (controller *Controller) markSessionActivity(sessionID string) {
	value, _ := controller.sessionActivity.LoadOrStore(sessionID, &atomic.Int64{})
	last := value.(*atomic.Int64)
	now := time.Now().UnixNano()
	for {
		previous := last.Load()
		if previous != 0 && now-previous < int64(5*time.Second) {
			return
		}
		if last.CompareAndSwap(previous, now) {
			_ = controller.store.MarkSessionActivity(context.Background(), sessionID)
			return
		}
	}
}

type sessionServer struct {
	integrationID        string
	integrationRevision  uint64
	server               *grpc.Server
	listener             net.Listener
	path                 string
	registrationID       string
	allocationID         string
	allocationGeneration uint64
	connections          atomic.Uint32
	cancelBuilds         context.CancelFunc
	stopOnce             sync.Once
	stopDone             chan struct{}
}

type sessionStats struct {
	controller  *Controller
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
		_ = handler.controller.store.MarkSessionConnected(context.Background(), handler.sessionID)
		if session, err := handler.controller.store.GetSession(context.Background(), handler.sessionID); err == nil && session.IntegrationID != "" {
			handler.controller.mu.Lock()
			tracked := handler.controller.sessions[handler.sessionID]
			handler.controller.mu.Unlock()
			if tracked != nil {
				_ = handler.controller.store.VerifyAgentIntegration(context.Background(), session.IntegrationID, tracked.integrationRevision)
			}
		}
	case *stats.ConnEnd:
		if handler.connections.Add(^uint32(0)) == 0 {
			go handler.controller.closeSession(handler.sessionID, "CLIENT_DISCONNECTED")
		}
	}
}

type readyListener struct {
	net.Listener
	ready chan struct{}
	once  sync.Once
}

func (listener *readyListener) Accept() (net.Conn, error) {
	listener.once.Do(func() { close(listener.ready) })
	return listener.Listener.Accept()
}

func (controller *Controller) stopSession(ctx context.Context, id string) error {
	controller.mu.Lock()
	session := controller.sessions[id]
	controller.mu.Unlock()
	if session != nil && session.cancelBuilds != nil {
		session.cancelBuilds()
	}
	return controller.stopSessionHandlers(ctx, id)
}

func (controller *Controller) stopSessionHandlers(ctx context.Context, id string) error {
	controller.mu.Lock()
	session := controller.sessions[id]
	if session != nil && session.stopDone == nil {
		session.stopDone = make(chan struct{})
	}
	controller.mu.Unlock()
	if session == nil {
		return nil
	}
	session.stopOnce.Do(func() {
		go func() {
			session.server.Stop()
			session.listener.Close()
			_ = os.Remove(session.path)
			controller.mu.Lock()
			if controller.sessions[id] == session {
				delete(controller.sessions, id)
			}
			controller.mu.Unlock()
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
