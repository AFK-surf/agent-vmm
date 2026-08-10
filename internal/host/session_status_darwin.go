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
	server      *grpc.Server
	listener    net.Listener
	path        string
	connections atomic.Uint32
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
			_ = handler.controller.store.VerifyAgentIntegration(context.Background(), session.IntegrationID)
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
