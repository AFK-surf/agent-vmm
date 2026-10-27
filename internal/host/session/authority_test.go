//go:build darwin || linux

package session

import (
	"context"
	"net"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/auth"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func TestIntegrationMutationRechecksRevisionAfterAdmissionLock(t *testing.T) {
	owner, integration := authorityFixture(t)
	client, probe, entered := authoritySessionProbe(t, owner, integration, "old")
	unlock, lockErr := owner.LockIntegration(context.Background(), integration.ID)
	if lockErr != nil {
		t.Fatal(lockErr)
	}

	result := make(chan error, 1)
	go func() {
		_, err := client.StartContainer(t.Context(), &hostv1.StartContainerRequest{RequestId: "not-yet-started", ContainerId: "component", ExpectedGenerationId: "generation"})
		result <- err
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		unlock()
		t.Fatal("old RPC did not reach admission")
	}
	_, err := owner.store.CommitAgentIntegration(t.Context(), "new", integration.ID, integration.Revision, "new", 2)
	owner.mu.Lock()
	delete(owner.integrationRevisions, integration.ID)
	owner.mu.Unlock()
	unlock()
	if err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		if status.Code(err) != codes.PermissionDenied || probe.starts.Load() != 0 {
			t.Fatalf("stale mutation=%v starts=%d", err, probe.starts.Load())
		}
	case <-time.After(time.Second):
		t.Fatal("stale RPC did not finish")
	}
	if _, err := client.ReleaseSession(t.Context(), &hostv1.ReleaseSessionRequest{RequestId: "release-own"}); err != nil {
		t.Fatalf("stale session could not release itself: %v", err)
	}
	until := time.Now().Add(time.Second)
	for {
		owner.mu.Lock()
		_, active := owner.servers["old"]
		owner.mu.Unlock()
		if !active {
			break
		}
		if time.Now().After(until) {
			t.Fatal("released stale session remained active")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestIntegrationCommitFencesSessionsPublishedBeforeWriteLock(t *testing.T) {
	t.Run("canceled-before-admission", func(t *testing.T) {
		owner, integration := authorityFixture(t)
		authoritySessionProbe(t, owner, integration, "existing")
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		if _, err := owner.LockIntegrationCommit(ctx, integration.ID, integration.Revision); err != context.Canceled {
			t.Fatalf("canceled commit admission=%v", err)
		}
		if err := owner.authorizeSession(t.Context(), "existing"); err != nil {
			t.Fatalf("canceled commit revoked the current authority: %v", err)
		}
		record, err := owner.store.GetSession(t.Context(), "existing")
		if err != nil || record.RevokedAt != nil {
			t.Fatalf("canceled commit changed the current receipt=%+v %v", record, err)
		}
	})
	for _, newerIntent := range []bool{false, true} {
		t.Run(map[bool]string{false: "prior-intent", true: "newer-intent"}[newerIntent], func(t *testing.T) {
			owner, integration := authorityFixture(t)
			authoritySessionProbe(t, owner, integration, "existing")
			owner.mu.Lock()
			lifetime := owner.servers["existing"].lifetime
			owner.mu.Unlock()
			unlock, err := owner.LockIntegration(t.Context(), integration.ID)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			committed := make(chan error, 1)
			go func() {
				release, err := owner.LockIntegrationCommit(ctx, integration.ID, integration.Revision)
				if err == nil {
					release()
				}
				committed <- err
			}()
			select {
			case <-lifetime.Done():
			case <-ctx.Done():
				unlock()
				t.Fatal("commit did not revoke the existing session")
			}
			pendingIntent := integration
			if newerIntent {
				pendingIntent, err = owner.store.CommitAgentIntegration(t.Context(), "new-intent", integration.ID, integration.Revision, "next", 2)
				if err != nil {
					unlock()
					t.Fatal(err)
				}
			}
			// The current creator still holds the write lock when it publishes its session.
			authoritySessionProbe(t, owner, pendingIntent, "pending")
			unlock()
			if err := <-committed; err != nil {
				t.Fatal(err)
			}
			authorizeErr := owner.authorizeSession(t.Context(), "pending")
			if newerIntent && authorizeErr != nil || !newerIntent && status.Code(authorizeErr) != codes.PermissionDenied {
				t.Fatalf("pending session authority crossed the intent revision: %v", authorizeErr)
			}
			record, err := owner.store.GetSession(t.Context(), "pending")
			if err != nil || (record.RevokedAt != nil) == newerIntent {
				t.Fatalf("pending session receipt=%+v %v", record, err)
			}
		})
	}
}

func TestSessionRevocationEndsMemoryAuthorityWhenPersistenceFails(t *testing.T) {
	owner, integration := authorityFixture(t)
	_, _, _ = authoritySessionProbe(t, owner, integration, "session")
	owner.mu.Lock()
	lifetime := owner.servers["session"].lifetime
	owner.mu.Unlock()
	if err := owner.store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := owner.Revoke(t.Context(), "session"); err == nil {
		t.Fatal("revocation hid persistence failure")
	}
	if lifetime.Err() != context.Canceled {
		t.Fatal("failed persistence left the session lifetime open")
	}
	if err := owner.authorizeSession(t.Context(), "session"); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("revoked memory authority = %v", err)
	}
}

type authorityRuntimeProbe struct {
	hostv1.UnimplementedAgentRuntimeServiceServer
	starts atomic.Int32
}

func (probe *authorityRuntimeProbe) StartContainer(context.Context, *hostv1.StartContainerRequest) (*hostv1.StartContainerResponse, error) {
	probe.starts.Add(1)
	return &hostv1.StartContainerResponse{}, nil
}
func (probe *authorityRuntimeProbe) ReleaseSession(ctx context.Context, _ *hostv1.ReleaseSessionRequest) (*hostv1.ReleaseSessionResponse, error) {
	EndAfterResponse(ctx)
	return &hostv1.ReleaseSessionResponse{}, nil
}
func authorityFixture(t *testing.T) (*Owner, store.AgentIntegration) {
	t.Helper()
	database, err := store.Open(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { database.Close() })
	if _, err := database.CreateEnvironment(t.Context(), store.Environment{ID: "environment", NamespaceID: "namespace", OwnerKind: store.OwnerLocal, PIDs: 1, DiskBytes: 1, EgressMode: 1}); err != nil {
		t.Fatal(err)
	}
	integration, err := database.CreateAgentIntegration(t.Context(), store.AgentIntegration{ID: "integration", EnvironmentID: "environment", AdapterID: "service", DisplayName: "Service", Scope: 1, ConfigTarget: "image", ConfigFingerprint: "first", DesiredState: 1, ConfigState: 2, PrepareRequestID: "prepare"})
	if err != nil {
		t.Fatal(err)
	}
	return New(Config{Store: database, MutationExempt: map[string]bool{hostv1.AgentRuntimeService_ReleaseSession_FullMethodName: true}}), integration
}
func authoritySessionProbe(t *testing.T, owner *Owner, integration store.AgentIntegration, id string) (hostv1.AgentRuntimeServiceClient, *authorityRuntimeProbe, <-chan string) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	lifetime, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	tracked := &server{timer: time.NewTimer(time.Hour), lifetime: lifetime, cancel: cancel, expiresAt: time.Now().Add(time.Hour), environmentID: integration.EnvironmentID, listener: listener, integrationID: integration.ID, integrationRevision: integration.Revision, stopDone: make(chan struct{})}
	entered := make(chan string, 8)
	identity := auth.Identity{Role: auth.Agent, SessionID: id, EnvironmentID: integration.EnvironmentID, NamespaceID: "namespace"}
	grpcServer := grpc.NewServer(grpc.WaitForHandlers(true), grpc.ChainUnaryInterceptor(auth.UnaryInterceptor(identity), func(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		entered <- info.FullMethod
		return handler(ctx, request)
	}, owner.unary(id)))
	tracked.server = grpcServer
	probe := new(authorityRuntimeProbe)
	hostv1.RegisterAgentRuntimeServiceServer(grpcServer, probe)
	if err := owner.store.CreateAuthorizedSession(t.Context(), store.Session{ID: id, IntegrationID: integration.ID, EnvironmentID: integration.EnvironmentID, SocketPath: listener.Addr().String(), Origin: 1, CreatedAt: time.Now(), ExpiresAt: time.Now().Add(time.Hour)}, integration.Revision); err != nil {
		t.Fatal(err)
	}
	owner.mu.Lock()
	owner.servers[id] = tracked
	owner.integrationRevisions[integration.ID] = integration.Revision
	owner.mu.Unlock()
	go grpcServer.Serve(listener)
	t.Cleanup(func() { grpcServer.Stop(); listener.Close() })
	connection, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { connection.Close() })
	return hostv1.NewAgentRuntimeServiceClient(connection), probe, entered
}

func TestIntegrationRevocationPropagatesStorageFailure(t *testing.T) {
	owner, integration := authorityFixture(t)
	authoritySessionProbe(t, owner, integration, "existing")
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := owner.RevokeIntegration(ctx, integration.ID); err == nil {
		t.Fatal("revocation swallowed storage failure")
	}
	if err := owner.authorizeSession(t.Context(), "existing"); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("memory authority remained available: %v", err)
	}
}
