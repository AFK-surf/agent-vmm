//go:build darwin || linux

package host

import (
	"context"
	"log/slog"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/session"
	"google.golang.org/grpc"
)

type runtimeSessionFactory struct{ controller *Controller }

func (factory runtimeSessionFactory) RuntimeService(lifetime context.Context) hostv1.AgentRuntimeServiceServer {
	return &agentServer{controller: factory.controller, buildLifetime: lifetime}
}

func assembleSessions(controller *Controller) *session.Owner {
	exempt := make(map[string]bool)
	for method, policy := range rpcPolicies {
		exempt[method] = policy.readOnly || policy.sessionLockExempt
	}
	return session.New(session.Config{Store: controller.store, Services: controller.services, Runtime: runtimeSessionFactory{controller}, Directory: controller.sessionDirectory, MaxMessageBytes: maxMessageBytes, UnaryInterceptors: []grpc.UnaryServerInterceptor{auditUnary(controller), deadlineUnary, errorDetailsUnary}, StreamInterceptors: []grpc.StreamServerInterceptor{auditStream(controller), deadlineStream, errorDetailsStream}, MutationExempt: exempt, Logger: slog.Default()})
}
