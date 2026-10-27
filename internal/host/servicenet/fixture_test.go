//go:build darwin || linux

package servicenet

import (
	"context"
	"net"
	"net/netip"
	"strings"
	"testing"

	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/auth"
	"github.com/AFK-surf/agent-vmm/internal/host/egress"
	"github.com/AFK-surf/agent-vmm/internal/host/guestconn"
	"github.com/AFK-surf/agent-vmm/internal/host/workload"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

type listenerWorkloadRun struct{ environmentID, namespaceID, containerID, instanceID string }
type listenerWorkloadState struct {
	workloadRuns map[string]*listenerWorkloadRun
}
type listenerWorkloads struct {
	*workload.Owner
	instances map[string]workload.Instance
}

func (value *listenerWorkloads) Instance(environment, container string) (workload.Instance, bool) {
	result, exists := value.instances[workloadRunKey(environment, container)]
	return result, exists
}
func workloadRunKey(environment, container string) string { return environment + "\x00" + container }
func newTestServiceOwner(t *testing.T, owner *Owner, state listenerWorkloadState) *Owner {
	t.Helper()
	fresh := New(Config{Store: owner.store, Egress: egress.New(), Guest: &guestconn.Connection{}})
	fresh.serviceListeners = owner.serviceListeners
	workloads := &listenerWorkloads{instances: make(map[string]workload.Instance)}
	for key, run := range state.workloadRuns {
		if run.environmentID == "" {
			parts := strings.SplitN(key, "\x00", 2)
			run.environmentID = parts[0]
			run.containerID = parts[1]
		}
		workloads.instances[workloadRunKey(run.environmentID, run.containerID)] = workload.Instance{InstanceID: run.instanceID}
	}
	fresh.BindWorkloads(workloads)
	return fresh
}
func setServiceTestInstance(owner *Owner, environment, container, instance string) {
	value := owner.workloads.(*listenerWorkloads)
	key := workloadRunKey(environment, container)
	if instance == "" {
		delete(value.instances, key)
	} else {
		value.instances[key] = workload.Instance{InstanceID: instance}
	}
}

func serviceRecoveryClient(t *testing.T, guest vmmdv1.VMMDServiceServer) vmmdv1.VMMDServiceClient {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	vmmdv1.RegisterVMMDServiceServer(server, guest)
	go server.Serve(listener)
	t.Cleanup(server.Stop)
	connection, err := grpc.NewClient("passthrough:///service-recovery", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { connection.Close() })
	return vmmdv1.NewVMMDServiceClient(connection)
}

func callAsOperator[T any](t *testing.T, call func(context.Context) (T, error)) (T, error) {
	t.Helper()
	var zero T
	value, err := auth.UnaryInterceptor(auth.Identity{Role: auth.Operator})(context.Background(), nil, &grpc.UnaryServerInfo{}, func(ctx context.Context, _ any) (any, error) {
		return call(ctx)
	})
	if err != nil {
		return zero, err
	}
	return value.(T), nil
}

type emptyServiceSources struct{}

func (emptyServiceSources) EgressSources(context.Context, vmmdv1.VMMDServiceClient, string) ([]egress.Source, error) {
	return nil, nil
}
func (emptyServiceSources) HostTCPInfrastructureEndpoints() []netip.AddrPort { return nil }
