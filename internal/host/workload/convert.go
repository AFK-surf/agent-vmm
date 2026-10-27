package workload

import (
	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
)

func EnvironmentFromStore(environment store.Environment) *hostv1.AgentEnvironment {
	origin := hostv1.EnvironmentOrigin_ENVIRONMENT_ORIGIN_LOCAL
	if environment.OwnerKind == store.OwnerRemoteRegistration {
		origin = hostv1.EnvironmentOrigin_ENVIRONMENT_ORIGIN_REMOTE
	}
	return &hostv1.AgentEnvironment{
		Id:          environment.ID,
		DisplayName: environment.DisplayName,
		NamespaceId: environment.NamespaceID,
		Limits: &hostv1.ResourceLimits{

			Pids:      new(environment.PIDs),
			DiskBytes: new(environment.DiskBytes),
		},
		EgressMode:     hostv1.EgressMode(environment.EgressMode),
		PolicyRevision: environment.PolicyRevision,
		Origin:         origin,
		Owner: &hostv1.EnvironmentOwner{
			Kind:           hostv1.EnvironmentOwnerKind(environment.OwnerKind),
			RegistrationId: environment.RegistrationID,
			AllocationId:   environment.AllocationID,
		},
	}
}
