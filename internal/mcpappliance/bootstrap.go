package mcpappliance

import (
	"errors"
	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	"github.com/AFK-surf/agent-vmm/internal/appliancebootstrap"
	"github.com/AFK-surf/agent-vmm/internal/mcptoken"
	"sync"
)

type Identity struct {
	mu             sync.RWMutex
	registrationID string
	tokenKey       []byte
}

func (identity *Identity) set(registrationID string, key []byte) {
	identity.mu.Lock()
	identity.registrationID = registrationID
	identity.tokenKey = append(identity.tokenKey[:0], key...)
	identity.mu.Unlock()
}

func (identity *Identity) RegistrationID() string {
	identity.mu.RLock()
	defer identity.mu.RUnlock()
	return identity.registrationID
}

type BootstrapServer struct {
	appliancev1.UnimplementedApplianceBootstrapServiceServer
	identity *Identity
	store    appliancebootstrap.SeedStore
}

func NewBootstrapServer(identity *Identity, stateDirectory string) *BootstrapServer {
	return &BootstrapServer{identity: identity, store: appliancebootstrap.SeedStore{Directory: stateDirectory}}
}
func (server *BootstrapServer) Bootstrap(stream appliancebootstrap.Stream) error {
	return appliancebootstrap.Serve(stream, appliancebootstrap.Hooks{
		OnClaim:    server.store.Initialize,
		OnEnvelope: server.store.Accept,
		BeforeReceipt: func(_ appliancebootstrap.Stream, claim *appliancev1.ApplianceLaunchClaim, _ *appliancev1.BootstrapReceipt) error {
			key, err := server.store.Seed()
			if err != nil {
				return err
			}
			defer clear(key)
			if len(key) != mcptoken.KeySize {
				return errors.New("MCP bootstrap key must contain 32 bytes")
			}
			server.identity.set(claim.GetRegistrationId(), key)
			return nil
		},
	})
}
