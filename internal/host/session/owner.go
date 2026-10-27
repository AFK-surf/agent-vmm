//go:build darwin || linux

package session

import (
	"context"
	"log/slog"
	"sync"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/locks"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/grpc"
)

const defaultSessionLifetime = 8 * time.Hour

type Services interface {
	ReconcileEnvironment(context.Context, store.Environment) error
}
type RuntimeFactory interface {
	RuntimeService(context.Context) hostv1.AgentRuntimeServiceServer
}

type Owner struct {
	store                *store.Store
	services             Services
	runtime              RuntimeFactory
	directory            string
	maxMessageBytes      int
	unaryInterceptors    []grpc.UnaryServerInterceptor
	streamInterceptors   []grpc.StreamServerInterceptor
	mutationExempt       map[string]bool
	logger               *slog.Logger
	mu                   sync.Mutex
	servers              map[string]*server
	integrationRevisions map[string]uint64
	integrationLocks     locks.Keyed[string]
	sessionActivity      sync.Map
}

type Config struct {
	Store              *store.Store
	Services           Services
	Runtime            RuntimeFactory
	Directory          string
	MaxMessageBytes    int
	UnaryInterceptors  []grpc.UnaryServerInterceptor
	StreamInterceptors []grpc.StreamServerInterceptor
	MutationExempt     map[string]bool
	Logger             *slog.Logger
}

func New(config Config) *Owner {
	logger := config.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &Owner{store: config.Store, services: config.Services, runtime: config.Runtime, directory: config.Directory, maxMessageBytes: config.MaxMessageBytes, unaryInterceptors: config.UnaryInterceptors, streamInterceptors: config.StreamInterceptors, mutationExempt: config.MutationExempt, logger: logger.With("component", "host.session"), servers: make(map[string]*server), integrationRevisions: make(map[string]uint64)}
}
