package mcpappliance

import (
	"context"
	"errors"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/AFK-surf/agent-vmm/internal/mcptoken"
	"github.com/modelcontextprotocol/go-sdk/auth"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const httpSessionIdleTimeout = 5 * time.Minute
const runtimeOpenAttempts = 3

type httpFrontend struct {
	controller *Controller
	context    context.Context

	mu     sync.Mutex
	agents map[string]*httpAgent
}

type httpAgent struct {
	ready    chan struct{}
	runtime  *clientRuntime
	server   *mcp.Server
	err      error
	lastUsed time.Time
	active   int
	sessions map[string]struct{}
}

func (controller *Controller) ServeHTTPFrontend(ctx context.Context, listener net.Listener) error {
	frontend := &httpFrontend{controller: controller, context: ctx, agents: make(map[string]*httpAgent)}
	handler := mcp.NewStreamableHTTPHandler(func(request *http.Request) *mcp.Server {
		identity := auth.TokenInfoFromContext(request.Context())
		frontend.mu.Lock()
		agent := frontend.agents[identity.UserID]
		frontend.mu.Unlock()
		if agent == nil {
			return nil
		}
		return agent.server
	}, &mcp.StreamableHTTPOptions{Logger: controller.logger, SessionTimeout: httpSessionIdleTimeout, MaxRequestBodyBytes: 4 << 20, PropagateRequestCancellation: true})
	authenticated := auth.RequireBearerToken(controller.validateHTTPBinding, &auth.RequireBearerTokenOptions{AllowMissingExpiration: true})(frontend.middleware(handler))
	server := &http.Server{Handler: authenticated, ReadHeaderTimeout: 5 * time.Second, IdleTimeout: httpSessionIdleTimeout, MaxHeaderBytes: 16 << 10}
	done := make(chan struct{})
	go func() {
		defer close(done)
		frontend.reap()
	}()
	go func() {
		<-ctx.Done()
		_ = server.Close()
	}()
	err := server.Serve(listener)
	frontend.closeAll()
	<-done
	if errors.Is(err, http.ErrServerClosed) || errors.Is(err, net.ErrClosed) || ctx.Err() != nil {
		return nil
	}
	return err
}

func (controller *Controller) validateHTTPBinding(_ context.Context, token string, _ *http.Request) (*auth.TokenInfo, error) {
	controller.identity.mu.RLock()
	valid := mcptoken.Verify(controller.identity.tokenKey, token)
	controller.identity.mu.RUnlock()
	if !valid {
		return nil, auth.ErrInvalidToken
	}
	return &auth.TokenInfo{UserID: allocationForBinding(token)}, nil
}

func (frontend *httpFrontend) middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		identity := auth.TokenInfoFromContext(request.Context())
		if request.Header.Get("Mcp-Session-Id") != "" {
			agent, tracked := frontend.track(identity.UserID)
			if !tracked {
				http.Error(response, "MCP session is unavailable", http.StatusNotFound)
				return
			}
			defer frontend.finish(identity.UserID, agent, request.Header.Get("Mcp-Session-Id"), "", request.Method == http.MethodDelete)
			next.ServeHTTP(response, request)
			return
		}
		agent, err := frontend.acquire(request.Context(), identity.UserID)
		if err != nil {
			frontend.controller.logger.Error("open MCP runtime", "allocation", identity.UserID, "error", err)
			response.Header().Set("Retry-After", "1")
			http.Error(response, "MCP runtime unavailable", http.StatusServiceUnavailable)
			return
		}
		defer func() {
			frontend.finish(identity.UserID, agent, request.Header.Get("Mcp-Session-Id"), response.Header().Get("Mcp-Session-Id"), request.Method == http.MethodDelete)
		}()
		next.ServeHTTP(response, request)
	})
}

func (frontend *httpFrontend) acquire(ctx context.Context, allocationID string) (*httpAgent, error) {
	frontend.mu.Lock()
	agent := frontend.agents[allocationID]
	opener := agent == nil
	if opener {
		agent = &httpAgent{ready: make(chan struct{}), sessions: make(map[string]struct{})}
		frontend.agents[allocationID] = agent
	}
	frontend.mu.Unlock()
	if opener {
		var runtime *clientRuntime
		var err error
		for attempt := 0; attempt < runtimeOpenAttempts; attempt++ {
			runtime, err = frontend.controller.openClientRuntime(frontend.context, allocationID)
			if err == nil {
				break
			}
			if attempt+1 == runtimeOpenAttempts {
				break
			}
			timer := time.NewTimer(time.Duration(1<<attempt) * 100 * time.Millisecond)
			select {
			case <-ctx.Done():
				if !timer.Stop() {
					<-timer.C
				}
				err = ctx.Err()
				attempt = runtimeOpenAttempts
			case <-timer.C:
			}
		}
		frontend.mu.Lock()
		retained := frontend.agents[allocationID] == agent
		agent.runtime, agent.err = runtime, err
		if runtime != nil {
			agent.server = runtime.adapter.Server()
			agent.lastUsed = time.Now()
		}
		close(agent.ready)
		frontend.mu.Unlock()
		if !retained && runtime != nil {
			runtime.Close()
		} else if retained && runtime != nil {
			go frontend.removeClosed(allocationID, agent, runtime)
		}
	}
	select {
	case <-agent.ready:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	frontend.mu.Lock()
	err := agent.err
	if err != nil && frontend.agents[allocationID] == agent {
		delete(frontend.agents, allocationID)
	}
	frontend.mu.Unlock()
	if err != nil {
		return nil, err
	}
	tracked, ok := frontend.track(allocationID)
	if !ok {
		return nil, errors.New("MCP HTTP agent is closed")
	}
	return tracked, nil
}

func (frontend *httpFrontend) track(allocationID string) (*httpAgent, bool) {
	frontend.mu.Lock()
	defer frontend.mu.Unlock()
	agent := frontend.agents[allocationID]
	if agent == nil || agent.runtime == nil || agent.runtime.context.Err() != nil {
		return nil, false
	}
	agent.active++
	agent.lastUsed = time.Now()
	return agent, true
}

func (frontend *httpFrontend) removeClosed(allocationID string, agent *httpAgent, runtime *clientRuntime) {
	<-runtime.done
	frontend.mu.Lock()
	if frontend.agents[allocationID] == agent {
		delete(frontend.agents, allocationID)
	}
	frontend.mu.Unlock()
}

func (frontend *httpFrontend) finish(allocationID string, agent *httpAgent, requestSessionID, responseSessionID string, remove bool) {
	frontend.mu.Lock()
	agent.active--
	agent.lastUsed = time.Now()
	if responseSessionID != "" {
		agent.sessions[responseSessionID] = struct{}{}
	}
	if remove {
		delete(agent.sessions, requestSessionID)
	}
	closeAgent := remove && len(agent.sessions) == 0 && frontend.agents[allocationID] == agent
	if closeAgent {
		delete(frontend.agents, allocationID)
	}
	frontend.mu.Unlock()
	if closeAgent {
		agent.runtime.Close()
	}
}

func (frontend *httpFrontend) reap() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-frontend.context.Done():
			return
		case now := <-ticker.C:
			frontend.mu.Lock()
			var expired []*httpAgent
			for allocationID, agent := range frontend.agents {
				if agent.runtime != nil && agent.active == 0 && now.Sub(agent.lastUsed) >= httpSessionIdleTimeout {
					delete(frontend.agents, allocationID)
					expired = append(expired, agent)
				}
			}
			frontend.mu.Unlock()
			for _, agent := range expired {
				agent.runtime.Close()
			}
		}
	}
}

func (frontend *httpFrontend) closeAll() {
	frontend.mu.Lock()
	agents := make([]*httpAgent, 0, len(frontend.agents))
	for _, agent := range frontend.agents {
		if agent.runtime != nil {
			agents = append(agents, agent)
		}
	}
	clear(frontend.agents)
	frontend.mu.Unlock()
	for _, agent := range agents {
		agent.runtime.Close()
	}
}
