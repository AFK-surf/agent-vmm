package mcpappliance

import (
	"context"
	"errors"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/modelcontextprotocol/go-sdk/auth"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const httpSessionIdleTimeout = 5 * time.Minute

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
		if identity == nil {
			return nil
		}
		frontend.mu.Lock()
		agent := frontend.agents[identity.UserID]
		frontend.mu.Unlock()
		if agent == nil {
			return nil
		}
		return agent.server
	}, &mcp.StreamableHTTPOptions{Logger: controller.logger, SessionTimeout: httpSessionIdleTimeout, MaxRequestBodyBytes: 4 << 20, PropagateRequestCancellation: true})
	authenticated := auth.RequireBearerToken(validateHTTPBinding, &auth.RequireBearerTokenOptions{AllowMissingExpiration: true})(frontend.middleware(handler))
	frontendHandler := http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Authorization") == "" {
			if token := request.URL.Query().Get("access_token"); token != "" {
				request.Header.Set("Authorization", "Bearer "+token)
			}
		}
		authenticated.ServeHTTP(response, request)
	})
	server := &http.Server{Handler: frontendHandler, ReadHeaderTimeout: 5 * time.Second, IdleTimeout: httpSessionIdleTimeout, MaxHeaderBytes: 16 << 10}
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

func validateHTTPBinding(_ context.Context, token string, _ *http.Request) (*auth.TokenInfo, error) {
	if len(token) != bindingTokenLength || token != strings.ToLower(token) {
		return nil, auth.ErrInvalidToken
	}
	for _, value := range token {
		if !((value >= '0' && value <= '9') || (value >= 'a' && value <= 'f')) {
			return nil, auth.ErrInvalidToken
		}
	}
	return &auth.TokenInfo{UserID: allocationForBinding(token)}, nil
}

func (frontend *httpFrontend) middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		identity := auth.TokenInfoFromContext(request.Context())
		if identity == nil {
			http.Error(response, "Unauthorized", http.StatusUnauthorized)
			return
		}
		if request.Header.Get("Mcp-Session-Id") != "" {
			frontend.mu.Lock()
			agent := frontend.agents[identity.UserID]
			if agent != nil && agent.runtime != nil && agent.runtime.context.Err() == nil {
				agent.active++
				agent.lastUsed = time.Now()
			}
			frontend.mu.Unlock()
			if agent == nil || agent.runtime == nil {
				next.ServeHTTP(response, request)
				return
			}
			defer frontend.finish(identity.UserID, agent, request.Header.Get("Mcp-Session-Id"), "", request.Method == http.MethodDelete)
			next.ServeHTTP(response, request)
			return
		}
		agent, err := frontend.acquire(request.Context(), identity.UserID)
		if err != nil {
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
		runtime, err := frontend.controller.openClientRuntime(frontend.context, allocationID)
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
	defer frontend.mu.Unlock()
	if agent.err != nil || agent.runtime == nil || agent.runtime.context.Err() != nil || frontend.agents[allocationID] != agent {
		if agent.err != nil {
			if frontend.agents[allocationID] == agent {
				delete(frontend.agents, allocationID)
			}
			return nil, agent.err
		}
		return nil, errors.New("MCP HTTP agent is closed")
	}
	agent.active++
	agent.lastUsed = time.Now()
	return agent, nil
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
