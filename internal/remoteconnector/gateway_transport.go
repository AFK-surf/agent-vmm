package remoteconnector

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"time"

	"google.golang.org/grpc/credentials"
)

const (
	gatewayDialBudget     = 20 * time.Second
	gatewayLookupBudget   = 2 * time.Second
	gatewayAttemptBudget  = 3 * time.Second
	gatewayCandidateLimit = 8
	gatewayRouteLimit     = 3
)

type gatewayRoute struct {
	index   int
	servers []string
}

type gatewayTransport struct {
	credentials.TransportCredentials
	routes        func(context.Context, string) []gatewayRoute
	lookup        func(context.Context, string, gatewayRoute) ([]netip.Addr, error)
	dial          func(context.Context, string, int) (net.Conn, error)
	attemptBudget time.Duration
	verifier      *byte
}

func newGatewayTransport(tls credentials.TransportCredentials) *gatewayTransport {
	return &gatewayTransport{TransportCredentials: tls, routes: gatewayRoutes, lookup: lookupGateway, dial: dialGatewaySocket, attemptBudget: gatewayAttemptBudget, verifier: new(byte)}
}

// TLS runs before the dialer returns so gRPC owns and cancels the winning socket.
// The stock credentials supply both verification and AuthInfo; this adapter never
// promotes an unverified connection to a secure transport.
type verifiedGatewayConn struct {
	net.Conn
	auth     credentials.AuthInfo
	verifier *byte
}

func (t *gatewayTransport) ClientHandshake(ctx context.Context, _ string, raw net.Conn) (net.Conn, credentials.AuthInfo, error) {
	conn, ok := raw.(*verifiedGatewayConn)
	if !ok || conn.verifier != t.verifier {
		raw.Close()
		return nil, nil, errors.New("gateway connection was not verified by this transport")
	}
	if err := ctx.Err(); err != nil {
		conn.Close()
		return nil, nil, err
	}
	return conn, conn.auth, nil
}
func (t *gatewayTransport) Clone() credentials.TransportCredentials {
	clone := *t
	clone.TransportCredentials = t.TransportCredentials.Clone()
	return &clone
}

func (t *gatewayTransport) Dial(ctx context.Context, endpoint string) (net.Conn, error) {
	ctx, cancel := context.WithTimeout(ctx, gatewayDialBudget)
	defer cancel()
	host, port, err := net.SplitHostPort(endpoint)
	if err != nil {
		return nil, err
	}
	seen := make(map[string]bool)
	attempts := 0
	var last error
	tryRoute := func(route gatewayRoute) (net.Conn, bool) {
		lookupCtx, stop := context.WithTimeout(ctx, gatewayLookupBudget)
		addresses, lookupErr := t.lookup(lookupCtx, host, route)
		stop()
		if lookupErr != nil {
			last = fmt.Errorf("gateway DNS: %w", lookupErr)
			return nil, false
		}
		// Leave room for another interface when one route returns many addresses.
		count := 0
		for _, ip := range addresses {
			if !ip.IsValid() || ip.IsUnspecified() || ip.IsMulticast() {
				continue
			}
			key := fmt.Sprintf("%d/%s", route.index, ip.Unmap())
			if seen[key] {
				continue
			}
			seen[key] = true
			if count >= 3 || attempts >= gatewayCandidateLimit || ctx.Err() != nil {
				break
			}
			count++
			if attempts > 0 {
				delay := 100 * time.Millisecond * time.Duration(1<<min(attempts-1, 2))
				timer := time.NewTimer(delay)
				select {
				case <-ctx.Done():
					timer.Stop()
					return nil, false
				case <-timer.C:
				}
			}
			attempts++
			attemptCtx, done := context.WithTimeout(ctx, t.attemptBudget)
			raw, dialErr := t.dial(attemptCtx, net.JoinHostPort(ip.String(), port), route.index)
			if dialErr == nil {
				var auth credentials.AuthInfo
				var secure net.Conn
				secure, auth, dialErr = t.TransportCredentials.ClientHandshake(attemptCtx, host, raw)
				if dialErr == nil {
					done()
					return &verifiedGatewayConn{Conn: secure, auth: auth, verifier: t.verifier}, true
				}
				raw.Close()
			}
			done()
			last = dialErr
		}
		return nil, false
	}
	if conn, ok := tryRoute(gatewayRoute{}); ok {
		return conn, nil
	}
	if ctx.Err() == nil && attempts < gatewayCandidateLimit {
		routes := t.routes(ctx, host)
		for _, route := range routes[:min(len(routes), gatewayRouteLimit)] {
			if conn, ok := tryRoute(route); ok {
				return conn, nil
			}
			if ctx.Err() != nil || attempts >= gatewayCandidateLimit {
				break
			}
		}
	}
	if ctx.Err() != nil {
		last = ctx.Err()
	}
	if last == nil {
		last = errors.New("no usable gateway addresses")
	}
	return nil, fmt.Errorf("gateway connection failed after %d candidates: %w", attempts, last)
}

func lookupGateway(ctx context.Context, host string, route gatewayRoute) ([]netip.Addr, error) {
	if route.index == 0 {
		return net.DefaultResolver.LookupNetIP(ctx, "ip", host)
	}

	return lookupScopedGateway(ctx, host, route, func(ctx context.Context, network, server string) (net.Conn, error) {
		dialer := net.Dialer{Control: gatewaySocketControl(route.index)}
		return dialer.DialContext(ctx, network, net.JoinHostPort(server, "53"))
	})
}

func lookupScopedGateway(ctx context.Context, host string, route gatewayRoute, dial func(context.Context, string, string) (net.Conn, error)) ([]netip.Addr, error) {
	ctx, cancel := context.WithTimeout(ctx, gatewayLookupBudget)
	defer cancel()
	deadline, _ := ctx.Deadline()
	var last error
	servers := route.servers[:min(len(route.servers), 2)]
	for i, server := range servers {
		// Reserve time for the second configured resolver when the first is silent.
		serverCtx, stop := context.WithTimeout(ctx, time.Until(deadline)/time.Duration(len(servers)-i))

		serverDeadline, _ := serverCtx.Deadline()
		for mode := 0; mode < 2; mode++ {
			queryCtx, finish := context.WithTimeout(serverCtx, time.Until(serverDeadline)/time.Duration(2-mode))
			resolver := &net.Resolver{PreferGo: true, Dial: func(ctx context.Context, network, _ string) (net.Conn, error) {
				if mode == 1 {
					network = "tcp"
				}
				return dial(ctx, network, server)
			}}
			ips, err := resolver.LookupNetIP(queryCtx, "ip", host)
			finish()
			if err == nil && len(ips) > 0 {
				stop()
				return ips, nil
			}
			last = err
			if serverCtx.Err() != nil {
				break
			}
		}
		stop()

		if ctx.Err() != nil {
			break
		}
	}
	return nil, last
}

func dialGatewaySocket(ctx context.Context, address string, index int) (net.Conn, error) {
	d := net.Dialer{KeepAlive: 30 * time.Second}
	if index != 0 {
		d.Control = gatewaySocketControl(index)
	}
	return d.DialContext(ctx, "tcp", address)
}
