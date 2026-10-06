package remoteconnector

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/netip"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	remotev1 "github.com/AFK-surf/agent-vmm/api/remote/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type routeController struct {
	remotev1.UnimplementedRemoteControllerServiceServer
	calls atomic.Int32
	valid atomic.Bool
	fail  bool
}

func (s *routeController) Enroll(ctx context.Context, _ *remotev1.EnrollRequest) (*remotev1.EnrollResponse, error) {
	s.calls.Add(1)
	md, _ := metadata.FromIncomingContext(ctx)
	s.valid.Store(len(md.Get("authorization")) == 1 && len(md.Get(":authority")) == 1 && strings.HasPrefix(md.Get(":authority")[0], "gateway.test"))
	if s.fail {
		return nil, status.Error(codes.Unavailable, "application unavailable")
	}
	return &remotev1.EnrollResponse{}, nil
}

// Use actual TCP, TLS, HTTP/2, and gRPC. Only OS route discovery is injected.
func TestGatewayRouteHandshakeRecovery(t *testing.T) {
	for _, first := range []string{"EOF", "stall", "wrong identity", "second address", "application unavailable"} {
		t.Run(first, func(t *testing.T) {
			server := &routeController{fail: first == "application unavailable"}
			endpoint, roots := startTLSServer(t, "gateway.test", server)
			transport := newGatewayTransport(credentials.NewTLS(&tls.Config{ServerName: "gateway.test", RootCAs: roots, MinVersion: tls.VersionTLS13}))
			transport.attemptBudget = 150 * time.Millisecond
			badClosed := make(chan struct{})
			var badEndpoint string
			if first == "wrong identity" {
				badEndpoint, _ = startTLSServer(t, "impostor.test", &routeController{})
				close(badClosed)
			} else {
				listener, err := net.Listen("tcp", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				defer listener.Close()
				badEndpoint = listener.Addr().String()
				go func() {
					conn, err := listener.Accept()
					if err != nil {
						return
					}
					defer conn.Close()
					defer close(badClosed)
					if first == "EOF" || first == "second address" {
						return
					}
					io.Copy(io.Discard, conn)
				}()
			}
			transport.lookup = func(_ context.Context, _ string, route gatewayRoute) ([]netip.Addr, error) {
				if first == "second address" {
					return []netip.Addr{netip.MustParseAddr("192.0.2.1"), netip.MustParseAddr("192.0.2.1"), netip.MustParseAddr("192.0.2.2")}, nil
				}
				return []netip.Addr{netip.MustParseAddr("192.0.2.1")}, nil // Same IP, different route.
			}
			transport.routes = func(context.Context, string) []gatewayRoute {
				if first == "second address" {
					t.Error("skipped the second address on the original route")
					return nil
				}
				return []gatewayRoute{{index: 9}}
			}
			var attempts atomic.Int32
			transport.dial = func(ctx context.Context, address string, index int) (net.Conn, error) {
				attempts.Add(1)
				if index == 0 && address == "192.0.2.1:7443" && first != "application unavailable" {
					return (&net.Dialer{}).DialContext(ctx, "tcp", badEndpoint)
				}
				if first != "application unavailable" && first != "wrong identity" {
					select {
					case <-badClosed:
					case <-time.After(time.Second):
						t.Error("previous socket not closed before next route")
					}
				}
				return (&net.Dialer{}).DialContext(ctx, "tcp", endpoint)
			}
			conn, err := grpc.NewClient("passthrough:///gateway.test:7443", grpc.WithTransportCredentials(transport.Clone()), grpc.WithContextDialer(transport.Dial), grpc.WithPerRPCCredentials(bearerCredential{value: []byte("test-token"), registration: "test-registration"}))
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			_, err = remotev1.NewRemoteControllerServiceClient(conn).Enroll(ctx, &remotev1.EnrollRequest{})
			if first == "application unavailable" {
				if status.Code(err) != codes.Unavailable {
					t.Fatalf("got %v", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if server.calls.Load() != 1 || !server.valid.Load() {
				t.Fatalf("RPC calls=%d metadata valid=%v", server.calls.Load(), server.valid.Load())
			}
			want := int32(2)
			if first == "application unavailable" {
				want = 1
			}
			if attempts.Load() != want {
				t.Fatalf("dial attempts=%d want=%d", attempts.Load(), want)
			}
			if first != "application unavailable" {
				// A successful connection must outlive the attempt timeout.
				time.Sleep(transport.attemptBudget + 10*time.Millisecond)
				_, err = remotev1.NewRemoteControllerServiceClient(conn).Enroll(ctx, &remotev1.EnrollRequest{})
				if err != nil || attempts.Load() != want {
					t.Fatalf("connection did not survive attempt deadline: %v", err)
				}
			}
		})
	}
}

func TestGatewayCancellation(t *testing.T) {
	for _, stage := range []string{"DNS", "TCP", "TLS", "backoff"} {
		t.Run(stage, func(t *testing.T) {
			transport := newGatewayTransport(credentials.NewTLS(&tls.Config{ServerName: "gateway.test"}))
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			entered := make(chan struct{})
			transport.routes = func(context.Context, string) []gatewayRoute { return nil }
			transport.lookup = func(ctx context.Context, _ string, _ gatewayRoute) ([]netip.Addr, error) {
				if stage == "DNS" {
					close(entered)
					<-ctx.Done()
					return nil, ctx.Err()
				}
				return []netip.Addr{netip.MustParseAddr("192.0.2.1"), netip.MustParseAddr("192.0.2.2")}, nil
			}
			var calls atomic.Int32
			var peer net.Conn
			transport.dial = func(ctx context.Context, _ string, _ int) (net.Conn, error) {
				calls.Add(1)
				close(entered)
				if stage == "TCP" {
					<-ctx.Done()
					return nil, ctx.Err()
				}
				if stage == "backoff" {
					return nil, errors.New("connection failed")
				}
				raw, other := net.Pipe()
				peer = other
				return raw, nil
			}
			finished := make(chan error, 1)
			go func() {
				conn, err := transport.Dial(ctx, "gateway.test:7443")
				if conn != nil {
					conn.Close()
				}
				finished <- err
			}()
			<-entered
			cancel()
			select {
			case err := <-finished:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("got %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("cancel did not stop route pass")
			}
			if peer != nil {
				defer peer.Close()
				peer.SetReadDeadline(time.Now().Add(time.Second))
				_, err := peer.Read(make([]byte, 1))
				if err != io.EOF {
					t.Fatalf("losing TLS socket stayed open: %v", err)
				}
			}
			if calls.Load() > 1 {
				t.Fatal("attempt continued after cancellation")
			}
		})
	}
}

func TestGatewayRejectsUntrustedTransport(t *testing.T) {
	service := &routeController{}
	endpoint, _ := startTLSServer(t, "impostor.test", service)
	_, roots := certificateFor(t, "gateway.test")
	transport := newGatewayTransport(credentials.NewTLS(&tls.Config{ServerName: "gateway.test", RootCAs: roots}))
	transport.lookup = func(context.Context, string, gatewayRoute) ([]netip.Addr, error) {
		return []netip.Addr{netip.MustParseAddr("127.0.0.1")}, nil
	}
	transport.routes = func(context.Context, string) []gatewayRoute { return nil }
	transport.dial = func(ctx context.Context, _ string, _ int) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "tcp", endpoint)
	}
	conn, err := transport.Dial(context.Background(), "gateway.test:7443")
	if err == nil || conn != nil || service.calls.Load() != 0 {
		t.Fatalf("untrusted gateway accepted: %v", err)
	}
	raw, peer := net.Pipe()
	defer peer.Close()
	if _, _, err := transport.ClientHandshake(context.Background(), "gateway.test", raw); err == nil {
		t.Fatal("unverified socket accepted")
	}
}

func TestGatewayDNSFailureFallsBack(t *testing.T) {
	service := &routeController{}
	endpoint, roots := startTLSServer(t, "gateway.test", service)
	transport := newGatewayTransport(credentials.NewTLS(&tls.Config{ServerName: "gateway.test", RootCAs: roots}))
	transport.lookup = func(_ context.Context, _ string, route gatewayRoute) ([]netip.Addr, error) {
		if route.index == 0 {
			return nil, &net.DNSError{IsTimeout: true}
		}
		return []netip.Addr{netip.MustParseAddr("127.0.0.1")}, nil
	}
	transport.routes = func(context.Context, string) []gatewayRoute { return []gatewayRoute{{index: 9}} }
	transport.dial = func(ctx context.Context, _ string, _ int) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "tcp", endpoint)
	}
	conn, err := transport.Dial(context.Background(), "gateway.test:7443")
	if err != nil {
		t.Fatal(err)
	}
	conn.Close()
}

func TestGatewayExplicitProxyPolicy(t *testing.T) {
	t.Setenv("HTTPS_PROXY", "http://proxy.example:8080")
	t.Setenv("https_proxy", "")
	t.Setenv("NO_PROXY", "")
	t.Setenv("no_proxy", "")
	config := Config{Endpoint: "gateway.test:7443", EndpointIdentity: "gateway.test"}
	target, _ := gatewayDialOptions(config)
	if target != config.Endpoint {
		t.Fatal("explicit proxy bypassed")
	}
	t.Setenv("NO_PROXY", "gateway.test")
	target, _ = gatewayDialOptions(config)
	if target != "passthrough:///"+config.Endpoint {
		t.Fatal("NO_PROXY did not select direct route")
	}
}

func TestGatewayHTTP2TimeoutClosesVerifiedSocket(t *testing.T) {
	certificate, roots := certificateFor(t, "gateway.test")
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	closed := make(chan struct{})
	go func() {
		raw, err := listener.Accept()
		if err != nil {
			return
		}
		defer raw.Close()
		defer close(closed)
		conn := tls.Server(raw, &tls.Config{Certificates: []tls.Certificate{certificate}, NextProtos: []string{"h2"}})
		if conn.Handshake() != nil {
			return
		}
		io.Copy(io.Discard, conn) // Never send the server HTTP/2 settings.
	}()
	transport := newGatewayTransport(credentials.NewTLS(&tls.Config{ServerName: "gateway.test", RootCAs: roots}))
	transport.lookup = func(context.Context, string, gatewayRoute) ([]netip.Addr, error) {
		return []netip.Addr{netip.MustParseAddr("127.0.0.1")}, nil
	}
	transport.dial = func(ctx context.Context, _ string, _ int) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "tcp", listener.Addr().String())
	}
	transport.routes = func(context.Context, string) []gatewayRoute { return nil }
	params := controllerConnectParams
	params.MinConnectTimeout = 100 * time.Millisecond
	conn, err := grpc.NewClient("passthrough:///gateway.test:7443", grpc.WithTransportCredentials(transport), grpc.WithContextDialer(transport.Dial), grpc.WithConnectParams(params))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err = remotev1.NewRemoteControllerServiceClient(conn).Enroll(ctx, &remotev1.EnrollRequest{})
	if err == nil {
		t.Fatal("HTTP/2 handshake unexpectedly succeeded")
	}
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("gRPC did not close winning TLS socket on HTTP/2 timeout")
	}
}

// This opt-in check sends no enrollment request or credential.
func TestGatewayConfiguredEndpoint(t *testing.T) {
	endpoint := os.Getenv("AGENT_VMM_TEST_GATEWAY_ENDPOINT")
	caPath := os.Getenv("AGENT_VMM_TEST_GATEWAY_CA")
	if endpoint == "" || caPath == "" {
		t.Skip("requires an explicitly selected gateway and CA")
	}
	host, _, err := net.SplitHostPort(endpoint)
	if err != nil {
		t.Fatal(err)
	}
	roots, err := LoadTLSRoots(caPath)
	if err != nil {
		t.Fatal(err)
	}
	transport := newGatewayTransport(credentials.NewTLS(&tls.Config{ServerName: host, RootCAs: roots, MinVersion: tls.VersionTLS13}))
	routes := transport.routes
	transport.routes = func(ctx context.Context, host string) []gatewayRoute {
		result := routes(ctx, host)
		t.Logf("system DNS routes: %+v", result)
		return result
	}
	lookup := transport.lookup
	transport.lookup = func(ctx context.Context, host string, route gatewayRoute) ([]netip.Addr, error) {
		result, err := lookup(ctx, host, route)
		t.Logf("lookup interface=%d addresses=%v error=%v", route.index, result, err)
		return result, err
	}
	conn, err := transport.Dial(context.Background(), endpoint)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	verified := conn.(*verifiedGatewayConn).auth.(credentials.TLSInfo)
	if verified.State.NegotiatedProtocol != "h2" {
		t.Fatalf("ALPN=%q", verified.State.NegotiatedProtocol)
	}
	t.Logf("verified gateway TLS; peer=%s, ALPN=%s", conn.RemoteAddr(), verified.State.NegotiatedProtocol)
}
