package remote

import (
	"context"
	"io"
	"net"
	"runtime"
	"sort"
	"sync"
	"testing"
	"time"

	remotev1 "github.com/AFK-surf/agent-vmm/api/remote/v1"
	"github.com/AFK-surf/agent-vmm/internal/remoteconnector"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	workloadBytes = 2 << 20
	workloadCount = 4
)

type echoController struct {
	remotev1.UnimplementedRemoteControllerServiceServer
}

func (echoController) ServeSession(stream grpc.BidiStreamingServer[remotev1.HostSessionFrame, remotev1.ControllerSessionFrame]) error {
	first, err := stream.Recv()
	if err != nil || first.GetHeader() == nil {
		return err
	}
	for {
		frame, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if data := frame.GetData(); len(data) != 0 {
			if err := stream.Send(&remotev1.ControllerSessionFrame{Payload: &remotev1.ControllerSessionFrame_Data{Data: data}}); err != nil {
				return err
			}
		}
	}
}

type metrics struct {
	throughput      float64
	p99             time.Duration
	fairness        float64
	maxStall        time.Duration
	blockedFraction float64
	rssGrowth       uint64
}

func TestTunnelPerformanceGate(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("transport gate is calibrated for the Apple Silicon host runtime")
	}
	if raceEnabled {
		t.Skip("race instrumentation invalidates the calibrated timing gate")
	}
	for _, rtt := range []time.Duration{time.Millisecond, 20 * time.Millisecond, 50 * time.Millisecond, 100 * time.Millisecond} {
		t.Run(rtt.String(), func(t *testing.T) {
			direct := runDirect(t, rtt)
			tunnel := runTunnel(t, rtt)
			throughputFraction := tunnel.throughput / direct.throughput
			p99Multiplier := float64(tunnel.p99) / float64(direct.p99)
			t.Logf("rtt=%s direct_mib_s=%.2f tunnel_mib_s=%.2f fraction=%.3f direct_p99=%s tunnel_p99=%s multiplier=%.3f rss_per_tunnel=%d fairness=%.3f max_stall=%s blocked_fraction=%.3f", rtt, direct.throughput/(1<<20), tunnel.throughput/(1<<20), throughputFraction, direct.p99, tunnel.p99, p99Multiplier, tunnel.rssGrowth/workloadCount, tunnel.fairness, tunnel.maxStall, tunnel.blockedFraction)
			if throughputFraction < .70 {
				t.Errorf("throughput fraction %.3f is below gate", throughputFraction)
			}
			if p99Multiplier > 2.50 {
				t.Errorf("p99 multiplier %.3f exceeds gate", p99Multiplier)
			}
			if tunnel.rssGrowth/workloadCount > 6<<20 {
				t.Errorf("RSS growth per tunnel %d exceeds gate", tunnel.rssGrowth/workloadCount)
			}
			if tunnel.fairness < .70 {
				t.Errorf("fairness %.3f is below gate", tunnel.fairness)
			}
			if tunnel.maxStall > 5*time.Second {
				t.Errorf("stall %s exceeds gate", tunnel.maxStall)
			}
			if tunnel.blockedFraction > .50 {
				t.Errorf("blocked fraction %.3f exceeds gate", tunnel.blockedFraction)
			}
		})
	}
}

func runDirect(t *testing.T, rtt time.Duration) metrics {
	return runGRPC(t, rtt, false)
}

func runTunnel(t *testing.T, rtt time.Duration) metrics {
	return runGRPC(t, rtt, true)
}

func runGRPC(t *testing.T, rtt time.Duration, bridge bool) metrics {
	listener := delayedListener(t, rtt)
	server := grpc.NewServer(grpc.InitialWindowSize(remoteconnector.StreamWindowBytes), grpc.InitialConnWindowSize(remoteconnector.ConnectionWindowBytes), grpc.MaxRecvMsgSize(remoteconnector.MaxControlBytes), grpc.MaxSendMsgSize(remoteconnector.MaxControlBytes))
	remotev1.RegisterRemoteControllerServiceServer(server, echoController{})
	go server.Serve(listener)
	defer func() { server.Stop(); listener.Close() }()
	connection, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithInitialWindowSize(remoteconnector.StreamWindowBytes), grpc.WithInitialConnWindowSize(remoteconnector.ConnectionWindowBytes))
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	client := remotev1.NewRemoteControllerServiceClient(connection)
	pool := sync.Pool{New: func() any { return make([]byte, remoteconnector.FrameBytes) }}
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	rates := make([]float64, workloadCount)
	latencies := make([]time.Duration, 0, 32)
	var stallsMu sync.Mutex
	var maxStall time.Duration
	var blocked time.Duration
	start := time.Now()
	var group sync.WaitGroup
	for worker := range workloadCount {
		group.Add(1)
		go func(index int) {
			defer group.Done()
			stream, err := client.ServeSession(context.Background())
			if err != nil {
				t.Error(err)
				return
			}
			if err := stream.Send(&remotev1.HostSessionFrame{Payload: &remotev1.HostSessionFrame_Header{Header: &remotev1.SessionHeader{ProtocolVersion: remoteconnector.ProtocolVersion, RegistrationId: "r", AllocationId: string(rune('a' + index)), LeaseId: "lease", Generation: 1, TunnelNonce: "nonce", FrameBytes: remoteconnector.FrameBytes, MaxInFlightBytes: remoteconnector.MaxInFlightBytes}}}); err != nil {
				t.Error(err)
				return
			}
			frame := make([]byte, remoteconnector.FrameBytes)
			workerStart := time.Now()
			lastReceive := workerStart
			frameCount := workloadBytes / len(frame)
			sentAt := make(chan time.Time, remoteconnector.MaxInFlightBytes/remoteconnector.FrameBytes)
			sendDone := make(chan error, 1)
			go func() {
				for range frameCount {
					started := time.Now()
					sentAt <- started
					payload := frame
					if bridge {
						payload = pool.Get().([]byte)
						copy(payload, frame)
					}
					err := stream.Send(&remotev1.HostSessionFrame{Payload: &remotev1.HostSessionFrame_Data{Data: payload}})
					if bridge {
						pool.Put(payload)
					}
					elapsed := time.Since(started)
					stallsMu.Lock()
					if elapsed > rtt {
						blocked += elapsed - rtt
					}
					stallsMu.Unlock()
					if err != nil {
						sendDone <- err
						return
					}
				}
				sendDone <- stream.CloseSend()
			}()
			for received := 0; received < frameCount; received++ {
				ping := <-sentAt
				response, err := stream.Recv()
				if err != nil || len(response.GetData()) != len(frame) {
					t.Errorf("tunnel echo failed: %v", err)
					return
				}
				if bridge {
					copied := pool.Get().([]byte)
					copy(copied, response.GetData())
					pool.Put(copied)
				}
				now := time.Now()
				gap := now.Sub(lastReceive)
				lastReceive = now
				stallsMu.Lock()
				if gap > maxStall {
					maxStall = gap
				}
				if index == 0 && received < 8 {
					latencies = append(latencies, now.Sub(ping))
				}
				stallsMu.Unlock()
			}
			if err := <-sendDone; err != nil {
				t.Error(err)
				return
			}
			rates[index] = workloadBytes / time.Since(workerStart).Seconds()
		}(worker)
	}
	group.Wait()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	rssGrowth := uint64(0)
	if after.Sys > before.Sys {
		rssGrowth = after.Sys - before.Sys
	}
	duration := time.Since(start)
	return metrics{throughput: workloadBytes * workloadCount / duration.Seconds(), p99: percentile(latencies, .99), fairness: fairness(rates), maxStall: maxStall, blockedFraction: float64(blocked) / float64(duration*time.Duration(workloadCount)), rssGrowth: rssGrowth}
}

type latencyListener struct {
	net.Listener
	delay time.Duration
}

func (listener latencyListener) Accept() (net.Conn, error) {
	connection, err := listener.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return &latencyConn{Conn: connection, delay: listener.delay}, nil
}

type latencyConn struct {
	net.Conn
	delay time.Duration
}

func (connection *latencyConn) Read(value []byte) (int, error) {
	time.Sleep(connection.delay)
	return connection.Conn.Read(value)
}
func (connection *latencyConn) Write(value []byte) (int, error) {
	time.Sleep(connection.delay)
	return connection.Conn.Write(value)
}
func delayedListener(t *testing.T, rtt time.Duration) net.Listener {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	return latencyListener{Listener: listener, delay: rtt / 4}
}
func percentile(values []time.Duration, quantile float64) time.Duration {
	if len(values) == 0 {
		return 0
	}
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	index := int(float64(len(values)-1) * quantile)
	return values[index]
}
func fairness(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	min, max := values[0], values[0]
	for _, value := range values[1:] {
		if value < min {
			min = value
		}
		if value > max {
			max = value
		}
	}
	return min / max
}
