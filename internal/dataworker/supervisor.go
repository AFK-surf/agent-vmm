package dataworker

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"
)

const maxResponseBytes = 4096

type Status struct {
	EndpointID         string          `json:"endpoint_id"`
	EndpointGeneration uint64          `json:"endpoint_generation"`
	EndpointAddr       json.RawMessage `json:"endpoint_addr"`
	Accepting          bool            `json:"accepting"`
	ActiveStreams      uint64          `json:"active_streams"`
	BytesIn            uint64          `json:"bytes_in"`
	BytesOut           uint64          `json:"bytes_out"`
	ProbePayload       string          `json:"probe_payload,omitempty"`
}

const (
	minRestartDelay    = time.Second
	maxRestartDelay    = 30 * time.Second
	restartStableAfter = time.Minute
)

// Supervisor keeps one data worker process running on fixed sockets. Each
// replacement process binds a new endpoint key and generation, so callers
// read Status again instead of keeping a previous endpoint identity.
type Supervisor struct {
	socket    string
	binary    string
	arguments []string
	stop      context.Context
	cancel    context.CancelFunc
	stopped   chan struct{}

	mu          sync.Mutex
	current     *workerProcess
	interrupted bool
}

type workerProcess struct {
	command *exec.Cmd
	exited  chan struct{}
	err     error
}

// Attach returns a control client for a data worker supervised by another
// composition root. The returned value must not be closed by the caller.
func Attach(socket string) (*Supervisor, error) {
	if !filepath.IsAbs(socket) {
		return nil, fmt.Errorf("data worker socket must be absolute")
	}
	return &Supervisor{socket: socket}, nil
}

// Start bounds the first readiness wait and all restarts by ctx. Close owns
// the process lifetime: it drains and interrupts the worker. A killed worker
// can take seconds to be reaped, so cancellation of ctx must not kill a ready
// worker. The worker reads its stdin as a lifeline and exits at EOF, which
// also happens when this process dies without calling Close. An unexpected
// exit starts a replacement on the same sockets with capped backoff.
func Start(ctx context.Context, binary, socket, serviceSocket, meshControlSocket string) (*Supervisor, error) {
	if !filepath.IsAbs(binary) || !filepath.IsAbs(socket) || !filepath.IsAbs(serviceSocket) || !filepath.IsAbs(meshControlSocket) {
		return nil, fmt.Errorf("data worker binary, control socket, service/mesh sockets must be absolute")
	}
	supervisor := &Supervisor{socket: socket, binary: binary, stopped: make(chan struct{}),
		arguments: []string{"--socket", socket, "--service-socket", serviceSocket, "--mesh-control-socket", meshControlSocket}}
	current, err := supervisor.launch(ctx)
	if err != nil {
		return nil, err
	}
	supervisor.current = current
	supervisor.stop, supervisor.cancel = context.WithCancel(ctx)
	go supervisor.supervise(current)
	return supervisor, nil
}

func (supervisor *Supervisor) launch(ctx context.Context) (*workerProcess, error) {
	lifeline, held, err := os.Pipe()
	if err != nil {
		return nil, err
	}
	defer lifeline.Close()
	command := exec.Command(supervisor.binary, supervisor.arguments...)
	command.Stdin = lifeline
	command.Stderr = os.Stderr
	if err := command.Start(); err != nil {
		held.Close()
		return nil, fmt.Errorf("start data worker: %w", err)
	}
	process := &workerProcess{command: command, exited: make(chan struct{})}
	go func() { process.err = command.Wait(); held.Close(); close(process.exited) }()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		probe, err := supervisor.Status(ctx)
		if err == nil && probe.EndpointID != "" && probe.EndpointGeneration > 0 {
			return process, nil
		}
		select {
		case <-process.exited:
			return nil, fmt.Errorf("data worker exited before ready: %w", process.err)
		case <-ctx.Done():
			process.kill()
			return nil, ctx.Err()
		case <-time.After(25 * time.Millisecond):
		}
	}
	process.kill()
	return nil, fmt.Errorf("data worker readiness timed out")
}

func (process *workerProcess) kill() {
	_ = process.command.Process.Kill()
	<-process.exited
}

func (supervisor *Supervisor) supervise(current *workerProcess) {
	defer close(supervisor.stopped)
	delay := time.Duration(0)
	for {
		ready := time.Now()
		<-current.exited
		if supervisor.stop.Err() != nil {
			return
		}
		if time.Since(ready) >= restartStableAfter {
			delay = 0
		}
		err := current.err
		for {
			delay = min(max(2*delay, minRestartDelay), maxRestartDelay)
			slog.Warn("data worker is not running; restarting", "error", err, "delay", delay)
			timer := time.NewTimer(delay)
			select {
			case <-supervisor.stop.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
			current, err = supervisor.launch(supervisor.stop)
			if err == nil {
				break
			}
			if supervisor.stop.Err() != nil {
				return
			}
		}
		supervisor.mu.Lock()
		supervisor.current = current
		if supervisor.interrupted {
			_ = current.command.Process.Signal(os.Interrupt)
		}
		supervisor.mu.Unlock()
	}
}

// DialServiceTunnel returns one transparent byte stream to the destination
// connector. The worker selects only the fixed service-tunnel ALPN; all route
// authorization remains inside the gRPC exchange carried by this connection.
func (supervisor *Supervisor) DialServiceTunnel(ctx context.Context, endpointAddr json.RawMessage) (net.Conn, error) {
	return dialTunnel(ctx, supervisor.socket, endpointAddr, "open_service_tunnel")
}

func DialServiceTunnel(ctx context.Context, socket string, endpointAddr json.RawMessage) (net.Conn, error) {
	return dialTunnel(ctx, socket, endpointAddr, "open_service_tunnel")
}

func DialMeshControlTunnel(ctx context.Context, socket string, endpointAddr json.RawMessage) (net.Conn, error) {
	return dialTunnel(ctx, socket, endpointAddr, "open_mesh_control_tunnel")
}

func dialTunnel(ctx context.Context, socket string, endpointAddr json.RawMessage, command string) (net.Conn, error) {
	if !filepath.IsAbs(socket) {
		return nil, fmt.Errorf("data worker socket must be absolute")
	}
	if len(endpointAddr) == 0 || len(endpointAddr) > maxResponseBytes {
		return nil, fmt.Errorf("a bounded endpoint address is required")
	}
	dialer := net.Dialer{Timeout: time.Second}
	connection, err := dialer.DialContext(ctx, "unix", socket)
	if err != nil {
		return nil, err
	}
	if deadline, ok := ctx.Deadline(); ok {
		_ = connection.SetDeadline(deadline)
	}
	if err := json.NewEncoder(connection).Encode(map[string]any{"command": command, "endpoint_addr": endpointAddr}); err != nil {
		connection.Close()
		return nil, err
	}
	reader := bufio.NewReaderSize(connection, maxResponseBytes)
	line, err := reader.ReadSlice('\n')
	if err != nil || string(line) != "{\"ready\":true}\n" || reader.Buffered() != 0 {
		connection.Close()
		return nil, fmt.Errorf("data worker did not open service tunnel")
	}
	_ = connection.SetDeadline(time.Time{})
	return connection, nil
}

func (supervisor *Supervisor) Status(ctx context.Context) (Status, error) {
	return supervisor.request(ctx, map[string]string{"command": "status"})
}

func GetStatus(ctx context.Context, socket string) (Status, error) {
	return (&Supervisor{socket: socket}).Status(ctx)
}

func (supervisor *Supervisor) Drain(ctx context.Context) (Status, error) {
	return supervisor.request(ctx, map[string]string{"command": "drain"})
}

func (supervisor *Supervisor) Probe(ctx context.Context, endpointAddr json.RawMessage, payload string) (Status, error) {
	if len(endpointAddr) == 0 || payload == "" || len(payload) > 64<<10 {
		return Status{}, fmt.Errorf("complete bounded endpoint address and payload are required")
	}
	return supervisor.request(ctx, map[string]any{"command": "probe", "endpoint_addr": endpointAddr, "payload": payload})
}

// Close stops restarts, drains the current worker, and interrupts it.
func (supervisor *Supervisor) Close(ctx context.Context) error {
	supervisor.cancel()
	status, err := supervisor.Drain(ctx)
	for err == nil && !status.Accepting && status.ActiveStreams != 0 && ctx.Err() == nil {
		timer := time.NewTimer(25 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
		case <-timer.C:
			status, err = supervisor.Status(ctx)
		}
	}
	if err == nil && (status.Accepting || status.ActiveStreams != 0) {
		err = fmt.Errorf("data worker did not drain: %d streams remain", status.ActiveStreams)
	}
	supervisor.mu.Lock()
	supervisor.interrupted = true
	_ = supervisor.current.command.Process.Signal(os.Interrupt)
	supervisor.mu.Unlock()
	select {
	case <-supervisor.stopped:
	case <-ctx.Done():
		supervisor.mu.Lock()
		_ = supervisor.current.command.Process.Kill()
		supervisor.mu.Unlock()
		if err == nil {
			err = ctx.Err()
		}
	}
	return err
}

func (supervisor *Supervisor) request(ctx context.Context, request any) (Status, error) {
	dialer := net.Dialer{Timeout: time.Second}
	connection, err := dialer.DialContext(ctx, "unix", supervisor.socket)
	if err != nil {
		return Status{}, err
	}
	defer connection.Close()
	deadline, ok := ctx.Deadline()
	if ok {
		_ = connection.SetDeadline(deadline)
	}
	if err := json.NewEncoder(connection).Encode(request); err != nil {
		return Status{}, err
	}
	reader := bufio.NewReaderSize(connection, maxResponseBytes)
	line, err := reader.ReadSlice('\n')
	if err != nil || len(line) >= maxResponseBytes {
		return Status{}, fmt.Errorf("invalid data worker response")
	}
	var status Status
	if err := json.Unmarshal(line, &status); err != nil {
		return Status{}, fmt.Errorf("invalid data worker response")
	}
	return status, nil
}
