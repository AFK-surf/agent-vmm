package dataworker

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
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

type Supervisor struct {
	command *exec.Cmd
	socket  string
	done    chan error
}

// Attach returns a control client for a data worker supervised by another
// composition root. The returned value must not be closed by the caller.
func Attach(socket string) (*Supervisor, error) {
	if !filepath.IsAbs(socket) {
		return nil, fmt.Errorf("data worker socket must be absolute")
	}
	return &Supervisor{socket: socket}, nil
}

// Start bounds only readiness by ctx. Close owns the process lifetime: it
// drains and interrupts the worker. A killed worker can take seconds to be
// reaped, so cancellation of ctx must not kill a ready worker. The worker
// reads its stdin as a lifeline and exits at EOF, which also happens when
// this process dies without calling Close.
func Start(ctx context.Context, binary, socket, serviceSocket, meshControlSocket, serviceCapabilityFile string) (*Supervisor, error) {
	if !filepath.IsAbs(binary) || !filepath.IsAbs(socket) || !filepath.IsAbs(serviceSocket) || !filepath.IsAbs(meshControlSocket) || !filepath.IsAbs(serviceCapabilityFile) {
		return nil, fmt.Errorf("data worker binary, control socket, service/mesh sockets, and capability file must be absolute")
	}
	lifeline, held, err := os.Pipe()
	if err != nil {
		return nil, err
	}
	defer lifeline.Close()
	command := exec.Command(binary, "--socket", socket, "--service-socket", serviceSocket, "--mesh-control-socket", meshControlSocket, "--service-capability-file", serviceCapabilityFile)
	command.Stdin = lifeline
	command.Stderr = os.Stderr
	if err := command.Start(); err != nil {
		held.Close()
		return nil, fmt.Errorf("start data worker: %w", err)
	}
	supervisor := &Supervisor{command: command, socket: socket, done: make(chan error, 1)}
	go func() { supervisor.done <- command.Wait(); held.Close() }()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		probe, err := supervisor.Status(ctx)
		if err == nil && probe.EndpointID != "" && probe.EndpointGeneration > 0 {
			return supervisor, nil
		}
		select {
		case err := <-supervisor.done:
			return nil, fmt.Errorf("data worker exited before ready: %w", err)
		case <-ctx.Done():
			_ = command.Process.Kill()
			return nil, ctx.Err()
		case <-time.After(25 * time.Millisecond):
		}
	}
	_ = command.Process.Kill()
	return nil, fmt.Errorf("data worker readiness timed out")
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

func (supervisor *Supervisor) Close(ctx context.Context) error {
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
	if supervisor.command.Process != nil {
		_ = supervisor.command.Process.Signal(os.Interrupt)
	}
	select {
	case waitErr := <-supervisor.done:
		if waitErr != nil && !errors.Is(waitErr, context.Canceled) && err == nil && supervisor.command.ProcessState.Success() {
			err = waitErr
		}
	case <-ctx.Done():
		_ = supervisor.command.Process.Kill()
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
