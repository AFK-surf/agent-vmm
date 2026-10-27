//go:build linux

package control

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	bootstrapFrameLimit = 64 << 10
	bootstrapTotalLimit = (1 << 20) + bootstrapFrameLimit
	frontendFrameLimit  = 1 << 20
)

func (server *Server) OpenSystemStream(stream grpc.BidiStreamingServer[vmmdv1.SystemStreamFrame, vmmdv1.SystemStreamFrame]) error {
	first, err := stream.Recv()
	if err != nil {
		return rpcError(err)
	}
	header := first.GetHeader()
	path, commit, release, err := server.appliances.acquireStream(header)
	if err != nil {
		return status.Error(codes.FailedPrecondition, boundedSystemError(err))
	}
	defer release()
	connection, err := dialSystemEndpoint(stream.Context().Done(), path, time.Now().Add(5*time.Second))
	if err != nil {
		return status.Error(codes.FailedPrecondition, boundedSystemError(errors.New("system appliance endpoint is unavailable")))
	}
	if err := commit(); err != nil {
		connection.Close()
		return status.Error(codes.FailedPrecondition, boundedSystemError(err))
	}
	defer connection.Close()
	closed := make(chan struct{})
	defer close(closed)
	go func() {
		select {
		case <-stream.Context().Done():
			_ = connection.Close()
		case <-closed:
		}
	}()
	idle := time.Duration(header.GetIdleSeconds()) * time.Second
	if idle == 0 {
		idle = 15 * time.Second
	}
	setDeadline := func() { _ = connection.SetDeadline(time.Now().Add(idle)) }
	setDeadline()
	send, stopSender := newSystemStreamSender(stream, idle)
	defer stopSender()

	boundedTotal := header.GetMaxTotalBytes() != 0
	inbound := make(chan error, 1)
	inputProgress := make(chan struct{}, 1)
	var lastInput atomic.Int64
	lastInput.Store(time.Now().UnixNano())
	go func() {
		var total uint64
		for {
			frame, receiveErr := stream.Recv()
			if errors.Is(receiveErr, io.EOF) {
				if unix, ok := connection.(*net.UnixConn); ok {
					_ = unix.CloseWrite()
				}
				inbound <- nil
				return
			}
			if receiveErr != nil {
				inbound <- receiveErr
				return
			}
			if frame.GetHeader() != nil {
				inbound <- errors.New("system stream header may only appear once")
				return
			}
			if frame.GetCloseWrite() {
				if unix, ok := connection.(*net.UnixConn); ok {
					_ = unix.CloseWrite()
				}
				inbound <- nil
				return
			}
			data := frame.GetData()
			if len(data) == 0 || uint32(len(data)) > header.GetMaxFrameBytes() || (boundedTotal && uint64(len(data)) > header.GetMaxTotalBytes()-total) {
				inbound <- errors.New("system stream input exceeds its bound")
				return
			}
			setDeadline()
			if _, writeErr := connection.Write(data); writeErr != nil {
				inbound <- writeErr
				return
			}
			lastInput.Store(time.Now().UnixNano())
			select {
			case inputProgress <- struct{}{}:
			default:
			}
			if boundedTotal {
				total += uint64(len(data))
			}
		}
	}()

	buffer := make([]byte, header.GetMaxFrameBytes())
	var outbound uint64
	for {
		setDeadline()
		count, readErr := connection.Read(buffer)
		if count != 0 {
			if boundedTotal && uint64(count) > header.GetMaxTotalBytes()-outbound {
				return rpcError(errors.New("system stream output exceeds its bound"))
			}
			if err := send(&vmmdv1.SystemStreamFrame{Payload: &vmmdv1.SystemStreamFrame_Data{Data: append([]byte(nil), buffer[:count]...)}}); err != nil {
				return err
			}
			if boundedTotal {
				outbound += uint64(count)
			}
		}
		if errors.Is(readErr, io.EOF) {
			if err := send(&vmmdv1.SystemStreamFrame{Payload: &vmmdv1.SystemStreamFrame_CloseWrite{CloseWrite: true}}); err != nil {
				return err
			}
			wait := time.NewTimer(idle)
			defer wait.Stop()
			for {
				select {
				case inboundErr := <-inbound:
					return systemStreamProxyError(inboundErr)
				case <-stream.Context().Done():
					return stream.Context().Err()
				case <-inputProgress:
					wait.Reset(time.Until(time.Unix(0, lastInput.Load()).Add(idle)))
				case <-wait.C:
					if remaining := time.Until(time.Unix(0, lastInput.Load()).Add(idle)); remaining > 0 {
						wait.Reset(remaining)
						continue
					}
					return status.Error(codes.DeadlineExceeded, "system stream input exceeded its idle bound after half-close")
				}
			}
		}
		if readErr != nil {
			return systemStreamProxyError(readErr)
		}
	}
}

func newSystemStreamSender(stream grpc.BidiStreamingServer[vmmdv1.SystemStreamFrame, vmmdv1.SystemStreamFrame], idle time.Duration) (func(*vmmdv1.SystemStreamFrame) error, func()) {
	frames := make(chan *vmmdv1.SystemStreamFrame)
	result := make(chan error, 1)
	stopped := make(chan struct{})
	go func() {
		for {
			select {
			case <-stopped:
				return
			case <-stream.Context().Done():
				return
			case frame := <-frames:
				err := stream.Send(frame)
				result <- err
				if err != nil {
					return
				}
			}
		}
	}()
	timer := time.NewTimer(idle)
	timer.Stop()
	send := func(frame *vmmdv1.SystemStreamFrame) error {
		timer.Reset(idle)
		defer timer.Stop()
		select {
		case frames <- frame:
		case <-stream.Context().Done():
			return stream.Context().Err()
		case <-timer.C:
			return status.Error(codes.DeadlineExceeded, "system stream receiver exceeded its idle bound")
		}
		select {
		case err := <-result:
			return err
		case <-stream.Context().Done():
			return stream.Context().Err()
		case <-timer.C:
			return status.Error(codes.DeadlineExceeded, "system stream receiver exceeded its idle bound")
		}
	}
	return send, func() { timer.Stop(); close(stopped) }
}

func systemStreamProxyError(err error) error {
	if err == nil {
		return nil
	}
	return status.Error(codes.FailedPrecondition, boundedSystemError(err))
}

func dialSystemEndpoint(done <-chan struct{}, path string, deadline time.Time) (net.Conn, error) {
	for {
		connection, err := dialPinnedSystemSocket(path)
		if err == nil {
			return connection, nil
		}
		if time.Now().After(deadline) {
			return nil, err
		}
		select {
		case <-done:
			return nil, context.Canceled
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func (supervisor *applianceSupervisor) acquireStream(header *vmmdv1.SystemStreamHeader) (string, func() error, func(), error) {
	if header == nil || len(header.GetNonce()) != 32 || validateSystemIdentity(header.GetIdentity(), supervisor.bootID, true) != nil {
		return "", nil, nil, errors.New("system stream identity is incomplete")
	}
	if err := validateSystemStreamBounds(header); err != nil {
		return "", nil, nil, err
	}
	supervisor.mu.Lock()
	defer supervisor.mu.Unlock()
	record := supervisor.records[header.GetIdentity().GetInstallationId()]
	if record == nil || record.stopping || record.status.GetState() != vmmdv1.SystemApplianceState_SYSTEM_APPLIANCE_STATE_RUNNING || !sameSystemLaunch(record.status.GetIdentity(), header.GetIdentity()) {
		return "", nil, nil, errors.New("system stream does not match the running appliance")
	}
	nonce := string(header.GetNonce())
	_, inFlight := record.inFlightNonces[nonce]
	_, bootstrapped := record.bootstrapNonces[nonce]
	if inFlight || bootstrapped {
		return "", nil, nil, errors.New("system stream nonce was already consumed")
	}
	key := header.GetKind().String() + "/" + header.GetName()
	if record.streams[key] >= header.GetMaxConcurrentStreams() {
		return "", nil, nil, errors.New("system stream concurrency bound reached")
	}
	record.inFlightNonces[nonce] = struct{}{}
	record.streams[key]++
	path := filepath.Join(record.runtimePath, systemSocketName(header))
	streams, inFlightNonces := record.streams, record.inFlightNonces
	commit := func() error {
		supervisor.mu.Lock()
		defer supervisor.mu.Unlock()
		if current := supervisor.records[header.GetIdentity().GetInstallationId()]; current != record || current.stopping || current.status.GetState() != vmmdv1.SystemApplianceState_SYSTEM_APPLIANCE_STATE_RUNNING || !sameSystemLaunch(current.status.GetIdentity(), header.GetIdentity()) {
			return errors.New("system appliance changed while the stream was connecting")
		}
		if header.GetKind() == vmmdv1.SystemStreamKind_SYSTEM_STREAM_KIND_BOOTSTRAP {
			if record.bootstrapNonces == nil {
				record.bootstrapNonces = make(map[string]struct{})
			}
			record.bootstrapNonces[nonce] = struct{}{}
		}
		return nil
	}
	release := func() {
		supervisor.mu.Lock()
		if current := supervisor.records[header.GetIdentity().GetInstallationId()]; current == record && streams[key] != 0 {
			streams[key]--
			delete(inFlightNonces, nonce)
		}
		supervisor.mu.Unlock()
	}
	return path, commit, release, nil
}

func validateSystemStreamBounds(header *vmmdv1.SystemStreamHeader) error {
	frameLimit, totalLimit, idleLimit := uint32(frontendFrameLimit), uint64(0), uint32(300)
	concurrentLimit := uint32(16)
	switch header.GetKind() {
	case vmmdv1.SystemStreamKind_SYSTEM_STREAM_KIND_BOOTSTRAP:
		frameLimit, totalLimit, idleLimit = bootstrapFrameLimit, bootstrapTotalLimit, 15
		concurrentLimit = 1
		if header.GetName() != "" {
			return errors.New("bootstrap stream cannot have a name")
		}
	case vmmdv1.SystemStreamKind_SYSTEM_STREAM_KIND_CONTROLLER:
		concurrentLimit = 1
		if header.GetMaxTotalBytes() != 0 {
			return errors.New("controller stream cannot have a lifetime byte limit")
		}
		if header.GetName() != "" {
			return errors.New("controller stream cannot have a name")
		}
	case vmmdv1.SystemStreamKind_SYSTEM_STREAM_KIND_FRONTEND:
		if validateID("frontend_name", header.GetName()) != nil {
			return errors.New("frontend stream name is invalid")
		}
	default:
		return errors.New("system stream kind is required")
	}
	if (header.GetMaxTotalBytes() != 0 && header.GetMaxTotalBytes() < uint64(header.GetMaxFrameBytes())) || (header.GetKind() == vmmdv1.SystemStreamKind_SYSTEM_STREAM_KIND_BOOTSTRAP && (header.GetMaxTotalBytes() == 0 || header.GetMaxTotalBytes() > totalLimit)) {
		return errors.New("system stream total bytes exceed the fixed ceiling")
	}
	if header.GetMaxFrameBytes() == 0 || header.GetMaxFrameBytes() > frameLimit || header.GetIdleSeconds() == 0 || header.GetIdleSeconds() > idleLimit || header.GetMaxConcurrentStreams() == 0 || header.GetMaxConcurrentStreams() > concurrentLimit {
		return errors.New("system stream bounds exceed the fixed ceiling")
	}
	return nil
}

func systemSocketName(header *vmmdv1.SystemStreamHeader) string {
	switch header.GetKind() {
	case vmmdv1.SystemStreamKind_SYSTEM_STREAM_KIND_BOOTSTRAP:
		return "bootstrap.sock"
	case vmmdv1.SystemStreamKind_SYSTEM_STREAM_KIND_CONTROLLER:
		return "controller.sock"
	default:
		return "frontend-" + header.GetName() + ".sock"
	}
}

func dialPinnedSystemSocket(path string) (net.Conn, error) {
	root, err := os.Open(filepath.Dir(path))
	if err != nil {
		return nil, err
	}
	defer root.Close()
	file, err := openBeneath(root, filepath.Base(path), unix.O_PATH|unix.O_NOFOLLOW)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if info.Mode()&os.ModeSocket == 0 {
		return nil, errors.New("system endpoint is not a socket")
	}
	return net.DialTimeout("unix", fmt.Sprintf("/proc/self/fd/%d", file.Fd()), 250*time.Millisecond)
}
