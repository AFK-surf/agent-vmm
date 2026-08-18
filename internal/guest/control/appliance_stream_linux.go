//go:build linux

package control

import (
	"context"
	"errors"
	"io"
	"net"
	"path/filepath"
	"time"

	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	bootstrapFrameLimit = 64 << 10
	bootstrapTotalLimit = (1 << 20) + bootstrapFrameLimit
	frontendFrameLimit  = 1 << 20
	frontendTotalLimit  = 1 << 30
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

	inbound := make(chan error, 1)
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
			if len(data) == 0 || uint32(len(data)) > header.GetMaxFrameBytes() || uint64(len(data)) > header.GetMaxTotalBytes()-total {
				inbound <- errors.New("system stream input exceeds its bound")
				return
			}
			setDeadline()
			if _, writeErr := connection.Write(data); writeErr != nil {
				inbound <- writeErr
				return
			}
			total += uint64(len(data))
		}
	}()

	buffer := make([]byte, header.GetMaxFrameBytes())
	var outbound uint64
	for {
		setDeadline()
		count, readErr := connection.Read(buffer)
		if count != 0 {
			if uint64(count) > header.GetMaxTotalBytes()-outbound {
				return rpcError(errors.New("system stream output exceeds its bound"))
			}
			if err := stream.Send(&vmmdv1.SystemStreamFrame{Payload: &vmmdv1.SystemStreamFrame_Data{Data: append([]byte(nil), buffer[:count]...)}}); err != nil {
				return err
			}
			outbound += uint64(count)
		}
		if errors.Is(readErr, io.EOF) {
			if err := stream.Send(&vmmdv1.SystemStreamFrame{Payload: &vmmdv1.SystemStreamFrame_CloseWrite{CloseWrite: true}}); err != nil {
				return err
			}
			select {
			case inboundErr := <-inbound:
				return systemStreamProxyError(inboundErr)
			case <-stream.Context().Done():
				return stream.Context().Err()
			}
		}
		if readErr != nil {
			return systemStreamProxyError(readErr)
		}
	}
}

func systemStreamProxyError(err error) error {
	if err == nil {
		return nil
	}
	return status.Error(codes.FailedPrecondition, boundedSystemError(err))
}

func dialSystemEndpoint(done <-chan struct{}, path string, deadline time.Time) (net.Conn, error) {
	for {
		connection, err := net.DialTimeout("unix", path, 250*time.Millisecond)
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
	if record == nil || record.status.GetState() != vmmdv1.SystemApplianceState_SYSTEM_APPLIANCE_STATE_RUNNING || !sameSystemLaunch(record.status.GetIdentity(), header.GetIdentity(), true) {
		return "", nil, nil, errors.New("system stream does not match the running appliance")
	}
	nonce := string(header.GetNonce())
	if _, exists := record.usedNonces[nonce]; exists {
		return "", nil, nil, errors.New("system stream nonce was already consumed")
	}
	if len(record.usedNonces) >= systemNonceLimit {
		return "", nil, nil, errors.New("system stream nonce bound reached")
	}
	key := header.GetKind().String() + "/" + header.GetName()
	if header.GetKind() == vmmdv1.SystemStreamKind_SYSTEM_STREAM_KIND_BOOTSTRAP {
		if record.bootstrapUse != 0 {
			return "", nil, nil, errors.New("bootstrap stream was already consumed")
		}
		record.bootstrapUse = 1
	}
	if record.streamUses == nil {
		record.streamUses = make(map[string]uint32)
	}
	if record.streams[key] >= header.GetMaxConcurrentStreams() {
		if header.GetKind() == vmmdv1.SystemStreamKind_SYSTEM_STREAM_KIND_BOOTSTRAP {
			record.bootstrapUse = 0
		}
		return "", nil, nil, errors.New("system stream concurrency bound reached")
	}
	if record.streamUses[key] >= header.GetMaxUseCount() {
		if header.GetKind() == vmmdv1.SystemStreamKind_SYSTEM_STREAM_KIND_BOOTSTRAP {
			record.bootstrapUse = 0
		}
		return "", nil, nil, errors.New("system stream use count bound reached")
	}
	record.usedNonces[nonce] = struct{}{}
	record.streams[key]++
	record.streamUses[key]++
	path := filepath.Join(record.runtimePath, systemSocketName(header))
	committed := false
	commit := func() error {
		supervisor.mu.Lock()
		defer supervisor.mu.Unlock()
		if current := supervisor.records[header.GetIdentity().GetInstallationId()]; current != record || current.status.GetState() != vmmdv1.SystemApplianceState_SYSTEM_APPLIANCE_STATE_RUNNING || !sameSystemLaunch(current.status.GetIdentity(), header.GetIdentity(), true) {
			return errors.New("system appliance changed while the stream was connecting")
		}
		committed = true
		if header.GetKind() == vmmdv1.SystemStreamKind_SYSTEM_STREAM_KIND_BOOTSTRAP {
			record.bootstrapUse = 2
		}
		return nil
	}
	release := func() {
		supervisor.mu.Lock()
		if current := supervisor.records[header.GetIdentity().GetInstallationId()]; current == record && record.streams[key] != 0 {
			record.streams[key]--
			// The controller stream is a retained transport, not a consumable
			// frontend request. A failed connection attempt must not permanently
			// consume one of its bounded lifetime uses: gRPC may redial before the
			// appliance accepts the control RPC.
			if header.GetKind() == vmmdv1.SystemStreamKind_SYSTEM_STREAM_KIND_CONTROLLER {
				delete(record.usedNonces, nonce)
				if record.streamUses[key] != 0 {
					record.streamUses[key]--
				}
			}
			if header.GetKind() == vmmdv1.SystemStreamKind_SYSTEM_STREAM_KIND_BOOTSTRAP && !committed && record.bootstrapUse == 1 {
				record.bootstrapUse = 0
				delete(record.usedNonces, nonce)
			}
			if !committed && record.streamUses[key] != 0 {
				record.streamUses[key]--
			}
		}
		supervisor.mu.Unlock()
	}
	return path, commit, release, nil
}

func validateSystemStreamBounds(header *vmmdv1.SystemStreamHeader) error {
	frameLimit, totalLimit, idleLimit := uint32(frontendFrameLimit), uint64(frontendTotalLimit), uint32(300)
	concurrentLimit, useLimit := uint32(16), uint32(1024)
	switch header.GetKind() {
	case vmmdv1.SystemStreamKind_SYSTEM_STREAM_KIND_BOOTSTRAP:
		frameLimit, totalLimit, idleLimit = bootstrapFrameLimit, bootstrapTotalLimit, 15
		concurrentLimit, useLimit = 1, 1
		if header.GetName() != "" {
			return errors.New("bootstrap stream cannot have a name")
		}
	case vmmdv1.SystemStreamKind_SYSTEM_STREAM_KIND_CONTROLLER:
		concurrentLimit = 1
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
	if header.GetMaxFrameBytes() == 0 || header.GetMaxFrameBytes() > frameLimit || header.GetMaxTotalBytes() < uint64(header.GetMaxFrameBytes()) || header.GetMaxTotalBytes() > totalLimit || header.GetIdleSeconds() == 0 || header.GetIdleSeconds() > idleLimit || header.GetMaxConcurrentStreams() == 0 || header.GetMaxConcurrentStreams() > concurrentLimit || header.GetMaxUseCount() == 0 || header.GetMaxUseCount() > useLimit {
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
