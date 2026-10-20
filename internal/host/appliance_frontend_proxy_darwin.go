//go:build darwin

package host

import (
	"context"
	"crypto/rand"
	"errors"
	"net"
	"time"

	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"google.golang.org/protobuf/proto"
)

func (runtime *applianceFrontendRuntime) reserve(connection net.Conn) bool {
	runtime.mu.Lock()
	if !runtime.available || uint32(len(runtime.connections)) >= runtime.frontend.MaxConcurrentStreams {
		runtime.mu.Unlock()
		return false
	}
	runtime.connections[connection] = struct{}{}
	runtime.mu.Unlock()
	return true
}

func (runtime *applianceFrontendRuntime) dialService(ctx context.Context) (net.Conn, error) {
	local, remote := net.Pipe()
	if !runtime.reserve(remote) {
		local.Close()
		remote.Close()
		return nil, errors.New("frontend stream capacity is exhausted")
	}
	streamContext, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(runtime.ctx, cancel)
	go func() {
		defer cancel()
		defer stop()
		closeLocal := context.AfterFunc(streamContext, func() { local.Close(); remote.Close() })
		defer closeLocal()
		runtime.proxy(streamContext, remote)
	}()
	return local, nil
}

func (runtime *applianceFrontendRuntime) proxy(parent context.Context, local net.Conn) {
	defer func() {
		local.Close()
		runtime.mu.Lock()
		delete(runtime.connections, local)
		runtime.mu.Unlock()
	}()
	nonce := make([]byte, 32)
	if _, err := rand.Read(nonce); err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(parent, time.Duration(runtime.frontend.DeadlineSeconds)*time.Second)
	defer cancel()
	remote, err := runtime.open(ctx, &vmmdv1.SystemStreamHeader{Identity: proto.Clone(runtime.identity).(*vmmdv1.SystemApplianceIdentity),
		Kind: vmmdv1.SystemStreamKind_SYSTEM_STREAM_KIND_FRONTEND, Name: runtime.name, Nonce: nonce,
		MaxFrameBytes: runtime.frontend.MaxFrameBytes, MaxTotalBytes: runtime.frontend.MaxTotalBytes,
		IdleSeconds: runtime.frontend.IdleSeconds, MaxConcurrentStreams: runtime.frontend.MaxConcurrentStreams})
	if err != nil {
		return
	}
	defer remote.Close()
	proxyFrontendBytes(local, remote, runtime.frontend.MaxFrameBytes, runtime.frontend.MaxTotalBytes, time.Duration(runtime.frontend.IdleSeconds)*time.Second)
}

func proxyFrontendBytes(left, right net.Conn, maxFrame uint32, maxTotal uint64, idle time.Duration) {
	touch := func() {
		deadline := time.Now().Add(idle)
		_ = left.SetDeadline(deadline)
		_ = right.SetDeadline(deadline)
	}
	touch()
	done := make(chan struct{}, 2)
	copyDirection := func(destination, source net.Conn) {
		defer func() { done <- struct{}{} }()
		buffer := make([]byte, maxFrame)
		var total uint64
		for {
			remaining := uint64(len(buffer))
			if maxTotal != 0 {
				remaining = maxTotal - total
			}
			readBytes := min(uint64(len(buffer)), remaining)
			if readBytes == 0 {
				readBytes = 1
			}
			count, err := source.Read(buffer[:readBytes])
			if count != 0 {
				if uint64(count) > remaining {
					return
				}
				touch()
				if _, writeErr := destination.Write(buffer[:count]); writeErr != nil {
					return
				}
				if maxTotal != 0 {
					total += uint64(count)
				}
			}
			if err != nil {
				return
			}
		}
	}
	go copyDirection(right, left)
	go copyDirection(left, right)
	<-done
	left.Close()
	right.Close()
	<-done
}
