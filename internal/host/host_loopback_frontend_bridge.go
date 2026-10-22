//go:build darwin || linux

package host

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"sync/atomic"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type hostFrontendStream interface {
	Send(*hostv1.HostFrontendFrame) error
	Recv() (*hostv1.ControllerFrontendFrame, error)
}

func bridgeHostFrontend(ctx context.Context, connection net.Conn, stream hostFrontendStream, capacity *hostv1.HostLoopbackFrontendCapacity, deadline time.Time) error {
	idle := time.Duration(capacity.GetConnectionIdleSeconds()) * time.Second
	setDeadline := func() { _ = connection.SetDeadline(minTime(deadline, time.Now().Add(idle))) }
	setDeadline()
	errChannel := make(chan error, 2)
	var total atomic.Uint64
	go func() {
		reader := bufio.NewReaderSize(connection, int(capacity.GetMaxFrameBytes()))
		for {
			setDeadline()
			buffer := make([]byte, capacity.GetMaxFrameBytes())
			count, err := reader.Read(buffer)
			if count > 0 {
				if total.Add(uint64(count)) > capacity.GetMaxConnectionBytes() {
					errChannel <- status.Error(codes.ResourceExhausted, "frontend byte limit exceeded")
					return
				}
				if sendErr := stream.Send(&hostv1.HostFrontendFrame{Payload: &hostv1.HostFrontendFrame_Data{Data: buffer[:count]}}); sendErr != nil {
					errChannel <- sendErr
					return
				}
			}
			if errors.Is(err, io.EOF) {
				errChannel <- stream.Send(&hostv1.HostFrontendFrame{Payload: &hostv1.HostFrontendFrame_CloseWrite{CloseWrite: true}})
				return
			}
			if err != nil {
				errChannel <- err
				return
			}
		}
	}()
	go func() {
		for {
			frame, err := stream.Recv()
			if err != nil {
				errChannel <- err
				return
			}
			switch payload := frame.GetPayload().(type) {
			case *hostv1.ControllerFrontendFrame_Data:
				if uint32(len(payload.Data)) > capacity.GetMaxFrameBytes() || total.Add(uint64(len(payload.Data))) > capacity.GetMaxConnectionBytes() {
					errChannel <- status.Error(codes.ResourceExhausted, "frontend byte limit exceeded")
					return
				}
				setDeadline()
				if _, err := connection.Write(payload.Data); err != nil {
					errChannel <- err
					return
				}
			case *hostv1.ControllerFrontendFrame_CloseWrite:
				if payload.CloseWrite {
					if tcp, ok := connection.(*net.TCPConn); ok {
						_ = tcp.CloseWrite()
					}
					errChannel <- nil
					return
				}
			default:
				errChannel <- status.Error(codes.InvalidArgument, "unexpected frontend frame")
				return
			}
		}
	}()
	completed := 0
	for completed < 2 {
		select {
		case err := <-errChannel:
			if err != nil {
				return err
			}
			completed++
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func lowerHex(value []byte) bool {
	if len(value) != 64 {
		return false
	}
	for _, character := range value {
		if !((character >= '0' && character <= '9') || (character >= 'a' && character <= 'f')) {
			return false
		}
	}
	return true
}

func minTime(left, right time.Time) time.Time {
	if left.Before(right) {
		return left
	}
	return right
}
