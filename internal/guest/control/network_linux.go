//go:build linux

package control

import (
	"context"
	"errors"
	"io"
	"net"

	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	"google.golang.org/grpc"
)

func (s *Server) ApplyNetworkLink(ctx context.Context, request *vmmdv1.ApplyNetworkLinkRequest) (*vmmdv1.ApplyNetworkLinkResponse, error) {
	value, err := s.mutate(ctx, "apply-network-link", request, func() (any, error) {
		return &vmmdv1.ApplyNetworkLinkResponse{}, s.runtime.network.ApplyLink(request.GetLink())
	})
	if err != nil {
		return nil, rpcError(err)
	}
	return value.(*vmmdv1.ApplyNetworkLinkResponse), nil
}

func (s *Server) DeleteNetworkLink(ctx context.Context, request *vmmdv1.DeleteNetworkLinkRequest) (*vmmdv1.DeleteNetworkLinkResponse, error) {
	value, err := s.mutate(ctx, "delete-network-link", request, func() (any, error) {
		return &vmmdv1.DeleteNetworkLinkResponse{}, s.runtime.network.DeleteLink(request.GetLinkId())
	})
	if err != nil {
		return nil, rpcError(err)
	}
	return value.(*vmmdv1.DeleteNetworkLinkResponse), nil
}

func (s *Server) ApplyServiceProjection(ctx context.Context, request *vmmdv1.ApplyServiceProjectionRequest) (*vmmdv1.ApplyServiceProjectionResponse, error) {
	value, err := s.mutate(ctx, "apply-service-projection", request, func() (any, error) {
		if err := s.runtime.network.ApplyServiceProjection(request.GetProjection()); err != nil {
			return nil, err
		}
		source, err := s.runtime.network.ServiceProjectionSourceAddress(request.GetProjection().GetId())
		return &vmmdv1.ApplyServiceProjectionResponse{SourceAddress: source}, err
	})
	if err != nil {
		return nil, rpcError(err)
	}
	return value.(*vmmdv1.ApplyServiceProjectionResponse), nil
}

func (s *Server) DeleteServiceProjection(ctx context.Context, request *vmmdv1.DeleteServiceProjectionRequest) (*vmmdv1.DeleteServiceProjectionResponse, error) {
	value, err := s.mutate(ctx, "delete-service-projection", request, func() (any, error) {
		return &vmmdv1.DeleteServiceProjectionResponse{}, s.runtime.network.DeleteServiceProjection(request.GetProjectionId())
	})
	if err != nil {
		return nil, rpcError(err)
	}
	return value.(*vmmdv1.DeleteServiceProjectionResponse), nil
}

func (s *Server) ForwardTCP(stream grpc.BidiStreamingServer[vmmdv1.ForwardTCPRequest, vmmdv1.ForwardTCPResponse]) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	header := first.GetHeader()
	if header == nil || header.GetActorNamespaceId() == "" || header.GetContainerId() == "" || header.GetContainerInstanceId() == "" {
		return invalid(errors.New("first forward frame must contain actor namespace and container"))
	}
	connection, err := s.runtime.network.DialContainer(stream.Context(), header.GetActorNamespaceId(), header.GetContainerId(), header.GetContainerInstanceId(), header.GetPort())
	if err != nil {
		return rpcError(err)
	}
	defer connection.Close()
	readResult := make(chan error, 1)
	go func() {
		buffer := make([]byte, 32<<10)
		for {
			count, err := connection.Read(buffer)
			if count != 0 {
				if sendErr := stream.Send(&vmmdv1.ForwardTCPResponse{Event: &vmmdv1.ForwardTCPResponse_Data{Data: append([]byte(nil), buffer[:count]...)}}); sendErr != nil {
					readResult <- sendErr
					return
				}
			}
			if err != nil {
				if errors.Is(err, io.EOF) {
					err = stream.Send(&vmmdv1.ForwardTCPResponse{Event: &vmmdv1.ForwardTCPResponse_CloseRead{CloseRead: true}})
				}
				readResult <- err
				return
			}
		}
	}()
	receiveResult := make(chan error, 1)
	go func() {
		for {
			request, err := stream.Recv()
			if err != nil {
				if errors.Is(err, io.EOF) {
					if tcp, ok := connection.(interface{ CloseWrite() error }); ok {
						err = tcp.CloseWrite()
					}
				}
				receiveResult <- err
				return
			}
			switch event := request.GetEvent().(type) {
			case *vmmdv1.ForwardTCPRequest_Data:
				if len(event.Data) == 0 {
					continue
				}
				if err := writeForwardData(connection, event.Data); err != nil {
					receiveResult <- err
					return
				}
			case *vmmdv1.ForwardTCPRequest_CloseWrite:
				if !event.CloseWrite {
					receiveResult <- errors.New("close_write must be true")
					return
				}
				if tcp, ok := connection.(interface{ CloseWrite() error }); ok {
					if err := tcp.CloseWrite(); err != nil {
						receiveResult <- err
						return
					}
				}
			default:
				receiveResult <- errors.New("forward header is only allowed in the first frame")
				return
			}
		}
	}()
	select {
	case err := <-readResult:
		return normalizeForwardError(err)
	case err := <-receiveResult:
		if errors.Is(err, io.EOF) {
			select {
			case err := <-readResult:
				return normalizeForwardError(err)
			case <-stream.Context().Done():
				return stream.Context().Err()
			}
		}
		return normalizeForwardError(err)
	case <-stream.Context().Done():
		return stream.Context().Err()
	}
}

func writeForwardData(connection net.Conn, data []byte) error {
	for len(data) != 0 {
		count, err := connection.Write(data)
		if err != nil {
			return err
		}
		if count == 0 {
			return io.ErrShortWrite
		}
		data = data[count:]
	}
	return nil
}

func normalizeForwardError(err error) error {
	if err == nil || errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
		return nil
	}
	return rpcError(err)
}
