package remoteconnector

import (
	"context"
	"errors"
	"io"
	"sync"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	remotev1 "github.com/AFK-surf/agent-vmm/api/remote/v1"
)

type frontendManager struct {
	cancel context.CancelFunc
	done   chan struct{}
	wg     sync.WaitGroup
}

func newFrontendManager(parent context.Context, controller remotev1.RemoteControllerServiceClient, local hostv1.RemoteComputeServiceClient, registrationID, connectionInstance string, connectionEpoch uint64, emit func(*hostv1.HostLoopbackFrontendEvent) error) *frontendManager {
	ctx, cancel := context.WithCancel(parent)
	manager := &frontendManager{cancel: cancel, done: make(chan struct{})}
	for range MaxActiveTunnels {
		manager.wg.Add(1)
		go func() {
			defer manager.wg.Done()
			for ctx.Err() == nil {
				if err := acceptFrontend(ctx, controller, local, registrationID, connectionInstance, connectionEpoch, emit); err != nil && ctx.Err() == nil {
					continue
				}
			}
		}()
	}
	go func() {
		manager.wg.Wait()
		close(manager.done)
	}()
	return manager
}

func (manager *frontendManager) Close() {
	if manager == nil {
		return
	}
	manager.cancel()
	<-manager.done
}

func acceptFrontend(ctx context.Context, controller remotev1.RemoteControllerServiceClient, local hostv1.RemoteComputeServiceClient, registrationID, connectionInstance string, connectionEpoch uint64, emit func(*hostv1.HostLoopbackFrontendEvent) error) error {
	hostStream, err := local.AcceptHostLoopbackFrontend(ctx)
	if err != nil {
		return err
	}
	if err := hostStream.Send(&hostv1.ControllerFrontendFrame{Payload: &hostv1.ControllerFrontendFrame_Accept{Accept: &hostv1.HostLoopbackFrontendAccept{RegistrationId: registrationID, ConnectionInstance: connectionInstance, ConnectionEpoch: connectionEpoch}}}); err != nil {
		return err
	}
	first, err := hostStream.Recv()
	if err != nil {
		return err
	}
	if event := first.GetEvent(); event != nil {
		return emit(event)
	}
	header := first.GetHeader()
	if header == nil || header.GetRegistrationId() != registrationID || header.GetConnectionInstance() != connectionInstance || header.GetConnectionEpoch() != connectionEpoch || header.GetConnectionId() == "" {
		return errors.New("host returned an invalid frontend header")
	}
	controllerStream, err := controller.ServeFrontend(ctx)
	if err != nil {
		return err
	}
	if err := controllerStream.Send(&hostv1.HostFrontendFrame{Payload: &hostv1.HostFrontendFrame_Header{Header: header}}); err != nil {
		return err
	}
	ackFrame, err := controllerStream.Recv()
	if err != nil {
		return err
	}
	ack := ackFrame.GetAck()
	if ack == nil || ack.GetConnectionId() != header.GetConnectionId() {
		return errors.New("controller returned an invalid frontend acknowledgement")
	}
	if err := hostStream.Send(&hostv1.ControllerFrontendFrame{Payload: &hostv1.ControllerFrontendFrame_Ack{Ack: ack}}); err != nil {
		return err
	}
	if !ack.GetAccepted() {
		return nil
	}
	errChannel := make(chan error, 2)
	go copyHostFrontendFrames(hostStream, controllerStream, errChannel)
	go copyControllerFrontendFrames(controllerStream, hostStream, errChannel)
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

type hostFrontendReceiver interface {
	Recv() (*hostv1.HostFrontendFrame, error)
}
type hostFrontendSender interface {
	Send(*hostv1.HostFrontendFrame) error
}
type controllerFrontendReceiver interface {
	Recv() (*hostv1.ControllerFrontendFrame, error)
}
type controllerFrontendSender interface {
	Send(*hostv1.ControllerFrontendFrame) error
}

func copyHostFrontendFrames(source hostFrontendReceiver, destination hostFrontendSender, result chan<- error) {
	for {
		frame, err := source.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				err = nil
			}
			result <- err
			return
		}
		if err := destination.Send(frame); err != nil {
			result <- err
			return
		}
		if frame.GetCloseWrite() {
			result <- nil
			return
		}
	}
}

func copyControllerFrontendFrames(source controllerFrontendReceiver, destination controllerFrontendSender, result chan<- error) {
	for {
		frame, err := source.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				err = nil
			}
			result <- err
			return
		}
		if err := destination.Send(frame); err != nil {
			result <- err
			return
		}
		if frame.GetCloseWrite() {
			result <- nil
			return
		}
	}
}
