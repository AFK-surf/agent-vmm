package remoteconnector

import (
	"context"
	"errors"
	"sync"

	remotev1 "github.com/AFK-surf/agent-vmm/api/remote/v1"
)

type hostControlStream interface {
	Send(*remotev1.HostControlMessage) error
}

type controlWrite struct {
	message *remotev1.HostControlMessage
	result  chan error
}

type controlWriter struct {
	queue    chan controlWrite
	finished chan struct{}
	mu       sync.Mutex
	err      error
}

func newControlWriter(ctx context.Context, stream hostControlStream, cancel context.CancelCauseFunc) *controlWriter {
	writer := &controlWriter{queue: make(chan controlWrite), finished: make(chan struct{})}
	go writer.run(ctx, stream, cancel)
	return writer
}

func (writer *controlWriter) run(ctx context.Context, stream hostControlStream, cancel context.CancelCauseFunc) {
	defer close(writer.finished)
	for {
		select {
		case <-ctx.Done():
			writer.setError(context.Cause(ctx))
			return
		case request := <-writer.queue:
			err := stream.Send(request.message)
			request.result <- err
			close(request.result)
			if err != nil {
				writer.setError(err)
				cancel(err)
				return
			}
		}
	}
}

func (writer *controlWriter) Send(ctx context.Context, message *remotev1.HostControlMessage) error {
	request := controlWrite{message: message, result: make(chan error, 1)}
	select {
	case writer.queue <- request:
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-writer.finished:
		return writer.Err()
	}
	select {
	case err := <-request.result:
		return err
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-writer.finished:
		return writer.Err()
	}
}

func (writer *controlWriter) Err() error {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	if writer.err == nil {
		return errors.New("controller control writer stopped")
	}
	return writer.err
}

func (writer *controlWriter) setError(err error) {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	writer.err = err
}
