//go:build darwin || linux

package guestconn

import (
	"context"
	"errors"
	"io"
	"time"

	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
)

const (
	MaxImportArchiveBytes    = 8 << 30
	ImportDownloadTimeout    = 10 * time.Minute
	ImportReceiveIdleTimeout = 30 * time.Second
)

func ReceiveImportRequest[T any](ctx context.Context, recv func() (*T, error), idle time.Duration) (*T, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	type result struct {
		request *T
		err     error
	}
	ready := make(chan result, 1)
	go func() { request, err := recv(); ready <- result{request, err} }()
	timer := time.NewTimer(idle)
	defer timer.Stop()
	select {
	case value := <-ready:
		return value.request, value.err
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-timer.C:
		return nil, context.DeadlineExceeded
	}
}
func SendImportHeader(send func(*vmmdv1.ImportImageRequest) error, closeAndRecv func() (*vmmdv1.Image, error), header *vmmdv1.ImportImageHeader) (*vmmdv1.Image, bool, error) {
	err := send(&vmmdv1.ImportImageRequest{Payload: &vmmdv1.ImportImageRequest_Header{Header: header}})
	if err == nil {
		return nil, false, nil
	}
	if !errors.Is(err, io.EOF) {
		return nil, false, err
	}
	image, err := closeAndRecv()
	if err != nil {
		return nil, false, err
	}
	return image, true, nil
}
