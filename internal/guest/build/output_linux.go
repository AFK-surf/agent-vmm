//go:build linux

package build

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/containerd/containerd/v2/pkg/cio"
)

const maxBuildOutputTailBytes = 16 << 10

type buildOutput struct {
	mu       sync.Mutex
	carry    []byte
	tail     []byte
	progress func(string) error
	cached   bool
}

func (w *buildOutput) Write(data []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.tail = append(w.tail, data[max(0, len(data)-maxBuildOutputTailBytes):]...)
	if len(w.tail) > maxBuildOutputTailBytes {
		copy(w.tail, w.tail[len(w.tail)-maxBuildOutputTailBytes:])
		w.tail = w.tail[:maxBuildOutputTailBytes]
	}
	boundary := append(append([]byte{}, w.carry...), data[:min(len(data), 6)]...)
	w.cached = w.cached || bytes.Contains(boundary, []byte(" CACHED")) || bytes.Contains(data, []byte(" CACHED"))
	if len(data) >= 6 {
		w.carry = append(w.carry[:0], data[len(data)-6:]...)
	} else {
		w.carry = append(w.carry[:0], boundary[max(0, len(boundary)-6):]...)
	}
	if w.progress != nil {
		remaining := data
		for len(remaining) > 0 {
			size := min(len(remaining), 16<<10)
			if err := w.progress(strings.ToValidUTF8(string(remaining[:size]), "\uFFFD")); err != nil {
				return len(data), err
			}
			remaining = remaining[size:]
		}
	}
	return len(data), nil
}

type ExitError struct {
	ExitCode   uint32
	OutputTail string
}

func (failure *ExitError) Summary() string {
	return fmt.Sprintf("isolated BuildKit build failed with exit code %d; see build progress or the image operation receipt", failure.ExitCode)
}

func (failure *ExitError) Error() string {
	message := failure.Summary()
	if tail := strings.TrimSpace(failure.OutputTail); tail != "" {
		message += "\noutput tail:\n" + tail
	}
	return message
}

func (w *buildOutput) failure(code uint32) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return &ExitError{ExitCode: code, OutputTail: strings.ToValidUTF8(string(w.tail), "\uFFFD")}
}

func (w *buildOutput) Cached() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.cached
}

func waitBuildIO(ctx context.Context, output cio.IO) error {
	if output == nil {
		return nil
	}
	done := make(chan struct{})
	go func() { output.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		output.Cancel()
		return ctx.Err()
	}
}
