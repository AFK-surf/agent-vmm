//go:build linux

package build

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/containerd/containerd/v2/pkg/cio"
)

func TestBuildOutputStreamsLateFailureAndRetainsCacheCarry(t *testing.T) {
	var received bytes.Buffer
	output := &buildOutput{progress: func(text string) error {
		if len(text) > 16<<10 {
			t.Fatalf("oversized frame: %d", len(text))
		}
		_, err := received.WriteString(text)
		return err
	}}
	for _, data := range [][]byte{[]byte(" CA"), []byte("CHED\n"), bytes.Repeat([]byte("x"), 2<<20), []byte("late cargo failure\n")} {
		if _, err := output.Write(data); err != nil {
			t.Fatal(err)
		}
	}
	if len(output.carry) != 6 || !strings.HasSuffix(received.String(), "late cargo failure\n") || received.Len() < 2<<20 || !output.Cached() {
		t.Fatalf("output was truncated or lost cache/failure: memory=%d streamed=%d cached=%v", len(output.carry), received.Len(), output.Cached())
	}
	failure := output.failure(1)
	var exit *ExitError
	if !errors.As(failure, &exit) || strings.Contains(exit.Summary(), "late cargo failure") || !strings.Contains(exit.Summary(), "exit code 1") {
		t.Fatalf("build exit summary lost identity or included output: %v", failure)
	}
	if len(output.tail) != maxBuildOutputTailBytes || !strings.Contains(failure.Error(), "exit code 1") || !strings.HasSuffix(failure.Error(), "late cargo failure") || len(failure.Error()) > maxBuildOutputTailBytes+len(exit.Summary())+20 {
		t.Fatalf("failure tail was lost or unbounded: tail=%d failure=%q", len(output.tail), failure)
	}
}

type buildTestIO struct {
	done     chan struct{}
	canceled chan struct{}
	drain    func()
}

func (io *buildTestIO) Config() cio.Config { return cio.Config{} }
func (io *buildTestIO) Wait() {
	<-io.done
	if io.drain != nil {
		io.drain()
	}
}
func (io *buildTestIO) Cancel()      { close(io.canceled); close(io.done) }
func (io *buildTestIO) Close() error { return nil }

func TestBuildIOWaitsForDrainAndCancelsAtDeadline(t *testing.T) {
	tail := new(buildOutput)
	output := &buildTestIO{done: make(chan struct{}), canceled: make(chan struct{}), drain: func() {
		_, _ = tail.Write([]byte("ERROR: failed to copy: disk quota exceeded\n"))
	}}
	result := make(chan error, 1)
	go func() { result <- waitBuildIO(context.Background(), output) }()
	select {
	case <-result:
		t.Fatal("returned before drain")
	default:
	}
	close(output.done)
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	if failure := tail.failure(1); !strings.Contains(failure.Error(), "disk quota exceeded") {
		t.Fatalf("drained final stderr missing from exit failure: %v", failure)
	}
	output = &buildTestIO{done: make(chan struct{}), canceled: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := waitBuildIO(ctx, output); !errors.Is(err, context.Canceled) {
		t.Fatalf("drain error: %v", err)
	}
	<-output.canceled
}
