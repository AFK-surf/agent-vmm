//go:build darwin || linux

package lifecycle

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestInventoryTransitionCancelsReadersAndBlocksNewReaders(t *testing.T) {
	t.Parallel()
	var fence InventoryFence
	reader, leave, err := fence.Enter(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	entered := make(chan func(), 1)
	go func() {
		release, err := fence.Transition(context.Background())
		if err == nil {
			entered <- release
		}
	}()
	select {
	case <-reader.Done():
	case <-time.After(time.Second):
		t.Fatal("transition did not cancel reader")
	}
	next, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, _, err := fence.Enter(next); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("new reader passed transition: %v", err)
	}
	leave()
	select {
	case release := <-entered:
		release()
	case <-time.After(time.Second):
		t.Fatal("transition did not acquire fence")
	}
	_, release, err := fence.Enter(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	release()
}
