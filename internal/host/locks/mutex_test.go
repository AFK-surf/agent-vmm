//go:build darwin || linux

package locks

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestContextRWMutexWriterPriorityAndCancellation(t *testing.T) {
	t.Parallel()
	var lock RWMutex
	if err := lock.RLock(context.Background()); err != nil {
		t.Fatal(err)
	}
	writer, cancel := context.WithCancel(context.Background())
	writerDone := make(chan error, 1)
	go func() { writerDone <- lock.Lock(writer) }()
	deadline := time.After(time.Second)
	for {
		lock.mu.Lock()
		waiting := lock.waitingWriters
		lock.mu.Unlock()
		if waiting != 0 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("writer did not wait")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	readerDone := make(chan error, 1)
	go func() { readerDone <- lock.RLock(context.Background()) }()
	select {
	case <-readerDone:
		t.Fatal("reader passed a waiting writer")
	case <-time.After(10 * time.Millisecond):
	}
	if lock.TryLock() {
		t.Fatal("TryLock passed a reader")
	}
	cancel()
	if err := <-writerDone; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	select {
	case err := <-readerDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled writer blocked readers")
	}
	lock.RUnlock()
	lock.RUnlock()
	if !lock.TryLock() {
		t.Fatal("TryLock could not take an unlocked mutex")
	}
	lock.Unlock()
}

func TestKeyedLocksRetainOwnerAcrossCanceledWaiter(t *testing.T) {
	for _, readOwner := range []bool{false, true} {
		for _, readWaiter := range []bool{false, true} {
			name := "write-owner/write-waiter"
			if readOwner {
				name = "read-owner/write-waiter"
			}
			if readWaiter {
				name = "write-owner/read-waiter"
			}
			if readOwner && readWaiter {
				continue
			}
			t.Run(name, func(t *testing.T) {
				var table Keyed[string]
				acquireOwner, acquireWaiter := table.Lock, table.Lock
				if readOwner {
					acquireOwner = table.RLock
				}
				if readWaiter {
					acquireWaiter = table.RLock
				}
				unlock, err := acquireOwner(context.Background(), "integration")
				if err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
				defer cancel()
				if _, err := acquireWaiter(ctx, "integration"); !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("waiter bypassed existing owner: %v", err)
				}
				unlock()
				table.mu.Lock()
				remaining := len(table.entries)
				table.mu.Unlock()
				if remaining != 0 {
					t.Fatalf("idle locks retained: %d", remaining)
				}
			})
		}
	}
}
