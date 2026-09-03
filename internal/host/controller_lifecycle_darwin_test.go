//go:build darwin

package host

import (
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/internal/vmm"
)

func TestVMLifecycleOwnerSerializesOperations(t *testing.T) {
	owner := &VMLifecycleOwner{shutdownDone: make(chan struct{})}
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	var active atomic.Int32
	var maximum atomic.Int32
	run := func() vmLifecycleResult {
		current := active.Add(1)
		for old := maximum.Load(); current > old && !maximum.CompareAndSwap(old, current); old = maximum.Load() {
		}
		entered <- struct{}{}
		<-release
		active.Add(-1)
		return vmLifecycleResult{}
	}
	done := make(chan struct{}, 2)
	go func() { owner.execute(context.Background(), vmLifecycleStart, run); done <- struct{}{} }()
	<-entered
	go func() { owner.execute(context.Background(), vmLifecycleStop, run); done <- struct{}{} }()
	select {
	case <-entered:
		t.Fatal("a second lifecycle operation executed concurrently")
	case <-time.After(25 * time.Millisecond):
	}
	release <- struct{}{}
	<-entered
	release <- struct{}{}
	<-done
	<-done
	if maximum.Load() != 1 {
		t.Fatalf("maximum concurrent lifecycle operations = %d, want 1", maximum.Load())
	}
}

func TestVMLifecycleOwnerRejectsOperationsAfterShutdownBegins(t *testing.T) {
	owner := &VMLifecycleOwner{closing: true, shutdownDone: make(chan struct{})}
	ran := false
	result := owner.execute(context.Background(), vmLifecycleStart, func() vmLifecycleResult {
		ran = true
		return vmLifecycleResult{}
	})
	if result.err == nil || ran {
		t.Fatal("lifecycle operation entered execute after shutdown began")
	}
}

func TestVMLifecycleOwnerDoesNotRunExpiredQueuedOperation(t *testing.T) {
	owner := &VMLifecycleOwner{shutdownDone: make(chan struct{})}
	entered := make(chan struct{})
	release := make(chan struct{})
	go owner.execute(context.Background(), vmLifecycleRecover, func() vmLifecycleResult {
		close(entered)
		<-release
		return vmLifecycleResult{}
	})
	<-entered

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	ran := false
	result := owner.execute(ctx, vmLifecycleStart, func() vmLifecycleResult {
		ran = true
		return vmLifecycleResult{}
	})
	close(release)

	if result.err == nil || ran {
		t.Fatal("expired queued lifecycle operation was executed")
	}
}

func TestVMLifecycleOwnerDoesNotRunExpiredOperationWhenLockIsFree(t *testing.T) {
	owner := &VMLifecycleOwner{shutdownDone: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	ran := false
	result := owner.execute(ctx, vmLifecycleStart, func() vmLifecycleResult {
		ran = true
		return vmLifecycleResult{}
	})
	if !errors.Is(result.err, context.Canceled) || ran {
		t.Fatalf("expired operation result=%v ran=%t", result.err, ran)
	}
}

func TestVMLifecycleOwnerTimedOutShutdownCanBeRetried(t *testing.T) {
	owner := &VMLifecycleOwner{manager: &vmm.Manager{}, shutdownDone: make(chan struct{})}
	entered := make(chan struct{})
	release := make(chan struct{})
	go owner.execute(context.Background(), vmLifecycleRecover, func() vmLifecycleResult {
		close(entered)
		<-release
		return vmLifecycleResult{}
	})
	<-entered

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	if err := owner.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("queued shutdown error = %v, want deadline exceeded", err)
	}
	owner.stateMu.Lock()
	closing := owner.closing
	owner.stateMu.Unlock()
	if closing {
		t.Fatal("timed-out queued shutdown permanently claimed shutdown ownership")
	}

	close(release)
	cleaned := false
	owner.shutdownCleanup = func(context.Context) error {
		cleaned = true
		return nil
	}
	if err := owner.Shutdown(context.Background()); err != nil || !cleaned {
		t.Fatalf("retried shutdown error = %v, cleaned = %t", err, cleaned)
	}
}

func TestVMLifecycleOwnerAdmissionStartsAndTransitionsClosed(t *testing.T) {
	owner := &VMLifecycleOwner{shutdownDone: make(chan struct{})}
	if owner.admitsRemoteWork() {
		t.Fatal("new lifecycle owner admitted remote work before a healthy commit")
	}
	owner.admission.Store(1)
	owner.closeAdmission()
	if owner.admitsRemoteWork() {
		t.Fatal("lifecycle transition did not synchronously close admission")
	}
}

func TestTerminalTransitionBetweenCommitAndOpenCannotReopenAdmission(t *testing.T) {
	owner := &VMLifecycleOwner{
		shutdownDone: make(chan struct{}),
		snapshot:     func() *hostv1.VM { return &hostv1.VM{State: hostv1.VMState_VM_STATE_RUNNING, BootId: "fresh-boot"} },
	}
	generation, _ := owner.admissionToken()
	owner.beforeAdmissionOpen = owner.closeAdmission
	if owner.openAdmission(generation, "fresh-boot") {
		t.Fatal("stale commit reopened admission after terminal transition")
	}
	if owner.admitsRemoteWork() {
		t.Fatal("terminal transition left admission open")
	}
}

func TestOnlyVMLifecycleOwnerCallsManagerStartStop(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || filepath.Ext(name) != ".go" || name == "vm_lifecycle_owner_darwin.go" || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(token.NewFileSet(), name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(file, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || (selector.Sel.Name != "Start" && selector.Sel.Name != "Stop") {
				return true
			}
			receiver, ok := selector.X.(*ast.SelectorExpr)
			if ok && receiver.Sel.Name == "manager" {
				t.Errorf("%s calls Manager.%s outside VMLifecycleOwner", name, selector.Sel.Name)
			}
			return true
		})
	}
}
