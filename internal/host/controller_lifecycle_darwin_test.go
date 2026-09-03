//go:build darwin

package host

import (
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
	go func() { owner.execute(vmLifecycleStart, run); done <- struct{}{} }()
	<-entered
	go func() { owner.execute(vmLifecycleStop, run); done <- struct{}{} }()
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
	result := owner.execute(vmLifecycleStart, func() vmLifecycleResult {
		ran = true
		return vmLifecycleResult{}
	})
	if result.err == nil || ran {
		t.Fatal("lifecycle operation entered execute after shutdown began")
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
