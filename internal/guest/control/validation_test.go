package control

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestValidation(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name  string
		value string
		valid bool
	}{
		{name: "opaque", value: "agent-01", valid: true},
		{name: "empty", value: "", valid: false},
		{name: "slash", value: "a/b", valid: false},
		{name: "long", value: string(make([]byte, 64)), valid: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := validateID("id", test.value) == nil; got != test.valid {
				t.Fatalf("valid = %v, want %v", got, test.valid)
			}
		})
	}
}

func TestInternalRuntimeNamespaceIsReserved(t *testing.T) {
	if err := validateID("actor_namespace_id", runtimeContainerdNamespace); err == nil {
		t.Fatal("internal runtime namespace was accepted as an actor namespace")
	}
}

func TestDeduperSingleFlight(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	d := new(deduper)
	operation := func() (any, error) {
		calls.Add(1)
		time.Sleep(20 * time.Millisecond)
		return "ok", nil
	}
	results := make(chan any, 2)
	for range 2 {
		go func() {
			value, _ := d.do(context.Background(), "create/r1", "same", operation)
			results <- value
		}()
	}
	for range 2 {
		if value := <-results; value != "ok" {
			t.Fatalf("result = %v", value)
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("calls = %d, want 1", calls.Load())
	}
}

func TestDeduperRejectsRequestIDReuse(t *testing.T) {
	t.Parallel()
	d := new(deduper)
	if _, err := d.do(context.Background(), "create/r1", "first", func() (any, error) { return "ok", nil }); err != nil {
		t.Fatal(err)
	}
	if _, err := d.do(context.Background(), "create/r1", "different", func() (any, error) { return "wrong", nil }); err == nil {
		t.Fatal("different input with the same request id was accepted")
	}
}

func TestVolumeDestination(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		path  string
		valid bool
	}{
		{path: "/data", valid: true},
		{path: "/workspace/cache", valid: true},
		{path: "relative"},
		{path: "/"},
		{path: "/data/../etc"},
		{path: "/proc/escape"},
		{path: "/sys/kernel"},
		{path: "/dev/shm"},
	} {
		t.Run(test.path, func(t *testing.T) {
			if got := validateDestination(test.path) == nil; got != test.valid {
				t.Fatalf("valid = %v, want %v", got, test.valid)
			}
		})
	}
}
