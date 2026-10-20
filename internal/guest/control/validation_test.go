package control

import (
	"context"
	"strings"
	"testing"
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

func TestRequestIdentityIsIndependentOfResourceNames(t *testing.T) {
	t.Parallel()
	for _, requestID := range []string{
		"VwpTUla9HPAJRBo1iOjwO:jobs",
		"_owner:register",
		"workflow/" + strings.Repeat("a", 128) + ":runner:daemon",
	} {
		if err := validateRequestID(requestID); err != nil {
			t.Fatalf("request identity %q: %v", requestID, err)
		}
		d := new(deduper)
		calls := 0
		for range 2 {
			if _, err := d.do(context.Background(), "create/"+requestID, "same", func() (any, error) {
				calls++
				return "receipt", nil
			}); err != nil {
				t.Fatal(err)
			}
		}
		if calls != 1 {
			t.Fatalf("original identity executed %d times", calls)
		}
		if _, err := d.do(context.Background(), "create/"+requestID, "changed", func() (any, error) {
			t.Fatal("conflicting payload executed")
			return nil, nil
		}); err == nil {
			t.Fatal("conflicting payload accepted")
		}
	}
	if err := validateRequestID(""); err == nil {
		t.Fatal("missing request identity accepted")
	}
}

func TestInternalRuntimeNamespaceIsReserved(t *testing.T) {
	if err := validateID("actor_namespace_id", runtimeContainerdNamespace); err == nil {
		t.Fatal("internal runtime namespace was accepted as an actor namespace")
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
