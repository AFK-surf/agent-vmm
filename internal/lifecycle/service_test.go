//go:build darwin || linux

package lifecycle

import (
	"reflect"
	"testing"
)

func TestExtractServiceRequestLeavesLifecycleArguments(t *testing.T) {
	request, remaining, err := extractServiceRequest([]string{
		"--operation-stdin", "--service-user", "bftvmm", "--request-id", "operation", "--service-type", "daemon",
		"--service-app", "/Users/bftvmm/Agent VMM Host.app",
	})
	if err != nil {
		t.Fatal(err)
	}
	if request.mode != serviceModeDaemon || request.user != "bftvmm" || request.app != "/Users/bftvmm/Agent VMM Host.app" {
		t.Fatalf("request=%+v", request)
	}
	if want := []string{"--operation-stdin", "--request-id", "operation"}; !reflect.DeepEqual(remaining, want) {
		t.Fatalf("remaining=%v want=%v", remaining, want)
	}
}

func TestExtractServiceRequestRejectsUntrustedServiceAppShape(t *testing.T) {
	for _, value := range []string{"Agent VMM Host.app", "/tmp/Agent VMM.app"} {
		if _, _, err := extractServiceRequest([]string{"--service-app", value}); err == nil {
			t.Fatalf("service app %q was accepted", value)
		}
	}
}

func TestExtractServiceRequestRejectsUnknownMode(t *testing.T) {
	if _, _, err := extractServiceRequest([]string{"--service-type", "login"}); err == nil {
		t.Fatal("unknown service type was accepted")
	}
}
