package hostv1

import (
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"strings"
	"testing"
	"time"
)

func TestErrorCatalog(t *testing.T) {
	for name, number := range ErrorReason_value {
		if number == 0 {
			continue
		}
		t.Run(name, func(t *testing.T) {
			reason := ErrorReason(number)
			err := NewError(codes.Aborted, reason, "changed", WithMetadata(map[string]string{"stage": "test"}), WithRetry(time.Second), WithResource("container", "id"))
			got, ok := ErrorReasonOf(err)
			if !ok || got != reason {
				t.Fatalf("reason = %v, %t", got, ok)
			}
			details := status.Convert(err).Details()
			info := details[0].(*errdetails.ErrorInfo)
			if info.Domain != ErrorDomain || info.Reason != strings.TrimPrefix(name, "ERROR_REASON_") || info.Metadata["stage"] != "test" {
				t.Fatalf("info = %v", info)
			}
			if len(details) != 3 {
				t.Fatalf("details = %v", details)
			}
		})
	}
	for _, domain := range []string{"agentvmm.afk.surf", "other"} {
		value, _ := status.New(codes.Aborted, "changed").WithDetails(&errdetails.ErrorInfo{Domain: domain, Reason: "REQUEST_ID_CONFLICT"})
		if _, ok := ErrorReasonOf(value.Err()); ok {
			t.Fatalf("accepted domain %q", domain)
		}
	}
}
