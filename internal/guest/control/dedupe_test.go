package control

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"google.golang.org/protobuf/proto"
)

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

func TestDeduperStopReceiptsRequireInstanceBoundRequestID(t *testing.T) {
	d := new(deduper)
	running := true
	stops := 0
	stop := func(request *vmmdv1.StopContainerRequest) *vmmdv1.StopContainerResponse {
		t.Helper()
		encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(request)
		if err != nil {
			t.Fatal(err)
		}
		result, err := d.do(context.Background(), "stop/"+request.GetRequestId(), fmt.Sprintf("%x", sha256.Sum256(encoded)), func() (any, error) {
			stops++
			running = false
			return &vmmdv1.StopContainerResponse{Container: &vmmdv1.Container{Id: "main", State: vmmdv1.ContainerState_CONTAINER_STATE_STOPPED}}, nil
		})
		if err != nil {
			t.Fatal(err)
		}
		return result.(*vmmdv1.StopContainerResponse)
	}
	request := &vmmdv1.StopContainerRequest{RequestId: "stop-instance-a", ActorNamespaceId: "namespace", ContainerId: "main"}
	stop(request)
	running = true
	oldReceipt := stop(request)
	if !running || stops != 1 || oldReceipt.GetContainer().GetInstanceId() != "" || oldReceipt.GetContainer().GetState() != vmmdv1.ContainerState_CONTAINER_STATE_STOPPED {
		t.Fatal("cached empty-instance receipt must not perform another Stop")
	}
	request = proto.Clone(request).(*vmmdv1.StopContainerRequest)
	request.RequestId = "stop-instance-b"
	stop(request)
	if running || stops != 2 {
		t.Fatal("instance-bound request reused the previous instance receipt")
	}
	stop(request)
	if stops != 2 {
		t.Fatal("same-instance request retry was not idempotent")
	}
}

func TestDeduperRetriesFailuresWithExactFingerprint(t *testing.T) {
	for name, failure := range map[string]error{"ordinary": errors.New("partial mutation"), "canceled": context.Canceled} {
		t.Run(name, func(t *testing.T) {
			d := new(deduper)
			calls := 0
			work := func() (any, error) {
				calls++
				if calls == 1 {
					return "partial", failure
				}
				return "ready", nil
			}
			if _, err := d.do(t.Context(), "create/r1", "original", work); !errors.Is(err, failure) {
				t.Fatal(err)
			}
			if _, err := d.do(t.Context(), "create/r1", "changed", work); err == nil || calls != 1 {
				t.Fatal("failed fingerprint replaced")
			}
			for range 2 {
				if value, err := d.do(t.Context(), "create/r1", "original", work); err != nil || value != "ready" || calls != 2 {
					t.Fatalf("retry value=%v calls=%d error=%v", value, calls, err)
				}
			}
		})
	}
}

func TestDeduperWaiterRetriesAfterOriginalCallerCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := new(deduper)
		ctx, cancel := context.WithCancel(t.Context())
		entered, original := make(chan struct{}), make(chan error, 1)
		go func() {
			_, err := d.do(ctx, "create/r1", "same", func() (any, error) { close(entered); <-ctx.Done(); return nil, ctx.Err() })
			original <- err
		}()
		<-entered
		var calls atomic.Int32
		finished := make(chan error, 1)
		go func() {
			value, err := d.do(t.Context(), "create/r1", "same", func() (any, error) { calls.Add(1); return "ready", nil })
			if err == nil && value != "ready" {
				err = fmt.Errorf("value=%v", value)
			}
			finished <- err
		}()
		synctest.Wait()
		if _, err := d.do(t.Context(), "create/r1", "changed", func() (any, error) { t.Fatal("inflight fingerprint replaced"); return nil, nil }); err == nil {
			t.Fatal("inflight conflict accepted")
		}
		cancel()
		if err := <-original; !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
		if err := <-finished; err != nil || calls.Load() != 1 {
			t.Fatalf("waiter err=%v calls=%d", err, calls.Load())
		}
	})
}
