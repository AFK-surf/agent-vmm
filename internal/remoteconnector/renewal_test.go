package remoteconnector

import (
	"bytes"
	"context"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	remotev1 "github.com/AFK-surf/agent-vmm/api/remote/v1"
	trustv1 "github.com/AFK-surf/agent-vmm/api/trust/v1"
	"github.com/AFK-surf/agent-vmm/internal/dataworker"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type recordingControlStream struct {
	mu       sync.Mutex
	messages []*remotev1.ConnectRequest
	sent     chan struct{}
}

func (stream *recordingControlStream) Send(message *remotev1.ConnectRequest) error {
	stream.mu.Lock()
	stream.messages = append(stream.messages, message)
	stream.mu.Unlock()
	select {
	case stream.sent <- struct{}{}:
	default:
	}
	return nil
}

func TestEndpointObservationRenewalUsesCurrentEpochAndAdvancesSharedState(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	stream := &recordingControlStream{sent: make(chan struct{}, 1)}
	writer := newControlWriter(ctx, stream, cancel)
	initial := &trustv1.EndpointObservation{DeviceId: "device-a", RootKeyRevision: 1, EndpointNodeId: []byte("endpoint"), EndpointGeneration: 7, SupportedAlpns: []string{"a"}, FeatureSet: []string{"direct"}, DeviceSignature: make([]byte, 64), ExpiresAt: timestamppb.New(time.Now().Add(50 * time.Millisecond))}
	state := newEndpointObservationState(initial)
	renewed := &trustv1.EndpointObservation{DeviceId: "device-a", RootKeyRevision: 1, EndpointNodeId: []byte("endpoint"), EndpointGeneration: 7, SupportedAlpns: []string{"a"}, FeatureSet: []string{"direct"}, DeviceSignature: bytes.Repeat([]byte{1}, 64), ExpiresAt: timestamppb.New(time.Now().Add(10 * time.Minute))}
	done := make(chan error, 1)
	go func() {
		done <- renewEndpointObservations(ctx, 41, state, writer, func(context.Context) (*trustv1.EndpointObservation, error) {
			return renewed, nil
		}, 30*time.Millisecond)
	}()
	select {
	case <-stream.sent:
	case <-time.After(time.Second):
		t.Fatal("renewal frame was not sent")
	}
	deadline := time.Now().Add(time.Second)
	for state.Load().GetExpiresAt().AsTime().Before(renewed.GetExpiresAt().AsTime()) {
		if time.Now().After(deadline) {
			t.Fatal("shared endpoint observation was not renewed")
		}
		time.Sleep(time.Millisecond)
	}
	cancel(context.Canceled)
	<-done
	stream.mu.Lock()
	message := stream.messages[0].GetEndpointObservationRenewal()
	stream.mu.Unlock()
	if message.GetConnectionEpoch() != 41 || len(message.GetEndpointObservation().GetDeviceSignature()) != 64 {
		t.Fatalf("renewal = %+v", message)
	}
}

func TestControlWriterSerializesConcurrentSends(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(context.Canceled)
	stream := &recordingControlStream{sent: make(chan struct{}, 32)}
	writer := newControlWriter(ctx, stream, cancel)
	var wait sync.WaitGroup
	for range 16 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			if err := writer.Send(ctx, &remotev1.ConnectRequest{}); err != nil {
				t.Errorf("Send error = %v", err)
			}
		}()
	}
	wait.Wait()
	stream.mu.Lock()
	defer stream.mu.Unlock()
	if len(stream.messages) != 16 {
		t.Fatalf("sent %d messages", len(stream.messages))
	}
}

func TestEndpointObservationContractRequiresValidInitialAndImmutableRenewalTuple(t *testing.T) {
	now := time.Now()
	valid := &trustv1.EndpointObservation{DeviceId: "device-a", RootKeyRevision: 1, EndpointNodeId: []byte("endpoint"), EndpointGeneration: 7,
		SupportedAlpns: []string{"a", "b"}, FeatureSet: []string{"direct", "relay"}, ObservedAddressesDigest: []byte("addresses"),
		DeviceSignature: make([]byte, 64), ExpiresAt: timestamppb.New(now.Add(10 * time.Minute))}
	if err := validateEndpointObservation(valid, now); err != nil {
		t.Fatalf("valid initial observation rejected: %v", err)
	}
	invalidInitial := proto.Clone(valid).(*trustv1.EndpointObservation)
	invalidInitial.DeviceSignature = nil
	if err := validateEndpointObservation(invalidInitial, now); err == nil {
		t.Fatal("unsigned initial observation accepted")
	}
	mutations := []func(*trustv1.EndpointObservation){
		func(value *trustv1.EndpointObservation) { value.DeviceId = "device-b" },
		func(value *trustv1.EndpointObservation) { value.RootKeyRevision++ },
		func(value *trustv1.EndpointObservation) { value.EndpointNodeId = []byte("other") },
		func(value *trustv1.EndpointObservation) { value.EndpointGeneration++ },
		func(value *trustv1.EndpointObservation) { value.SupportedAlpns = []string{"a", "c"} },
		func(value *trustv1.EndpointObservation) { value.FeatureSet = []string{"direct"} },
		func(value *trustv1.EndpointObservation) { value.ObservedAddressesDigest = []byte("other") },
	}
	for index, mutate := range mutations {
		renewal := proto.Clone(valid).(*trustv1.EndpointObservation)
		mutate(renewal)
		if sameEndpointObservationTuple(valid, renewal) {
			t.Fatalf("immutable tuple mutation %d accepted", index)
		}
	}
	equalExpiry := proto.Clone(valid).(*trustv1.EndpointObservation)
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(context.Canceled)
	writer := newControlWriter(ctx, &recordingControlStream{sent: make(chan struct{}, 1)}, cancel)
	if err := renewEndpointObservations(ctx, 41, newEndpointObservationState(valid), writer, func(context.Context) (*trustv1.EndpointObservation, error) {
		return equalExpiry, nil
	}, 11*time.Minute); err == nil {
		t.Fatal("renewal with unchanged expiry accepted")
	}
}

func TestEndpointIdentityWatchEndsOnlyOnChangedIdentity(t *testing.T) {
	sent := newEndpointObservationState(&trustv1.EndpointObservation{EndpointNodeId: []byte("endpoint-a"), EndpointGeneration: 1})
	unavailable, err := dataworker.Attach(filepath.Join(t.TempDir(), "missing.sock"))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := watchEndpointIdentity(ctx, sent, unavailable, 10*time.Millisecond); err != nil {
		t.Fatalf("restarting worker ended the connection: %v", err)
	}
	var endpoint atomic.Pointer[dataworker.Status]
	endpoint.Store(&dataworker.Status{EndpointID: "endpoint-a", EndpointGeneration: 1})
	worker, err := dataworker.Attach(startStatusWorker(t, &endpoint))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := watchEndpointIdentity(ctx, sent, worker, 10*time.Millisecond); err != nil {
		t.Fatalf("unchanged worker ended the connection: %v", err)
	}
	endpoint.Store(&dataworker.Status{EndpointID: "endpoint-b", EndpointGeneration: 2})
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := watchEndpointIdentity(ctx, sent, worker, 10*time.Millisecond); err == nil {
		t.Fatal("replaced worker kept the connection")
	}
}
