//go:build linux

package control

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/resourcev2"
	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/core/images"
	"github.com/containerd/containerd/v2/core/leases"
	"github.com/containerd/containerd/v2/core/metadata"
	"github.com/containerd/containerd/v2/pkg/namespaces"
	"github.com/containerd/errdefs"
	digest "github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	bolt "go.etcd.io/bbolt"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type operationNamespaces struct {
	namespaces.Store
	mu           sync.Mutex
	labels       map[string]map[string]string
	labelReads   int
	beforeLabels func(string)
}

func (store *operationNamespaces) List(context.Context) ([]string, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	result := make([]string, 0, len(store.labels))
	for name := range store.labels {
		result = append(result, name)
	}
	return result, nil
}

func (store *operationNamespaces) Labels(_ context.Context, namespace string) (map[string]string, error) {
	if store.beforeLabels != nil {
		store.beforeLabels(namespace)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	store.labelReads++
	result := map[string]string{}
	for key, value := range store.labels[namespace] {
		result[key] = value
	}
	return result, nil
}
func (store *operationNamespaces) SetLabel(_ context.Context, namespace, key, value string) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.labels[namespace] == nil {
		store.labels[namespace] = map[string]string{}
	}
	store.labels[namespace][key] = value
	return nil
}

type operationLeases struct {
	leases.Manager
	mu      sync.Mutex
	failure error
	ids     []string
}

func (store *operationLeases) List(context.Context, ...string) ([]leases.Lease, error) {
	return nil, nil
}

func (store *operationLeases) Create(ctx context.Context, opts ...leases.Opt) (leases.Lease, error) {
	value := leases.Lease{}
	for _, opt := range opts {
		if err := opt(&value); err != nil {
			return leases.Lease{}, err
		}
	}
	return value, nil
}

func (store *operationLeases) Delete(_ context.Context, lease leases.Lease, opts ...leases.DeleteOpt) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.ids = append(store.ids, lease.ID)
	return store.failure
}

type operationImages struct {
	images.Store
	record *images.Image
}

func (store *operationImages) Get(context.Context, string) (images.Image, error) {
	if store.record != nil {
		return *store.record, nil
	}
	return images.Image{}, errdefs.ErrNotFound
}

func operationFixture(t *testing.T) (*Server, *operationNamespaces, *operationLeases, *vmmdv1.ImageOperationReceipt, *vmmdv1.ImageOperationRequest) {
	t.Helper()
	metadata := &operationNamespaces{labels: map[string]map[string]string{}}
	leaseStore := &operationLeases{}
	client, err := containerd.New("", containerd.WithServices(containerd.WithNamespaceService(metadata), containerd.WithLeasesService(leaseStore), containerd.WithImageStore(&operationImages{})))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	receipt := &vmmdv1.ImageOperationReceipt{RequestId: "original-copy", Kind: vmmdv1.ImageOperationKind_IMAGE_OPERATION_KIND_COPY, InputSha256: strings.Repeat("a", 64), ImageReference: "image:test"}
	request := &vmmdv1.ImageOperationRequest{RequestId: receipt.RequestId, Kind: receipt.Kind, InputSha256: receipt.InputSha256, ActorNamespaceId: "owner"}
	return &Server{runtime: &Runtime{client: client, inventory: newRuntimeInventoryIndex(), imageOperationWriterID: "same-boot:41:100", logger: slog.Default().With("component", "guest-control")}}, metadata, leaseStore, receipt, request
}

func TestImageOperationDurableIdentityAndTerminalStates(t *testing.T) {
	for _, test := range []struct {
		name                     string
		operationErr, cleanupErr error
		canceled                 bool
		state                    vmmdv1.ImageOperationState
		settled                  bool
	}{
		{"success", nil, nil, false, vmmdv1.ImageOperationState_IMAGE_OPERATION_STATE_SUCCEEDED, true},
		{"success cleanup incomplete", nil, errors.New("temporary files remain"), false, vmmdv1.ImageOperationState_IMAGE_OPERATION_STATE_SUCCEEDED, false},
		{"failed", errors.New("BuildKit exited 1"), nil, false, vmmdv1.ImageOperationState_IMAGE_OPERATION_STATE_FAILED, true},
		{"builder-image capacity", resourceCapacityExhausted(resourcev2.CapacityStorageHeadroom, "import_admission", "builder image capacity"), nil, false, vmmdv1.ImageOperationState_IMAGE_OPERATION_STATE_FAILED, true},
		{"cache-credit capacity", errors.Join(resourceCapacityExhausted(resourcev2.CapacityStorageHeadroom, "import_admission", "cache credit capacity"), nil), nil, false, vmmdv1.ImageOperationState_IMAGE_OPERATION_STATE_FAILED, true},
		{"output-import capacity", atRuntimeStage("output_import", resourceCapacityExhausted(resourcev2.CapacityStorageHeadroom, "import_admission", "output import capacity")), nil, false, vmmdv1.ImageOperationState_IMAGE_OPERATION_STATE_FAILED, true},
		{"stopped", context.Canceled, nil, true, vmmdv1.ImageOperationState_IMAGE_OPERATION_STATE_STOPPED, true},
		{"cleanup incomplete", context.Canceled, errors.New("task still running"), true, vmmdv1.ImageOperationState_IMAGE_OPERATION_STATE_UNKNOWN, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			server, _, _, receipt, request := operationFixture(t)
			run, prior, err := server.runtime.acceptImageOperation(context.Background(), "owner", receipt, "", func() {})
			if err != nil || prior != nil {
				t.Fatalf("accept: %v %v", prior, err)
			}
			observed, err := server.ObserveImageOperation(context.Background(), request)
			if err != nil || observed.State != vmmdv1.ImageOperationState_IMAGE_OPERATION_STATE_RUNNING || observed.WriterSettled {
				t.Fatalf("running: %v %v", observed, err)
			}
			if err := server.runtime.finishImageOperation("owner", receipt, run, test.operationErr, test.cleanupErr, test.canceled); err != nil {
				t.Fatal(err)
			}
			persisted, err := server.runtime.imageOperation(context.Background(), "owner", receipt.RequestId, receipt.Kind)
			if err != nil || persisted.State != test.state || persisted.WriterSettled != test.settled {
				t.Fatalf("terminal: %v %v", persisted, err)
			}
			if status.Code(test.operationErr) == codes.ResourceExhausted {
				if persisted.Status.GetCode() != int32(codes.ResourceExhausted) || !proto.Equal(persisted.Status.Details[0], status.Convert(test.operationErr).Proto().Details[0]) {
					t.Fatalf("capacity detail lost: %v", persisted.Status)
				}
			}
			if _, err := server.ForgetImageOperation(context.Background(), request); test.settled && err != nil {
				t.Fatal(err)
			} else if !test.settled && status.Code(err) != codes.FailedPrecondition {
				t.Fatalf("unsettled forget=%v", err)
			}
			if test.settled {
				if _, err := server.ForgetImageOperation(context.Background(), request); err != nil {
					t.Fatalf("forget replay=%v", err)
				}
				if _, err := server.runtime.imageOperation(context.Background(), "owner", receipt.RequestId, receipt.Kind); status.Code(err) != codes.NotFound {
					t.Fatalf("forgotten receipt=%v", err)
				}
			}
		})
	}
}

func TestImageOperationCancelWaitsForExecutionAndCleanup(t *testing.T) {
	server, _, _, receipt, request := operationFixture(t)
	canceled := make(chan struct{})
	cleanup := make(chan struct{})
	run, _, err := server.runtime.acceptImageOperation(context.Background(), "owner", receipt, "", func() { close(canceled) })
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		<-canceled
		<-cleanup
		_ = server.runtime.finishImageOperation("owner", receipt, run, context.Canceled, nil, true)
	}()
	result := make(chan *vmmdv1.ImageOperationReceipt, 1)
	go func() {
		value, err := server.CancelImageOperation(context.Background(), request)
		if err != nil {
			t.Error(err)
		}
		result <- value
	}()
	<-canceled
	select {
	case value := <-result:
		t.Fatalf("cancel returned before cleanup: %v", value)
	case <-time.After(20 * time.Millisecond):
	}
	close(cleanup)
	value := <-result
	if !value.WriterSettled || value.State != vmmdv1.ImageOperationState_IMAGE_OPERATION_STATE_STOPPED {
		t.Fatalf("cancel receipt: %v", value)
	}
}

func TestImageOperationRestartSettlesExactCopyWithoutInventingResult(t *testing.T) {
	server, _, leaseStore, receipt, request := operationFixture(t)
	run, _, err := server.runtime.acceptImageOperation(context.Background(), "owner", receipt, "", func() {})
	if err != nil {
		t.Fatal(err)
	}
	// A new Runtime has persistent metadata but no in-memory execution handles.
	server.runtime.imageOperationsRecovered = true
	server.runtime.imageOperationWriterID = "same-boot:42:200"
	server.runtime.imageOperations.Delete(imageOperationRecordKey("owner", receipt.RequestId, receipt.Kind))
	close(run.done)
	observed, err := server.ObserveImageOperation(context.Background(), request)
	if err != nil || !observed.WriterSettled || observed.State != vmmdv1.ImageOperationState_IMAGE_OPERATION_STATE_UNKNOWN {
		t.Fatalf("restart: %v %v", observed, err)
	}
	if len(leaseStore.ids) != 1 || leaseStore.ids[0] != imageOperationLeaseID("owner", request.RequestId, request.Kind) {
		t.Fatalf("wrong cleanup identity: %v", leaseStore.ids)
	}
}

func TestImageOperationMissingHandlerNeedsWriterExitAndStartupCleanup(t *testing.T) {
	for _, recovered := range []bool{false, true} {
		t.Run(fmt.Sprint(recovered), func(t *testing.T) {
			server, _, leaseStore, receipt, request := operationFixture(t)
			run, _, err := server.runtime.acceptImageOperation(context.Background(), "owner", receipt, "", func() {})
			if err != nil {
				t.Fatal(err)
			}
			server.runtime.imageOperations.Delete(imageOperationRecordKey("owner", receipt.RequestId, receipt.Kind))
			close(run.done)
			server.runtime.imageOperationsRecovered = recovered
			observed, err := server.ObserveImageOperation(context.Background(), request)
			if err != nil || observed.WriterSettled || observed.State != vmmdv1.ImageOperationState_IMAGE_OPERATION_STATE_UNKNOWN || len(leaseStore.ids) != 0 {
				t.Fatalf("missing handler invented stop proof: %v %v leases=%v", observed, err, leaseStore.ids)
			}
		})
	}
}

func TestImageOperationPublishedReceiptRecoversKnownCopyWithLiveDaemon(t *testing.T) {
	server, _, _, receipt, request := operationFixture(t)
	run, _, err := server.runtime.acceptImageOperation(context.Background(), "owner", receipt, "", func() {})
	if err != nil {
		t.Fatal(err)
	}
	record := &images.Image{Target: ocispec.Descriptor{Digest: digest.Digest("sha256:" + strings.Repeat("b", 64))}}
	if err := setImageOperationReceipt(record, "copy", receipt); err != nil {
		t.Fatal(err)
	}
	server.runtime.client.ImageService().(*operationImages).record = record
	server.runtime.imageOperations.Delete(imageOperationRecordKey("owner", receipt.RequestId, receipt.Kind))
	close(run.done)
	observed, err := server.ObserveImageOperation(context.Background(), request)
	if err != nil || !observed.WriterSettled || observed.State != vmmdv1.ImageOperationState_IMAGE_OPERATION_STATE_SUCCEEDED || observed.ManifestDigest != record.Target.Digest.String() || observed.Status != nil {
		t.Fatalf("published result lost: %v %v", observed, err)
	}
}

func TestImageOperationFailedCleanupDoesNotInventWriterSettlement(t *testing.T) {
	server, _, leaseStore, receipt, request := operationFixture(t)
	run, _, err := server.runtime.acceptImageOperation(context.Background(), "owner", receipt, "", func() {})
	if err != nil {
		t.Fatal(err)
	}
	failure := errors.New("lease cleanup failed")
	if err := server.runtime.finishImageOperation("owner", receipt, run, &imageCleanupFailure{failure}, nil, false); err != nil {
		t.Fatal(err)
	}
	leaseStore.failure = failure
	observed, err := server.ObserveImageOperation(context.Background(), request)
	if err == nil || observed.WriterSettled {
		t.Fatalf("cleanup failure settled writer: %v %v", observed, err)
	}
	leaseStore.failure = nil
	observed, err = server.ObserveImageOperation(context.Background(), request)
	if err != nil || !observed.WriterSettled || observed.State != vmmdv1.ImageOperationState_IMAGE_OPERATION_STATE_FAILED {
		t.Fatalf("cleanup recovery lost failed result: %v %v", observed, err)
	}
}

type diskOperationNamespaces struct {
	namespaces.Store
	db *bolt.DB
}

func (store *diskOperationNamespaces) Labels(ctx context.Context, name string) (labels map[string]string, err error) {
	err = store.db.View(func(tx *bolt.Tx) error { labels, err = metadata.NewNamespaceStore(tx).Labels(ctx, name); return err })
	return
}
func (store *diskOperationNamespaces) List(ctx context.Context) (names []string, err error) {
	err = store.db.View(func(tx *bolt.Tx) error { names, err = metadata.NewNamespaceStore(tx).List(ctx); return err })
	return
}
func (store *diskOperationNamespaces) Delete(ctx context.Context, name string, opts ...namespaces.DeleteOpts) error {
	return store.db.Update(func(tx *bolt.Tx) error { return metadata.NewNamespaceStore(tx).Delete(ctx, name, opts...) })
}

func (store *diskOperationNamespaces) SetLabel(ctx context.Context, name, key, value string) error {
	return store.db.Update(func(tx *bolt.Tx) error { return metadata.NewNamespaceStore(tx).SetLabel(ctx, name, key, value) })
}
func (store *diskOperationNamespaces) Create(ctx context.Context, name string, labels map[string]string) error {
	return store.db.Update(func(tx *bolt.Tx) error { return metadata.NewNamespaceStore(tx).Create(ctx, name, labels) })
}

func TestShutdownClosesImageAdmissionAndSettlesOriginalWriter(t *testing.T) {
	for _, kind := range []vmmdv1.ImageOperationKind{vmmdv1.ImageOperationKind_IMAGE_OPERATION_KIND_BUILD, vmmdv1.ImageOperationKind_IMAGE_OPERATION_KIND_COPY} {
		t.Run(kind.String(), func(t *testing.T) {
			server, _, leases, receipt, request := operationFixture(t)
			receipt.Kind, request.Kind = kind, kind
			ctx, cancel := context.WithCancel(context.Background())
			run, _, err := server.runtime.acceptImageOperation(ctx, "owner", receipt, "", cancel)
			if err != nil {
				t.Fatal(err)
			}
			cleanupFailure := errors.New("writer cleanup unavailable")
			run.cleanup = func(context.Context) error { return cleanupFailure }
			leases.failure = cleanupFailure
			finished := make(chan struct{})
			go func() {
				<-ctx.Done()
				_ = server.runtime.finishImageOperation("owner", receipt, run, ctx.Err(), cleanupFailure, true)
				close(finished)
			}()
			if err := server.beginShutdown(context.Background()); err != nil {
				t.Fatal(err)
			}
			<-finished
			if _, _, err := server.runtime.acceptImageOperation(context.Background(), "owner", proto.Clone(receipt).(*vmmdv1.ImageOperationReceipt), "", func() {}); status.Code(err) != codes.Unavailable {
				t.Fatalf("writer admitted after shutdown: %v", err)
			}
			if err := server.settleImageOperations(context.Background()); err == nil {
				t.Fatal("unsettled writer allowed shutdown")
			}
			observed, err := server.runtime.imageOperation(context.Background(), "owner", request.RequestId, kind)
			if err != nil || observed.WriterSettled || observed.State != vmmdv1.ImageOperationState_IMAGE_OPERATION_STATE_UNKNOWN {
				t.Fatalf("unknown cleanup: %v %v", observed, err)
			}
			cleanupFailure, leases.failure = nil, nil
			if err := server.settleImageOperations(context.Background()); err != nil {
				t.Fatal(err)
			}
			observed, err = server.ObserveImageOperation(context.Background(), request)
			if err != nil || !observed.WriterSettled {
				t.Fatalf("retry did not settle original writer: %v %v", observed, err)
			}
		})
	}
}

func TestShutdownSettlesArchiveOnlyRunWithoutImageReceipt(t *testing.T) {
	server, metadata, _, _, _ := operationFixture(t)
	canceled := make(chan struct{})
	done := make(chan struct{})
	failure := errors.New("archive writer cleanup unavailable")
	cleanupCalls := 0
	run := &imageOperationRun{cancel: func() { close(canceled) }, done: done, cleanup: func(context.Context) error { cleanupCalls++; return failure }}
	server.runtime.imageOperations.Store("appliance-build\x00actual-workspace", run)
	go func() { <-canceled; close(done) }()
	if err := server.beginShutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := server.settleImageOperations(context.Background()); err == nil {
		t.Fatal("archive cleanup failure allowed shutdown")
	}
	if _, found := server.runtime.imageOperations.Load("appliance-build\x00actual-workspace"); !found {
		t.Fatal("failed archive cleanup lost its original workspace")
	}
	failure = nil
	if err := server.settleImageOperations(context.Background()); err != nil {
		t.Fatal(err)
	}
	if cleanupCalls != 2 {
		t.Fatalf("cleanup attempts = %d", cleanupCalls)
	}
	if _, found := server.runtime.imageOperations.Load("appliance-build\x00actual-workspace"); found {
		t.Fatal("settled archive workspace retained")
	}
	if len(metadata.labels) != 0 {
		t.Fatal("archive-only build persisted an image receipt")
	}
}
