//go:build darwin || linux

package appliance

import (
	"context"
	"errors"
	"net"
	"testing"

	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type applianceDataRemovalGuest struct {
	vmmdv1.UnimplementedVMMDServiceServer
	requests chan applianceDataRemovalCall
	shutdown chan struct{}
}

type applianceDataRemovalCall struct {
	request  *vmmdv1.DeleteSystemApplianceDataRequest
	reply    chan bool
	finished chan struct{}
}

func (guest *applianceDataRemovalGuest) DeleteSystemApplianceData(ctx context.Context, request *vmmdv1.DeleteSystemApplianceDataRequest) (*vmmdv1.DeleteSystemApplianceDataResponse, error) {
	call := applianceDataRemovalCall{request: proto.Clone(request).(*vmmdv1.DeleteSystemApplianceDataRequest), reply: make(chan bool, 1), finished: make(chan struct{})}
	defer close(call.finished)
	select {
	case guest.requests <- call:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	select {
	case fail := <-call.reply:
		if fail {
			return nil, status.Error(codes.Unavailable, "target data cleanup is not confirmed")
		}
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return &vmmdv1.DeleteSystemApplianceDataResponse{}, nil
}

func (guest *applianceDataRemovalGuest) PrepareShutdown(context.Context, *vmmdv1.PrepareShutdownRequest) (*vmmdv1.PrepareShutdownResponse, error) {
	guest.shutdown <- struct{}{}
	return nil, status.Error(codes.Internal, "unmount /state: device or resource busy")
}

func TestApplianceDataRemovalOwnsOnlyTargetAndRetainsSecretsUntilGuestConfirmation(t *testing.T) {
	ctx := context.Background()
	secrets, err := NewSecretStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"target", "neighbor"} {
		if err := secrets.StoreBootstrap(ctx, id, "bootstrap", []byte("test seed")); err != nil {
			t.Fatal(err)
		}
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	guest := &applianceDataRemovalGuest{requests: make(chan applianceDataRemovalCall), shutdown: make(chan struct{}, 1)}
	vmmdv1.RegisterVMMDServiceServer(server, guest)
	go server.Serve(listener)
	t.Cleanup(server.Stop)
	controller := newTestOwner(&Owner{})
	controller.applianceSecrets = secrets
	installation := store.ApplianceInstallation{ID: "target", DataVolumeID: "target-volume"}
	var original *vmmdv1.DeleteSystemApplianceDataRequest
	for index, fail := range []bool{true, false, false} {
		connection, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() {
			done <- controller.removeApplianceData(ctx, vmmdv1.NewVMMDServiceClient(connection), installation)
		}()
		call := <-guest.requests
		call.reply <- fail
		err = <-done
		if (err != nil) != fail {
			t.Fatalf("target cleanup attempt %d: %v", index, err)
		}
		request := call.request
		if request.InstallationId != installation.ID || request.DataVolumeId != installation.DataVolumeID || request.RequestId == "" {
			t.Fatalf("cleanup escaped target: %v", request)
		}
		if original == nil {
			original = request
		} else if !proto.Equal(original, request) {
			t.Fatalf("retry changed target cleanup request: %v", request)
		}
		_, err = secrets.LoadBootstrap(ctx, installation.ID, "bootstrap")
		if fail && err != nil || !fail && !errors.Is(err, errBootstrapNotFound) {
			t.Fatalf("target secret did not follow Guest confirmation: %v", err)
		}
		if _, err := secrets.LoadBootstrap(ctx, "neighbor", "bootstrap"); err != nil {
			t.Fatalf("neighbor secret was removed: %v", err)
		}
	}
	select {
	case <-guest.shutdown:
		t.Fatal("data removal attempted shared VM shutdown")
	default:
	}
}
