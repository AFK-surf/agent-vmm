//go:build darwin || linux

package lifecycle

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"google.golang.org/grpc"
)

type updateVMClientStub struct {
	state        hostv1.VMState
	bootID       string
	replaceBoot  bool
	stopRequests []*hostv1.StopVMRequest
	forceErr     error
}

type startVMClientStub struct {
	hostv1.OperatorServiceClient
	summary *hostv1.GetVMImpactSummaryResponse
	request *hostv1.StartVMRequest
}

func (client *startVMClientStub) GetVMImpactSummary(context.Context, *hostv1.GetVMImpactSummaryRequest, ...grpc.CallOption) (*hostv1.GetVMImpactSummaryResponse, error) {
	return client.summary, nil
}

func (client *startVMClientStub) StartVM(_ context.Context, request *hostv1.StartVMRequest, _ ...grpc.CallOption) (*hostv1.OperationResponse, error) {
	client.request = request
	return &hostv1.OperationResponse{}, nil
}

func TestStartVMWithProgressAcknowledgesWholeVMImpact(t *testing.T) {
	client := &startVMClientStub{summary: &hostv1.GetVMImpactSummaryResponse{
		Vm: &hostv1.VM{State: hostv1.VMState_VM_STATE_STOPPED}, Complete: false,
		Issue: "bounded summary is partial",
	}}
	if err := startVMWithProgress(context.Background(), client, "start-stopped", time.Now()); err != nil {
		t.Fatal(err)
	}
	if client.request == nil || client.request.GetRequestId() != "start-stopped" || !client.request.GetFence().GetWholeVmImpactAcknowledged() {
		t.Fatalf("request=%+v", client.request)
	}
}

func (client *updateVMClientStub) GetVM(context.Context, *hostv1.GetVMRequest, ...grpc.CallOption) (*hostv1.VMResponse, error) {
	return &hostv1.VMResponse{Vm: &hostv1.VM{State: client.state, BootId: client.bootID}}, nil
}

func (client *updateVMClientStub) StopVM(_ context.Context, request *hostv1.StopVMRequest, _ ...grpc.CallOption) (*hostv1.OperationResponse, error) {
	client.stopRequests = append(client.stopRequests, request)
	if !request.GetForce() {
		if client.replaceBoot {
			client.bootID = "replacement-boot"
		}
		return nil, errors.New("graceful deadline")
	}
	if client.forceErr != nil {
		return nil, client.forceErr
	}
	client.state = hostv1.VMState_VM_STATE_STOPPED
	return &hostv1.OperationResponse{}, nil
}

func TestUpdateDrainForcesAndConfirmsTerminalAfterGracefulFailure(t *testing.T) {
	client := &updateVMClientStub{state: hostv1.VMState_VM_STATE_FAILED}
	if err := stopVMForUpdateWith(context.Background(), client, "update-attempt"); err != nil {
		t.Fatal(err)
	}
	if len(client.stopRequests) != 2 || client.stopRequests[0].GetForce() || !client.stopRequests[1].GetForce() {
		t.Fatalf("stop requests=%+v", client.stopRequests)
	}
	if client.stopRequests[0].GetRequestId() == client.stopRequests[1].GetRequestId() {
		t.Fatal("graceful and force stop reused one durable request")
	}
	replaced := &updateVMClientStub{state: hostv1.VMState_VM_STATE_RUNNING, bootID: "original-boot", replaceBoot: true}
	if err := stopVMForUpdateWith(context.Background(), replaced, "replaced-update"); err == nil || !strings.Contains(err.Error(), "boot identity changed") || len(replaced.stopRequests) != 1 {
		t.Fatalf("replacement boot was stopped: requests=%v error=%v", replaced.stopRequests, err)
	}
}

func TestUpdateDrainPreservesFailedForceStop(t *testing.T) {
	client := &updateVMClientStub{state: hostv1.VMState_VM_STATE_FAILED, forceErr: errors.New("retained instance stop failed")}
	err := stopVMForUpdateWith(context.Background(), client, "failed-stop-update")
	if err == nil || !strings.Contains(err.Error(), "retained instance stop failed") || client.state != hostv1.VMState_VM_STATE_FAILED || len(client.stopRequests) != 2 {
		t.Fatalf("err=%v state=%v requests=%d", err, client.state, len(client.stopRequests))
	}
}
