package appliancebootstrap

import (
	"errors"
	"time"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	"google.golang.org/grpc"
)

type Stream = grpc.BidiStreamingServer[appliancev1.BootstrapRequest, appliancev1.BootstrapResponse]

type Hooks struct {
	OnClaim       func(*appliancev1.ApplianceLaunchClaim) error
	OnEnvelope    func(*appliancev1.ApplianceLaunchClaim, *appliancev1.OpaqueBootstrapEnvelope) error
	BeforeReceipt func(Stream, *appliancev1.ApplianceLaunchClaim, *appliancev1.BootstrapReceipt) error
}

func Serve(stream Stream, hooks Hooks) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	claim := first.GetLaunchClaim()
	if err := VerifyLaunchClaim(claim, time.Now()); err != nil {
		return err
	}
	if err := hooks.OnClaim(claim); err != nil {
		return err
	}
	second, err := stream.Recv()
	if err != nil {
		return err
	}
	var receipt *appliancev1.BootstrapReceipt
	if envelope := second.GetEnvelope(); envelope != nil {
		if err := ValidateEnvelope(claim, envelope); err != nil {
			clear(envelope.Ciphertext)
			return err
		}
		if err := hooks.OnEnvelope(claim, envelope); err != nil {
			clear(envelope.Ciphertext)
			return err
		}
		receipt = &appliancev1.BootstrapReceipt{InstallationId: claim.GetInstallationId(), BootstrapId: envelope.GetBootstrapId(), Nonce: append([]byte(nil), envelope.GetNonce()...)}
		clear(envelope.Ciphertext)
		third, err := stream.Recv()
		if err != nil || !third.GetCloseWrite() {
			return errors.New("bootstrap stream was not closed after the seed")
		}
	} else {
		if !second.GetCloseWrite() {
			return errors.New("bootstrap stream has an unexpected frame")
		}
		if err := hooks.OnEnvelope(claim, nil); err != nil {
			return err
		}
	}
	if err := hooks.BeforeReceipt(stream, claim, receipt); err != nil {
		return err
	}
	if receipt != nil {
		if err := stream.Send(&appliancev1.BootstrapResponse{Payload: &appliancev1.BootstrapResponse_Receipt{Receipt: receipt}}); err != nil {
			return err
		}
	}
	health := &appliancev1.ApplianceHealth{InstallationId: claim.GetInstallationId(), RegistrationId: claim.GetRegistrationId(), BootId: claim.GetBootId(), ContainerInstance: claim.GetContainerInstance(), ConfigRevision: claim.GetConfigRevision(), Healthy: true, Status: "ready"}
	if err := stream.Send(&appliancev1.BootstrapResponse{Payload: &appliancev1.BootstrapResponse_Health{Health: health}}); err != nil {
		return err
	}
	return stream.Send(&appliancev1.BootstrapResponse{Payload: &appliancev1.BootstrapResponse_CloseWrite{CloseWrite: true}})
}
