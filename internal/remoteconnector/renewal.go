package remoteconnector

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"time"

	remotev1 "github.com/AFK-surf/agent-vmm/api/remote/v1"
	trustv1 "github.com/AFK-surf/agent-vmm/api/trust/v1"
)

const endpointRefreshWindow = 6 * time.Minute

type endpointObservationRenewer func(context.Context) (*trustv1.EndpointObservation, error)

func renewEndpointObservations(ctx context.Context, epoch uint64, state *endpointObservationState, writer *controlWriter, renew endpointObservationRenewer, refreshWindow time.Duration) error {
	for {
		current := state.Load()
		if current == nil || current.GetExpiresAt() == nil {
			return errors.New("endpoint observation is unavailable")
		}
		wait := time.Until(current.GetExpiresAt().AsTime().Add(-refreshWindow))
		if wait < 0 {
			wait = 0
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return context.Cause(ctx)
		case <-timer.C:
		}
		observation, err := renew(ctx)
		if err != nil {
			return err
		}
		if validateEndpointObservation(observation, time.Now()) != nil || !sameEndpointObservationTuple(current, observation) ||
			!observation.GetExpiresAt().AsTime().After(current.GetExpiresAt().AsTime()) {
			return errors.New("endpoint observation renewal is invalid")
		}
		message := &remotev1.HostControlMessage{Payload: &remotev1.HostControlMessage_EndpointObservationRenewal{
			EndpointObservationRenewal: &remotev1.EndpointObservationRenewal{ConnectionEpoch: epoch, EndpointObservation: observation},
		}}
		if err := writer.Send(ctx, message); err != nil {
			return err
		}
		state.Store(observation)
	}
}

func validateEndpointObservation(observation *trustv1.EndpointObservation, now time.Time) error {
	if observation == nil || observation.GetDeviceId() == "" || observation.GetRootKeyRevision() == 0 || len(observation.GetEndpointNodeId()) == 0 ||
		observation.GetEndpointGeneration() == 0 || len(observation.GetDeviceSignature()) != 64 || observation.GetExpiresAt() == nil ||
		!observation.GetExpiresAt().AsTime().After(now) || observation.GetExpiresAt().AsTime().Sub(now) > 15*time.Minute ||
		!strictlySorted(observation.GetSupportedAlpns()) || !strictlySorted(observation.GetFeatureSet()) {
		return errors.New("endpoint observation is invalid")
	}
	return nil
}

func sameEndpointObservationTuple(left, right *trustv1.EndpointObservation) bool {
	return left != nil && right != nil && left.GetDeviceId() == right.GetDeviceId() && left.GetRootKeyRevision() == right.GetRootKeyRevision() &&
		left.GetEndpointGeneration() == right.GetEndpointGeneration() && bytes.Equal(left.GetEndpointNodeId(), right.GetEndpointNodeId()) &&
		bytes.Equal(left.GetObservedAddressesDigest(), right.GetObservedAddressesDigest()) && slices.Equal(left.GetSupportedAlpns(), right.GetSupportedAlpns()) &&
		slices.Equal(left.GetFeatureSet(), right.GetFeatureSet())
}

func strictlySorted(values []string) bool {
	for index := 1; index < len(values); index++ {
		if values[index-1] >= values[index] {
			return false
		}
	}
	return true
}
