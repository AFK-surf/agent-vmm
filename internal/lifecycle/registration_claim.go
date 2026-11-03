//go:build darwin || linux

package lifecycle

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"google.golang.org/protobuf/types/known/durationpb"
)

const registrationClaimDescriptorMaxBytes = 4096
const registrationClaimPath = "/v1/compute/agent-vmm/registrations/claim"

var operationIDPattern = regexp.MustCompile(`\A[A-Za-z0-9_-]{1,128}\z`)
var registrationHTTPClient = newRegistrationHTTPClient()
var errRegistrationPlanUnavailable = errors.New("registration claim plan is unavailable; reauthorize the same registration with its original Host identity")

type registrationClaimDescriptor struct {
	Version        int    `json:"version"`
	RegistrationID string `json:"registration_id"`
	RequestID      string `json:"request_id,omitempty"`
	ClaimURL       string `json:"claim_url"`
	Ticket         string `json:"ticket"`
	ExpiresAt      string `json:"expires_at"`
}
type resourcePolicy struct {
	PIDs      uint32 `json:"pids"`
	DiskBytes uint64 `json:"disk_bytes"`
}
type remotePoolMaterial struct {
	PerEnvironmentLimits resourcePolicy `json:"per_environment_limits"`
	MaxEgressMode        string         `json:"max_egress_mode"`
	StopGraceSeconds     uint64         `json:"stop_grace_seconds"`
}
type remoteConnectionMaterial struct {
	GatewayEndpoint string             `json:"gateway_endpoint"`
	TrustBundle     string             `json:"trust_bundle"`
	PolicyRevision  uint64             `json:"policy_revision"`
	PoolPolicy      remotePoolMaterial `json:"pool_policy"`
}
type registrationConfiguration struct {
	Version          int                      `json:"version"`
	RegistrationID   string                   `json:"registration_id"`
	RemoteConnection remoteConnectionMaterial `json:"remote_connection"`
	Registration     json.RawMessage          `json:"registration,omitempty"`
}
type registrationClaimPlan struct {
	Version             int                         `json:"version"`
	RequestID           string                      `json:"request_id"`
	Descriptor          registrationClaimDescriptor `json:"descriptor"`
	DeviceID            string                      `json:"device_id"`
	RootPublicKey       []byte                      `json:"root_public_key"`
	RootKeyRevision     uint64                      `json:"root_key_revision"`
	CandidateCredential []byte                      `json:"candidate_credential"`
	Material            *registrationConfiguration  `json:"material,omitempty"`
	RecoveryURL         string                      `json:"recovery_url,omitempty"`
}
type registrationHTTPError struct {
	operation string
	status    int
	code      string
}

func (err registrationHTTPError) Error() string {
	if err.code != "" {
		return fmt.Sprintf("registration %s returned HTTP %d: %s", err.operation, err.status, err.code)
	}
	return fmt.Sprintf("registration %s returned HTTP %d", err.operation, err.status)
}
func decodeRegistrationClaimDescriptor(input io.Reader) (registrationClaimDescriptor, error) {
	decoder := json.NewDecoder(io.LimitReader(input, registrationClaimDescriptorMaxBytes+1))
	decoder.DisallowUnknownFields()
	var descriptor registrationClaimDescriptor
	if err := decoder.Decode(&descriptor); err != nil {
		return descriptor, fmt.Errorf("decode registration descriptor: %w", err)
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return descriptor, errors.New("registration descriptor must contain one JSON object")
	}
	if err := validateClaimDescriptor(descriptor); err != nil {
		return descriptor, err
	}
	return descriptor, nil
}
func validateClaimDescriptor(descriptor registrationClaimDescriptor) error {
	parsed, err := url.Parse(descriptor.ClaimURL)
	if descriptor.Version != 1 || !operationIDPattern.MatchString(descriptor.RegistrationID) || descriptor.Ticket == "" || len(descriptor.Ticket) > 128 || err != nil || !validRegistrationClaimURL(parsed) {
		return errors.New("registration descriptor is invalid")
	}
	// The server owns ticket expiry. A saved candidate can observe a committed claim after expiry.
	if _, err := time.Parse(time.RFC3339, descriptor.ExpiresAt); err != nil {
		return errors.New("registration descriptor expiry is invalid")
	}
	return nil
}
func (adapter *productionLifecycleAdapter) ensureRegistrationPlan(ctx context.Context) error {
	if adapter.recovery != nil {
		return adapter.ensureRecoveryPlan(ctx)
	}
	if adapter.descriptor == nil && adapter.plan == nil {
		return nil
	}
	plan := adapter.plan
	if plan == nil {
		saved, err := loadClaimPlan(adapter.value, adapter.requestID)
		if err == nil {
			if saved.Descriptor.RegistrationID != adapter.descriptor.RegistrationID || saved.Descriptor.ClaimURL != adapter.descriptor.ClaimURL {
				return errors.New("registration claim request target changed")
			}
			if adapter.descriptor.Ticket != saved.Descriptor.Ticket {
				saved.Descriptor = *adapter.descriptor
				if err := persistClaimPlan(adapter.value, saved); err != nil {
					return err
				}
			}
			plan = saved
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	if plan == nil {
		connection, err := operatorConnection(ctx, filepath.Join(adapter.value.applicationData, "operator.sock"))
		if err != nil {
			return errors.New("Host identity is unavailable for registration claim")
		}
		identityResponse, err := hostv1.NewOperatorServiceClient(connection).GetDeviceIdentity(ctx, &hostv1.GetDeviceIdentityRequest{})
		connection.Close()
		if err != nil || identityResponse.GetState() != hostv1.DeviceIdentityState_DEVICE_IDENTITY_STATE_READY {
			return errors.New("Host identity is not ready")
		}
		identity := identityResponse.GetIdentity()
		candidate, err := newCandidateCredential()
		if err != nil {
			return err
		}
		plan = &registrationClaimPlan{Version: 2, RequestID: adapter.requestID, Descriptor: *adapter.descriptor, DeviceID: identity.GetDeviceId(), RootPublicKey: identity.GetRootPublicKey(), RootKeyRevision: identity.GetRootKeyRevision(), CandidateCredential: candidate}
		// Nothing may reach the server before this candidate and its exact target are durable.
		if err := persistClaimPlan(adapter.value, plan); err != nil {
			return err
		}
	}
	adapter.plan = plan
	if plan.Material == nil {
		material, err := claimRegistration(ctx, plan)
		if err != nil {
			return err
		}
		if err := validateRegistrationConfiguration(material, plan.Descriptor); err != nil {
			return err
		}
		plan.Material = &material
		if err := persistClaimPlan(adapter.value, plan); err != nil {
			return err
		}
	}
	adapter.material = plan.Material
	return nil
}
func claimRegistration(ctx context.Context, plan *registrationClaimPlan) (registrationConfiguration, error) {
	body, err := json.Marshal(map[string]any{"version": 1, "registration_id": plan.Descriptor.RegistrationID, "device_id": plan.DeviceID, "root_public_key": base64.StdEncoding.EncodeToString(plan.RootPublicKey), "root_key_revision": plan.RootKeyRevision, "candidate_credential": base64.StdEncoding.EncodeToString(plan.CandidateCredential)})
	if err != nil {
		return registrationConfiguration{}, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, plan.Descriptor.ClaimURL, bytes.NewReader(body))
	if err != nil {
		return registrationConfiguration{}, errors.New("create registration claim request failed")
	}
	request.Header.Set("Authorization", "RegistrationTicket "+plan.Descriptor.Ticket)
	request.Header.Set("Content-Type", "application/json")
	response, err := registrationHTTPClient.Do(request)
	if err != nil {
		return registrationConfiguration{}, errors.New("registration claim request failed")
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return registrationConfiguration{}, readRegistrationHTTPError(response, "claim")
	}
	decoder := json.NewDecoder(io.LimitReader(response.Body, registrationPlanMaxBytes+1))
	decoder.DisallowUnknownFields()
	var material registrationConfiguration
	if err := decoder.Decode(&material); err != nil {
		return material, errors.New("decode registration configuration failed")
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return material, errors.New("registration configuration has trailing data")
	}
	return material, nil
}
func validateRegistrationConfiguration(material registrationConfiguration, descriptor registrationClaimDescriptor) error {
	if material.Version != 1 || material.RegistrationID != descriptor.RegistrationID {
		return errors.New("registration claim identity mismatch")
	}
	pool := material.RemoteConnection.PoolPolicy
	if material.RemoteConnection.PolicyRevision == 0 || material.RemoteConnection.GatewayEndpoint == "" || pool.PerEnvironmentLimits.PIDs == 0 || pool.PerEnvironmentLimits.DiskBytes == 0 || (pool.MaxEgressMode != "deny_all" && pool.MaxEgressMode != "public_internet") || pool.StopGraceSeconds == 0 {
		return errors.New("registration connection policy is incomplete")
	}
	return nil
}
func (material registrationConfiguration) remotePoolPolicy() (*hostv1.RemotePoolPolicy, error) {
	policy := material.RemoteConnection.PoolPolicy
	egress := hostv1.EgressMode_EGRESS_MODE_UNSPECIFIED
	switch policy.MaxEgressMode {
	case "deny_all":
		egress = hostv1.EgressMode_EGRESS_MODE_DENY_ALL
	case "public_internet":
		egress = hostv1.EgressMode_EGRESS_MODE_PUBLIC_INTERNET
	default:
		return nil, errors.New("remote pool egress mode is invalid")
	}
	limits := func(value resourcePolicy) *hostv1.ResourceLimits {
		return &hostv1.ResourceLimits{Pids: &value.PIDs, DiskBytes: &value.DiskBytes}
	}
	return &hostv1.RemotePoolPolicy{PerEnvironmentLimits: limits(policy.PerEnvironmentLimits), MaxEgressMode: egress,
		StopGrace: durationpb.New(time.Duration(policy.StopGraceSeconds) * time.Second)}, nil
}

func (adapter *productionLifecycleAdapter) applyRegistration(ctx context.Context, request LifecycleRequest) error {
	if adapter.material == nil {
		return nil
	}
	material := adapter.material
	pool, err := material.remotePoolPolicy()
	if err != nil {
		return err
	}
	trustBundle, err := base64.StdEncoding.DecodeString(material.RemoteConnection.TrustBundle)
	if err != nil {
		return errors.New("decode public TLS configuration failed")
	}
	connection, err := operatorConnection(ctx, filepath.Join(adapter.value.applicationData, "operator.sock"))
	if err != nil {
		return err
	}
	defer connection.Close()
	response, err := hostv1.NewOperatorServiceClient(connection).ApplyRemoteRegistration(ctx, &hostv1.ApplyRemoteRegistrationRequest{Registration: &hostv1.RemoteRegistrationConnection{RequestId: request.RequestID + ":registration", RegistrationId: material.RegistrationID, Endpoint: material.RemoteConnection.GatewayEndpoint, DisplayName: "Managed compute", Pool: pool, TrustBundle: trustBundle, SourcePolicyRevision: material.RemoteConnection.PolicyRevision}, Credential: adapter.plan.CandidateCredential})
	if err != nil {
		return fmt.Errorf("apply registration: %w", err)
	}
	if response.GetRegistration().GetId() != material.RegistrationID {
		return errors.New("registration identity mismatch")
	}
	return nil
}

func readRegistrationHTTPError(response *http.Response, operation string) error {
	result := registrationHTTPError{operation: operation, status: response.StatusCode}
	var payload struct {
		Error json.RawMessage `json:"error"`
	}
	if json.NewDecoder(io.LimitReader(response.Body, 4097)).Decode(&payload) != nil {
		return result
	}
	var code string
	if json.Unmarshal(payload.Error, &code) != nil {
		var detail struct {
			Code string `json:"code"`
		}
		if json.Unmarshal(payload.Error, &detail) == nil {
			code = detail.Code
		}
	}
	switch code {
	case "invalid_ticket", "not_found", "ticket_expired", "registration_revoked", "authorization_expired", "authorization_changed", "host_identity_mismatch", "credential_reauthorization_required", "scope_host_bound", "unavailable", "connection_config_unavailable", "invalid_request", "invalid_candidate_credential", "invalid_host_identity", "invalid_recovery_challenge", "invalid_recovery_proof", "recovery_target_unavailable", "original_subject_unknown":
		result.code = code
	}
	return result
}
