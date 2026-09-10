package main

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
	"strings"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/internal/remoteconnector"
	"google.golang.org/protobuf/types/known/durationpb"
)

const operationHandoffDescriptorMaxBytes = 4096
const operationHandoffExchangePath = "/v1/compute/agent-vmm/install-operations/exchange"

var operationIDPattern = regexp.MustCompile(`\A[A-Za-z0-9_-]{1,128}\z`)
var installHTTPClient = &http.Client{Timeout: 20 * time.Second}
var errManagedInstallPlanUnavailable = errors.New("managed install plan is unavailable")

type operationHandoffDescriptor struct {
	Version       int    `json:"version"`
	OperationID   string `json:"operation_id"`
	ExchangeURL   string `json:"exchange_url"`
	OneTimeSecret string `json:"one_time_secret"`
	ExpiresAt     string `json:"expires_at"`
}

type applianceMaterial struct {
	InstallationID string `json:"installation_id"`
	ApprovedGrant  string `json:"approved_grant"`
	BootstrapSeed  string `json:"bootstrap_seed"`
}

type resourcePolicy struct {
	CPUMillis   uint32 `json:"cpu_millis"`
	MemoryBytes uint64 `json:"memory_bytes"`
	PIDs        uint32 `json:"pids"`
	DiskBytes   uint64 `json:"disk_bytes"`
}

type remotePoolMaterial struct {
	MaxEnvironments        uint32         `json:"max_environments"`
	PerEnvironmentLimits   resourcePolicy `json:"per_environment_limits"`
	MaxEgressMode          string         `json:"max_egress_mode"`
	MaxLeaseSeconds        uint64         `json:"max_lease_seconds"`
	DisconnectGraceSeconds uint64         `json:"disconnect_grace_seconds"`
	StopGraceSeconds       uint64         `json:"stop_grace_seconds"`
}

type remoteEnrollmentMaterial struct {
	GatewayEndpoint string             `json:"gateway_endpoint"`
	EnrollmentToken string             `json:"enrollment_token"`
	TrustBundle     string             `json:"trust_bundle"`
	PolicyRevision  uint64             `json:"policy_revision"`
	PoolPolicy      remotePoolMaterial `json:"pool_policy"`
}

type operationHandoffPlan struct {
	Version            int                      `json:"version"`
	OperationID        string                   `json:"operation_id"`
	RegistrationID     string                   `json:"registration_id"`
	ScopeDigest        string                   `json:"scope_digest"`
	Appliance          applianceMaterial        `json:"appliance"`
	RemoteEnrollment   remoteEnrollmentMaterial `json:"remote_enrollment"`
	HostIdentityDigest string                   `json:"host_identity_digest"`
}

type persistedInstallOperation struct {
	Version            int                         `json:"version"`
	OperationID        string                      `json:"operation_id"`
	DeadlineUnixMillis int64                       `json:"deadline_unix_millis"`
	Acknowledged       bool                        `json:"acknowledged"`
	AckDescriptor      *operationHandoffDescriptor `json:"ack_descriptor,omitempty"`
	Material           operationHandoffPlan        `json:"material"`
}

type completedOperationReceipt struct {
	Version        int    `json:"version"`
	OperationID    string `json:"operation_id"`
	RegistrationID string `json:"registration_id"`
	InstallationID string `json:"installation_id"`
}

type operationHandoffHTTPError struct {
	operation string
	status    int
}

func (err operationHandoffHTTPError) Error() string {
	return fmt.Sprintf("operation handoff %s returned HTTP %d", err.operation, err.status)
}

func terminalOperationHandoffError(err error) bool {
	var responseError operationHandoffHTTPError
	return errors.As(err, &responseError) && responseError.status >= 400 && responseError.status < 500
}

// newProductionLifecycleAdapterForResume follows the durable
// plan/ACK-capability recovery modeled in the Cue repository at
// tla/salix/VMMInstallHandoff.tla. Unknown ACK keeps the exact capability;
// known ACK rewrites the plan without it.
func newProductionLifecycleAdapterForResume(value paths, operationID string) (LifecycleAdapter, error) {
	adapter, err := productionLifecycleAdapterForResume(value, operationID)
	if err != nil {
		return nil, err
	}
	return lifecycleAdapterFor(adapter), nil
}

func productionLifecycleAdapterForResume(value paths, operationID string) (*productionLifecycleAdapter, error) {
	directory := filepath.Join(value.applicationData, "install-operations", operationID)
	data, err := readInstallOperationState(value, operationID, "plan.json", lifecycleOperationHandoffMaxBytes)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, errManagedInstallPlanUnavailable
		}
		return nil, fmt.Errorf("read managed install plan: %w", err)
	}
	var plan persistedInstallOperation
	if len(data) > lifecycleOperationHandoffMaxBytes || json.Unmarshal(data, &plan) != nil ||
		plan.Version != 1 || plan.OperationID != operationID || plan.Material.OperationID != operationID ||
		plan.DeadlineUnixMillis <= 0 || !validPersistedAcknowledgement(plan, operationID) {
		return nil, errors.New("managed install plan is invalid")
	}
	adapter := &productionLifecycleAdapter{
		value:      value,
		configPath: filepath.Join(directory, "appliance.json"),
		descriptor: plan.AckDescriptor,
		material:   &plan.Material,
	}
	return adapter, nil
}

func decodeOperationHandoffDescriptor(input io.Reader) (operationHandoffDescriptor, error) {
	decoder := json.NewDecoder(io.LimitReader(input, operationHandoffDescriptorMaxBytes+1))
	decoder.DisallowUnknownFields()
	var descriptor operationHandoffDescriptor
	if err := decoder.Decode(&descriptor); err != nil {
		return descriptor, fmt.Errorf("decode install descriptor: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return descriptor, errors.New("install descriptor must contain one JSON object")
	}
	parsed, err := url.Parse(descriptor.ExchangeURL)
	if descriptor.Version != 1 || !operationIDPattern.MatchString(descriptor.OperationID) || descriptor.OneTimeSecret == "" || len(descriptor.OneTimeSecret) > 128 ||
		err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" ||
		parsed.Path != operationHandoffExchangePath {
		return descriptor, errors.New("install descriptor is invalid")
	}
	expiresAt, err := time.Parse(time.RFC3339, descriptor.ExpiresAt)
	if err != nil || time.Now().After(expiresAt) {
		return descriptor, errors.New("install descriptor is expired")
	}
	return descriptor, nil
}

func (adapter *productionLifecycleAdapter) ensureInstallPlan(ctx context.Context) error {
	if adapter.descriptor == nil && adapter.material == nil {
		return nil
	}
	operationID := ""
	if adapter.material != nil {
		operationID = adapter.material.OperationID
	}
	if adapter.descriptor != nil {
		operationID = adapter.descriptor.OperationID
	}
	directory := filepath.Join(adapter.value.applicationData, "install-operations", operationID)
	if data, err := readInstallOperationState(adapter.value, operationID, "plan.json", lifecycleOperationHandoffMaxBytes); err == nil {
		var plan persistedInstallOperation
		if json.Unmarshal(data, &plan) != nil || plan.Version != 1 || plan.OperationID != operationID || plan.Material.OperationID != operationID ||
			plan.DeadlineUnixMillis <= 0 || !validPersistedAcknowledgement(plan, operationID) {
			return errors.New("managed install plan is invalid")
		}
		if !plan.Acknowledged {
			if !validPersistedAckDescriptor(plan.AckDescriptor, operationID) {
				return errors.New("managed install acknowledgement capability is unavailable")
			}
			if adapter.descriptor != nil && *adapter.descriptor != *plan.AckDescriptor {
				return errors.New("managed install acknowledgement capability changed")
			}
			if err := acknowledgeOperationHandoff(ctx, *plan.AckDescriptor, plan.Material); err != nil {
				if terminalOperationHandoffError(err) {
					return terminalManagedInstallError(err)
				}
				return err
			}
			if _, err := persistOperationHandoff(adapter.value, directory, plan.Material, plan.DeadlineUnixMillis, true); err != nil {
				return err
			}
		}
		adapter.material = &plan.Material
		adapter.descriptor = nil
		adapter.configPath = filepath.Join(directory, "appliance.json")
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("read durable install plan: %w", err)
	}
	if adapter.descriptor == nil {
		return errors.New("managed install acknowledgement capability is unavailable")
	}

	connection, err := operatorConnection(ctx, filepath.Join(adapter.value.applicationData, "operator.sock"))
	if err != nil {
		return errors.New("Agent VMM operator is not readable for operation exchange")
	}
	identityResponse, err := hostv1.NewOperatorServiceClient(connection).GetDeviceIdentity(ctx, &hostv1.GetDeviceIdentityRequest{})
	connection.Close()
	if err != nil || identityResponse.GetState() != hostv1.DeviceIdentityState_DEVICE_IDENTITY_STATE_READY {
		return errors.New("Agent VMM device identity is not ready")
	}
	identity := identityResponse.GetIdentity()
	requestBody, err := managedInstallExchangeRequest(
		adapter.descriptor.OperationID,
		identity.GetDeviceId(),
		identity.GetRootPublicKey(),
		identity.GetRootKeyRevision(),
	)
	if err != nil {
		return terminalManagedInstallError(err)
	}
	material, err := exchangeOperationHandoff(ctx, *adapter.descriptor, requestBody)
	if err != nil {
		return err
	}
	if err := validateOperationHandoff(material, *adapter.descriptor); err != nil {
		return terminalManagedInstallError(err)
	}
	deadlineUnixMillis := time.Now().Add(defaultCoordinatorOperationTimeout).UnixMilli()
	configPath, err := persistOperationHandoff(adapter.value, directory, material, deadlineUnixMillis, false, adapter.descriptor)
	if err != nil {
		return err
	}
	if err := acknowledgeOperationHandoff(ctx, *adapter.descriptor, material); err != nil {
		if terminalOperationHandoffError(err) {
			return terminalManagedInstallError(err)
		}
		return err
	}
	if _, err := persistOperationHandoff(adapter.value, directory, material, deadlineUnixMillis, true); err != nil {
		return err
	}
	adapter.material = &material
	adapter.descriptor = nil
	adapter.configPath = configPath
	return nil
}

func managedInstallExchangeRequest(operationID, deviceID string, rootPublicKey []byte, rootKeyRevision uint64) ([]byte, error) {
	return json.Marshal(map[string]any{
		"version": 2, "operation_id": operationID, "device_id": deviceID,
		"root_public_key": base64.StdEncoding.EncodeToString(rootPublicKey), "root_key_revision": rootKeyRevision,
	})
}

func validPersistedAckDescriptor(descriptor *operationHandoffDescriptor, operationID string) bool {
	if descriptor == nil || descriptor.Version != 1 || descriptor.OperationID != operationID ||
		descriptor.OneTimeSecret == "" || len(descriptor.OneTimeSecret) > 128 {
		return false
	}
	parsed, err := url.Parse(descriptor.ExchangeURL)
	return err == nil && parsed.Scheme == "https" && parsed.Host != "" && parsed.User == nil &&
		parsed.RawQuery == "" && parsed.Fragment == "" && parsed.Path == operationHandoffExchangePath
}

func validPersistedAcknowledgement(plan persistedInstallOperation, operationID string) bool {
	if plan.Acknowledged {
		return plan.AckDescriptor == nil
	}
	return validPersistedAckDescriptor(plan.AckDescriptor, operationID)
}

func exchangeOperationHandoff(ctx context.Context, descriptor operationHandoffDescriptor, body []byte) (operationHandoffPlan, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, descriptor.ExchangeURL, bytes.NewReader(body))
	if err != nil {
		return operationHandoffPlan{}, errors.New("create install exchange request failed")
	}
	request.Header.Set("Authorization", "InstallOperation "+descriptor.OneTimeSecret)
	request.Header.Set("Content-Type", "application/json")
	response, err := installHTTPClient.Do(request)
	if err != nil {
		return operationHandoffPlan{}, errors.New("operation handoff exchange failed")
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return operationHandoffPlan{}, operationHandoffHTTPError{operation: "exchange", status: response.StatusCode}
	}
	decoder := json.NewDecoder(io.LimitReader(response.Body, lifecycleOperationHandoffMaxBytes+1))
	decoder.DisallowUnknownFields()
	var material operationHandoffPlan
	if err := decoder.Decode(&material); err != nil {
		return operationHandoffPlan{}, fmt.Errorf("decode install material: %w", err)
	}
	return material, nil
}

func acknowledgeOperationHandoff(ctx context.Context, descriptor operationHandoffDescriptor, material operationHandoffPlan) error {
	parsed, _ := url.Parse(descriptor.ExchangeURL)
	parsed.Path = strings.TrimSuffix(parsed.Path, "/exchange") + "/ack"
	body, _ := json.Marshal(map[string]string{"operation_id": material.OperationID, "host_identity_digest": material.HostIdentityDigest})
	var lastStatus int
	for attempt := 0; attempt < 3; attempt++ {
		request, err := http.NewRequestWithContext(ctx, http.MethodPost, parsed.String(), bytes.NewReader(body))
		if err == nil {
			request.Header.Set("Authorization", "InstallOperation "+descriptor.OneTimeSecret)
			request.Header.Set("Content-Type", "application/json")
			response, requestErr := installHTTPClient.Do(request)
			if requestErr == nil {
				lastStatus = response.StatusCode
				io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
				response.Body.Close()
				if response.StatusCode == http.StatusOK {
					return nil
				}
				if !retryableOperationHandoffStatus(response.StatusCode) {
					return operationHandoffHTTPError{operation: "acknowledgement", status: response.StatusCode}
				}
			}
		}
		if attempt < 2 {
			timer := time.NewTimer(time.Duration(attempt+1) * 250 * time.Millisecond)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
		}
	}
	if lastStatus != 0 {
		return operationHandoffHTTPError{operation: "acknowledgement", status: lastStatus}
	}
	return errors.New("operation handoff acknowledgement failed")
}

func retryableOperationHandoffStatus(status int) bool {
	return status == http.StatusRequestTimeout || status == http.StatusTooManyRequests || status >= 500
}

func validateOperationHandoff(material operationHandoffPlan, descriptor operationHandoffDescriptor) error {
	if material.Version != 1 || material.OperationID != descriptor.OperationID || !operationIDPattern.MatchString(material.RegistrationID) {
		return errors.New("operation handoff identity is invalid")
	}
	for name, value := range map[string]string{"scope_digest": material.ScopeDigest, "host_identity_digest": material.HostIdentityDigest} {
		decoded, decodeErr := base64.RawURLEncoding.DecodeString(value)
		if decodeErr != nil || len(decoded) != 32 {
			return fmt.Errorf("install material %s is invalid", name)
		}
	}
	if material.RemoteEnrollment.PolicyRevision == 0 || material.RemoteEnrollment.GatewayEndpoint == "" || material.RemoteEnrollment.PoolPolicy.MaxEnvironments == 0 {
		return errors.New("install material remote enrollment policy is incomplete")
	}
	return nil
}

func persistOperationHandoff(value paths, directory string, material operationHandoffPlan, deadlineUnixMillis int64, acknowledged bool, descriptors ...*operationHandoffDescriptor) (string, error) {
	return persistOperationHandoffOwned(value, directory, material, deadlineUnixMillis, acknowledged, lifecycleOperationOwner(value), descriptors...)
}

func persistOperationHandoffOwned(value paths, directory string, material operationHandoffPlan, deadlineUnixMillis int64, acknowledged bool, owner *operationFileOwner, descriptors ...*operationHandoffDescriptor) (string, error) {
	if deadlineUnixMillis <= 0 {
		return "", errors.New("managed install deadline is invalid")
	}
	if !operationIDPattern.MatchString(material.OperationID) || filepath.Clean(directory) != filepath.Join(value.applicationData, "install-operations", material.OperationID) {
		return "", errors.New("managed install operation directory is invalid")
	}
	operationDirectory, err := openInstallOperationDirectory(value, material.OperationID, owner)
	if err != nil {
		return "", fmt.Errorf("create install operation directory: %w", err)
	}
	defer operationDirectory.Close()
	var ackDescriptor *operationHandoffDescriptor
	if !acknowledged && len(descriptors) != 0 {
		ackDescriptor = descriptors[0]
	}
	if !acknowledged && !validPersistedAckDescriptor(ackDescriptor, material.OperationID) {
		return "", errors.New("managed install acknowledgement capability is invalid")
	}
	grant, err := decodeOperationField("approved_grant", material.Appliance.ApprovedGrant, 1<<20)
	if err != nil {
		return "", err
	}
	seed, err := decodeOperationField("bootstrap_seed", material.Appliance.BootstrapSeed, 128<<10)
	if err != nil {
		return "", err
	}
	manifestPath := filepath.Join(directory, "manifest.pb")
	if exists, err := regularStateFileExistsAt(operationDirectory, "manifest.pb"); err != nil {
		return "", err
	} else if exists {
		if err := handoffExistingStateFileAt(operationDirectory, "manifest.pb", owner); err != nil {
			return "", err
		}
	} else if !exists {
		manifest, readErr := os.ReadFile(filepath.Join(value.resources, "Guest", "mcp-appliance", "manifest.pb"))
		if readErr != nil || len(manifest) == 0 || len(manifest) > 4<<20 {
			return "", errors.New("bundled appliance manifest is unavailable")
		}
		if err := atomicWriteStateFileAt(operationDirectory, "manifest.pb", manifest, 0o600, owner); err != nil {
			return "", err
		}
	}
	artifacts := []struct {
		name string
		data []byte
	}{{"approved-grant.pb", grant}, {"bootstrap-seed.bin", seed}}
	for _, artifact := range artifacts {
		if err := atomicWriteStateFileAt(operationDirectory, artifact.name, artifact.data, 0o600, owner); err != nil {
			return "", err
		}
	}
	config := applianceLifecycleConfig{Version: 1, InstallationID: material.Appliance.InstallationID, ManifestPath: manifestPath, ApprovedGrantPath: filepath.Join(directory, "approved-grant.pb"), BootstrapSeedPath: filepath.Join(directory, "bootstrap-seed.bin")}
	configBytes, _ := json.Marshal(config)
	configPath := filepath.Join(directory, "appliance.json")
	if err := atomicWriteStateFileAt(operationDirectory, "appliance.json", configBytes, 0o600, owner); err != nil {
		return "", err
	}
	planBytes, _ := json.Marshal(persistedInstallOperation{Version: 1, OperationID: material.OperationID, DeadlineUnixMillis: deadlineUnixMillis, Acknowledged: acknowledged, AckDescriptor: ackDescriptor, Material: material})
	if err := atomicWriteStateFileAt(operationDirectory, "plan.json", planBytes, 0o600, owner); err != nil {
		return "", err
	}
	return configPath, nil
}

func (material operationHandoffPlan) remotePoolPolicy() (*hostv1.RemotePoolPolicy, error) {
	policy := material.RemoteEnrollment.PoolPolicy
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
		return &hostv1.ResourceLimits{CpuMillis: &value.CPUMillis, MemoryBytes: &value.MemoryBytes, Pids: &value.PIDs, DiskBytes: &value.DiskBytes}
	}
	return &hostv1.RemotePoolPolicy{MaxEnvironments: policy.MaxEnvironments, PerEnvironmentLimits: limits(policy.PerEnvironmentLimits), MaxEgressMode: egress,
		MaxLeaseDuration: durationpb.New(time.Duration(policy.MaxLeaseSeconds) * time.Second), DisconnectGrace: durationpb.New(time.Duration(policy.DisconnectGraceSeconds) * time.Second), StopGrace: durationpb.New(time.Duration(policy.StopGraceSeconds) * time.Second)}, nil
}

func (adapter *productionLifecycleAdapter) applyManagedEnrollment(ctx context.Context, request LifecycleRequest) error {
	if adapter.material == nil {
		return nil
	}
	material := adapter.material
	pool, err := material.remotePoolPolicy()
	if err != nil {
		return terminalManagedInstallError(err)
	}
	trustBundle, err := base64.StdEncoding.DecodeString(material.RemoteEnrollment.TrustBundle)
	if err != nil {
		return terminalManagedInstallError(errors.New("decode managed trust bundle failed"))
	}
	token, err := base64.StdEncoding.DecodeString(material.RemoteEnrollment.EnrollmentToken)
	if err != nil {
		return terminalManagedInstallError(errors.New("decode managed enrollment token failed"))
	}
	connection, err := operatorConnection(ctx, filepath.Join(adapter.value.applicationData, "operator.sock"))
	if err != nil {
		return err
	}
	defer connection.Close()
	client := hostv1.NewOperatorServiceClient(connection)
	response, err := client.ApplyManagedRemoteEnrollment(ctx, &hostv1.ApplyManagedRemoteEnrollmentRequest{Registration: &hostv1.BeginRemoteEnrollmentRequest{
		RequestId: request.RequestID + ":managed-enrollment", RegistrationId: material.RegistrationID, Endpoint: material.RemoteEnrollment.GatewayEndpoint,
		DisplayName: "Salix managed compute", ProtocolVersion: remoteconnector.ProtocolVersion, Pool: pool, TrustBundle: trustBundle,
		ScopeDigest: material.ScopeDigest, SourcePolicyRevision: material.RemoteEnrollment.PolicyRevision}, EnrollmentToken: token})
	for index := range token {
		token[index] = 0
	}
	if err != nil {
		return fmt.Errorf("apply managed remote enrollment: %w", err)
	}
	if response.GetRegistration().GetId() != material.RegistrationID {
		return terminalManagedInstallError(errors.New("managed enrollment registration identity mismatch"))
	}
	deadline := time.NewTicker(250 * time.Millisecond)
	defer deadline.Stop()
	for {
		observed, observeErr := client.GetRemoteRegistration(ctx, &hostv1.GetRemoteRegistrationRequest{RegistrationId: material.RegistrationID})
		registration := observed.GetRegistration()
		if observeErr == nil && registration.GetEnrollmentState() == hostv1.RemoteEnrollmentState_REMOTE_ENROLLMENT_STATE_READY && registration.GetConnectionState() == hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_CONNECTED {
			return nil
		}
		select {
		case <-ctx.Done():
			return errors.New("managed connector readiness was not observed")
		case <-deadline.C:
		}
	}
}

func cleanupInstallPlan(value paths, operationID string) error {
	return removeInstallOperationState(value, operationID)
}

func completedInstallPath(value paths, operationID string) string {
	return filepath.Join(value.applicationData, "install-completions", operationID+".json")
}

func persistCompletedInstall(value paths, adapter *productionLifecycleAdapter) error {
	return persistCompletedInstallOwned(value, adapter, lifecycleOperationOwner(value))
}

func persistCompletedInstallOwned(value paths, adapter *productionLifecycleAdapter, owner *operationFileOwner) error {
	if adapter == nil || adapter.material == nil {
		return errors.New("managed install completion material is unavailable")
	}
	material := adapter.material
	receipt := completedOperationReceipt{
		Version:        1,
		OperationID:    material.OperationID,
		RegistrationID: material.RegistrationID,
		InstallationID: material.Appliance.InstallationID,
	}
	if !validCompletedInstallReceipt(receipt, material.OperationID) {
		return errors.New("managed install completion receipt is invalid")
	}
	directory, err := openInstallCompletionDirectory(value, owner)
	if err != nil {
		return fmt.Errorf("create install completion directory: %w", err)
	}
	defer directory.Close()
	encoded, _ := json.Marshal(receipt)
	return atomicWriteStateFileAt(directory, material.OperationID+".json", encoded, 0o600, owner)
}

func completedInstallExists(value paths, operationID string) (bool, error) {
	data, err := readInstallCompletionState(value, operationID, 4096)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("read managed install completion: %w", err)
	}
	var receipt completedOperationReceipt
	if len(data) > 4096 || json.Unmarshal(data, &receipt) != nil || !validCompletedInstallReceipt(receipt, operationID) {
		return false, errors.New("managed install completion receipt is invalid")
	}
	return true, nil
}

func validCompletedInstallReceipt(receipt completedOperationReceipt, operationID string) bool {
	return receipt.Version == 1 && receipt.OperationID == operationID &&
		operationIDPattern.MatchString(operationID) && operationIDPattern.MatchString(receipt.RegistrationID) &&
		receipt.InstallationID != "" && len(receipt.InstallationID) <= 160
}
