//go:build darwin || linux

package lifecycle

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/trust"
)

type registrationRecoveryDescriptor struct {
	Version        int    `json:"version"`
	RegistrationID string `json:"registration_id"`
	RecoveryURL    string `json:"recovery_url"`
	Authorization  string `json:"authorization"`
}

var recoveryPathPattern = regexp.MustCompile(`^/v1/(?:comma/workspaces/[A-Za-z0-9_-]+/compute-nodes|orgs/[A-Za-z0-9_-]+/projects/[A-Za-z0-9_-]+/compute-nodes)/agent-vmm/registrations/([A-Za-z0-9_-]+)/recovery$`)

func validRecoveryURL(raw, registrationID string) bool {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return false
	}
	match := recoveryPathPattern.FindStringSubmatch(parsed.Path)
	return len(match) == 2 && match[1] == registrationID && (parsed.Scheme == "https" || parsed.Scheme == "http" && isRegistrationLoopbackHost(parsed.Hostname()))
}
func decodeRecoveryDescriptor(input io.Reader) (registrationRecoveryDescriptor, error) {
	var descriptor registrationRecoveryDescriptor
	decoder := json.NewDecoder(io.LimitReader(input, 8193))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&descriptor) != nil || decoder.Decode(new(any)) != io.EOF || descriptor.Version != 1 || !operationIDPattern.MatchString(descriptor.RegistrationID) || !validRecoveryURL(descriptor.RecoveryURL, descriptor.RegistrationID) || len(descriptor.Authorization) == 0 || len(descriptor.Authorization) > 4096 || strings.ContainsAny(descriptor.Authorization, "\r\n") {
		return descriptor, errors.New("credential replacement descriptor is invalid")
	}
	return descriptor, nil
}
func (adapter *productionLifecycleAdapter) ensureRecoveryPlan(ctx context.Context) error {
	descriptor := adapter.recovery
	plan, err := loadClaimPlan(adapter.value, adapter.requestID)
	if err != nil && !errors.Is(err, os.ErrNotExist) && !errors.Is(err, errRegistrationPlanUnavailable) {
		return err
	}
	if plan != nil && (plan.Descriptor.RegistrationID != descriptor.RegistrationID || plan.RecoveryURL != "" && plan.RecoveryURL != descriptor.RecoveryURL) {
		return errors.New("credential replacement request target changed")
	}
	if plan == nil {
		connection, err := operatorConnection(ctx, filepath.Join(adapter.value.applicationData, "operator.sock"))
		if err != nil {
			return err
		}
		identity, err := hostv1.NewOperatorServiceClient(connection).GetDeviceIdentity(ctx, &hostv1.GetDeviceIdentityRequest{})
		connection.Close()
		if err != nil || identity.GetState() != hostv1.DeviceIdentityState_DEVICE_IDENTITY_STATE_READY {
			return errors.New("original Host identity is unavailable")
		}
		candidate, err := newCandidateCredential()
		if err != nil {
			return err
		}
		plan = &registrationClaimPlan{Version: 2, RequestID: adapter.requestID, Descriptor: registrationClaimDescriptor{Version: 1, RegistrationID: descriptor.RegistrationID}, RecoveryURL: descriptor.RecoveryURL, DeviceID: identity.GetIdentity().GetDeviceId(), RootPublicKey: identity.GetIdentity().GetRootPublicKey(), RootKeyRevision: identity.GetIdentity().GetRootKeyRevision(), CandidateCredential: candidate}
		if err := persistClaimPlan(adapter.value, plan); err != nil {
			return err
		}
	}
	adapter.plan = plan
	if plan.Material == nil {
		hash := sha256.Sum256(plan.CandidateCredential)
		var response struct {
			RegistrationID string          `json:"registration_id"`
			Challenge      json.RawMessage `json:"challenge"`
		}
		if err := recoveryPOST(ctx, descriptor, descriptor.RecoveryURL+"/challenge", map[string]any{"candidate_credential_hash": base64.StdEncoding.EncodeToString(hash[:])}, &response); err != nil {
			return err
		}
		challenge, _, err := trust.Parse(response.Challenge, time.Now())
		if err != nil || response.RegistrationID != descriptor.RegistrationID || challenge.RegistrationID != descriptor.RegistrationID || challenge.CandidateCredentialHash != base64.StdEncoding.EncodeToString(hash[:]) {
			return errors.New("credential replacement challenge target mismatch")
		}
		connection, err := operatorConnection(ctx, filepath.Join(adapter.value.applicationData, "operator.sock"))
		if err != nil {
			return err
		}
		proof, err := hostv1.NewOperatorServiceClient(connection).SignRegistrationRecovery(ctx, &hostv1.SignRegistrationRecoveryRequest{ChallengeJson: response.Challenge})
		connection.Close()
		if err != nil {
			return err
		}
		material := registrationConfiguration{}
		body := map[string]any{"candidate_credential": base64.StdEncoding.EncodeToString(plan.CandidateCredential), "proof": map[string]any{"device_id": proof.GetIdentity().GetDeviceId(), "root_public_key": base64.StdEncoding.EncodeToString(proof.GetIdentity().GetRootPublicKey()), "root_key_revision": proof.GetIdentity().GetRootKeyRevision(), "nonce": challenge.Nonce, "signature": base64.StdEncoding.EncodeToString(proof.GetSignature())}}
		if err := recoveryPOST(ctx, descriptor, descriptor.RecoveryURL, body, &material); err != nil {
			return err
		}
		if material.RegistrationID != descriptor.RegistrationID || material.Version != 1 {
			return errors.New("credential replacement result target mismatch")
		}
		plan.Material = &material
		if err := persistClaimPlan(adapter.value, plan); err != nil {
			return err
		}
	}
	adapter.material = plan.Material
	return nil
}
func recoveryPOST(ctx context.Context, descriptor *registrationRecoveryDescriptor, endpoint string, body any, result any) error {
	raw, err := json.Marshal(body)
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(raw))
	if err != nil {
		return err
	}
	request.Header.Set("Authorization", "Bearer "+descriptor.Authorization)
	request.Header.Set("Content-Type", "application/json")
	response, err := registrationHTTPClient.Do(request)
	if err != nil {
		return errors.New("credential replacement request failed")
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return readRegistrationHTTPError(response, "credential-replace")
	}
	decoder := json.NewDecoder(io.LimitReader(response.Body, registrationPlanMaxBytes+1))
	if decoder.Decode(result) != nil || decoder.Decode(new(any)) != io.EOF {
		return errors.New("credential replacement response is invalid")
	}
	return nil
}

func newCandidateCredential() ([]byte, error) {
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return nil, err
	}
	return []byte(base64.RawURLEncoding.EncodeToString(raw)), nil
}
