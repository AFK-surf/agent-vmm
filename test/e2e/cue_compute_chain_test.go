//go:build darwin && e2e

package e2e

import (
	"bytes"
	"context"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/internal/remoteconnector"
	"google.golang.org/protobuf/types/known/durationpb"
)

// This process owns only a temporary VM. The lifecycle transport forwards
// Electron commands to real Operator RPCs; it does not install a LaunchAgent.
func TestCueComputeSettingsBridgeE2E(t *testing.T) {
	path := os.Getenv("COMMA_VMM_E2E_CONFIG")
	if path == "" {
		t.Skip("requires Cue's isolated settings chain")
	}
	var fixture struct {
		Archive string `json:"archive_path"`
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(raw, &fixture); err != nil {
		t.Fatal(err)
	}
	h := startHostWithState(t, 16<<30)
	h.start(t)
	t.Cleanup(func() { h.stop(t, true) })
	archiveName := "import-cue-settings.tar"
	source, err := os.Open(fixture.Archive)
	if err != nil {
		t.Fatal(err)
	}
	dest, err := os.Create(filepath.Join(h.runtimeClient.archiveDirectory, archiveName))
	if err != nil {
		t.Fatal(err)
	}
	_, err = io.Copy(dest, source)
	source.Close()
	dest.Close()
	if err != nil {
		t.Fatal(err)
	}
	ready, _ := json.Marshal(map[string]string{"archive_url": h.runtimeClient.archiveURL + "/" + archiveName, "serial_log": h.serial})
	if err = os.WriteFile(path+".host-ready", ready, 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	var connectorConfig remoteconnector.Config
	var stopConnector context.CancelFunc
	var connectorDone chan error
	startConnector := func() {
		child, stop := context.WithCancel(ctx)
		stopConnector = stop
		connectorDone = make(chan error, 1)
		go func() { connectorDone <- remoteconnector.Run(child, connectorConfig) }()
	}
	disconnect := func() {
		if stopConnector != nil {
			stopConnector()
			<-connectorDone
			stopConnector = nil
		}
	}
	defer disconnect()
	commandDir := path + ".commands"
	if err = os.MkdirAll(commandDir, 0700); err != nil {
		t.Fatal(err)
	}
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			t.Fatal("settings chain exceeded fifteen minutes")
		case <-ticker.C:
		}
		if _, err = os.Stat(path + ".stop"); err == nil {
			return
		}
		entries, err := os.ReadDir(commandDir)
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) > 64 {
			t.Fatal("fixture transport exceeded bounded pending commands")
		}
		for _, entry := range entries {
			if !strings.HasSuffix(entry.Name(), ".request") {
				continue
			}
			requestPath := filepath.Join(commandDir, entry.Name())
			raw, err := os.ReadFile(requestPath)
			if err != nil {
				t.Fatal(err)
			}
			var command struct {
				Args  []string `json:"args"`
				Stdin string   `json:"stdin"`
			}
			if err = json.Unmarshal(raw, &command); err != nil {
				t.Fatal(err)
			}
			result := any(map[string]any{})
			operationCtx, stop := context.WithTimeout(ctx, 90*time.Second)
			arg := func(name string) string {
				for i, v := range command.Args {
					if v == name && i+1 < len(command.Args) {
						return command.Args[i+1]
					}
				}
				return ""
			}
			var operationErr error
			if len(command.Args) == 0 {
				operationErr = fmt.Errorf("missing operation")
			} else {
				switch command.Args[0] {
				case "maintenance-status":
					result = nil
				case "status":
					vm, readErr := h.client.GetVM(operationCtx, &hostv1.GetVMRequest{})
					operationErr = readErr
					value := map[string]any{"hostInstalled": true, "hostLoaded": h.process != nil, "hostReadable": operationErr == nil, "hostHealthy": operationErr == nil && vm.GetVm().GetState() == hostv1.VMState_VM_STATE_RUNNING, "connectorInstalled": connectorConfig.RegistrationID != "", "connectorLoaded": stopConnector != nil}
					if id := arg("--registration-id"); id != "" {
						registration, readErr := h.client.GetRemoteRegistration(operationCtx, &hostv1.GetRemoteRegistrationRequest{RegistrationId: id})
						if readErr != nil {
							value["registrationRead"] = "absent"
						} else {
							value["registrationRead"] = "present"
							states := map[hostv1.RemoteRegistrationState]string{hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_ENABLED: "enabled", hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_DRAINING: "draining", hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_REVOKED: "revoked"}
							value["registrationState"] = states[registration.GetRegistration().GetState()]
						}
					}
					result = value
				case "install":
					var descriptor struct {
						Operation string `json:"operation_id"`
						URL       string `json:"exchange_url"`
						Secret    string `json:"one_time_secret"`
					}
					operationErr = json.Unmarshal([]byte(command.Stdin), &descriptor)
					if operationErr == nil {
						identity, identityErr := h.client.GetDeviceIdentity(operationCtx, &hostv1.GetDeviceIdentityRequest{})
						operationErr = identityErr
						if operationErr == nil {
							body := map[string]any{"version": 2, "operation_id": descriptor.Operation, "device_id": identity.GetIdentity().GetDeviceId(), "root_public_key": base64.StdEncoding.EncodeToString(identity.GetIdentity().GetRootPublicKey()), "root_key_revision": identity.GetIdentity().GetRootKeyRevision()}
							var material struct {
								Registration   string `json:"registration_id"`
								Scope          string `json:"scope_digest"`
								IdentityDigest string `json:"host_identity_digest"`
								Remote         struct {
									Endpoint string `json:"gateway_endpoint"`
									Token    []byte `json:"enrollment_token"`
									Trust    []byte `json:"trust_bundle"`
									Revision uint64 `json:"policy_revision"`
									Pool     struct {
										Limits struct {
											PIDs uint32 `json:"pids"`
											Disk uint64 `json:"disk_bytes"`
										} `json:"per_environment_limits"`
										Egress string `json:"max_egress_mode"`
										Grace  uint64 `json:"stop_grace_seconds"`
									} `json:"pool_policy"`
								} `json:"remote_enrollment"`
							}
							operationErr = cueChainPost(operationCtx, descriptor.URL, descriptor.Secret, body, &material)
							if operationErr == nil {
								roots := x509.NewCertPool()
								if !roots.AppendCertsFromPEM(material.Remote.Trust) {
									operationErr = fmt.Errorf("invalid real enrollment trust")
								} else {
									egress := hostv1.EgressMode_EGRESS_MODE_DENY_ALL
									if material.Remote.Pool.Egress == "public_internet" {
										egress = hostv1.EgressMode_EGRESS_MODE_PUBLIC_INTERNET
									}
									limits := material.Remote.Pool.Limits
									begun, beginErr := h.client.BeginRemoteEnrollment(operationCtx, &hostv1.BeginRemoteEnrollmentRequest{RequestId: descriptor.Operation + ":managed-enrollment", RegistrationId: material.Registration, Endpoint: material.Remote.Endpoint, DisplayName: "Isolated settings fixture", ProtocolVersion: remoteconnector.ProtocolVersion, TrustBundle: material.Remote.Trust, ScopeDigest: material.Scope, SourcePolicyRevision: material.Remote.Revision, Pool: &hostv1.RemotePoolPolicy{PerEnvironmentLimits: &hostv1.ResourceLimits{Pids: &limits.PIDs, DiskBytes: &limits.Disk}, MaxEgressMode: egress, StopGrace: durationpb.New(time.Duration(material.Remote.Pool.Grace) * time.Second)}})
									operationErr = beginErr
									if operationErr == nil {
										connectorConfig = remoteconnector.Config{Endpoint: material.Remote.Endpoint, EndpointIdentity: "localhost", RegistrationID: material.Registration, RemoteComputeSocket: h.remoteComputeSocket, TrustSocket: h.trustSocket, TrustCapabilityFile: h.trustCapabilityFile, CredentialStore: &e2eCredentialStore{}, TLSRoots: roots}
										_, operationErr = remoteconnector.Enroll(operationCtx, connectorConfig, material.Remote.Token)
										if operationErr == nil {
											_, operationErr = h.client.CompleteRemoteEnrollment(operationCtx, &hostv1.CompleteRemoteEnrollmentRequest{RequestId: descriptor.Operation + ":complete", RegistrationId: material.Registration, ExpectedRevision: begun.GetRegistration().GetRevision()})
										}
										if operationErr == nil {
											startConnector()
											operationErr = cueChainPost(operationCtx, strings.TrimSuffix(descriptor.URL, "/exchange")+"/ack", descriptor.Secret, map[string]string{"operation_id": descriptor.Operation, "host_identity_digest": material.IdentityDigest}, nil)
										}
									}
								}
							}
						}
					}
				case "registration-state":
					id := arg("--registration-id")
					observed, readErr := h.client.GetRemoteRegistration(operationCtx, &hostv1.GetRemoteRegistrationRequest{RegistrationId: id})
					operationErr = readErr
					if operationErr == nil {
						state := hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_ENABLED
						if arg("--state") == "draining" {
							state = hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_DRAINING
						}
						_, operationErr = h.client.SetRemoteRegistrationState(operationCtx, &hostv1.SetRemoteRegistrationStateRequest{RequestId: arg("--request-id"), RegistrationId: id, ExpectedRevision: observed.GetRegistration().GetRevision(), State: state})
					}
				case "fixture-disconnect":
					disconnect()
				case "fixture-reconnect":
					if stopConnector == nil && connectorConfig.RegistrationID != "" {
						startConnector()
					} else {
						operationErr = fmt.Errorf("connector is not disconnected")
					}
				default:
					operationErr = fmt.Errorf("unsupported fixture operation %s", command.Args[0])
				}
			}
			stop()
			response := map[string]any{"result": result}
			if operationErr != nil {
				response["error"] = operationErr.Error()
			}
			raw, _ = json.Marshal(response)
			if err = os.WriteFile(strings.TrimSuffix(requestPath, ".request")+".response", raw, 0600); err != nil {
				t.Fatal(err)
			}
			if err = os.Remove(requestPath); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func cueChainPost(ctx context.Context, url, secret string, body any, result any) error {
	raw, err := json.Marshal(body)
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(raw))
	if err != nil {
		return err
	}
	request.Header.Set("Authorization", "InstallOperation "+secret)
	request.Header.Set("Content-Type", "application/json")
	response, err := (&http.Client{Timeout: 30 * time.Second}).Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		raw, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return fmt.Errorf("enrollment HTTP %d: %s", response.StatusCode, raw)
	}
	if result != nil {
		return json.NewDecoder(io.LimitReader(response.Body, 65536)).Decode(result)
	}
	return nil
}
