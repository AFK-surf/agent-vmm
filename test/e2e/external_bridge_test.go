//go:build darwin && e2e

package e2e

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
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
func TestExternalSettingsBridgeE2E(t *testing.T) {
	path := os.Getenv("AGENT_VMM_EXTERNAL_E2E_CONFIG")
	if path == "" {
		t.Skip("requires an external settings fixture")
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
	// Two 2 GiB Shells must fit after the 4 GiB emergency floor, metadata
	// growth and each namespace's remaining quota commitment.
	h := startHostWithState(t, 24<<30)
	h.start(t)
	t.Cleanup(func() { h.stop(t, true) })
	archiveName := "import-external-settings.tar"
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
				case "registration-claim":
					var descriptor struct {
						RegistrationID string `json:"registration_id"`
						URL            string `json:"claim_url"`
						Ticket         string `json:"ticket"`
					}
					operationErr = json.Unmarshal([]byte(command.Stdin), &descriptor)
					if operationErr == nil {
						identity, identityErr := h.client.GetDeviceIdentity(operationCtx, &hostv1.GetDeviceIdentityRequest{})
						operationErr = identityErr
						candidatePath := filepath.Join(commandDir, arg("--request-id")+".candidate")
						candidate, readErr := os.ReadFile(candidatePath)
						if errors.Is(readErr, os.ErrNotExist) {
							raw := make([]byte, 32)
							_, operationErr = rand.Read(raw)
							candidate = []byte(base64.RawURLEncoding.EncodeToString(raw))
							if operationErr == nil {
								operationErr = os.WriteFile(candidatePath, candidate, 0600)
							}
						} else if readErr != nil {
							operationErr = readErr
						}
						if operationErr == nil {
							body := map[string]any{"version": 1, "registration_id": descriptor.RegistrationID, "device_id": identity.GetIdentity().GetDeviceId(), "root_public_key": base64.StdEncoding.EncodeToString(identity.GetIdentity().GetRootPublicKey()), "root_key_revision": identity.GetIdentity().GetRootKeyRevision(), "candidate_credential": base64.StdEncoding.EncodeToString(candidate)}
							var material struct {
								RegistrationID string `json:"registration_id"`
								Remote         struct {
									Endpoint string `json:"gateway_endpoint"`
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
								} `json:"remote_connection"`
							}
							operationErr = externalChainPost(operationCtx, descriptor.URL, descriptor.Ticket, body, &material)
							if operationErr == nil {
								egress := hostv1.EgressMode_EGRESS_MODE_DENY_ALL
								if material.Remote.Pool.Egress == "public_internet" {
									egress = hostv1.EgressMode_EGRESS_MODE_PUBLIC_INTERNET
								}
								limits := material.Remote.Pool.Limits
								_, operationErr = h.client.ApplyRemoteRegistration(operationCtx, &hostv1.ApplyRemoteRegistrationRequest{Registration: &hostv1.RemoteRegistrationConnection{RequestId: arg("--request-id") + ":registration", RegistrationId: material.RegistrationID, Endpoint: material.Remote.Endpoint, DisplayName: "Isolated settings fixture", TrustBundle: material.Remote.Trust, SourcePolicyRevision: material.Remote.Revision, Pool: &hostv1.RemotePoolPolicy{PerEnvironmentLimits: &hostv1.ResourceLimits{Pids: &limits.PIDs, DiskBytes: &limits.Disk}, MaxEgressMode: egress, StopGrace: durationpb.New(time.Duration(material.Remote.Pool.Grace) * time.Second)}}, Credential: candidate})
								if operationErr == nil {
									roots := x509.NewCertPool()
									if !roots.AppendCertsFromPEM(material.Remote.Trust) {
										operationErr = fmt.Errorf("invalid fixture TLS trust")
									} else {
										connectorConfig = remoteconnector.Config{RegistrationID: material.RegistrationID, RemoteComputeSocket: h.remoteComputeSocket, Transport: remoteconnector.ExternalTransport{Endpoint: material.Remote.Endpoint, Credentials: &e2eCredentialStore{value: candidate}, Roots: roots, Trust: &remoteconnector.TrustBoundary{Socket: h.trustSocket}}}
										startConnector()
									}
								}
							}
						}
					}
					result = map[string]any{"requestId": arg("--request-id"), "operationId": arg("--request-id"), "targetRegistrationId": descriptor.RegistrationID, "kind": "registration-claim", "outcome": "succeeded"}
				case "registration-enable", "registration-drain":
					id := arg("--registration-id")
					observed, readErr := h.client.GetRemoteRegistration(operationCtx, &hostv1.GetRemoteRegistrationRequest{RegistrationId: id})
					operationErr = readErr
					if operationErr == nil {
						state := hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_ENABLED
						if command.Args[0] == "registration-drain" {
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

func externalChainPost(ctx context.Context, url, secret string, body any, result any) error {
	raw, err := json.Marshal(body)
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(raw))
	if err != nil {
		return err
	}
	request.Header.Set("Authorization", "RegistrationTicket "+secret)
	request.Header.Set("Content-Type", "application/json")
	response, err := (&http.Client{Timeout: 30 * time.Second}).Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		raw, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return fmt.Errorf("claim HTTP %d: %s", response.StatusCode, raw)
	}
	if result != nil {
		return json.NewDecoder(io.LimitReader(response.Body, 65536)).Decode(result)
	}
	return nil
}

// TestExternalPressureBridgeE2E is driven by an external pressure fixture.
// All registrations and VM state belong to that fixture, never a saved host.
func TestExternalPressureBridgeE2E(t *testing.T) {
	configPath := os.Getenv("AGENT_VMM_EXTERNAL_E2E_CONFIG")
	if configPath == "" {
		t.Skip("set AGENT_VMM_EXTERNAL_E2E_CONFIG from the external pressure fixture")
	}
	var fixture struct {
		Endpoint      string `json:"remote_endpoint"`
		CA            string `json:"ca_file"`
		Archive       string `json:"archive_path"`
		Registrations []struct {
			ID         string `json:"id"`
			Credential []byte `json:"credential"`
		} `json:"registrations"`
	}
	raw, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &fixture); err != nil {
		t.Fatal(err)
	}
	if len(fixture.Registrations) != 6 {
		t.Fatal("pressure fixture requires exactly six isolated registrations")
	}
	roots := x509.NewCertPool()
	raw, err = os.ReadFile(fixture.CA)
	if err != nil || !roots.AppendCertsFromPEM(raw) {
		t.Fatal("invalid fixture CA")
	}
	harness := startHostWithResources(t, 32<<30, 8<<30)
	harness.start(t)
	t.Cleanup(func() { harness.stop(t, true) })
	t.Cleanup(func() {
		for suffix, name := range map[string]string{"": "guest.log", ".host.log": "host-runtime.log"} {
			if err := os.WriteFile(filepath.Join(filepath.Dir(configPath), name), readFileTail(harness.serial+suffix, 256<<10), 0600); err != nil {
				t.Errorf("preserve pressure fixture diagnostics: %v", err)
			}
		}
	})
	archiveName := "import-external-pressure.tar"
	source, err := os.Open(fixture.Archive)
	if err != nil {
		t.Fatal(err)
	}
	destination, err := os.Create(filepath.Join(harness.runtimeClient.archiveDirectory, archiveName))
	if err != nil {
		source.Close()
		t.Fatal(err)
	}
	_, copyErr := io.Copy(destination, source)
	if err := errors.Join(copyErr, source.Close(), destination.Close()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	failures := make(chan error, 6)
	for _, registration := range fixture.Registrations {
		pool := &hostv1.RemotePoolPolicy{PerEnvironmentLimits: isolationLimits(512, 3<<30), MaxEgressMode: hostv1.EgressMode_EGRESS_MODE_PUBLIC_INTERNET, StopGrace: durationpb.New(time.Second)}
		_, err := harness.client.ApplyRemoteRegistration(ctx, &hostv1.ApplyRemoteRegistrationRequest{Registration: &hostv1.RemoteRegistrationConnection{RequestId: registration.ID + "-begin", RegistrationId: registration.ID, Endpoint: fixture.Endpoint, DisplayName: "external pressure fixture", Pool: pool}, Credential: []byte("credential")})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := harness.client.GetRemoteRegistration(ctx, &hostv1.GetRemoteRegistrationRequest{RegistrationId: registration.ID}); err != nil {
			t.Fatal(err)
		}
		go func() {
			failures <- remoteconnector.Run(ctx, remoteconnector.Config{RegistrationID: registration.ID, RemoteComputeSocket: harness.remoteComputeSocket, Transport: remoteconnector.ExternalTransport{Endpoint: fixture.Endpoint, Credentials: &e2eCredentialStore{value: registration.Credential}, Roots: roots}})
		}()
	}
	ready, _ := json.Marshal(map[string]string{"archive_url": harness.runtimeClient.archiveURL + "/" + archiveName, "serial_log": harness.serial})
	if err := os.WriteFile(configPath+".host-ready", ready, 0600); err != nil {
		t.Fatal(err)
	}
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case err := <-failures:
			t.Fatalf("fixture remote connector exited: %v", err)
		case <-ctx.Done():
			t.Fatal("external pressure fixture exceeded 15 minutes")
		case <-ticker.C:
			if _, err := os.Stat(configPath + ".stop"); err == nil {
				return
			}
		}
	}
}
