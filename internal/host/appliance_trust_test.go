//go:build darwin || linux

package host

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"errors"
	"io"
	"math/big"
	"net"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	trustv1 "github.com/AFK-surf/agent-vmm/api/trust/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/egress"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"github.com/AFK-surf/agent-vmm/internal/host/trust"
	"github.com/AFK-surf/agent-vmm/internal/host/trustkeys"
	"github.com/AFK-surf/agent-vmm/internal/vmm"
	"github.com/AFK-surf/agent-vmm/pkg/signing"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type renewalAppliance struct {
	appliancev1.UnimplementedApplianceBootstrapServiceServer
	anchor              *trustv1.TrustAnchor
	credential          *trustv1.MembershipCredential
	calls               atomic.Int32
	omitTrust           atomic.Bool
	omitHealth          atomic.Bool
	omitReceipt         atomic.Bool
	expectedSeed        []byte
	expectedBootstrapID string
	receipts            chan *appliancev1.BootstrapReceipt
	failed              atomic.Bool
	claimSeen           chan *appliancev1.ApplianceLaunchClaim
	callbackMu          sync.Mutex
	beforeReply         func()
}

func (appliance *renewalAppliance) Bootstrap(stream grpc.BidiStreamingServer[appliancev1.HostBootstrapFrame, appliancev1.ApplianceBootstrapFrame]) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	claim := first.GetLaunchClaim()
	if claim == nil {
		return errors.New("missing claim")
	}
	appliance.calls.Add(1)
	appliance.claimSeen <- proto.Clone(claim).(*appliancev1.ApplianceLaunchClaim)
	second, err := stream.Recv()
	if err != nil {
		return err
	}
	if envelope := second.GetEnvelope(); envelope != nil {
		appliance.callbackMu.Lock()
		expectedSeed := append([]byte(nil), appliance.expectedSeed...)
		expectedID := appliance.expectedBootstrapID
		appliance.callbackMu.Unlock()
		if len(expectedSeed) == 0 || envelope.GetBootstrapId() != expectedID || claim.GetBootstrapId() != expectedID || !bytes.Equal(envelope.GetCiphertext(), expectedSeed) {
			return errors.New("seed replay did not match initial launch")
		}
		closing, err := stream.Recv()
		if err != nil {
			return err
		}
		if !closing.GetCloseWrite() {
			return errors.New("seed not closed")
		}
		receipt := &appliancev1.BootstrapReceipt{InstallationId: claim.GetInstallationId(), BootstrapId: expectedID, Nonce: envelope.GetNonce()}
		if appliance.receipts != nil {
			appliance.receipts <- proto.Clone(receipt).(*appliancev1.BootstrapReceipt)
		}
		if !appliance.omitReceipt.Load() {
			if err := stream.Send(&appliancev1.ApplianceBootstrapFrame{Payload: &appliancev1.ApplianceBootstrapFrame_Receipt{Receipt: receipt}}); err != nil {
				return err
			}
		}
	} else if !second.GetCloseWrite() || claim.GetBootstrapId() != "" {
		return errors.New("renewal reintroduced bootstrap seed")
	}
	if appliance.failed.Load() {
		return errors.New("temporary refresh failure")
	}
	appliance.callbackMu.Lock()
	callback := appliance.beforeReply
	appliance.callbackMu.Unlock()
	if callback != nil {
		callback()
	}
	if !appliance.omitTrust.Load() {
		bundle := &appliancev1.ApplianceManagedTrustBundle{InstallationId: claim.GetInstallationId(), RegistrationId: claim.GetRegistrationId(), ConfigRevision: claim.GetConfigRevision(), BootId: claim.GetBootId(), ContainerInstance: claim.GetContainerInstance(), BootNonce: claim.GetBootNonce(), TrustAnchor: appliance.anchor, MembershipCredential: appliance.credential}
		if err := stream.Send(&appliancev1.ApplianceBootstrapFrame{Payload: &appliancev1.ApplianceBootstrapFrame_ManagedTrust{ManagedTrust: bundle}}); err != nil {
			return err
		}
	}
	if !appliance.omitHealth.Load() {
		health := &appliancev1.ApplianceHealth{InstallationId: claim.GetInstallationId(), RegistrationId: claim.GetRegistrationId(), BootId: claim.GetBootId(), ContainerInstance: claim.GetContainerInstance(), ConfigRevision: claim.GetConfigRevision(), Healthy: true, ReadinessNonce: make([]byte, 32)}
		if err := stream.Send(&appliancev1.ApplianceBootstrapFrame{Payload: &appliancev1.ApplianceBootstrapFrame_Health{Health: health}}); err != nil {
			return err
		}
	}
	return stream.Send(&appliancev1.ApplianceBootstrapFrame{Payload: &appliancev1.ApplianceBootstrapFrame_CloseWrite{CloseWrite: true}})
}

type renewalVMMD struct {
	vmmdv1.UnimplementedVMMDServiceServer
	endpoint string
	headers  chan *vmmdv1.SystemStreamHeader
}

func (server *renewalVMMD) OpenSystemStream(stream grpc.BidiStreamingServer[vmmdv1.SystemStreamFrame, vmmdv1.SystemStreamFrame]) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	header := first.GetHeader()
	if header == nil {
		return errors.New("missing header")
	}
	server.headers <- header
	connection, err := net.Dial("tcp", server.endpoint)
	if err != nil {
		return err
	}
	defer connection.Close()
	done := make(chan error, 2)
	go func() {
		for {
			frame, err := stream.Recv()
			if err != nil {
				done <- err
				return
			}
			if frame.GetCloseWrite() {
				done <- nil
				return
			}
			if _, err := connection.Write(frame.GetData()); err != nil {
				done <- err
				return
			}
		}
	}()
	go func() {
		buffer := make([]byte, header.GetMaxFrameBytes())
		for {
			n, err := connection.Read(buffer)
			if n > 0 {
				if sendErr := stream.Send(&vmmdv1.SystemStreamFrame{Payload: &vmmdv1.SystemStreamFrame_Data{Data: append([]byte(nil), buffer[:n]...)}}); sendErr != nil {
					done <- sendErr
					return
				}
			}
			if err != nil {
				done <- err
				return
			}
		}
	}()
	select {
	case err := <-done:
		if errors.Is(err, io.EOF) {
			return nil
		}
		return err
	case <-stream.Context().Done():
		return stream.Context().Err()
	}
}

func renewalHost(t *testing.T, issued time.Time) (*Controller, store.ApplianceInstallation, *vmmdv1.SystemApplianceIdentity, *renewalAppliance, vmmdv1.VMMDServiceClient, *renewalVMMD) {
	manifest, err := proto.Marshal(recoveryContentManifest())
	if err != nil {
		t.Fatal(err)
	}
	t.Helper()
	ctx := context.Background()
	database, err := openTestStore(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { database.Close() })
	provider := trustkeys.NewFileProvider(filepath.Join(t.TempDir(), "private", "device-root.key"))
	manager, err := trust.New(ctx, database, provider)
	if err != nil {
		t.Fatal(err)
	}
	identity, err := manager.Identity(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = database.StageApplianceArtifact(ctx, store.ApplianceArtifact{ID: "artifact", ProductID: "product", CanonicalHash: []byte("hash"), ImageDigest: "digest", ImageReference: "image", Platform: "linux/arm64", Release: "1", Manifest: manifest}, store.ApplianceOperation{ID: "stage", RequestID: "stage", State: 2, Phase: 5})
	if err != nil {
		t.Fatal(err)
	}
	installation, _, _, err := database.InstallApplianceDomain(ctx, store.ApplianceInstallation{ID: "installation", ProductID: "product", ArtifactID: "artifact", State: 1, Revision: 1, DataVolumeID: "volume", DataVolumeBytes: 1 << 20}, store.ApplianceGrant{Revision: 1, System: store.ResourceAmount{PIDs: 1, DiskBytes: 1}, PerEnvironment: store.ResourceAmount{PIDs: 1, DiskBytes: 1}, EgressMode: 1, MaxEgressMode: 1}, store.RemoteRegistration{ID: "registration", InstallationID: "installation", OwnerKind: store.RemoteRegistrationPrivate, DisplayName: "private", ProtocolVersion: "1", PerEnvPIDs: 1, PerEnvDiskBytes: 1, MaxEgressMode: 1, StopGraceSeconds: 1, State: 1, ConnectionState: 1, EnrollmentState: 3, ConnectorPhase: 1}, nil, store.ApplianceOperation{ID: "install", RequestID: "install", State: 1, Phase: 2})
	if err != nil {
		t.Fatal(err)
	}
	installation.DesiredEnabled = true
	installation.State = 3
	installation.CurrentBootID = "boot"
	installation.CurrentContainerInstance = "instance"
	installation, err = database.UpdateApplianceInstallation(ctx, installation, installation.Revision)
	if err != nil {
		t.Fatal(err)
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	bundle := func(at time.Time, revision uint64) (*trustv1.TrustAnchor, *trustv1.MembershipCredential) {
		authority := &trustv1.AuthorityDescriptor{AuthorityId: "authority", AuthorityClass: trustv1.AuthorityClass_AUTHORITY_CLASS_MANAGED_CONTROLLER, KeyId: "key", Revision: revision}
		a := &trustv1.TrustAnchor{Authority: authority, Revision: revision, VerificationKeys: []*trustv1.VerificationKey{{KeyId: "key", PublicKey: elliptic.MarshalCompressed(key.Curve, key.X, key.Y), SignatureSuite: trustv1.SignatureSuite_SIGNATURE_SUITE_P256_SHA256}}, AllowedCapabilityKinds: []trustv1.CapabilityKind{trustv1.CapabilityKind_CAPABILITY_KIND_SERVICE_ROUTE}, NotBefore: timestamppb.New(at.Add(-time.Minute)), NotAfter: timestamppb.New(at.Add(365 * 24 * time.Hour))}
		digest := sha256.Sum256([]byte("claims"))
		c := &trustv1.MembershipCredential{Authority: proto.Clone(authority).(*trustv1.AuthorityDescriptor), SubjectDeviceId: identity.Identity.GetDeviceId(), SubjectRootKeyRevision: identity.Identity.GetRootKeyRevision(), SubjectRootPublicKey: identity.Identity.GetRootPublicKey(), OpaqueClaimsDigest: digest[:], PolicyRevision: revision, ExpiresAt: timestamppb.New(at.Add(30 * 24 * time.Hour))}
		payload, err := signing.CanonicalInput(c)
		if err != nil {
			t.Fatal(err)
		}
		hash := sha256.Sum256(payload)
		r, s, err := ecdsa.Sign(rand.Reader, key, hash[:])
		if err != nil {
			t.Fatal(err)
		}
		half := new(big.Int).Rsh(new(big.Int).Set(key.Params().N), 1)
		if s.Cmp(half) > 0 {
			s.Sub(key.Params().N, s)
		}
		c.Signature = make([]byte, 64)
		r.FillBytes(c.Signature[:32])
		s.FillBytes(c.Signature[32:])
		return a, c
	}
	// A zero issue time models an appliance that never issued managed trust.
	if !issued.IsZero() {
		initialAnchor, initialCredential := bundle(issued, 1)
		binding := store.ApplianceTrustAuthority{AuthorityID: "authority", InstallationID: installation.ID, RegistrationID: installation.RegistrationID, BootID: "boot", ContainerInstance: "instance", ConfigRevision: installation.Revision}
		if err := database.InstallApplianceManagedTrust(ctx, binding, initialAnchor, initialCredential); err != nil {
			t.Fatal(err)
		}
	}
	nextAnchor, nextCredential := bundle(time.Now(), 2)
	appliance := &renewalAppliance{anchor: nextAnchor, credential: nextCredential, claimSeen: make(chan *appliancev1.ApplianceLaunchClaim, 20)}
	applianceListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	applianceServer := grpc.NewServer()
	appliancev1.RegisterApplianceBootstrapServiceServer(applianceServer, appliance)
	go applianceServer.Serve(applianceListener)
	t.Cleanup(applianceServer.Stop)
	vmmd := &renewalVMMD{endpoint: applianceListener.Addr().String(), headers: make(chan *vmmdv1.SystemStreamHeader, 20)}
	vmmdListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	vmmdServer := grpc.NewServer()
	vmmdv1.RegisterVMMDServiceServer(vmmdServer, vmmd)
	go vmmdServer.Serve(vmmdListener)
	t.Cleanup(vmmdServer.Stop)
	connection, err := grpc.NewClient(vmmdListener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { connection.Close() })
	runtime := &vmmdv1.SystemApplianceIdentity{InstallationId: installation.ID, BootId: "boot", NamespaceId: "system", ContainerInstanceId: "instance", BootNonce: make([]byte, 32)}
	return &Controller{store: database, trust: manager, trustKeyProvider: provider}, installation, runtime, appliance, vmmdv1.NewVMMDServiceClient(connection), vmmd
}

func TestApplianceTrustRefreshUsesFreshBoundedBootstrapAndEmptySeed(t *testing.T) {
	for _, days := range []int{29, 30, 366} {
		t.Run(time.Duration(days).String(), func(t *testing.T) {
			now := time.Now()
			controller, installation, identity, appliance, client, vmmd := renewalHost(t, now.Add(-time.Duration(days)*24*time.Hour))
			status, err := controller.refreshApplianceTrust(context.Background(), client, installation, identity, now)
			if err != nil || status != "" {
				t.Fatalf("refresh: %s %v", status, err)
			}
			claim := <-appliance.claimSeen
			header := <-vmmd.headers
			if claim.GetBootstrapId() != "" || claim.GetConfigRevision() != installation.Revision || claim.GetContainerInstance() != identity.GetContainerInstanceId() {
				t.Fatalf("claim fence %v", claim)
			}
			if header.GetMaxConcurrentStreams() != 1 || len(header.GetNonce()) != 32 || header.GetIdleSeconds() != 15 || header.GetMaxFrameBytes() != 64<<10 {
				t.Fatalf("stream bounds %v", header)
			}
			stored, err := controller.store.GetApplianceMembershipCredential(context.Background(), installation.ID)
			if err != nil || !proto.Equal(stored, appliance.credential) {
				t.Fatalf("credential not committed: %v", err)
			}
			if _, err := controller.refreshApplianceTrust(context.Background(), client, installation, identity, now); err != nil {
				t.Fatal(err)
			}
			if appliance.calls.Load() != 1 {
				t.Fatal("not-due trust refreshed")
			}
		})
	}
}

func TestApplianceTrustRefreshBackoffAndMissingBundle(t *testing.T) {
	now := time.Now()
	controller, installation, identity, appliance, client, _ := renewalHost(t, now.Add(-31*24*time.Hour))
	appliance.omitTrust.Store(true)
	// A renewal without a bundle installs nothing, so the expired credential stays.
	if status, err := controller.refreshApplianceTrust(context.Background(), client, installation, identity, now); err != nil || status != "MANAGED_TRUST_EXPIRED" {
		t.Fatalf("missing renewal bundle: %s %v", status, err)
	}
	if status, err := controller.refreshApplianceTrust(context.Background(), client, installation, identity, now.Add(time.Minute)); err != nil || status != "MANAGED_TRUST_EXPIRED" {
		t.Fatalf("backoff %s %v", status, err)
	}
	if appliance.calls.Load() != 1 {
		t.Fatal("failure backoff ignored")
	}
	current, err := controller.store.GetApplianceInstallation(context.Background(), installation.ID)
	if err != nil || current.Revision != installation.Revision || current.CurrentContainerInstance != installation.CurrentContainerInstance {
		t.Fatalf("refresh failure changed instance: %v", err)
	}
	appliance.omitTrust.Store(false)
	if _, err := controller.refreshApplianceTrust(context.Background(), client, installation, identity, now.Add(time.Hour)); err != nil {
		t.Fatalf("retry failed %v", err)
	}
}

// An appliance without managed route capabilities, such as the bundled MCP
// appliance, bootstraps without a bundle and holds no managed authority.
func TestApplianceWithoutManagedTrustBootstrapsWithoutAuthority(t *testing.T) {
	controller, installation, identity, appliance, client, _ := renewalHost(t, time.Time{})
	appliance.omitTrust.Store(true)
	grant, err := controller.store.GetApplianceGrant(context.Background(), installation.ID)
	if err != nil {
		t.Fatal(err)
	}
	installation.BootstrapID = ""
	if health, err := controller.bootstrapAppliance(context.Background(), client, installation, grant, identity, installation.Revision); err != nil || !health.GetHealthy() {
		t.Fatalf("bootstrap without managed trust = %v, %v", health, err)
	}
	if _, err := controller.store.GetApplianceMembershipCredential(context.Background(), installation.ID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("appliance without a bundle holds managed authority: %v", err)
	}
	if status, err := controller.refreshApplianceTrust(context.Background(), client, installation, identity, time.Now()); err != nil || status != "" || appliance.calls.Load() != 1 {
		t.Fatalf("refresh without managed trust = %q, %v, calls=%d", status, err, appliance.calls.Load())
	}
}

func TestApplianceTrustRefreshRejectsLifecycleChangeBetweenClaimAndResponse(t *testing.T) {
	for _, change := range []string{"revoke", "restart", "config"} {
		t.Run(change, func(t *testing.T) {
			now := time.Now()
			controller, installation, identity, appliance, client, _ := renewalHost(t, now.Add(-29*24*time.Hour))
			appliance.callbackMu.Lock()
			appliance.beforeReply = func() {
				if change == "revoke" {
					if err := controller.store.RevokeApplianceTrustAuthority(context.Background(), installation.ID); err != nil {
						t.Error(err)
					}
					return
				}
				updated := installation
				if change == "restart" {
					updated.CurrentBootID = "new-boot"
					updated.CurrentContainerInstance = "new-instance"
				}
				if _, err := controller.store.UpdateApplianceInstallation(context.Background(), updated, installation.Revision); err != nil {
					t.Error(err)
				}
			}
			appliance.callbackMu.Unlock()
			if _, err := controller.refreshApplianceTrust(context.Background(), client, installation, identity, now); err == nil {
				t.Fatal("in-flight renewal crossed lifecycle fence")
			}
			stored, err := controller.store.GetMembershipCredential(context.Background(), "authority", appliance.credential.GetSubjectDeviceId())
			if err != nil || stored.GetPolicyRevision() != 1 {
				t.Fatalf("stale response committed: %v", err)
			}
		})
	}
}

func TestApplianceTrustIncompleteResponseRetriesIdenticalBundleWithFreshNonce(t *testing.T) {
	now := time.Now()
	controller, installation, identity, appliance, client, vmmd := renewalHost(t, now.Add(-29*24*time.Hour))
	grant, err := controller.store.GetApplianceGrant(context.Background(), installation.ID)
	if err != nil {
		t.Fatal(err)
	}
	appliance.omitHealth.Store(true)
	if _, err := controller.bootstrapAppliance(context.Background(), client, installation, grant, identity, installation.Revision); err == nil {
		t.Fatal("incomplete response accepted")
	}
	first := <-vmmd.headers
	stored, err := controller.store.GetApplianceMembershipCredential(context.Background(), installation.ID)
	if err != nil || stored.GetPolicyRevision() != 1 {
		t.Fatalf("incomplete response committed: %v", err)
	}
	appliance.omitHealth.Store(false)
	if _, err := controller.bootstrapAppliance(context.Background(), client, installation, grant, identity, installation.Revision); err != nil {
		t.Fatal(err)
	}
	second := <-vmmd.headers
	if bytes.Equal(first.GetNonce(), second.GetNonce()) {
		t.Fatal("nonce reused")
	}
	stored, err = controller.store.GetApplianceMembershipCredential(context.Background(), installation.ID)
	if err != nil || !proto.Equal(stored, appliance.credential) {
		t.Fatalf("identical issuance retry failed: %v", err)
	}
}

func TestPendingApplianceBootstrapRecoversLostReceiptAfterHostRestart(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	original, installation, identity, appliance, client, vmmd := renewalHost(t, now)
	secrets, err := newApplianceSecretStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	seed := bytes.Repeat([]byte{3}, 32)
	installation.State = int32(appliancev1.ApplianceState_APPLIANCE_STATE_STARTING)
	installation.BootstrapID = "initial-bootstrap"
	installation, err = original.store.UpdateApplianceInstallation(ctx, installation, installation.Revision)
	if err != nil {
		t.Fatal(err)
	}
	if err := secrets.StoreBootstrap(ctx, installation.ID, installation.BootstrapID, seed); err != nil {
		t.Fatal(err)
	}
	original.applianceSecrets = secrets
	registration, err := original.store.GetRemoteRegistration(ctx, installation.RegistrationID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := original.store.SetRemoteEnrollment(ctx, registration.ID, registration.Revision, registration.EnrollmentState, 2, "", ""); err != nil {
		t.Fatal(err)
	}
	_, initialAnchor, err := original.store.GetTrustAnchor(ctx, "authority")
	if err != nil {
		t.Fatal(err)
	}
	initialCredential, err := original.store.GetApplianceMembershipCredential(ctx, installation.ID)
	if err != nil {
		t.Fatal(err)
	}
	appliance.callbackMu.Lock()
	appliance.anchor = initialAnchor
	appliance.credential = initialCredential
	appliance.expectedSeed = append([]byte(nil), seed...)
	appliance.expectedBootstrapID = installation.BootstrapID
	appliance.receipts = make(chan *appliancev1.BootstrapReceipt, 2)
	appliance.callbackMu.Unlock()
	appliance.omitReceipt.Store(true)
	grant, err := original.store.GetApplianceGrant(ctx, installation.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := original.bootstrapAppliance(ctx, client, installation, grant, identity, installation.Revision); err == nil {
		t.Fatal("lost initial receipt was accepted")
	}
	firstClaim := <-appliance.claimSeen
	firstReceipt := <-appliance.receipts
	firstHeader := <-vmmd.headers
	if _, err := secrets.LoadBootstrap(ctx, installation.ID, installation.BootstrapID); err != nil {
		t.Fatalf("lost receipt discarded retained seed: %v", err)
	}
	appliance.omitReceipt.Store(false)
	restarted := &Controller{store: original.store, trust: original.trust, trustKeyProvider: original.trustKeyProvider, applianceSecrets: secrets,
		privateControllers: map[string]*privateControllerRoute{installation.ID: {identity: privateControllerIdentity(identity), cancel: func() {}, done: make(chan error), admitted: make(chan struct{})}}}
	runtime := &vmmdv1.SystemAppliance{Identity: identity, ConfigRevision: installation.Revision, State: vmmdv1.SystemApplianceState_SYSTEM_APPLIANCE_STATE_RUNNING}
	if health := restarted.pendingApplianceHealthProjection(installation.ID); health != nil {
		t.Fatal("test retained pre-crash health")
	}
	if err := restarted.retryPendingApplianceCleanup(ctx, client, installation, runtime, nil); err != nil {
		t.Fatalf("same live instance remained STARTING after receipt replay: %v", err)
	}
	replayClaim := <-appliance.claimSeen
	replayReceipt := <-appliance.receipts
	replayHeader := <-vmmd.headers
	if firstClaim.GetBootstrapId() != replayClaim.GetBootstrapId() || firstClaim.GetConfigRevision() != replayClaim.GetConfigRevision() || firstClaim.GetContainerInstance() != replayClaim.GetContainerInstance() {
		t.Fatal("receipt replay changed launch identity")
	}
	if bytes.Equal(firstReceipt.GetNonce(), replayReceipt.GetNonce()) || bytes.Equal(firstHeader.GetNonce(), replayHeader.GetNonce()) {
		t.Fatal("receipt or stream nonce was reused")
	}
	current, err := restarted.store.GetApplianceInstallation(ctx, installation.ID)
	if err != nil || current.State != int32(appliancev1.ApplianceState_APPLIANCE_STATE_RUNNING) || current.BootstrapID != "" || current.CurrentBootID != installation.CurrentBootID || current.CurrentContainerInstance != installation.CurrentContainerInstance {
		t.Fatalf("recovery did not settle same instance: %+v %v", current, err)
	}
	if _, err := secrets.LoadBootstrap(ctx, installation.ID, installation.BootstrapID); !errors.Is(err, errBootstrapNotFound) {
		t.Fatalf("consumed seed remained: %v", err)
	}
	stored, err := restarted.store.GetApplianceMembershipCredential(ctx, installation.ID)
	if err != nil || !proto.Equal(stored, initialCredential) {
		t.Fatalf("receipt replay changed trust bytes: %v", err)
	}
	if health := restarted.applianceHealthProjection(installation.ID); !health.GetHealthy() || health.GetContainerInstance() != identity.GetContainerInstanceId() {
		t.Fatalf("live instance was not republished: %v", health)
	}
	if appliance.calls.Load() != 2 {
		t.Fatalf("cleanup replaced/retried live instance: calls=%d", appliance.calls.Load())
	}
}

func TestDisabledApplianceReconciliationPreservesStoppedTrustAndFencesStaleSnapshot(t *testing.T) {
	ctx := context.Background()
	controller, installation, identity, _, client, _ := renewalHost(t, time.Now().Add(-time.Hour))
	controller.manager = &vmm.Manager{}
	controller.egress = egress.New()
	registrationBefore, err := controller.store.GetRemoteRegistration(ctx, installation.RegistrationID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := controller.store.SetRemoteEnrollment(ctx, registrationBefore.ID, registrationBefore.Revision, registrationBefore.EnrollmentState, int32(hostv1.RemoteEnrollmentState_REMOTE_ENROLLMENT_STATE_READY), "", ""); err != nil {
		t.Fatal(err)
	}
	installation.DesiredEnabled = false
	installation.State = int32(appliancev1.ApplianceState_APPLIANCE_STATE_STOPPED)
	stopped, err := controller.store.UpdateApplianceInstallation(ctx, installation, installation.Revision)
	if err != nil {
		t.Fatal(err)
	}
	_, err = controller.store.CreateApplianceOperation(ctx, store.ApplianceOperation{ID: "stop", RequestID: "stop", InstallationID: installation.ID, Kind: int32(appliancev1.ApplianceOperationKind_APPLIANCE_OPERATION_KIND_STOP), ExpectedRevision: installation.Revision, State: 2, Phase: 5})
	if err != nil {
		t.Fatal(err)
	}
	if err := controller.reconcileDisabledAppliance(ctx, stopped); err != nil {
		t.Fatal(err)
	}
	authority, err := controller.store.GetApplianceTrustAuthority(ctx, "authority")
	if err != nil || authority.State != store.ApplianceTrustActive {
		t.Fatalf("stop revoked trust: %+v %v", authority, err)
	}
	registration, err := controller.store.GetRemoteRegistration(ctx, installation.RegistrationID)
	if err != nil || registration.State == int32(hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_REVOKED) {
		t.Fatalf("stop revoked registration: %+v %v", registration, err)
	}
	started := stopped
	started.DesiredEnabled = true
	started.State = int32(appliancev1.ApplianceState_APPLIANCE_STATE_STARTING)
	started.CurrentContainerInstance = "replacement"
	started, err = controller.store.UpdateApplianceInstallation(ctx, started, stopped.Revision)
	if err != nil {
		t.Fatal(err)
	}
	if err := controller.reconcileDisabledAppliance(ctx, stopped); err != nil {
		t.Fatal(err)
	}
	identity.ContainerInstanceId = started.CurrentContainerInstance
	if _, err := controller.bootstrapAppliance(ctx, client, started, store.ApplianceGrant{}, identity, started.Revision); err != nil {
		t.Fatalf("stopped appliance cannot bootstrap again: %v", err)
	}
	started.DesiredEnabled = false
	started.State = int32(appliancev1.ApplianceState_APPLIANCE_STATE_STOPPED)
	revoked, err := controller.store.UpdateApplianceInstallation(ctx, started, started.Revision)
	if err != nil {
		t.Fatal(err)
	}
	_, err = controller.store.CreateApplianceOperation(ctx, store.ApplianceOperation{ID: "revoke", RequestID: "revoke", InstallationID: installation.ID, Kind: int32(appliancev1.ApplianceOperationKind_APPLIANCE_OPERATION_KIND_REVOKE), ExpectedRevision: started.Revision, State: int32(appliancev1.ApplianceOperationState_APPLIANCE_OPERATION_STATE_FAILED), Phase: 5})
	if err != nil {
		t.Fatal(err)
	}
	server := &applianceAdminServer{controller: controller}
	_, err = callAsOperator(t, func(ctx context.Context) (*appliancev1.ApplianceOperationResponse, error) {
		mutation := applianceMutation{revoked.ID, "activate-after-revoke", revoked.Revision, time.Now().Add(time.Minute).UnixMilli()}
		return server.setDesiredState(ctx, mutation, revoked.Revision, true, appliancev1.ApplianceState_APPLIANCE_STATE_STARTING, appliancev1.ApplianceOperationKind_APPLIANCE_OPERATION_KIND_ACTIVATE, "activation-pending")
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("activation bypassed admitted revocation: %v", err)
	}
	laterStop, err := controller.store.UpdateApplianceInstallation(ctx, revoked, revoked.Revision)
	if err != nil {
		t.Fatal(err)
	}
	_, err = controller.store.CreateApplianceOperation(ctx, store.ApplianceOperation{ID: "later-stop", RequestID: "later-stop", InstallationID: installation.ID, Kind: int32(appliancev1.ApplianceOperationKind_APPLIANCE_OPERATION_KIND_STOP), ExpectedRevision: revoked.Revision, State: 2, Phase: 5})
	if err != nil {
		t.Fatal(err)
	}
	revoked = laterStop
	if err := controller.reconcileDisabledAppliance(ctx, revoked); err != nil {
		t.Fatal(err)
	}
	authority, err = controller.store.GetApplianceTrustAuthority(ctx, "authority")
	if err != nil || authority.State != store.ApplianceTrustRevoked {
		t.Fatalf("revoke preserved authority: %+v %v", authority, err)
	}
	registration, err = controller.store.GetRemoteRegistration(ctx, installation.RegistrationID)
	if err != nil || registration.State != int32(hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_REVOKED) {
		t.Fatalf("revoke preserved registration: %+v %v", registration, err)
	}
	if _, err := controller.bootstrapAppliance(ctx, client, started, store.ApplianceGrant{}, identity, started.Revision); !errors.Is(err, store.ErrTrustRevisionConflict) {
		t.Fatalf("revoked authority revived: %v", err)
	}
}
