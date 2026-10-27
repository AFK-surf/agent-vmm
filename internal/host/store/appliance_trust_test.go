package store

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	trustv1 "github.com/AFK-surf/agent-vmm/api/trust/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func applianceTrustStore(t *testing.T) (*Store, ApplianceTrustAuthority) {
	t.Helper()
	database, err := Open(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { database.Close() })
	ctx := context.Background()
	_, _, err = database.StageApplianceArtifact(ctx, ApplianceArtifact{ID: "artifact", ProductID: "product", CanonicalHash: []byte("hash"), ImageDigest: "digest", ImageReference: "image", Platform: "linux/arm64", Release: "1", Manifest: []byte("manifest")}, ApplianceOperation{ID: "stage", RequestID: "stage", State: 2, Phase: 5})
	if err != nil {
		t.Fatal(err)
	}
	installation, _, _, err := database.InstallApplianceDomain(ctx,
		ApplianceInstallation{ID: "installation", ProductID: "product", ArtifactID: "artifact", State: 1, Revision: 1, DataVolumeID: "volume", DataVolumeBytes: 1 << 20},
		ApplianceGrant{Revision: 1, System: ResourceAmount{PIDs: 1, DiskBytes: 1}, PerEnvironment: ResourceAmount{PIDs: 1, DiskBytes: 1}, EgressMode: 1, MaxEgressMode: 1},
		RemoteRegistration{ID: "registration", InstallationID: "installation", OwnerKind: RemoteRegistrationPrivate, DisplayName: "private", PerEnvPIDs: 1, PerEnvDiskBytes: 1, MaxEgressMode: 1, StopGraceSeconds: 1, State: 1, ConnectionState: 1, EnrollmentState: 3, ConnectorPhase: 1},
		nil, ApplianceOperation{ID: "install", RequestID: "install", State: 1, Phase: 2})
	if err != nil {
		t.Fatal(err)
	}
	installation.DesiredEnabled = true
	installation.State = int32(appliancev1.ApplianceState_APPLIANCE_STATE_RUNNING)
	installation.CurrentBootID, installation.CurrentContainerInstance = "boot", "instance"
	installation, err = database.UpdateApplianceInstallation(ctx, installation, installation.Revision)
	if err != nil {
		t.Fatal(err)
	}
	return database, ApplianceTrustAuthority{AuthorityID: "authority", InstallationID: "installation", RegistrationID: "registration", BootID: "boot", ContainerInstance: "instance", ConfigRevision: installation.Revision}
}

func applianceTrustArtifacts(now time.Time, revision uint64) (*trustv1.TrustAnchor, *trustv1.MembershipCredential) {
	authority := &trustv1.AuthorityDescriptor{AuthorityId: "authority", AuthorityClass: trustv1.AuthorityClass_AUTHORITY_CLASS_MANAGED_CONTROLLER, KeyId: "key", TrustDomainRef: []byte("domain"), Revision: revision}
	anchor := &trustv1.TrustAnchor{Authority: authority, Revision: revision,
		VerificationKeys:       []*trustv1.VerificationKey{{KeyId: "key", PublicKey: []byte("public-key"), SignatureSuite: trustv1.SignatureSuite_SIGNATURE_SUITE_P256_SHA256}},
		AllowedCapabilityKinds: []trustv1.CapabilityKind{trustv1.CapabilityKind_CAPABILITY_KIND_SERVICE_ROUTE}, NotBefore: timestamppb.New(now.Add(-time.Minute)), NotAfter: timestamppb.New(now.Add(365 * 24 * time.Hour))}
	credential := &trustv1.MembershipCredential{Authority: proto.Clone(authority).(*trustv1.AuthorityDescriptor), SubjectDeviceId: "device", SubjectRootKeyRevision: 1, SubjectRootPublicKey: []byte("root-key"), OpaqueClaimsDigest: []byte("claims"), PolicyRevision: revision, ExpiresAt: timestamppb.New(now.Add(30 * 24 * time.Hour)), Signature: []byte("signature")}
	return anchor, credential
}

func TestApplianceManagedTrustRenewalPreservesScopeAndRevision(t *testing.T) {
	mutations := map[string]func(*trustv1.TrustAnchor, *trustv1.MembershipCredential){
		"authority": func(a *trustv1.TrustAnchor, c *trustv1.MembershipCredential) {
			a.Authority.AuthorityId = "other"
			c.Authority.AuthorityId = "other"
		},
		"key": func(a *trustv1.TrustAnchor, _ *trustv1.MembershipCredential) {
			a.VerificationKeys[0].PublicKey = []byte("other")
		},
		"key ID": func(a *trustv1.TrustAnchor, c *trustv1.MembershipCredential) {
			a.Authority.KeyId = "other"
			c.Authority.KeyId = "other"
		},
		"domain": func(a *trustv1.TrustAnchor, c *trustv1.MembershipCredential) {
			a.Authority.TrustDomainRef = []byte("other")
			c.Authority.TrustDomainRef = []byte("other")
		},
		"capabilities": func(a *trustv1.TrustAnchor, _ *trustv1.MembershipCredential) { a.AllowedCapabilityKinds = nil },
		"subject":      func(_ *trustv1.TrustAnchor, c *trustv1.MembershipCredential) { c.SubjectDeviceId = "other" },
		"root": func(_ *trustv1.TrustAnchor, c *trustv1.MembershipCredential) {
			c.SubjectRootPublicKey = []byte("other")
		},
		"claims":          func(_ *trustv1.TrustAnchor, c *trustv1.MembershipCredential) { c.OpaqueClaimsDigest = []byte("other") },
		"policy rollback": func(_ *trustv1.TrustAnchor, c *trustv1.MembershipCredential) { c.PolicyRevision = 1 },
		"anchor rollback": func(a *trustv1.TrustAnchor, c *trustv1.MembershipCredential) {
			a.Revision = 1
			a.Authority.Revision = 1
			c.Authority.Revision = 1
		},
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			database, binding := applianceTrustStore(t)
			now := time.Now()
			anchor, credential := applianceTrustArtifacts(now, 1)
			if err := database.InstallApplianceManagedTrust(context.Background(), binding, anchor, credential); err != nil {
				t.Fatal(err)
			}
			nextAnchor, nextCredential := applianceTrustArtifacts(now.Add(29*24*time.Hour), 2)
			mutate(nextAnchor, nextCredential)
			if err := database.InstallApplianceManagedTrust(context.Background(), binding, nextAnchor, nextCredential); !errors.Is(err, ErrTrustRevisionConflict) {
				t.Fatalf("changed scope accepted: %v", err)
			}
			stored, err := database.GetMembershipCredential(context.Background(), "authority", "device")
			if err != nil || !proto.Equal(stored, credential) {
				t.Fatalf("failed renewal changed credential: %v", err)
			}
		})
	}
	database, binding := applianceTrustStore(t)
	ctx := context.Background()
	var sequence int
	var databaseName, databasePath string
	if err := database.db.QueryRow("PRAGMA database_list").Scan(&sequence, &databaseName, &databasePath); err != nil {
		t.Fatal(err)
	}
	defer func() { database.Close() }()
	now := time.Now()
	initialAnchor, initialCredential := applianceTrustArtifacts(now, 1)
	if err := database.InstallApplianceManagedTrust(ctx, binding, initialAnchor, initialCredential); err != nil {
		t.Fatal(err)
	}
	for _, days := range []int{29, 366} {
		revision := uint64(2)
		if days == 366 {
			revision = 3
		}
		a, c := applianceTrustArtifacts(now.Add(time.Duration(days)*24*time.Hour), revision)
		if err := database.InstallApplianceManagedTrust(ctx, binding, a, c); err != nil {
			t.Fatal(err)
		}
		if err := database.InstallApplianceManagedTrust(ctx, binding, a, c); err != nil {
			t.Fatalf("lost ACK retry failed: %v", err)
		}
		if err := database.Close(); err != nil {
			t.Fatal(err)
		}
		reopened, err := Open(databasePath)
		if err != nil {
			t.Fatal(err)
		}
		database = reopened
		stored, err := database.GetMembershipCredential(ctx, "authority", "device")
		if err != nil || !proto.Equal(stored, c) {
			t.Fatalf("reopened renewal changed credential: %v", err)
		}
		changed := proto.Clone(c).(*trustv1.MembershipCredential)
		changed.Signature = []byte("different")
		if err := database.InstallApplianceManagedTrust(ctx, binding, a, changed); !errors.Is(err, ErrTrustRevisionConflict) {
			t.Fatalf("same revision different bytes accepted: %v", err)
		}
	}
	if err := database.RevokeApplianceTrustAuthority(ctx, binding.InstallationID); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(databasePath)
	if err != nil {
		t.Fatal(err)
	}
	database = reopened
	a, c := applianceTrustArtifacts(now.Add(400*24*time.Hour), 4)
	if err := database.InstallApplianceManagedTrust(ctx, binding, a, c); !errors.Is(err, ErrTrustRevisionConflict) {
		t.Fatalf("revoked authority revived: %v", err)
	}
}

func TestApplianceManagedTrustRefreshRejectsLifecycleChanges(t *testing.T) {
	for _, mutation := range []string{"revision", "boot", "instance", "disabled"} {
		t.Run(mutation, func(t *testing.T) {
			database, binding := applianceTrustStore(t)
			ctx := context.Background()
			now := time.Now()
			a, c := applianceTrustArtifacts(now, 1)
			if err := database.InstallApplianceManagedTrust(ctx, binding, a, c); err != nil {
				t.Fatal(err)
			}
			installation, err := database.GetApplianceInstallation(ctx, binding.InstallationID)
			if err != nil {
				t.Fatal(err)
			}
			switch mutation {
			case "boot":
				installation.CurrentBootID = "new"
			case "instance":
				installation.CurrentContainerInstance = "new"
			case "disabled":
				installation.DesiredEnabled = false
			}
			if _, err := database.UpdateApplianceInstallation(ctx, installation, installation.Revision); err != nil {
				t.Fatal(err)
			}
			a, c = applianceTrustArtifacts(now.Add(29*24*time.Hour), 2)
			if err := database.InstallApplianceManagedTrust(ctx, binding, a, c); !errors.Is(err, ErrTrustRevisionConflict) {
				t.Fatalf("stale lifecycle accepted: %v", err)
			}
		})
	}
}
