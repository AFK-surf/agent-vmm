//go:build darwin || linux

package host

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"path/filepath"
	"testing"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/auth"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"github.com/AFK-surf/agent-vmm/internal/host/trust"
	"github.com/AFK-surf/agent-vmm/internal/host/trustkeys"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRegistrationRecoveryKeepsIdentityProofIndependentOfEnrollmentHealth(t *testing.T) {
	ctx := context.Background()
	database, err := openTestStore(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	identity, err := trust.New(ctx, database, trustkeys.NewFileProvider(filepath.Join(t.TempDir(), "private", "device-root.key")))
	if err != nil {
		t.Fatal(err)
	}
	registration, err := database.CreateRemoteRegistration(ctx, store.RemoteRegistration{
		PerEnvPIDs: 128, PerEnvDiskBytes: 4 << 30, MaxEgressMode: 1, StopGraceSeconds: 1,
		ID: "original", OwnerKind: store.RemoteRegistrationExternal, Endpoint: "controller", ConnectionState: 3, ConnectorPhase: 1,
		State:           int32(hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_DRAINING),
		EnrollmentState: int32(hostv1.RemoteEnrollmentState_REMOTE_ENROLLMENT_STATE_SETUP_INCOMPLETE),
	})
	if err != nil {
		t.Fatal(err)
	}
	server := &operatorServer{controller: newTestController(&Controller{store: database, trust: identity})}
	now := time.Now()
	challenge := trust.Challenge{Purpose: trust.Purpose, Audience: "https://controller.example", Subject: "owner", SessionID: "new-session", ControllerBinding: `["tenant","group","workspace"]`, EnvironmentID: "environment", OperationID: "operation", RegistrationID: registration.ID, Nonce: base64.RawURLEncoding.EncodeToString(make([]byte, 32)), Revision: 1, ExpiresAt: now.Add(time.Minute).Unix()}
	call := func() (*hostv1.SignRegistrationRecoveryResponse, error) {
		raw, _ := json.Marshal(challenge)
		value, err := auth.UnaryInterceptor(auth.Identity{Role: auth.Operator})(ctx, nil, &grpc.UnaryServerInfo{}, func(ctx context.Context, _ any) (any, error) {
			return server.SignRegistrationRecovery(ctx, &hostv1.SignRegistrationRecoveryRequest{ChallengeJson: raw})
		})
		if err != nil {
			return nil, err
		}
		result := value.(*hostv1.SignRegistrationRecoveryResponse)
		_, wire, err := trust.Parse(raw, now)
		if err != nil {
			t.Fatal(err)
		}
		x, y := elliptic.UnmarshalCompressed(elliptic.P256(), result.Identity.RootPublicKey)
		digest := sha256.Sum256(wire)
		if !ecdsa.Verify(&ecdsa.PublicKey{Curve: elliptic.P256(), X: x, Y: y}, digest[:], new(big.Int).SetBytes(result.Signature[:32]), new(big.Int).SetBytes(result.Signature[32:])) {
			t.Fatal("proof does not bind the original device and challenge")
		}
		return result, nil
	}
	if _, err := call(); err != nil {
		t.Fatalf("incomplete enrollment should retain recovery proof: %v", err)
	}
	unchanged, err := database.GetRemoteRegistration(ctx, registration.ID)
	if err != nil || unchanged.EnrollmentState != registration.EnrollmentState || unchanged.State != registration.State {
		t.Fatal("signing proof changed registration authority")
	}
	unchanged, err = database.SetRemoteEnrollment(ctx, registration.ID, unchanged.Revision,
		int32(hostv1.RemoteEnrollmentState_REMOTE_ENROLLMENT_STATE_SETUP_INCOMPLETE),
		int32(hostv1.RemoteEnrollmentState_REMOTE_ENROLLMENT_STATE_READY), "", "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.SetRemoteRegistrationState(ctx, registration.ID, unchanged.Revision, int32(hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_REVOKED)); err != nil {
		t.Fatal(err)
	}
	if _, err := call(); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("revoked registration signed proof: %v", err)
	}
	challenge.RegistrationID = "other"
	if _, err := call(); err == nil {
		t.Fatal("missing exact local registration signed proof")
	}
}
