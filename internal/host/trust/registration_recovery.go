package trust

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	trustv1 "github.com/AFK-surf/agent-vmm/api/trust/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/protobuf/proto"
)

func (manager *Manager) SignRegistrationRecovery(ctx context.Context, raw []byte) (*trustv1.DeviceIdentity, []byte, error) {
	_, wire, err := Parse(raw, manager.now())
	if err != nil {
		return nil, nil, err
	}
	record, err := manager.store.GetDeviceIdentity(ctx)
	if err != nil || record.State != store.DeviceIdentityReady {
		return nil, nil, ErrNeedsRepair
	}
	digest := sha256.Sum256(wire)
	signature, err := manager.keys.Sign(ctx, record.Identity.GetRootKeyRevision(), digest[:])
	if err != nil {
		return nil, nil, err
	}
	return proto.Clone(record.Identity).(*trustv1.DeviceIdentity), signature, nil
}

const Purpose = "agent-vmm/credential-replace/1"
const TTL = 120 * time.Second

type Challenge struct {
	Purpose                 string `json:"purpose"`
	Audience                string `json:"audience"`
	Subject                 string `json:"subject"`
	SessionID               string `json:"session_id"`
	ControllerBinding       string `json:"controller_binding"`
	EnvironmentID           string `json:"environment_id"`
	CandidateCredentialHash string `json:"candidate_credential_hash"`
	RegistrationID          string `json:"registration_id"`
	Nonce                   string `json:"nonce"`
	Revision                uint64 `json:"revision"`
	ExpiresAt               int64  `json:"expires_at"`
}

func Parse(raw []byte, now time.Time) (Challenge, []byte, error) {
	var challenge Challenge
	if len(raw) == 0 || len(raw) > 4096 {
		return challenge, nil, errors.New("invalid recovery challenge size")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&challenge); err != nil {
		return challenge, nil, err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return challenge, nil, errors.New("unexpected trailing recovery challenge")
	}
	if challenge.Purpose != Purpose || challenge.Revision == 0 || challenge.ExpiresAt <= now.Unix() || challenge.ExpiresAt > now.Add(TTL).Unix() {
		return challenge, nil, errors.New("invalid recovery purpose, revision, or expiry")
	}
	values := []string{challenge.Audience, challenge.Subject, challenge.SessionID, challenge.ControllerBinding, challenge.EnvironmentID, challenge.RegistrationID, challenge.CandidateCredentialHash, challenge.Nonce}
	wire := strings.Builder{}
	wire.WriteString(Purpose + "\n")
	for index, value := range values {
		limit := 500
		if index == 3 {
			limit = 1500
		}
		if len(value) == 0 || len(value) > limit || !utf8.ValidString(value) {
			return challenge, nil, errors.New("invalid recovery binding")
		}
		wire.WriteString(base64.RawURLEncoding.EncodeToString([]byte(value)) + "\n")
	}
	candidateHash, err := base64.StdEncoding.DecodeString(challenge.CandidateCredentialHash)
	if err != nil || len(candidateHash) != 32 {
		return challenge, nil, errors.New("invalid candidate credential hash")
	}
	nonce, err := base64.RawURLEncoding.DecodeString(challenge.Nonce)
	if err != nil || len(nonce) != 32 {
		return challenge, nil, errors.New("invalid recovery nonce")
	}
	wire.WriteString(strconv.FormatUint(challenge.Revision, 10) + "\n" + strconv.FormatInt(challenge.ExpiresAt, 10) + "\n")
	return challenge, []byte(wire.String()), nil
}
