// Package comma_recovery implements only the Comma installation recovery proof purpose.
package comma_recovery

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

const Purpose = "agent-vmm/comma-recovery/1"
const TTL = 120 * time.Second

type Challenge struct {
	Purpose        string `json:"purpose"`
	Audience       string `json:"audience"`
	Subject        string `json:"subject"`
	SessionID      string `json:"session_id"`
	TenantID       string `json:"tenant_id"`
	GroupID        string `json:"group_id"`
	ScopeKey       string `json:"scope_key"`
	EnvironmentID  string `json:"environment_id"`
	OperationID    string `json:"operation_id"`
	RegistrationID string `json:"registration_id"`
	Nonce          string `json:"nonce"`
	Revision       uint64 `json:"revision"`
	ExpiresAt      int64  `json:"expires_at"`
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
	values := []string{challenge.Audience, challenge.Subject, challenge.SessionID, challenge.TenantID, challenge.GroupID, challenge.ScopeKey, challenge.EnvironmentID, challenge.OperationID, challenge.RegistrationID, challenge.Nonce}
	wire := strings.Builder{}
	wire.WriteString(Purpose + "\n")
	for _, value := range values {
		if len(value) == 0 || len(value) > 500 || !utf8.ValidString(value) {
			return challenge, nil, errors.New("invalid recovery binding")
		}
		wire.WriteString(base64.RawURLEncoding.EncodeToString([]byte(value)) + "\n")
	}
	nonce, err := base64.RawURLEncoding.DecodeString(challenge.Nonce)
	if err != nil || len(nonce) != 32 {
		return challenge, nil, errors.New("invalid recovery nonce")
	}
	wire.WriteString(strconv.FormatUint(challenge.Revision, 10) + "\n" + strconv.FormatInt(challenge.ExpiresAt, 10) + "\n")
	return challenge, []byte(wire.String()), nil
}
