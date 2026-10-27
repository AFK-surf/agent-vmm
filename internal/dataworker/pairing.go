package dataworker

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"time"
)

const maxPairingResponseBytes = 128 << 10

// PairingBytes has an explicit JSON integer-array encoding so Go and the Rust
// worker do not silently disagree about encoding/json's []byte base64 special
// case. Pairing command buffers are zeroized by the worker after parsing.
type PairingBytes []byte

func (value PairingBytes) MarshalJSON() ([]byte, error) {
	numbers := make([]uint16, len(value))
	for index, item := range value {
		numbers[index] = uint16(item)
	}
	return json.Marshal(numbers)
}

func (value *PairingBytes) UnmarshalJSON(raw []byte) error {
	var numbers []uint16
	if err := json.Unmarshal(raw, &numbers); err != nil {
		return err
	}
	decoded := make([]byte, len(numbers))
	for index, item := range numbers {
		if item > 255 {
			return fmt.Errorf("pairing byte is outside range")
		}
		decoded[index] = byte(item)
	}
	*value = decoded
	return nil
}

type PairingContext struct {
	SessionID        PairingBytes `json:"session_id"`
	MeshID           PairingBytes `json:"mesh_id"`
	InviteRevision   uint64       `json:"invite_revision"`
	ManagerIdentity  PairingBytes `json:"manager_identity"`
	JoinerIdentity   PairingBytes `json:"joiner_identity"`
	ManagerRootKey   PairingBytes `json:"manager_root_key"`
	JoinerRootKey    PairingBytes `json:"joiner_root_key"`
	RolesAndFeatures PairingBytes `json:"roles_and_features"`
}

// Digest returns the application context digest that a device signs in its
// PairingProof. It mirrors the worker's length-prefixed PAKE AAD exactly.
func (pairingContext PairingContext) Digest() ([sha256.Size]byte, error) {
	var encoded bytes.Buffer
	for _, field := range [][]byte{
		[]byte("agent-vmm/pairing"), []byte("2"), pairingContext.SessionID,
		pairingContext.MeshID,
	} {
		if err := appendPairingField(&encoded, field); err != nil {
			return [sha256.Size]byte{}, err
		}
	}
	if err := binary.Write(&encoded, binary.BigEndian, pairingContext.InviteRevision); err != nil {
		return [sha256.Size]byte{}, err
	}
	for _, field := range [][]byte{
		pairingContext.ManagerIdentity, pairingContext.JoinerIdentity,
		pairingContext.ManagerRootKey, pairingContext.JoinerRootKey,
		pairingContext.RolesAndFeatures,
	} {
		if err := appendPairingField(&encoded, field); err != nil {
			return [sha256.Size]byte{}, err
		}
	}
	return sha256.Sum256(encoded.Bytes()), nil
}

func appendPairingField(target *bytes.Buffer, value []byte) error {
	if len(value) > 4096 {
		return fmt.Errorf("pairing transcript field exceeds size limit")
	}
	if err := binary.Write(target, binary.BigEndian, uint32(len(value))); err != nil {
		return err
	}
	_, err := target.Write(value)
	return err
}

type PairingRole string
type PairingState string
type PairingFailure string

const (
	PairingRoleManager        PairingRole  = "manager"
	PairingRoleJoiner         PairingRole  = "joiner"
	PairingStateReadyToCommit PairingState = "ready_to_commit"
)

var ErrPairingNotFound = errors.New("pairing session not found")

func (status PairingStatus) Terminal() bool {
	return status.State == "committed" || status.State == "rejected" || status.State == "expired"
}

type PairingStatus struct {
	SessionID         PairingBytes   `json:"session_id"`
	Role              PairingRole    `json:"role"`
	State             PairingState   `json:"state"`
	AttemptsRemaining uint8          `json:"attempts_remaining"`
	SAS               string         `json:"sas,omitempty"`
	PeerPayload       PairingBytes   `json:"peer_payload,omitempty"`
	PeerIdentity      PairingBytes   `json:"peer_identity,omitempty"`
	PeerRootKey       PairingBytes   `json:"peer_root_key,omitempty"`
	LastFailure       PairingFailure `json:"last_failure,omitempty"`
}

func (supervisor *Supervisor) PreparePairingManager(ctx context.Context, pairingContext PairingContext, pinOrQRSecret, acceptance []byte, qr bool) (PairingStatus, error) {
	kind := "pin"
	if qr {
		kind = "qr"
	}
	return supervisor.pairingRequest(ctx, map[string]any{
		"command": "prepare_pairing_manager", "context": pairingContext,
		"secret_kind": kind, "secret": PairingBytes(pinOrQRSecret),
		"acceptance": PairingBytes(acceptance),
	})
}

func PreparePairingManager(ctx context.Context, socket string, pairingContext PairingContext, pinOrQRSecret, acceptance []byte, qr bool) (PairingStatus, error) {
	return (&Supervisor{socket: socket}).PreparePairingManager(ctx, pairingContext, pinOrQRSecret, acceptance, qr)
}

func (supervisor *Supervisor) JoinPairing(ctx context.Context, endpointAddr json.RawMessage, pairingContext PairingContext, pinOrQRSecret, proposal []byte, qr bool) (PairingStatus, error) {
	if len(endpointAddr) == 0 || len(endpointAddr) > maxPairingResponseBytes {
		return PairingStatus{}, fmt.Errorf("a bounded pairing endpoint address is required")
	}
	kind := "pin"
	if qr {
		kind = "qr"
	}
	return supervisor.pairingRequest(ctx, map[string]any{
		"command": "join_pairing", "endpoint_addr": endpointAddr,
		"context": pairingContext, "secret_kind": kind,
		"secret": PairingBytes(pinOrQRSecret), "proposal": PairingBytes(proposal),
	})
}

func JoinPairing(ctx context.Context, socket string, endpointAddr json.RawMessage, pairingContext PairingContext, pinOrQRSecret, proposal []byte, qr bool) (PairingStatus, error) {
	return (&Supervisor{socket: socket}).JoinPairing(ctx, endpointAddr, pairingContext, pinOrQRSecret, proposal, qr)
}

func (supervisor *Supervisor) PairingStatus(ctx context.Context, sessionID []byte) (PairingStatus, error) {
	return supervisor.pairingRequest(ctx, map[string]any{"command": "pairing_status", "session_id": PairingBytes(sessionID)})
}

func GetPairingStatus(ctx context.Context, socket string, sessionID []byte) (PairingStatus, error) {
	return (&Supervisor{socket: socket}).PairingStatus(ctx, sessionID)
}

func (supervisor *Supervisor) ConfirmPairing(ctx context.Context, sessionID []byte) (PairingStatus, error) {
	return supervisor.pairingRequest(ctx, map[string]any{"command": "confirm_pairing", "session_id": PairingBytes(sessionID)})
}

func ConfirmPairing(ctx context.Context, socket string, sessionID []byte) (PairingStatus, error) {
	return (&Supervisor{socket: socket}).ConfirmPairing(ctx, sessionID)
}

func (supervisor *Supervisor) CommitPairing(ctx context.Context, sessionID []byte, registryCASSucceeded bool) (PairingStatus, error) {
	return supervisor.pairingRequest(ctx, map[string]any{
		"command": "commit_pairing", "session_id": PairingBytes(sessionID),
		"registry_cas_succeeded": registryCASSucceeded,
	})
}

func CommitPairing(ctx context.Context, socket string, sessionID []byte, registryCASSucceeded bool) (PairingStatus, error) {
	return (&Supervisor{socket: socket}).CommitPairing(ctx, sessionID, registryCASSucceeded)
}

func (supervisor *Supervisor) RejectPairing(ctx context.Context, sessionID []byte) (PairingStatus, error) {
	return supervisor.pairingRequest(ctx, map[string]any{"command": "reject_pairing", "session_id": PairingBytes(sessionID)})
}

func RejectPairing(ctx context.Context, socket string, sessionID []byte) (PairingStatus, error) {
	return (&Supervisor{socket: socket}).RejectPairing(ctx, sessionID)
}

func RejectPairingContext(ctx context.Context, socket string, sessionID []byte) (PairingStatus, error) {
	return (&Supervisor{socket: socket}).pairingRequest(ctx, map[string]any{"command": "reject_pairing", "session_id": PairingBytes(sessionID), "context_mismatch": true})
}

func (supervisor *Supervisor) pairingRequest(ctx context.Context, request any) (PairingStatus, error) {
	if !filepath.IsAbs(supervisor.socket) {
		return PairingStatus{}, fmt.Errorf("data worker socket must be absolute")
	}
	dialer := net.Dialer{Timeout: time.Second}
	connection, err := dialer.DialContext(ctx, "unix", supervisor.socket)
	if err != nil {
		return PairingStatus{}, err
	}
	defer connection.Close()
	if deadline, ok := ctx.Deadline(); ok {
		_ = connection.SetDeadline(deadline)
	}
	var encoded bytes.Buffer
	if err := json.NewEncoder(&encoded).Encode(request); err != nil {
		return PairingStatus{}, err
	}
	commandBytes := encoded.Bytes()
	defer clear(commandBytes)
	if _, err := connection.Write(commandBytes); err != nil {
		return PairingStatus{}, err
	}
	reader := bufio.NewReaderSize(connection, maxPairingResponseBytes)
	line, err := reader.ReadSlice('\n')
	if err != nil || len(line) >= maxPairingResponseBytes {
		return PairingStatus{}, fmt.Errorf("invalid data worker pairing response")
	}
	var failure struct {
		Error string `json:"error"`
	}
	if json.Unmarshal(line, &failure) == nil && failure.Error == "pairing_not_found" {
		return PairingStatus{}, ErrPairingNotFound
	}
	var status PairingStatus
	if err := json.Unmarshal(line, &status); err != nil {
		return PairingStatus{}, fmt.Errorf("invalid data worker pairing response")
	}
	if len(status.SessionID) != 16 || status.Role == "" || status.State == "" {
		return PairingStatus{}, fmt.Errorf("incomplete data worker pairing response")
	}
	return status, nil
}
