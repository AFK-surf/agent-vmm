package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/binary"
	"errors"
	"math/big"
	"os"
	"path/filepath"
	"time"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	trustv1 "github.com/AFK-surf/agent-vmm/api/trust/v1"
	"github.com/AFK-surf/agent-vmm/pkg/signing"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const referenceAuthorityState = "managed-authority.bin"

type referenceAuthority struct {
	key        *ecdsa.PrivateKey
	anchor     *trustv1.TrustAnchor
	credential *trustv1.MembershipCredential
}

func loadOrCreateManagedTrust(claim *appliancev1.ApplianceLaunchClaim) (*appliancev1.ApplianceManagedTrustBundle, error) {
	state, err := loadReferenceAuthority(filepath.Join(stateDirectory, referenceAuthorityState))
	if errors.Is(err, os.ErrNotExist) {
		state, err = newReferenceAuthority(claim)
		if err == nil {
			err = storeReferenceAuthority(filepath.Join(stateDirectory, referenceAuthorityState), state)
		}
	}
	if err != nil {
		return nil, err
	}
	if state.credential.GetSubjectDeviceId() != claim.GetDeviceIdentity().GetDeviceId() ||
		state.credential.GetSubjectRootKeyRevision() != claim.GetDeviceIdentity().GetRootKeyRevision() ||
		!proto.Equal(state.credential.GetAuthority(), state.anchor.GetAuthority()) {
		return nil, errors.New("managed authority does not match the exact device identity")
	}
	return &appliancev1.ApplianceManagedTrustBundle{InstallationId: claim.GetInstallationId(), RegistrationId: claim.GetRegistrationId(),
		ConfigRevision: claim.GetConfigRevision(), BootId: claim.GetBootId(), ContainerInstance: claim.GetContainerInstance(), BootNonce: append([]byte(nil), claim.GetBootNonce()...),
		TrustAnchor: proto.Clone(state.anchor).(*trustv1.TrustAnchor), MembershipCredential: proto.Clone(state.credential).(*trustv1.MembershipCredential)}, nil
}

func newReferenceAuthority(claim *appliancev1.ApplianceLaunchClaim) (referenceAuthority, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return referenceAuthority{}, err
	}
	now := time.Now().UTC()
	authority := &trustv1.AuthorityDescriptor{AuthorityId: "reference:" + claim.GetInstallationId(),
		AuthorityClass: trustv1.AuthorityClass_AUTHORITY_CLASS_MANAGED_CONTROLLER, KeyId: "route-v1",
		TrustDomainRef: []byte(claim.GetInstallationId()), Revision: 1}
	anchor := &trustv1.TrustAnchor{Authority: authority, Revision: 1, NotBefore: timestamppb.New(now.Add(-time.Minute)), NotAfter: timestamppb.New(now.Add(365 * 24 * time.Hour)),
		AllowedCapabilityKinds: []trustv1.CapabilityKind{trustv1.CapabilityKind_CAPABILITY_KIND_SERVICE_ROUTE},
		VerificationKeys:       []*trustv1.VerificationKey{{KeyId: authority.GetKeyId(), PublicKey: elliptic.MarshalCompressed(elliptic.P256(), key.X, key.Y), SignatureSuite: trustv1.SignatureSuite_SIGNATURE_SUITE_P256_SHA256}}}
	claims := sha256.Sum256([]byte(claim.GetInstallationId() + "\x00" + claim.GetRegistrationId()))
	credential := &trustv1.MembershipCredential{Authority: proto.Clone(authority).(*trustv1.AuthorityDescriptor), SubjectDeviceId: claim.GetDeviceIdentity().GetDeviceId(),
		SubjectRootKeyRevision: claim.GetDeviceIdentity().GetRootKeyRevision(), SubjectRootPublicKey: append([]byte(nil), claim.GetDeviceIdentity().GetRootPublicKey()...),
		OpaqueClaimsDigest: claims[:], PolicyRevision: 1, ExpiresAt: timestamppb.New(now.Add(365 * 24 * time.Hour))}
	credential.Signature, err = signReferenceMessage(key, credential)
	return referenceAuthority{key: key, anchor: anchor, credential: credential}, err
}

func signReferenceMessage(key *ecdsa.PrivateKey, message proto.Message) ([]byte, error) {
	input, err := signing.CanonicalInput(message)
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256(input)
	r, s, err := ecdsa.Sign(rand.Reader, key, digest[:])
	if err != nil {
		return nil, err
	}
	halfOrder := new(big.Int).Rsh(new(big.Int).Set(elliptic.P256().Params().N), 1)
	if s.Cmp(halfOrder) > 0 {
		s.Sub(elliptic.P256().Params().N, s)
	}
	result := make([]byte, 64)
	r.FillBytes(result[:32])
	s.FillBytes(result[32:])
	return result, nil
}

func storeReferenceAuthority(path string, state referenceAuthority) error {
	key, err := x509.MarshalECPrivateKey(state.key)
	if err != nil {
		return err
	}
	anchor, err := proto.MarshalOptions{Deterministic: true}.Marshal(state.anchor)
	if err != nil {
		return err
	}
	credential, err := proto.MarshalOptions{Deterministic: true}.Marshal(state.credential)
	if err != nil {
		return err
	}
	payload := make([]byte, 12+len(key)+len(anchor)+len(credential))
	binary.BigEndian.PutUint32(payload[0:4], uint32(len(key)))
	binary.BigEndian.PutUint32(payload[4:8], uint32(len(anchor)))
	binary.BigEndian.PutUint32(payload[8:12], uint32(len(credential)))
	copy(payload[12:], key)
	copy(payload[12+len(key):], anchor)
	copy(payload[12+len(key)+len(anchor):], credential)
	file, err := os.CreateTemp(filepath.Dir(path), ".managed-authority-*.tmp")
	if err != nil {
		return err
	}
	temporary := file.Name()
	defer os.Remove(temporary)
	if err := file.Chmod(0o600); err != nil {
		file.Close()
		return err
	}
	if _, err := file.Write(payload); err != nil {
		file.Close()
		return err
	}
	if err := errors.Join(file.Sync(), file.Close()); err != nil {
		return err
	}
	if err := os.Rename(temporary, path); err != nil {
		return err
	}
	directory, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	return errors.Join(directory.Sync(), directory.Close())
}

func loadReferenceAuthority(path string) (referenceAuthority, error) {
	payload, err := os.ReadFile(path)
	if err != nil {
		return referenceAuthority{}, err
	}
	if len(payload) < 12 {
		return referenceAuthority{}, errors.New("managed authority state is truncated")
	}
	keyBytes, anchorBytes, credentialBytes := int(binary.BigEndian.Uint32(payload[:4])), int(binary.BigEndian.Uint32(payload[4:8])), int(binary.BigEndian.Uint32(payload[8:12]))
	if keyBytes <= 0 || anchorBytes <= 0 || credentialBytes <= 0 || 12+keyBytes+anchorBytes+credentialBytes != len(payload) {
		return referenceAuthority{}, errors.New("managed authority state has invalid bounds")
	}
	key, err := x509.ParseECPrivateKey(payload[12 : 12+keyBytes])
	if err != nil {
		return referenceAuthority{}, err
	}
	anchor, credential := new(trustv1.TrustAnchor), new(trustv1.MembershipCredential)
	if err := proto.Unmarshal(payload[12+keyBytes:12+keyBytes+anchorBytes], anchor); err != nil {
		return referenceAuthority{}, err
	}
	if err := proto.Unmarshal(payload[12+keyBytes+anchorBytes:], credential); err != nil {
		return referenceAuthority{}, err
	}
	return referenceAuthority{key: key, anchor: anchor, credential: credential}, nil
}
