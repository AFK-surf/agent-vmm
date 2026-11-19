//! `agent-vmm/signature/v1` canonical signing vectors.
//!
//! `golden_vectors/agent_vmm_signing_vectors.json` is byte-identical to
//! `systems/apps/salix_store/test/testdata/agent_vmm_signing_vectors.json` in
//! Cue and to `api/trust/v1/testdata/signing-vectors.json` in agent-vmm.
//!
//! Canonical input (agent-vmm `pkg/signing/canonical.go:14-46`, Cue
//! `personal_mesh_proto_vectors_test.exs:5,23`):
//!
//! ```text
//! "agent-vmm/signature/v1\0" || type name || "\0" || deterministic protobuf
//! bytes of the message with its signature field cleared
//! ```
//!
//! The signature is raw `r || s` (64 bytes), low-S, ECDSA P-256 over
//! SHA-256(canonical input); the public key is SEC1 compressed (33 bytes).

use p256::ecdsa::signature::hazmat::PrehashVerifier;
use p256::ecdsa::signature::Verifier;
use p256::ecdsa::{Signature, VerifyingKey};
use serde_json::Value;
use sha2::{Digest, Sha256};

pub const DOMAIN: &[u8] = b"agent-vmm/signature/v1\0";

/// Signed type name, protobuf message, and the field cleared before signing
/// (agent-vmm `pkg/signing/canonical.go:48-80`).
pub const SIGNED_TYPES: &[(&str, &str, &str)] = &[
    ("ApplianceLaunchClaim", "agentvmm.appliance.v1.ApplianceLaunchClaim", "signature"),
    ("PersonalMeshDescriptor", "agentvmm.trust.v1.PersonalMeshDescriptor", "genesis_signature"),
    ("MembershipCredential", "agentvmm.trust.v1.MembershipCredential", "signature"),
    ("EndpointObservation", "agentvmm.trust.v1.EndpointObservation", "device_signature"),
    ("PairingProof", "agentvmm.trust.v1.PairingProof", "signature"),
    ("RouteCapability", "agentvmm.trust.v1.RouteCapability", "signature"),
    ("RegistryOperation", "agentvmm.trust.v1.RegistryOperation", "signature"),
    ("RegistryReceipt", "agentvmm.trust.v1.RegistryReceipt", "signature"),
    ("PersonalRouteRequest", "agentvmm.service.v1.RequestPersonalRouteRequest", "source_signature"),
];

/// The seven types Salix itself encodes and verifies
/// (Cue `personal_mesh_proto_vectors_test.exs:4`).
pub const SALIX_VERIFIED_TYPES: &[&str] = &[
    "PersonalMeshDescriptor",
    "MembershipCredential",
    "EndpointObservation",
    "PairingProof",
    "RouteCapability",
    "RegistryOperation",
    "RegistryReceipt",
];

#[derive(Debug, Clone)]
pub struct Vector {
    pub type_name: String,
    /// The message in protojson (default options).
    pub message: Value,
    pub canonical: Vec<u8>,
    pub digest: Vec<u8>,
    pub public_key: Vec<u8>,
    pub signature: Vec<u8>,
}

pub fn load_vectors() -> Vec<Vector> {
    let hex_field = |entry: &Value, key: &str| {
        hex::decode(entry[key].as_str().unwrap_or_else(|| panic!("{key} must be a string")))
            .unwrap_or_else(|error| panic!("{key}: {error}"))
    };
    crate::golden("agent_vmm_signing_vectors.json")
        .as_array()
        .expect("vectors must be an array")
        .iter()
        .map(|entry| Vector {
            type_name: entry["type"].as_str().expect("type").to_string(),
            message: entry["message"].clone(),
            canonical: hex_field(entry, "canonical_hex"),
            digest: hex_field(entry, "digest_hex"),
            public_key: hex_field(entry, "public_key_hex"),
            signature: hex_field(entry, "signature_hex"),
        })
        .collect()
}

/// Builds the canonical input from a type name and the deterministic protobuf
/// bytes of the message whose signature field is already cleared.
pub fn canonical_input(type_name: &str, cleared_message: &[u8]) -> Vec<u8> {
    let mut input = Vec::with_capacity(DOMAIN.len() + type_name.len() + 1 + cleared_message.len());
    input.extend_from_slice(DOMAIN);
    input.extend_from_slice(type_name.as_bytes());
    input.push(0);
    input.extend_from_slice(cleared_message);
    input
}

/// The protobuf payload of a canonical input, after the domain and type name.
pub fn canonical_payload(vector: &Vector) -> Result<&[u8], String> {
    let prefix = canonical_input(&vector.type_name, &[]);
    vector.canonical.strip_prefix(prefix.as_slice()).ok_or_else(|| format!("{}: canonical input lacks the domain and type prefix", vector.type_name))
}

/// Verifies a raw low-S P-256 signature over `canonical`, as both verifiers do:
/// Go verifies the SHA-256 digest (canonical_test.go:85-99), Erlang hashes the
/// input itself (personal_mesh_proto_vectors_test.exs:26-31).
pub fn verify(public_key: &[u8], canonical: &[u8], signature: &[u8]) -> Result<(), String> {
    if public_key.len() != 33 || !matches!(public_key[0], 2 | 3) {
        return Err("public key must be a 33-byte SEC1 compressed P-256 point".into());
    }
    if signature.len() != 64 {
        return Err(format!("raw signature length = {}", signature.len()));
    }
    let key = VerifyingKey::from_sec1_bytes(public_key).map_err(|error| format!("public key: {error}"))?;
    let signature = Signature::from_slice(signature).map_err(|error| format!("signature: {error}"))?;
    if signature.normalize_s().is_some() {
        return Err("signature is not low-S".into());
    }
    let digest = Sha256::digest(canonical);
    key.verify_prehash(&digest, &signature).map_err(|error| format!("prehash verification: {error}"))?;
    key.verify(canonical, &signature).map_err(|error| format!("message verification: {error}"))
}

/// Checks one vector's internal consistency: prefix, digest and signature.
pub fn check_vector(vector: &Vector) -> Result<(), String> {
    canonical_payload(vector)?;
    if Sha256::digest(&vector.canonical)[..] != vector.digest[..] {
        return Err(format!("{}: digest differs from SHA-256 of the canonical input", vector.type_name));
    }
    verify(&vector.public_key, &vector.canonical, &vector.signature).map_err(|error| format!("{}: {error}", vector.type_name))
}

/// One top-level field of a protobuf message: `(number, wire type, value bytes)`.
pub type WireField<'a> = (u64, u8, &'a [u8]);

/// Splits a protobuf message into its top-level fields without a schema.
pub fn top_level_fields(mut payload: &[u8]) -> Result<Vec<WireField<'_>>, String> {
    fn varint(input: &mut &[u8]) -> Result<u64, String> {
        let mut value = 0u64;
        for shift in (0..64).step_by(7) {
            let (&byte, rest) = input.split_first().ok_or("truncated varint")?;
            *input = rest;
            value |= u64::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                return Ok(value);
            }
        }
        Err("varint exceeds 64 bits".into())
    }
    let mut fields = Vec::new();
    while !payload.is_empty() {
        let tag = varint(&mut payload)?;
        let (number, wire) = (tag >> 3, (tag & 7) as u8);
        let length = match wire {
            0 => {
                let before = payload;
                varint(&mut payload)?;
                fields.push((number, wire, &before[..before.len() - payload.len()]));
                continue;
            }
            1 => 8,
            2 => usize::try_from(varint(&mut payload)?).map_err(|_| "length overflow")?,
            5 => 4,
            other => return Err(format!("unsupported wire type {other}")),
        };
        if payload.len() < length {
            return Err("truncated field".into());
        }
        let (value, rest) = payload.split_at(length);
        fields.push((number, wire, value));
        payload = rest;
    }
    Ok(fields)
}

/// Contract harness for an implementation's canonical encoder.
///
/// `encode(type_name, protojson_message)` must return the canonical input for
/// the message after it sets the signature field to arbitrary bytes, exactly
/// as the Go test does (canonical_test.go:66-84). Call it from a test of the
/// crate that owns the protobuf types.
pub fn assert_canonical_encoder(encode: impl Fn(&str, &Value) -> Result<Vec<u8>, String>) {
    for vector in load_vectors() {
        let actual = encode(&vector.type_name, &vector.message).unwrap_or_else(|error| panic!("{}: {error}", vector.type_name));
        assert_eq!(hex::encode(&actual), hex::encode(&vector.canonical), "{}: canonical input differs from the shared vector", vector.type_name);
    }
}

/// Contract harness for an implementation's verifier: it must accept every
/// vector and reject a flipped message bit, a high-S twin and a wrong key.
pub fn assert_verifier(verify: impl Fn(&[u8], &[u8], &[u8]) -> bool) {
    for vector in load_vectors() {
        let name = &vector.type_name;
        assert!(verify(&vector.public_key, &vector.canonical, &vector.signature), "{name}: valid signature rejected");
        let mut tampered = vector.canonical.clone();
        *tampered.last_mut().expect("canonical input is not empty") ^= 1;
        assert!(!verify(&vector.public_key, &tampered, &vector.signature), "{name}: tampered message accepted");
        assert!(!verify(&vector.public_key, &vector.canonical, &high_s_twin(&vector.signature)), "{name}: high-S signature accepted");
        let mut other_key = vector.public_key.clone();
        other_key[0] ^= 1;
        assert!(!verify(&other_key, &vector.canonical, &vector.signature), "{name}: wrong public key accepted");
    }
}

/// `(r, n - s)`: the malleable twin that a low-S verifier must reject.
pub fn high_s_twin(signature: &[u8]) -> Vec<u8> {
    const ORDER: [u8; 32] = [
        0xff, 0xff, 0xff, 0xff, 0x00, 0x00, 0x00, 0x00, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xbc, 0xe6, 0xfa, 0xad, 0xa7, 0x17, 0x9e,
        0x84, 0xf3, 0xb9, 0xca, 0xc2, 0xfc, 0x63, 0x25, 0x51,
    ];
    let mut twin = signature.to_vec();
    let mut borrow = 0i16;
    for index in (0..32).rev() {
        let difference = i16::from(ORDER[index]) - i16::from(signature[32 + index]) - borrow;
        borrow = i16::from(difference < 0);
        twin[32 + index] = (difference + 256 * borrow) as u8;
    }
    twin
}
