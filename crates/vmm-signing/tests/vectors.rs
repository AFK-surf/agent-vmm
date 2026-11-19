use prost::Message;
use prost_reflect::{DynamicMessage, Value};
use serde_json::{Value as Json, json};
use sha2::{Digest, Sha256};
use vmm_signing::{
    Error, FileIdentityKey, IdentitySigner, RecoveryChallenge, SignedKind, SignedMessage,
};

fn vectors() -> Vec<Json> {
    serde_json::from_str(include_str!(
        "../../../api/trust/v1/testdata/signing-vectors.json"
    ))
    .unwrap()
}

fn message(kind: SignedKind, entry: &Json) -> DynamicMessage {
    let text = entry["message"].to_string();
    let mut decoder = serde_json::Deserializer::from_str(&text);
    DynamicMessage::deserialize(kind.descriptor(), &mut decoder).unwrap()
}

fn wire_hex(entry: &Json, key: &str) -> Vec<u8> {
    hex::decode(entry[key].as_str().unwrap()).unwrap()
}

#[test]
fn all_nine_go_and_salix_vectors() {
    let key = FileIdentityKey::generate();
    let vectors = vectors();
    assert_eq!(vectors.len(), SignedKind::ALL.len());
    for kind in SignedKind::ALL {
        let vector = vectors.iter().find(|v| v["type"] == kind.name()).unwrap();
        let mut original = message(kind, vector);
        let signature = wire_hex(vector, "signature_hex");
        let signature_name = match kind {
            SignedKind::PersonalMeshDescriptor => "genesis_signature",
            SignedKind::EndpointObservation => "device_signature",
            SignedKind::PersonalRouteRequest => "source_signature",
            _ => "signature",
        };
        original.set_field_by_name(signature_name, Value::Bytes(signature.clone().into()));
        let signed = SignedMessage::decode(kind, &original.encode_to_vec()).unwrap();
        let canonical = signed.canonical_bytes();
        assert_eq!(
            canonical,
            wire_hex(vector, "canonical_hex"),
            "{}",
            kind.name()
        );
        assert_eq!(
            &Sha256::digest(&canonical)[..],
            wire_hex(vector, "digest_hex")
        );
        let public_key = wire_hex(vector, "public_key_hex");
        signed.verify(&public_key).unwrap();
        let signing_key = key.public_key().unwrap();
        vmm_signing::verify(&signing_key, &canonical, &signed.sign(&key).unwrap()).unwrap();
        let mut corrupted = canonical.clone();
        *corrupted.last_mut().unwrap() ^= 1;
        assert!(vmm_signing::verify(&public_key, &corrupted, &signature).is_err());
        assert!(vmm_signing::verify(&signing_key, &canonical, &signature).is_err());
        assert!(matches!(
            vmm_signing::verify(&public_key, &canonical, &high_s(&signature)),
            Err(Error::HighS)
        ));

        let mut unknown = original.encode_to_vec();
        unknown.extend_from_slice(&[0xf8, 0x07, 0x01]); // field 127, varint 1
        assert!(matches!(
            SignedMessage::decode(kind, &unknown),
            Err(Error::UnknownFields(_))
        ));
    }
}

fn high_s(signature: &[u8]) -> Vec<u8> {
    let order =
        hex::decode("ffffffff00000000ffffffffffffffffbce6faada7179e84f3b9cac2fc632551").unwrap();
    let mut twin = signature.to_vec();
    let mut borrow = 0;
    for index in (0..32).rev() {
        let difference = i16::from(order[index]) - i16::from(signature[32 + index]) - borrow;
        borrow = i16::from(difference < 0);
        twin[32 + index] = (difference + 256 * borrow) as u8;
    }
    twin
}

#[test]
fn rejects_unknown_nested_fields() {
    let kind = SignedKind::RouteCapability;
    let vectors = vectors();
    let vector = vectors.iter().find(|v| v["type"] == kind.name()).unwrap();
    let mut outer = message(kind, vector);
    let nested_field = kind
        .descriptor()
        .get_field_by_name("source_membership_credential")
        .unwrap();
    let value = outer.get_field(&nested_field);
    let nested = value.as_message().unwrap();
    let mut wire = nested.encode_to_vec();
    wire.extend_from_slice(&[0xf8, 0x07, 0x01]);
    let unknown =
        DynamicMessage::decode(SignedKind::MembershipCredential.descriptor(), &wire[..]).unwrap();
    outer.set_field(&nested_field, Value::Message(unknown));
    assert!(matches!(
        SignedMessage::from_dynamic(kind, outer),
        Err(Error::UnknownFields(_))
    ));
}

#[test]
fn credential_replacement_matches_salix_wire() {
    let fixture: Json = serde_json::from_str(include_str!(
        "../../../internal/host/trust/testdata/registration_recovery_wire.json"
    ))
    .unwrap();
    let now = fixture["challenge"]["expires_at"].as_i64().unwrap() - 120;
    let challenge =
        RecoveryChallenge::parse(fixture["challenge"].to_string().as_bytes(), now).unwrap();
    let wire = challenge.canonical_bytes(now).unwrap();
    assert_eq!(wire, fixture["wire"].as_str().unwrap().as_bytes());
    let key = FileIdentityKey::generate();
    let restored = FileIdentityKey::from_sec1_der(&key.to_sec1_der().unwrap()).unwrap();
    let public_key = key.public_key().unwrap();
    assert_eq!(public_key, restored.public_key().unwrap());
    let signed = challenge.sign(&restored, now).unwrap();
    vmm_signing::verify(&public_key, &wire, &signed).unwrap();
    assert!(challenge.canonical_bytes(now - 1).is_err());
    assert!(challenge.canonical_bytes(now + 120).is_err());
    let mut invalid = fixture["challenge"].clone();
    invalid["unknown"] = json!(true);
    assert!(RecoveryChallenge::parse(invalid.to_string().as_bytes(), now).is_err());
}
