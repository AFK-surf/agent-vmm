use crate::{Error, IdentitySigner, Result, sign, verify};
use agent_vmm_proto::{appliance::v1 as appliance, service::v1 as service, trust::v1 as trust};
use prost::Message;
use prost_reflect::{DescriptorPool, DynamicMessage, MessageDescriptor, ReflectMessage, Value};
use std::sync::LazyLock;

const DOMAIN: &[u8] = b"agent-vmm/signature/v1\0";

static DESCRIPTORS: LazyLock<DescriptorPool> = LazyLock::new(|| {
    DescriptorPool::decode(agent_vmm_proto::FILE_DESCRIPTOR_SET)
        .expect("generated agent-vmm descriptors")
});

macro_rules! signed_kinds {
    ($(($kind:ident, $message:path, $package:literal, $proto:literal, $signature:literal)),+ $(,)?) => {
        #[derive(Clone, Copy, Debug, Eq, PartialEq)]
        pub enum SignedKind {
            $($kind),+
        }

        impl SignedKind {
            pub const ALL: [Self; 9] = [$(Self::$kind),+];

            pub fn name(self) -> &'static str {
                match self { $(Self::$kind => stringify!($kind)),+ }
            }

            pub fn descriptor(self) -> MessageDescriptor {
                let name = match self { $(Self::$kind => concat!("agentvmm.", $package, ".v1.", $proto)),+ };
                DESCRIPTORS.get_message_by_name(name).expect("generated signed message descriptor")
            }

            fn signature_field(self) -> u32 {
                match self { $(Self::$kind => $signature),+ }
            }
        }

        $(impl TryFrom<&$message> for SignedMessage {
            type Error = Error;
            fn try_from(message: &$message) -> Result<Self> {
                Self::decode(SignedKind::$kind, &message.encode_to_vec())
            }
        })+
    };
}

signed_kinds!(
    (
        ApplianceLaunchClaim,
        appliance::ApplianceLaunchClaim,
        "appliance",
        "ApplianceLaunchClaim",
        11
    ),
    (
        PersonalMeshDescriptor,
        trust::PersonalMeshDescriptor,
        "trust",
        "PersonalMeshDescriptor",
        7
    ),
    (
        MembershipCredential,
        trust::MembershipCredential,
        "trust",
        "MembershipCredential",
        7
    ),
    (
        EndpointObservation,
        trust::EndpointObservation,
        "trust",
        "EndpointObservation",
        9
    ),
    (
        PairingProof,
        trust::PairingProof,
        "trust",
        "PairingProof",
        11
    ),
    (
        RouteCapability,
        trust::RouteCapability,
        "trust",
        "RouteCapability",
        17
    ),
    (
        RegistryOperation,
        trust::RegistryOperation,
        "trust",
        "RegistryOperation",
        12
    ),
    (
        RegistryReceipt,
        trust::RegistryReceipt,
        "trust",
        "RegistryReceipt",
        9
    ),
    (
        PersonalRouteRequest,
        service::RequestPersonalRouteRequest,
        "service",
        "RequestPersonalRouteRequest",
        13
    ),
);

/// Untrusted protobuf must enter through decode so unknown fields survive parsing.
#[derive(Debug)]
pub struct SignedMessage {
    kind: SignedKind,
    message: DynamicMessage,
}

impl SignedMessage {
    pub fn decode(kind: SignedKind, wire: &[u8]) -> Result<Self> {
        let message = DynamicMessage::decode(kind.descriptor(), wire)?;
        Self::from_dynamic(kind, message)
    }

    pub fn from_dynamic(kind: SignedKind, message: DynamicMessage) -> Result<Self> {
        if message.descriptor() != kind.descriptor() {
            return Err(Error::Descriptor);
        }
        validate_fields(&message)?;
        Ok(Self { kind, message })
    }

    pub fn canonical_bytes(&self) -> Vec<u8> {
        let mut message = self.message.clone();
        message.clear_field_by_number(self.kind.signature_field());
        let mut result =
            Vec::with_capacity(DOMAIN.len() + self.kind.name().len() + 1 + message.encoded_len());
        result.extend_from_slice(DOMAIN);
        result.extend_from_slice(self.kind.name().as_bytes());
        result.push(0);
        // DynamicMessage encodes set fields in field-number order, including nested messages.
        message.encode_raw(&mut result);
        result
    }

    pub fn sign(&self, signer: &dyn IdentitySigner) -> Result<[u8; 64]> {
        sign(signer, &self.canonical_bytes())
    }

    pub fn verify(&self, public_key: &[u8]) -> Result<()> {
        let value = self
            .message
            .get_field_by_number(self.kind.signature_field());
        let signature = value
            .as_deref()
            .and_then(Value::as_bytes)
            .ok_or(Error::Signature)?;
        verify(public_key, &self.canonical_bytes(), signature)
    }
}

fn validate_fields(message: &DynamicMessage) -> Result<()> {
    if message.unknown_fields().next().is_some() {
        return Err(Error::UnknownFields(
            message.descriptor().full_name().into(),
        ));
    }
    if message.descriptor().fields().any(|field| field.is_map()) {
        return Err(Error::MapField);
    }
    for (_, value) in message.fields() {
        match value {
            Value::Message(nested) => validate_fields(nested)?,
            Value::List(list) => {
                for value in list {
                    if let Value::Message(nested) = value {
                        validate_fields(nested)?;
                    }
                }
            }
            _ => {}
        }
    }
    Ok(())
}
