//! Non-interactive, application-tagged device keys in the data-protection Keychain.
use crate::identity::{DeviceRoots, Root};
use core_foundation::{
    base::{CFType, TCFType},
    boolean::CFBoolean,
    data::CFData,
    dictionary::CFDictionary,
    number::CFNumber,
    string::{CFString, CFStringRef},
};
use p256::{PublicKey, ecdsa::Signature, elliptic_curve::sec1::ToEncodedPoint};
use security_framework::{
    access_control::{ProtectionMode, SecAccessControl},
    key::{Algorithm, SecKey},
};
use security_framework_sys::{
    access_control::kSecAccessControlPrivateKeyUsage, item::*, keychain_item::SecItemCopyMatching,
};
use vmm_signing::{Error, IdentitySigner, Result};

unsafe extern "C" {
    static kSecAttrApplicationTag: CFStringRef;
    static kSecUseAuthenticationUIFail: CFStringRef;
}
macro_rules! symbol {
    ($constant:ident) => {{
        // SAFETY: each argument is a process-lifetime CFString exported by Security.framework.
        unsafe { CFString::wrap_under_get_rule($constant) }
    }};
}
pub(crate) struct AppleRoots(pub String);
struct AppleSigner(SecKey);

impl IdentitySigner for AppleSigner {
    fn public_key(&self) -> Result<[u8; 33]> {
        let bytes = self
            .0
            .public_key()
            .and_then(|key| key.external_representation())
            .ok_or(Error::Unavailable)?;
        PublicKey::from_sec1_bytes(bytes.bytes())
            .map_err(|_| Error::Key)?
            .to_encoded_point(true)
            .as_bytes()
            .try_into()
            .map_err(|_| Error::Key)
    }
    fn sign_digest(&self, digest: &[u8; 32]) -> Result<[u8; 64]> {
        let der = self
            .0
            .create_signature(Algorithm::ECDSASignatureDigestX962SHA256, digest)
            .map_err(|_| Error::Unavailable)?;
        let sig = Signature::from_der(&der).map_err(|_| Error::Signature)?;
        Ok(sig.normalize_s().unwrap_or(sig).to_bytes().into())
    }
}
type Attributes = Vec<(CFString, CFType)>;
fn common() -> Attributes {
    vec![
        (
            symbol!(kSecUseDataProtectionKeychain),
            CFBoolean::true_value().as_CFType(),
        ),
        (
            symbol!(kSecUseAuthenticationUI),
            symbol!(kSecUseAuthenticationUIFail).as_CFType(),
        ),
        (
            symbol!(kSecAttrKeyType),
            symbol!(kSecAttrKeyTypeECSECPrimeRandom).as_CFType(),
        ),
    ]
}
fn tagged(tag: &CFData) -> Attributes {
    vec![
        (symbol!(kSecAttrApplicationTag), tag.as_CFType()),
        (
            symbol!(kSecAttrAccessGroup),
            CFString::new(crate::keychain::GROUP).as_CFType(),
        ),
    ]
}
fn find(tag: &CFData) -> Result<Option<SecKey>> {
    let mut query = common();
    query.extend(tagged(tag));
    query.extend([
        (symbol!(kSecClass), symbol!(kSecClassKey).as_CFType()),
        (
            symbol!(kSecAttrKeyClass),
            symbol!(kSecAttrKeyClassPrivate).as_CFType(),
        ),
        (symbol!(kSecReturnRef), CFBoolean::true_value().as_CFType()),
    ]);
    let query = CFDictionary::from_CFType_pairs(&query);
    let mut raw = std::ptr::null();
    // SAFETY: query stays alive, raw is an initialized writable CFTypeRef out-parameter.
    let status = unsafe { SecItemCopyMatching(query.as_concrete_TypeRef(), &mut raw) };
    match status {
        -25300 => Ok(None),
        0 if !raw.is_null() => {
            // SAFETY: SecItemCopyMatching returns one owned reference on success.
            let value = unsafe { CFType::wrap_under_create_rule(raw) };
            if !value.instance_of::<SecKey>() {
                return Err(Error::Key);
            }
            // SAFETY: the runtime type was checked and value owns the reference until retained.
            Ok(Some(unsafe {
                SecKey::wrap_under_get_rule(raw.cast_mut().cast())
            }))
        }
        _ => Err(Error::Unavailable),
    }
}
fn create(tag: &CFData, enclave: bool) -> std::result::Result<SecKey, i64> {
    let mut private = tagged(tag);
    let access = SecAccessControl::create_with_protection(
        Some(ProtectionMode::AccessibleWhenUnlockedThisDeviceOnly),
        if enclave {
            kSecAccessControlPrivateKeyUsage
        } else {
            0
        },
    )
    .map_err(|e| i64::from(e.code()))?;
    private.extend([
        (
            symbol!(kSecAttrIsPermanent),
            CFBoolean::true_value().as_CFType(),
        ),
        (symbol!(kSecAttrAccessControl), access.as_CFType()),
    ]);
    let mut attributes = common();
    attributes.extend([
        (
            symbol!(kSecAttrKeySizeInBits),
            CFNumber::from(256).as_CFType(),
        ),
        (
            symbol!(kSecPrivateKeyAttrs),
            CFDictionary::from_CFType_pairs(&private).as_CFType(),
        ),
    ]);
    if enclave {
        attributes.push((
            symbol!(kSecAttrTokenID),
            symbol!(kSecAttrTokenIDSecureEnclave).as_CFType(),
        ));
    }
    // The high-level generation options cannot express the private application tag or access group.
    #[allow(deprecated)]
    SecKey::generate(CFDictionary::from_CFType_pairs(&attributes).into_untyped())
        .map_err(|e| e.code() as i64)
}
impl DeviceRoots for AppleRoots {
    fn open(&self, revision: u64, create_missing: bool) -> Result<Root> {
        if revision == 0 {
            return Err(Error::Key);
        }
        let mut account = format!("{}/device-root.key", self.0);
        if revision > 1 {
            account.push_str(&format!(".{revision}"));
        }
        let tag = CFData::from_buffer(account.as_bytes());
        let key = match find(&tag)? {
            Some(key) => key,
            None if !create_missing => return Err(Error::Io(std::io::ErrorKind::NotFound.into())),
            None => {
                let generated = match create(&tag, true) {
                    // Unsupported token/algorithm or no hardware token on this Mac.
                    Err(-4 | -50 | -25291) => create(&tag, false),
                    other => other,
                };
                match generated {
                    Ok(key) => key,
                    Err(-25299) => find(&tag)?.ok_or(Error::Unavailable)?,
                    Err(_) => return Err(Error::Unavailable),
                }
            }
        };
        let attributes = key.attributes();
        let enclave = attributes
            .find(symbol!(kSecAttrTokenID).as_CFTypeRef())
            .is_some_and(|entry| {
                // SAFETY: this is a borrowed CF value from the live SecKey attribute dictionary.
                (unsafe { CFType::wrap_under_get_rule(*entry) })
                    == symbol!(kSecAttrTokenIDSecureEnclave).as_CFType()
            });
        Ok(Root {
            custody: if enclave {
                "secure-enclave"
            } else {
                "keychain"
            },
            signer: Box::new(AppleSigner(key)),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn verifies_security_framework_signature_without_exporting_private_key() {
        let attrs = CFDictionary::from_CFType_pairs(&[
            (
                symbol!(kSecAttrKeyType),
                symbol!(kSecAttrKeyTypeECSECPrimeRandom).as_CFType(),
            ),
            (
                symbol!(kSecAttrKeySizeInBits),
                CFNumber::from(256).as_CFType(),
            ),
        ]);
        #[allow(deprecated)]
        let signer = AppleSigner(SecKey::generate(attrs.into_untyped()).unwrap());
        let payload = b"registration recovery";
        let signature = vmm_signing::sign(&signer, payload).unwrap();
        vmm_signing::verify(&signer.public_key().unwrap(), payload, &signature).unwrap();
    }
}
