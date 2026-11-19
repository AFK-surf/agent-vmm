use security_framework::{
    access_control::{ProtectionMode, SecAccessControl},
    passwords::{
        PasswordOptions, delete_generic_password_options, generic_password,
        set_generic_password_options,
    },
};
use vmm_signing::{Error, Keychain, Result};
use zeroize::Zeroizing;
const SERVICE: &str = "com.afk-surf.agent-vmm-host";
pub(crate) const GROUP: &str = "D9AAN3VJK8.com.afk-surf.agent-vmm-host";

pub(crate) struct PlatformKeys(pub String);
impl PlatformKeys {
    fn options(&self, item: &str) -> PasswordOptions {
        let mut opts =
            PasswordOptions::new_generic_password(SERVICE, &format!("{}/{item}", self.0));
        opts.set_access_group(GROUP);
        opts.use_protected_keychain();
        opts
    }
}
fn map(error: security_framework::base::Error) -> Error {
    match error.code() {
        -25300 => Error::Io(std::io::ErrorKind::NotFound.into()),
        _ => Error::Unavailable,
    }
}
impl Keychain for PlatformKeys {
    fn probe(&self) -> Result<()> {
        let id = format!("probe/{}", uuid::Uuid::new_v4().simple());
        let mut value = Zeroizing::new([0; 32]);
        getrandom::fill(value.as_mut_slice()).map_err(|_| Error::Unavailable)?;
        self.replace(&id, value.as_ref())?;
        let verified = self.load(&id).and_then(|read| {
            if read.as_slice() == value.as_slice() {
                Ok(())
            } else {
                Err(Error::Unavailable)
            }
        });
        let deleted = self.delete(&id);
        verified.and(deleted)
    }
    fn load(&self, item: &str) -> Result<Zeroizing<Vec<u8>>> {
        generic_password(self.options(item))
            .map(Zeroizing::new)
            .map_err(map)
    }
    fn replace(&self, item: &str, value: &[u8]) -> Result<()> {
        let mut options = self.options(item);
        options.set_access_control(
            SecAccessControl::create_with_protection(
                Some(ProtectionMode::AccessibleWhenUnlockedThisDeviceOnly),
                0,
            )
            .map_err(map)?,
        );
        set_generic_password_options(value, options).map_err(map)
    }
    fn delete(&self, item: &str) -> Result<()> {
        match delete_generic_password_options(self.options(item)) {
            Err(e) if e.code() == -25300 => Ok(()),
            result => result.map_err(map),
        }
    }
}
