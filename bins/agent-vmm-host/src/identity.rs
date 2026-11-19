//! Durable public identity and access to its original signing key.
use crate::registrations::ExternalRegistrations;
use agent_vmm_proto::{host::v1 as h, trust::v1 as trust};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use std::{
    path::Path,
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};
use vmm_rpc::Status;
use vmm_compat::operator::DeviceTrust;
use vmm_signing::{Error, FileIdentityStore, IdentitySigner, RecoveryChallenge};

pub(crate) struct Root {
    pub custody: &'static str,
    pub signer: Box<dyn IdentitySigner>,
}
pub(crate) trait DeviceRoots: Send + Sync {
    fn open(&self, revision: u64, create: bool) -> vmm_signing::Result<Root>;
}
impl DeviceRoots for FileIdentityStore {
    fn open(&self, revision: u64, create: bool) -> vmm_signing::Result<Root> {
        let key = if create {
            self.ensure(revision)?
        } else {
            self.load(revision)?
        };
        Ok(Root {
            custody: "file",
            signer: Box::new(key),
        })
    }
}

pub(crate) struct HostIdentity {
    database: Mutex<Connection>,
    roots: Box<dyn DeviceRoots>,
    registrations: Arc<ExternalRegistrations>,
}
fn unavailable<E>(_: E) -> Status {
    Status::unavailable("device identity unavailable")
}
fn now() -> Result<i64, Status> {
    let seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(unavailable)?
        .as_secs();
    i64::try_from(seconds).map_err(unavailable)
}
struct Verified {
    identity: trust::DeviceIdentity,
    root: Option<Root>,
}
impl HostIdentity {
    pub fn open(
        path: &Path,
        roots: Box<dyn DeviceRoots>,
        registrations: Arc<ExternalRegistrations>,
    ) -> Result<Self, Status> {
        let database = Connection::open(path).map_err(unavailable)?;
        database.execute_batch("CREATE TABLE IF NOT EXISTS device_identity(singleton INTEGER PRIMARY KEY CHECK(singleton=1),device TEXT NOT NULL,revision INTEGER NOT NULL CHECK(revision>0),public BLOB NOT NULL,custody TEXT NOT NULL,needs_repair INTEGER NOT NULL)").map_err(unavailable)?;
        let manager = Self {
            database: Mutex::new(database),
            roots,
            registrations,
        };
        manager.verified()?;
        Ok(manager)
    }
    // Hold the write transaction through first creation and publication. A restart
    // after key creation adopts the already published key before assigning an ID.
    fn verified(&self) -> Result<Verified, Status> {
        let mut db = self.database.lock().map_err(unavailable)?;
        let tx = db
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(unavailable)?;
        let known = tx.query_row("SELECT device,revision,public,custody,needs_repair FROM device_identity WHERE singleton=1",[],|r| Ok((r.get::<_,String>(0)?,r.get::<_,i64>(1)?,r.get::<_,Vec<u8>>(2)?,r.get::<_,String>(3)?,r.get::<_,bool>(4)?))).optional().map_err(unavailable)?;
        let (device_id, root_key_revision, root_public_key, root) = match known {
            Some((id, revision, public, custody, repair)) => {
                let revision = u64::try_from(revision).map_err(unavailable)?;
                let root = if repair {
                    None
                } else {
                    match self
                        .roots
                        .open(revision, false)
                        .and_then(|k| Ok((k.signer.public_key()?, k)))
                    {
                        Ok((actual, k)) if actual.as_slice() == public && k.custody == custody => {
                            Some(k)
                        }
                        Ok(_) | Err(Error::Key) => None,
                        Err(Error::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => None,
                        Err(e) => return Err(unavailable(e)),
                    }
                };
                if !repair && root.is_none() {
                    tx.execute(
                        "UPDATE device_identity SET needs_repair=1 WHERE singleton=1",
                        [],
                    )
                    .map_err(unavailable)?;
                }
                (id, revision, public, root)
            }
            None => {
                let root = self.roots.open(1, true).map_err(unavailable)?;
                let public = root.signer.public_key().map_err(unavailable)?.to_vec();
                let id = uuid::Uuid::new_v4().simple().to_string();
                tx.execute(
                    "INSERT INTO device_identity VALUES(1,?,1,?,?,0)",
                    params![id, public, root.custody],
                )
                .map_err(unavailable)?;
                (id, 1, public, Some(root))
            }
        };
        tx.commit().map_err(unavailable)?;
        Ok(Verified {
            identity: trust::DeviceIdentity {
                device_id,
                root_key_revision,
                root_public_key,
                signature_suite: trust::SignatureSuite::P256Sha256 as i32,
            },
            root,
        })
    }
}
impl DeviceTrust for HostIdentity {
    fn identity(&self) -> Result<h::GetDeviceIdentityResponse, Status> {
        let verified = self.verified()?;
        Ok(h::GetDeviceIdentityResponse {
            identity: Some(verified.identity),
            state: if verified.root.is_some() {
                h::DeviceIdentityState::Ready
            } else {
                h::DeviceIdentityState::NeedsRepair
            } as i32,
        })
    }
    fn sign_service_endpoint(
        &self,
        endpoint: &str,
        generation: u64,
    ) -> Result<trust::EndpointObservation, Status> {
        let verified = self.verified()?;
        let key = verified
            .root
            .ok_or_else(|| Status::failed_precondition("original device root needs repair"))?;
        let mut observation = trust::EndpointObservation {
            device_id: verified.identity.device_id,
            root_key_revision: verified.identity.root_key_revision,
            endpoint_node_id: endpoint.as_bytes().to_vec(),
            endpoint_generation: generation,
            supported_alpns: vec!["agent-vmm/service-tunnel/1".into()],
            feature_set: vec!["direct".into(), "relay".into()],
            expires_at: Some((SystemTime::now() + std::time::Duration::from_secs(600)).into()),
            ..Default::default()
        };
        observation.device_signature = vmm_signing::SignedMessage::try_from(&observation)
            .and_then(|message| message.sign(key.signer.as_ref()))
            .map_err(unavailable)?
            .to_vec();
        Ok(observation)
    }
    fn sign_recovery(&self, bytes: &[u8]) -> Result<h::SignRegistrationRecoveryResponse, Status> {
        let challenge = RecoveryChallenge::parse(bytes, now()?).map_err(|_| {
            vmm_compat::error::invalid_field(
                "challenge_json",
                "a live registration recovery challenge is required",
            )
        })?;
        self.registrations
            .with_recoverable(&challenge.registration_id, || {
                let verified = self.verified()?;
                let root = verified.root.ok_or_else(|| {
                    Status::failed_precondition("original device root needs repair")
                })?;
                let signature = challenge.sign(root.signer.as_ref(), now()?).map_err(|_| {
                    Status::failed_precondition(
                        "the original device cannot sign this recovery challenge",
                    )
                })?;
                Ok(h::SignRegistrationRecoveryResponse {
                    identity: Some(verified.identity),
                    signature: signature.to_vec(),
                })
            })
    }
}
