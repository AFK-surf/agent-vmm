use crate::*;
use rusqlite::{Connection, OptionalExtension, params};
use sha2::{Digest as _, Sha256};

#[derive(Clone, Copy, Debug)]
pub struct RequestKey<'a> {
    pub domain: &'a str,
    pub scope_id: &'a str,
    pub request_id: &'a str,
}
impl<'a> RequestKey<'a> {
    fn parts(self) -> [&'a str; 3] {
        [self.domain, self.scope_id, self.request_id]
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReceiptState {
    Running,
    Succeeded,
    Failed,
    Retryable,
    Interrupted,
    Fenced,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OperationReceipt {
    pub fingerprint: Option<Digest>,
    pub state: ReceiptState,
    pub response: Vec<u8>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BeginOperation {
    Accepted,
    Replay(OperationReceipt),
}

/// Exact request identity. This marker carries no registration or reset-completion authority.
#[derive(Clone, Copy, Debug)]
pub struct ResetFence<'a> {
    pub key: RequestKey<'a>,
}

fn read_receipt(db: &Connection, key: RequestKey<'_>) -> Result<Option<OperationReceipt>> {
    let row = db
        .query_row(
            "SELECT r.fingerprint,r.state,r.response,EXISTS(SELECT 1 FROM reset_fences f
             WHERE (f.domain,f.scope,f.request)=(r.domain,r.scope,r.request))
             FROM receipts r WHERE domain=?1 AND scope=?2 AND request=?3
             UNION ALL SELECT NULL,'fenced',X'',1 FROM reset_fences
             WHERE domain=?1 AND scope=?2 AND request=?3 LIMIT 1",
            key.parts(),
            |r| Ok((r.get(0)?, r.get::<_, String>(1)?, r.get(2)?, r.get(3)?)),
        )
        .optional()?;
    row.map(|(fingerprint, state, response, fenced)| {
        let state = if fenced {
            ReceiptState::Fenced
        } else {
            match &state[..] {
                "running" => ReceiptState::Running,
                "succeeded" => ReceiptState::Succeeded,
                "failed" => ReceiptState::Failed,
                "retryable" => ReceiptState::Retryable,
                "interrupted" => ReceiptState::Interrupted,
                _ => return Err(Error::Invalid("receipt state")),
            }
        };
        Ok(OperationReceipt {
            fingerprint,
            state,
            response,
        })
    })
    .transpose()
}

impl Store {
    pub fn receipt(&self, key: RequestKey<'_>) -> Result<Option<OperationReceipt>> {
        self.read(|db| read_receipt(db, key))
    }
    /// Callers supply canonical request bytes, including each behavior-changing argument.
    pub fn begin_operation(&self, key: RequestKey<'_>, request: &[u8]) -> Result<BeginOperation> {
        if key.domain.is_empty() || key.request_id.is_empty() {
            return Err(Error::Invalid("request key"));
        }
        let fingerprint: Digest = Sha256::digest(request).into();
        self.write(|db| {
            if let Some(receipt) = read_receipt(db, key)? {
                if receipt
                    .fingerprint
                    .is_some_and(|known| known != fingerprint)
                {
                    return Err(Error::Conflict("request fingerprint"));
                }
                if receipt.state == ReceiptState::Fenced {
                    return Err(Error::Fenced);
                }
                return Ok(BeginOperation::Replay(receipt));
            }
            db.execute(
                "INSERT INTO receipts(domain,scope,request,fingerprint,state,response) VALUES(?,?,?,?,'running',X'')",
                params![key.domain, key.scope_id, key.request_id, fingerprint],
            )?;
            Ok(BeginOperation::Accepted)
        })
    }

    /// Restart an operation whose caller has reconciled or can safely resume its effects.
    pub fn resume_operation(&self, key: RequestKey<'_>, request: &[u8]) -> Result<()> {
        self.resume(key, request, false)
    }

    /// Restart an operation that repeats safely. The caller decides that a
    /// `Failed` receipt does not hold a deterministic error.
    pub fn resume_reentrant_operation(&self, key: RequestKey<'_>, request: &[u8]) -> Result<()> {
        if !matches!(
            key.domain,
            "runtime-import-image" | "runtime-pull-image" | "runtime-start-container"
        ) {
            return Err(Error::Invalid("reentrant domain"));
        }
        self.resume(key, request, true)
    }

    fn resume(&self, key: RequestKey<'_>, request: &[u8], allow_failed: bool) -> Result<()> {
        let fingerprint: Digest = Sha256::digest(request).into();
        self.write(|db| {
            let receipt = read_receipt(db, key)?.ok_or(Error::Missing("receipt"))?;
            if receipt.fingerprint != Some(fingerprint) {
                return Err(Error::Conflict("request fingerprint"));
            }
            match receipt.state {
                ReceiptState::Retryable | ReceiptState::Interrupted => {}
                ReceiptState::Failed if allow_failed => {}
                _ => return Err(Error::Conflict("operation cannot be resumed")),
            }
            db.execute(
                "UPDATE receipts SET state='running',response=X'' WHERE domain=? AND scope=? AND request=?",
                key.parts(),
            )?;
            Ok(())
        })
    }

    /// Make `failed` receipts of `domains` retryable when `should_retry` accepts the stored
    /// response. A release before re-entrant image operations stored each error as `failed`.
    /// A fenced request keeps its receipt.
    pub fn normalize_failed_receipts(
        &self,
        domains: &[&str],
        should_retry: impl Fn(&[u8]) -> bool,
    ) -> Result<usize> {
        self.write(|db| {
            let mut updated = 0;
            for domain in domains {
                let failed = db
                    .prepare(
                        "SELECT scope,request,response FROM receipts r
                         WHERE r.domain=?1 AND r.state='failed' AND NOT EXISTS(
                             SELECT 1 FROM reset_fences f
                             WHERE (f.domain,f.scope,f.request)=(r.domain,r.scope,r.request))",
                    )?
                    .query_map([domain], |r| {
                        Ok((
                            r.get::<_, String>(0)?,
                            r.get::<_, String>(1)?,
                            r.get::<_, Vec<u8>>(2)?,
                        ))
                    })?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                for (scope, request, response) in failed {
                    if should_retry(&response) {
                        updated += db.execute(
                            "UPDATE receipts SET state='retryable'
                             WHERE domain=?1 AND scope=?2 AND request=?3 AND state='failed'",
                            params![domain, scope, request],
                        )?;
                    }
                }
            }
            Ok(updated)
        })
    }

    pub fn finish_operation(
        &self,
        key: RequestKey<'_>,
        request: &[u8],
        state: ReceiptState,
        response: &[u8],
    ) -> Result<()> {
        let stored = match state {
            ReceiptState::Succeeded => "succeeded",
            ReceiptState::Failed => "failed",
            ReceiptState::Retryable => "retryable",
            _ => return Err(Error::Invalid("completion state")),
        };
        self.write(|db| {
            let receipt = read_receipt(db, key)?.ok_or(Error::Missing("receipt"))?;
            if receipt.state == ReceiptState::Fenced {
                return Err(Error::Fenced);
            }
            if receipt.fingerprint != Some(Digest::from(Sha256::digest(request))) {
                return Err(Error::Conflict("request fingerprint"));
            }
            if receipt.state == state && receipt.response == response {
                return Ok(());
            }
            if receipt.state != ReceiptState::Running {
                return Err(Error::Conflict("operation already finished"));
            }
            db.execute(
                "UPDATE receipts SET state=?,response=? WHERE domain=? AND scope=? AND request=?",
                params![stored, response, key.domain, key.scope_id, key.request_id],
            )?;
            Ok(())
        })
    }

    /// Commit all identities before the caller removes the associated plans.
    /// Repeating the reset after a crash preserves the same fence.
    pub fn fence_reset_requests(&self, fences: &[ResetFence<'_>]) -> Result<()> {
        self.write(|db| {
            for fence in fences {
                db.execute(
                    "INSERT INTO reset_fences VALUES(?,?,?) ON CONFLICT DO NOTHING",
                    fence.key.parts(),
                )?;
            }
            Ok(())
        })
    }
}
