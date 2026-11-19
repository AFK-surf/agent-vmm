use agent_vmm_proto::appliance::v1 as a;
use prost::Message;
use rusqlite::{Connection, params};
use std::sync::Arc;
use vmm_compat::operator::McpManagement;
use vmm_mcp::{FileSigningKeys, TokenAuthority};

#[test]
fn pending_rotation_recovers_on_either_side_of_key_replacement() {
    for replacement_durable in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("compat.sqlite");
        let keys = TokenAuthority::new(Arc::new(FileSigningKeys::open(directory.path()).unwrap()));
        let owner = McpManagement::open(&path, keys.clone()).unwrap();
        let old_token = owner.issue().unwrap();
        let prior = keys.key_id().unwrap();
        drop(owner);

        let db = Connection::open(&path).unwrap();
        let intent = a::ApplianceOperation {
            installation_id: "agent-vmm-mcp".into(),
            operation_id: "rotation".into(),
            kind: a::ApplianceOperationKind::Update as i32,
            phase: a::ApplianceOperationPhase::Switching as i32,
            state: a::ApplianceOperationState::Running as i32,
            ..Default::default()
        };
        db.execute(
            "INSERT INTO mcp_rotations(id, prior, operation) VALUES(?, ?, ?)",
            params![
                intent.operation_id,
                prior.as_slice(),
                intent.encode_to_vec()
            ],
        )
        .unwrap();

        let issued_after_replacement = replacement_durable.then(|| {
            keys.rotate().unwrap();
            keys.issue().unwrap()
        });
        let restarted = McpManagement::open(&path, keys.clone()).unwrap();
        assert!(keys.verify(&old_token).unwrap().is_none());
        if let Some(token) = issued_after_replacement {
            assert!(keys.verify(&token).unwrap().is_some());
        }
        let bytes: Vec<u8> = db
            .query_row(
                "SELECT operation FROM mcp_rotations WHERE id='rotation' AND prior IS NULL",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let receipt = a::ApplianceOperation::decode(bytes.as_slice()).unwrap();
        assert_eq!(receipt.operation_id, intent.operation_id);
        assert_eq!(
            receipt.terminal_reason(),
            a::ApplianceTerminalReason::Succeeded
        );
        let fresh = restarted.issue().unwrap();
        drop(restarted);
        drop(McpManagement::open(&path, keys.clone()).unwrap());
        assert!(keys.verify(&fresh).unwrap().is_some());
    }
}
