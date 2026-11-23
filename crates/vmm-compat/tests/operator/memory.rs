use super::{disposal::Connectors, *};
use h::operator_service_client::OperatorServiceClient;
use std::sync::atomic::{AtomicU32, Ordering};
use vmm_compat::operator::{LocalOperator, LocalOperatorServer, VmControl};
use vmm_rpc::Status;

struct Vm(h::VmState);
#[async_trait::async_trait]
impl VmControl for Vm {
    async fn observe(&self) -> Result<h::Vm, Status> {
        Ok(h::Vm {
            state: self.0 as i32,
            ..Default::default()
        })
    }
    async fn change(&self, _: bool, _: bool, _: &str) -> Result<(), Status> {
        panic!("read-only request changed the VM")
    }
}

fn sample() -> h::GetMemorySummaryResponse {
    h::GetMemorySummaryResponse {
        sampled_at: None,
        carrier_footprint_bytes: 1,
        pool: Some(h::MemoryPoolSummary {
            clean_bytes: 2,
            free_bytes: 3,
            inflated_bytes: 4,
            guest_plugged_bytes: 5,
        }),
        dom0_limit_bytes: 6,
        dom0_used_bytes: 7,
        environments: vec![h::EnvironmentMemorySummary {
            environment_id: "owned".into(),
            boot_bytes: 8,
            grant_bytes: 9,
            plugged_bytes: 10,
            zram_bytes: 11,
            ebusy_count: 12,
        }],
    }
}

#[tokio::test]
async fn memory_summary_needs_a_configured_sample_and_a_running_vm() {
    let dir = tempfile::tempdir().unwrap();
    let samples = Arc::new(AtomicU32::new(0));
    // No sample, a stopped VM, a running VM, and a sample that fails.
    let mut answers = Vec::new();
    for (case, state) in [
        (0, h::VmState::Running),
        (1, h::VmState::Stopped),
        (2, h::VmState::Running),
        (3, h::VmState::Running),
    ] {
        let mut api = LocalOperator::open(
            &dir.path().join(format!("compat-{case}")),
            Arc::new(Store::open(dir.path().join(format!("host-{case}"))).unwrap()),
            Arc::new(Connectors::default()),
            None,
        )
        .unwrap();
        if case > 0 {
            let samples = samples.clone();
            api = api.with_memory_sample(move || {
                samples.fetch_add(1, Ordering::Relaxed);
                if case == 3 {
                    return Err(vmm_core::Error::Interrupted);
                }
                Ok(sample())
            });
        }
        api.vm_control = Some(Arc::new(Vm(state)));
        let endpoint = dir.path().join(uuid::Uuid::new_v4().simple().to_string());
        let listener = tokio::net::UnixListener::bind(&endpoint).unwrap();
        let server = tokio::spawn(
            vmm_rpc::Router::new(LocalOperatorServer(Arc::new(api))).serve_unix(
                listener,
                128,
                std::future::pending(),
            ),
        );
        let answer = OperatorServiceClient::new(vmm_rpc::Client::unix(&endpoint))
            .get_memory_summary(h::GetMemorySummaryRequest {})
            .await
            .map(|response| response.into_inner())
            .map_err(|status| (status.code(), status.message().to_owned()));
        answers.push((answer, samples.load(Ordering::Relaxed)));
        server.abort();
    }
    let [unconfigured, stopped, running, failed] = answers.try_into().unwrap();
    assert_eq!(unconfigured.0.unwrap_err().0, Code::Unimplemented);
    assert_eq!(
        stopped,
        (
            Err((Code::FailedPrecondition, "VM is not running".into())),
            0
        )
    );
    let mut summary = running.0.unwrap();
    assert!(summary.sampled_at.take().is_some());
    assert_eq!((summary, running.1), (sample(), 1));
    assert_eq!((failed.0.unwrap_err().0, failed.1), (Code::Unavailable, 2));
}
