#![cfg(feature = "test-support")]

mod support;

use std::str::FromStr;
use std::time::Duration;

use coding_agent_app::{
    DeliveryAcceptRequest, DeliveryMergeAcceptanceOutcome, DeliveryMergeReceiptDisposition,
    DeliveryPreflightOutcome, DeliveryPreflightRequest, DeliveryPreflightState,
};
use coding_agent_domain::ClientRequestId;
use coding_agent_store::{
    AcceptMergeCommandRequest, DeliveryOperationSnapshot, DeliverySourceState, GitBranchRef,
    GitCommitOid, MergeOperationRecord, MergeOperationState, PreflightCommandRequest,
};

use support::delivery::ControlledDeliveryFixture;

static E2E_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

// The production actor gives each live runtime stage ten minutes and reserves
// one additional minute for the runtime to prove process-tree cleanup. The E2E
// observer must not declare the delivery hung before that production contract
// has had a chance to produce its typed outcome.
const PRODUCTION_DELIVERY_STAGE_TIMEOUT: Duration = Duration::from_secs(11 * 60);

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn approved_task_requires_explicit_accept_and_merges_exact_no_ff_commit() {
    let _guard = E2E_LOCK.lock().await;
    let fixture = ControlledDeliveryFixture::new().await;
    let completed = fixture.approve_task().await;
    let artifact = fixture.approved_artifact(completed.id).await;
    let source_ref = format!("refs/heads/{}", artifact.branch_name);

    assert_eq!(
        fixture.git_line(&["rev-parse", &source_ref]),
        fixture.base_head,
        "approval alone must not create or advance a delivery source commit"
    );
    assert_eq!(fixture.git_line(&["rev-parse", "HEAD"]), fixture.base_head);
    assert_eq!(
        fixture.git_line(&["rev-list", "--count", "--all"]),
        "1",
        "approval alone must not create a reachable source or merge commit"
    );
    assert!(
        fixture
            .handles()
            .store
            .delivery_eligibility_snapshot(completed.id)
            .await
            .expect("load pre-click delivery snapshot")
            .expect("approved task exists")
            .ownership
            .source
            .is_none(),
        "approval alone must not create a durable delivery source"
    );

    let target_before = TargetSnapshot::capture(&fixture);
    let target_branch = GitBranchRef::from_str("refs/heads/main").expect("valid main ref");
    let target_head = GitCommitOid::from_str(&fixture.base_head).expect("valid target head");
    let preflight = fixture
        .handles()
        .delivery_manager
        .preflight(DeliveryPreflightRequest::new(
            PreflightCommandRequest::try_new(
                ClientRequestId::new(),
                completed.id,
                target_branch.clone(),
                target_head.clone(),
            )
            .expect("valid real preflight request"),
        ))
        .await
        .expect("delivery manager remains open");
    let operation_id = match preflight {
        DeliveryPreflightOutcome::Durable(operation) => {
            if operation.state() != DeliveryPreflightState::PreflightReady {
                let persisted = merge_operation(&fixture, operation.operation_id()).await;
                panic!(
                    "real clean preflight did not become ready: projection={operation:?}; persisted={persisted:?}; failure={:?}",
                    persisted.failure_code
                );
            }
            operation.operation_id()
        }
        other => panic!("real clean preflight did not become ready: {other:?}"),
    };
    assert_eq!(
        TargetSnapshot::capture(&fixture),
        target_before,
        "clean preflight must leave the target checkout byte-for-byte unchanged"
    );

    let operation = merge_operation(&fixture, operation_id).await;
    let snapshot = fixture
        .handles()
        .store
        .delivery_eligibility_snapshot(completed.id)
        .await
        .expect("load approved acceptance snapshot")
        .expect("approved acceptance task exists");
    let evidence = snapshot
        .evidence_identity
        .expect("approved task has exact review evidence");
    let acceptance = fixture
        .handles()
        .delivery_manager
        .accept_merge(DeliveryAcceptRequest::new(
            AcceptMergeCommandRequest::try_new(
                ClientRequestId::new(),
                completed.id,
                operation_id,
                operation.version,
                evidence.workspace_generation(),
                evidence.workspace_fingerprint().clone(),
                target_branch,
                target_head,
            )
            .expect("valid exact acceptance request"),
        ))
        .await
        .expect("delivery manager remains open");
    let acceptance = match acceptance {
        DeliveryMergeAcceptanceOutcome::Durable(acceptance) => acceptance,
        other => panic!("explicit acceptance was not durable: {other:?}"),
    };
    assert_eq!(
        acceptance.receipt(),
        DeliveryMergeReceiptDisposition::Created
    );
    assert_eq!(acceptance.operation_id(), operation_id);

    let merged = wait_for_merge(&fixture, operation_id).await;
    let source = fixture
        .handles()
        .store
        .delivery_eligibility_snapshot(completed.id)
        .await
        .expect("load merged delivery snapshot")
        .expect("merged task exists")
        .ownership
        .source
        .expect("merged task retains source proof");
    assert_eq!(source.state, DeliverySourceState::Committed);
    let source_commit = source
        .expected_source_commit
        .expect("committed source has an exact commit");
    let merge_commit = merged
        .expected_merge_commit
        .expect("merged operation has an exact merge commit");

    assert_eq!(
        fixture.git_line(&["rev-parse", "HEAD"]),
        merge_commit.as_str()
    );
    assert_eq!(
        fixture.git_line(&["rev-parse", &source_ref]),
        source_commit.as_str()
    );
    assert_eq!(
        fixture.git_line(&["show", "-s", "--format=%P", "HEAD"]),
        format!("{} {}", fixture.base_head, source_commit.as_str()),
        "the explicit delivery must be an exact two-parent no-ff merge"
    );
    assert!(
        fixture
            .git_output(&["status", "--porcelain=v1", "-z"])
            .stdout
            .is_empty(),
        "successful delivery must leave the target checkout clean"
    );
    assert_eq!(
        std::fs::read_to_string(fixture.repository_path.join("src/lib.rs"))
            .expect("read delivered target source"),
        "pub fn answer() -> u32 { 42 }\n// approved controlled delivery\n"
    );
    assert!(
        fixture.provider_calls() >= 10,
        "all three real roles must run"
    );

    fixture.shutdown().await;
}

#[derive(Debug, PartialEq, Eq)]
struct TargetSnapshot {
    head: String,
    status: Vec<u8>,
    source: Vec<u8>,
    index: Vec<u8>,
}

impl TargetSnapshot {
    fn capture(fixture: &ControlledDeliveryFixture) -> Self {
        Self {
            head: fixture.git_line(&["rev-parse", "HEAD"]),
            status: fixture
                .git_output(&["status", "--porcelain=v1", "-z"])
                .stdout,
            source: std::fs::read(fixture.repository_path.join("src/lib.rs"))
                .expect("capture target source"),
            index: fixture.git_output(&["show", ":src/lib.rs"]).stdout,
        }
    }
}

async fn merge_operation(
    fixture: &ControlledDeliveryFixture,
    operation_id: coding_agent_store::DeliveryOperationId,
) -> MergeOperationRecord {
    match fixture
        .handles()
        .store
        .delivery_operation_snapshot(operation_id)
        .await
        .expect("load merge operation")
        .expect("merge operation exists")
    {
        DeliveryOperationSnapshot::Merge(operation) => *operation,
        DeliveryOperationSnapshot::Cleanup(_) => panic!("merge operation resolved as cleanup"),
    }
}

async fn wait_for_merge(
    fixture: &ControlledDeliveryFixture,
    operation_id: coding_agent_store::DeliveryOperationId,
) -> MergeOperationRecord {
    let result = tokio::time::timeout(PRODUCTION_DELIVERY_STAGE_TIMEOUT, async {
        loop {
            let operation = merge_operation(fixture, operation_id).await;
            if operation.state == MergeOperationState::Merged {
                return operation;
            }
            if matches!(
                operation.state,
                MergeOperationState::Conflict
                    | MergeOperationState::Failed
                    | MergeOperationState::ReconciliationRequired
            ) {
                panic!(
                    "real delivery failed in {:?}: {operation:?}",
                    operation.state
                );
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    match result {
        Ok(operation) => operation,
        Err(_) => {
            let operation = merge_operation(fixture, operation_id).await;
            panic!(
                "real delivery did not converge to merged within {PRODUCTION_DELIVERY_STAGE_TIMEOUT:?}: state={:?}; version={}; failure_code={:?}; expected_merge_commit={:?}",
                operation.state,
                operation.version,
                operation.failure_code,
                operation.expected_merge_commit,
            );
        }
    }
}
