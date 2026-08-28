use coding_agent_domain::{ClientRequestId, TaskId};
use coding_agent_store::{
    AcceptMergeCommandRequest, AdvanceDeliverySourceObjectRequest, BeginMergeAbortRequest,
    BindMergePreflightInputsRequest, CommitDeliverySourceRequest, CompleteBranchCleanupRequest,
    CompleteMergeAbortRequest, CompleteMergeRequest, CompleteWorktreeCleanupRequest,
    CreateDeliverySourceRequest, CreatePreflightRequest, DeleteBranchCommandRequest,
    DeliveryMutationEntityId, DeliveryMutationEntityKind, DeliveryMutationKind,
    DeliveryMutationRequest, DeliveryOperationId, DeliveryVersion, EnterMergePendingRequest,
    EnterWorktreeRemovePendingRequest, FailUnboundMergePreflightRequest, MarkPreflightStaleRequest,
    PreflightStaleReason, ReconcileBranchCleanupRequest, ReconcileDeliverySourceRequest,
    ReconcileMergeRequest, ReconcileWorktreeCleanupRequest, RecordBranchCleanupFailureRequest,
    RecordDeliverySourceRetryRequest, RecordMergeKnownFailureRequest,
    RecordMergePreflightResultRequest, RecordWorktreeCleanupFailureRequest,
    RecordWorktreeUnlockedRequest, RefreshBranchCleanupTargetRequest, RemoveWorktreeCommandRequest,
};

const OID: &str = "1111111111111111111111111111111111111111";
const DIGEST: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

fn assert_mutation_request<Request: DeliveryMutationRequest>() {}

#[test]
fn all_twenty_eight_store_delivery_mutation_requests_have_sealed_keys() {
    assert_mutation_request::<CreateDeliverySourceRequest>();
    assert_mutation_request::<AdvanceDeliverySourceObjectRequest>();
    assert_mutation_request::<CommitDeliverySourceRequest>();
    assert_mutation_request::<RecordDeliverySourceRetryRequest>();
    assert_mutation_request::<ReconcileDeliverySourceRequest>();
    assert_mutation_request::<CreatePreflightRequest>();
    assert_mutation_request::<BindMergePreflightInputsRequest>();
    assert_mutation_request::<FailUnboundMergePreflightRequest>();
    assert_mutation_request::<MarkPreflightStaleRequest>();
    assert_mutation_request::<RecordMergePreflightResultRequest>();
    assert_mutation_request::<AcceptMergeCommandRequest>();
    assert_mutation_request::<EnterMergePendingRequest>();
    assert_mutation_request::<CompleteMergeRequest>();
    assert_mutation_request::<BeginMergeAbortRequest>();
    assert_mutation_request::<CompleteMergeAbortRequest>();
    assert_mutation_request::<RecordMergeKnownFailureRequest>();
    assert_mutation_request::<ReconcileMergeRequest>();
    assert_mutation_request::<RemoveWorktreeCommandRequest>();
    assert_mutation_request::<RecordWorktreeUnlockedRequest>();
    assert_mutation_request::<EnterWorktreeRemovePendingRequest>();
    assert_mutation_request::<CompleteWorktreeCleanupRequest>();
    assert_mutation_request::<RecordWorktreeCleanupFailureRequest>();
    assert_mutation_request::<ReconcileWorktreeCleanupRequest>();
    assert_mutation_request::<DeleteBranchCommandRequest>();
    assert_mutation_request::<RefreshBranchCleanupTargetRequest>();
    assert_mutation_request::<CompleteBranchCleanupRequest>();
    assert_mutation_request::<RecordBranchCleanupFailureRequest>();
    assert_mutation_request::<ReconcileBranchCleanupRequest>();
}

#[test]
fn receipt_backed_and_entity_only_keys_expose_exact_read_only_identity() {
    let task_id = TaskId::new();
    let operation_id = DeliveryOperationId::new();
    let expected_version = DeliveryVersion::try_new(3).unwrap();
    let accept = AcceptMergeCommandRequest::try_new(
        ClientRequestId::new(),
        task_id,
        operation_id,
        expected_version,
        1,
        DIGEST.parse().unwrap(),
        "refs/heads/main".parse().unwrap(),
        OID.parse().unwrap(),
    )
    .unwrap();

    let accept_key = accept.delivery_mutation_key();
    assert_eq!(accept_key.kind(), DeliveryMutationKind::AcceptMerge);
    assert_eq!(accept_key.task_id(), task_id);
    assert_eq!(accept_key.entities().len(), 1);
    assert_eq!(
        accept_key.entities()[0].kind(),
        DeliveryMutationEntityKind::MergeOperation
    );
    assert_eq!(
        accept_key.entities()[0].id(),
        Some(DeliveryMutationEntityId::Operation(operation_id))
    );
    assert_eq!(
        accept_key.entities()[0].expected_version(),
        Some(expected_version)
    );
    let receipt = accept_key.receipt().unwrap();
    assert_eq!(receipt.client_request_id(), accept.client_request_id());
    assert_eq!(receipt.operation_anchor(), Some(operation_id));
    assert_eq!(
        receipt.expected_accepted_version(),
        DeliveryVersion::try_new(4).unwrap()
    );
    assert_eq!(
        receipt.canonical_request_hash(),
        &accept.canonical_request_hash()
    );
    let key_debug = format!("{accept_key:?}");
    assert!(key_debug.contains("<redacted>"));
    assert!(!key_debug.contains(accept.canonical_request_hash().as_str()));

    let source_key = CreateDeliverySourceRequest::try_new(accept)
        .unwrap()
        .delivery_mutation_key();
    assert_eq!(
        source_key.kind(),
        DeliveryMutationKind::CreateDeliverySource
    );
    assert_eq!(source_key.entities().len(), 2);
    assert_eq!(
        source_key.entities()[0].id(),
        Some(DeliveryMutationEntityId::Task(task_id))
    );
    assert_eq!(source_key.entities()[0].expected_version(), None);
    assert_eq!(
        source_key.entities()[1].expected_version(),
        Some(DeliveryVersion::try_new(4).unwrap())
    );

    let stale_key = MarkPreflightStaleRequest::try_new(
        task_id,
        operation_id,
        expected_version,
        PreflightStaleReason::EvidenceStale,
    )
    .unwrap()
    .delivery_mutation_key();
    assert_eq!(
        stale_key.kind(),
        DeliveryMutationKind::MarkMergePreflightStale
    );
    assert_eq!(stale_key.task_id(), task_id);
    assert_eq!(stale_key.receipt(), None);
    assert_eq!(
        stale_key.entities()[0].expected_version(),
        Some(expected_version)
    );

    let bind_key = BindMergePreflightInputsRequest::try_new(
        task_id,
        operation_id,
        DeliveryVersion::initial(),
        OID.parse().unwrap(),
        OID.parse().unwrap(),
    )
    .unwrap()
    .delivery_mutation_key();
    assert_eq!(
        bind_key.kind(),
        DeliveryMutationKind::BindMergePreflightInputs
    );
    assert_eq!(bind_key.task_id(), task_id);
    assert_eq!(bind_key.receipt(), None);
    assert_eq!(
        bind_key.entities()[0].expected_version(),
        Some(DeliveryVersion::initial())
    );
}

#[test]
fn cleanup_acceptance_key_binds_pending_operation_disposition_and_receipt() {
    let task_id = TaskId::new();
    let merge_operation_id = DeliveryOperationId::new();
    let expected_disposition_version = DeliveryVersion::try_new(4).unwrap();
    let request = RemoveWorktreeCommandRequest::try_new(
        ClientRequestId::new(),
        task_id,
        expected_disposition_version,
        merge_operation_id,
        "refs/heads/codex/task".parse().unwrap(),
        OID.parse().unwrap(),
    )
    .unwrap();

    let key = request.delivery_mutation_key();
    assert_eq!(key.kind(), DeliveryMutationKind::AcceptWorktreeCleanup);
    assert_eq!(key.task_id(), task_id);
    assert_eq!(key.entities().len(), 2);
    assert_eq!(
        key.entities()[0].kind(),
        DeliveryMutationEntityKind::CleanupOperation
    );
    assert_eq!(key.entities()[0].id(), None);
    assert_eq!(
        key.entities()[1].kind(),
        DeliveryMutationEntityKind::WorktreeDisposition
    );
    assert_eq!(
        key.entities()[1].expected_version(),
        Some(expected_disposition_version)
    );
    assert_eq!(
        key.receipt().unwrap().operation_anchor(),
        Some(merge_operation_id)
    );
}
