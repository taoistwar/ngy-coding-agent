use crate::StoreError;

use super::super::{
    DeliveryCommitMetadata, DeliverySourceRecord, DeliverySourceState, GitCommitOid,
    GitObjectAlgorithm, MergeOperationRecord, MergeOperationState,
};
use super::ownership_invariant;
use crate::delivery::merges::merge_failure_code_is_valid;

pub(super) fn validate_source_current_shape(
    source: &DeliverySourceRecord,
) -> Result<(), StoreError> {
    let algorithm = source.provenance.base_commit.algorithm();
    let oid_shape_is_valid = source.candidate_tree.algorithm() == algorithm
        && source.expected_parent.algorithm() == algorithm
        && source
            .expected_source_commit
            .as_ref()
            .is_none_or(|oid| oid.algorithm() == algorithm);
    let state_shape_is_valid = match source.state {
        DeliverySourceState::ObjectPending => {
            source.expected_source_commit.is_none() && pending_source_failure_is_valid(source)
        }
        DeliverySourceState::CommitPending => {
            source.expected_source_commit.is_some() && pending_source_failure_is_valid(source)
        }
        DeliverySourceState::Committed => {
            source.expected_source_commit.is_some() && source.failure_code.is_none()
        }
        DeliverySourceState::ReconciliationRequired => source
            .failure_code
            .as_ref()
            .is_some_and(|failure| reconciliation_failure_is_allowlisted(failure.as_str())),
    };
    let fields_are_coupled = source.expected_parent == source.provenance.base_commit
        && source_commit_metadata_is_coupled(source);
    require_shape(oid_shape_is_valid && state_shape_is_valid && fields_are_coupled)
}

fn pending_source_failure_is_valid(source: &DeliverySourceRecord) -> bool {
    source
        .failure_code
        .as_ref()
        .is_none_or(|failure| failure.as_str() == "COMMAND_TIMED_OUT")
}

pub(super) fn reconciliation_failure_is_allowlisted(failure: &str) -> bool {
    matches!(
        failure,
        "DELIVERY_SOURCE_INCONSISTENT" | "PROCESS_TREE_CLEANUP_FAILED"
    )
}

pub(super) fn validate_merge_current_shape(
    operation: &MergeOperationRecord,
) -> Result<(), StoreError> {
    let valid = merge_oid_algorithms_match(operation)
        && merge_object_ids_are_non_degenerate(operation)
        && merge_identity_fields_are_coupled(operation)
        && merge_state_fields_are_coupled(operation)
        && merge_abort_fields_are_coupled(operation)
        && merge_conflicts_are_coupled(operation);
    require_shape(valid)
}

fn merge_object_ids_are_non_degenerate(operation: &MergeOperationRecord) -> bool {
    let distinct_parents = operation
        .source_commit
        .as_ref()
        .is_none_or(|source| *source != operation.expected_target_head);
    let merge_is_distinct = operation
        .expected_merge_commit
        .as_ref()
        .is_none_or(|merge| {
            *merge != operation.expected_target_head
                && operation
                    .source_commit
                    .as_ref()
                    .is_none_or(|source| merge != source)
        });
    distinct_parents && merge_is_distinct
}

fn merge_oid_algorithms_match(operation: &MergeOperationRecord) -> bool {
    let algorithm = operation.provenance.base_commit.algorithm();
    operation.candidate_tree.algorithm() == algorithm
        && operation.preflight_source_commit.algorithm() == algorithm
        && operation.expected_target_head.algorithm() == algorithm
        && optional_oid_algorithm(operation.source_commit.as_ref(), algorithm)
        && optional_oid_algorithm(operation.merge_base.as_ref(), algorithm)
        && operation
            .candidate_merge_tree
            .as_ref()
            .is_none_or(|oid| oid.algorithm() == algorithm)
        && optional_oid_algorithm(operation.expected_merge_commit.as_ref(), algorithm)
        && optional_oid_algorithm(operation.abort_merge_head.as_ref(), algorithm)
}

fn merge_identity_fields_are_coupled(operation: &MergeOperationRecord) -> bool {
    let source_link_is_valid = match (
        operation.delivery_source_task_id,
        operation.source_commit.as_ref(),
    ) {
        (None, None) => true,
        (Some(task_id), Some(_)) => task_id == operation.provenance.identity.task_id(),
        _ => false,
    };
    let receipts_are_distinct = operation
        .accept_receipt_id
        .is_none_or(|receipt| receipt != operation.preflight_receipt_id);
    let preflight_source_is_distinct =
        operation.preflight_source_commit != operation.provenance.base_commit;
    source_link_is_valid
        && receipts_are_distinct
        && preflight_source_is_distinct
        && operation.provenance.source_branch != operation.target_branch
}

fn merge_state_fields_are_coupled(operation: &MergeOperationRecord) -> bool {
    use MergeOperationState::{
        AbortPending, Accepted, Conflict, Failed, MergePending, Merged, PreflightPending,
        PreflightReady, ReconciliationRequired, Rejected, Stale, Superseded,
    };

    let early_without_accept = matches!(
        operation.state,
        PreflightPending | PreflightReady | Rejected | Stale | Superseded
    );
    let preflight_result_required = matches!(
        operation.state,
        PreflightReady | Accepted | MergePending | Merged | AbortPending | Conflict
    );
    let metadata_required = matches!(
        operation.state,
        Accepted | MergePending | Merged | AbortPending | Failed
    );
    let source_link_required = matches!(
        operation.state,
        MergePending | Merged | AbortPending | Failed
    );
    let expected_merge_required = matches!(operation.state, MergePending | Merged | AbortPending);
    let metadata_allowed = operation.merge_metadata.is_none()
        || matches!(
            operation.state,
            Accepted
                | MergePending
                | Merged
                | AbortPending
                | Conflict
                | Failed
                | ReconciliationRequired
        );
    let failure_shape_is_valid = merge_failure_code_is_valid(
        operation.state,
        operation
            .failure_code
            .as_ref()
            .map(|failure| failure.as_str()),
    );
    let disposition_shape_is_valid = if operation.state == Merged {
        operation.merged_disposition_task_id == Some(operation.provenance.identity.task_id())
    } else {
        operation.merged_disposition_task_id.is_none()
    };

    (!early_without_accept
        || (operation.accept_receipt_id.is_none()
            && operation.delivery_source_task_id.is_none()
            && operation.source_commit.is_none()))
        && (!preflight_result_required
            || (operation.merge_base.is_some() && operation.candidate_merge_tree.is_some()))
        && (!metadata_required
            || (operation.accept_receipt_id.is_some() && operation.merge_metadata.is_some()))
        && metadata_allowed
        && operation.merge_metadata.as_ref().is_none_or(|metadata| {
            commit_metadata_is_coupled(metadata)
                && metadata.message_bytes
                    == format!(
                        "coding-agent: merge task {} attempt {}\n",
                        operation.provenance.identity.task_id(),
                        operation.provenance.identity.attempt()
                    )
                    .as_bytes()
        })
        && (!source_link_required
            || (operation.delivery_source_task_id.is_some() && operation.source_commit.is_some()))
        && (!expected_merge_required || operation.expected_merge_commit.is_some())
        && failure_shape_is_valid
        && disposition_shape_is_valid
}

fn merge_abort_fields_are_coupled(operation: &MergeOperationRecord) -> bool {
    let presence = [
        operation.abort_child_receipt_id.is_some(),
        operation.abort_merge_head.is_some(),
        operation.abort_index_stages_digest.is_some(),
        operation.abort_worktree_digest.is_some(),
        operation.abort_merge_autostash_proof.is_some(),
    ];
    let group_is_valid = presence.iter().all(|present| !present)
        || (presence.iter().all(|present| *present)
            && operation.abort_merge_autostash_proof.as_deref() == Some("absent"));
    let abort_pending_is_valid = operation.state != MergeOperationState::AbortPending
        || (operation.abort_child_receipt_id.is_some()
            && operation.abort_merge_head.as_ref() == operation.source_commit.as_ref());
    group_is_valid && abort_pending_is_valid
}

fn merge_conflicts_are_coupled(operation: &MergeOperationRecord) -> bool {
    match (operation.state, operation.conflict_path_count) {
        (MergeOperationState::Conflict, Some(count)) => {
            operation.conflicts.len() == usize::from(count)
        }
        (MergeOperationState::Conflict, None) => false,
        (_, None) => operation.conflicts.is_empty(),
        (_, Some(_)) => false,
    }
}

fn commit_metadata_is_coupled(metadata: &DeliveryCommitMetadata) -> bool {
    metadata.author_name == "Coding Agent"
        && metadata.author_email == "coding-agent@localhost"
        && metadata.committer_name == "Coding Agent"
        && metadata.committer_email == "coding-agent@localhost"
        && metadata.author_date_bytes == metadata.committer_date_bytes
        && canonical_git_date(&metadata.author_date_bytes)
        && metadata.message_template_version == 1
        && (1..=512).contains(&metadata.message_bytes.len())
}

fn source_commit_metadata_is_coupled(source: &DeliverySourceRecord) -> bool {
    let expected_date = format!(
        "{} +0000",
        source
            .created_at
            .as_utc()
            .as_offset_date_time()
            .unix_timestamp()
    );
    commit_metadata_is_coupled(&source.commit_metadata)
        && source.commit_metadata.author_date_bytes == expected_date
        && source.commit_metadata.committer_date_bytes == expected_date
        && source.commit_metadata.message_bytes
            == format!(
                "coding-agent: deliver task {} attempt {}\n",
                source.provenance.identity.task_id(),
                source.provenance.identity.attempt()
            )
            .as_bytes()
}

fn canonical_git_date(value: &str) -> bool {
    let Some(epoch) = value.strip_suffix(" +0000") else {
        return false;
    };
    (7..=64).contains(&value.len())
        && epoch
            .parse::<i64>()
            .is_ok_and(|parsed| parsed.to_string() == epoch)
}

fn optional_oid_algorithm(oid: Option<&GitCommitOid>, algorithm: GitObjectAlgorithm) -> bool {
    oid.is_none_or(|oid| oid.algorithm() == algorithm)
}

fn require_shape(valid: bool) -> Result<(), StoreError> {
    if valid {
        Ok(())
    } else {
        Err(ownership_invariant())
    }
}
