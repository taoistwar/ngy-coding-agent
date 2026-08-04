use std::collections::HashSet;
use std::str::FromStr;

use coding_agent_domain::{ClientRequestId, TaskId};
use coding_agent_store::{
    AcceptMergeCommandRequest, DeleteBranchCommandRequest, DeliveryCommandId, DeliveryOperationId,
    DeliveryVersion, GitBranchRef, GitCommitOid, PreflightCommandRequest,
    RemoveWorktreeCommandRequest, Sha256Digest,
};
use sha2::{Digest, Sha256};

const TASK_ID: &str = "22222222-2222-4222-8222-222222222222";
const PREFLIGHT_OPERATION_ID: &str = "44444444-4444-4444-8444-444444444444";
const TARGET_BRANCH: &str = "refs/heads/main";
const TARGET_HEAD: &str = "dddddddddddddddddddddddddddddddddddddddd";
const SOURCE_REF: &str = "refs/heads/codex/task";
const SOURCE_OID: &str = "1111111111111111111111111111111111111111";
const WORKSPACE_FINGERPRINT: &str =
    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

#[test]
fn canonical_request_hash_vectors_lock_all_tlv_frames_and_field_order() {
    let client_request_id =
        ClientRequestId::from_str("33333333-3333-4333-8333-333333333333").unwrap();
    let task_id = TaskId::from_str(TASK_ID).unwrap();
    let operation_id = DeliveryOperationId::from_str(PREFLIGHT_OPERATION_ID).unwrap();
    let target_branch = GitBranchRef::from_str(TARGET_BRANCH).unwrap();
    let target_head = GitCommitOid::from_str(TARGET_HEAD).unwrap();
    let source_ref = GitBranchRef::from_str(SOURCE_REF).unwrap();
    let source_oid = GitCommitOid::from_str(SOURCE_OID).unwrap();
    let fingerprint = Sha256Digest::from_str(WORKSPACE_FINGERPRINT).unwrap();

    let preflight = PreflightCommandRequest::try_new(
        client_request_id,
        task_id,
        target_branch.clone(),
        target_head.clone(),
    )
    .unwrap();
    let accept = AcceptMergeCommandRequest::try_new(
        client_request_id,
        task_id,
        operation_id,
        DeliveryVersion::try_new(2).unwrap(),
        7,
        fingerprint,
        target_branch.clone(),
        target_head.clone(),
    )
    .unwrap();
    let remove = RemoveWorktreeCommandRequest::try_new(
        client_request_id,
        task_id,
        DeliveryVersion::initial(),
        operation_id,
        source_ref.clone(),
        source_oid.clone(),
    )
    .unwrap();
    let delete = DeleteBranchCommandRequest::try_new(
        client_request_id,
        task_id,
        DeliveryVersion::initial(),
        operation_id,
        source_ref,
        source_oid,
        target_branch,
        target_head,
    )
    .unwrap();

    assert_eq!(
        preflight.client_request_id().to_string(),
        client_request_id.to_string()
    );
    assert_eq!(preflight.task_id(), task_id);
    assert_eq!(preflight.target_branch().as_str(), TARGET_BRANCH);
    assert_eq!(preflight.expected_target_head().as_str(), TARGET_HEAD);
    assert_eq!(accept.preflight_operation_id(), operation_id);
    assert_eq!(accept.expected_operation_version().get(), 2);
    assert_eq!(accept.expected_review_generation(), 7);
    assert_eq!(
        accept.expected_workspace_fingerprint().as_str(),
        WORKSPACE_FINGERPRINT
    );
    assert_eq!(accept.target_branch().as_str(), TARGET_BRANCH);
    assert_eq!(accept.expected_target_head().as_str(), TARGET_HEAD);
    assert_eq!(remove.expected_disposition_version().get(), 1);
    assert_eq!(remove.expected_merge_operation_id(), operation_id);
    assert_eq!(remove.expected_source_ref().as_str(), SOURCE_REF);
    assert_eq!(remove.expected_source_oid().as_str(), SOURCE_OID);
    assert_eq!(delete.expected_disposition_version().get(), 1);
    assert_eq!(delete.expected_merge_operation_id(), operation_id);
    assert_eq!(delete.expected_source_ref().as_str(), SOURCE_REF);
    assert_eq!(delete.expected_source_oid().as_str(), SOURCE_OID);
    assert_eq!(delete.target_branch().as_str(), TARGET_BRANCH);
    assert_eq!(delete.target_head().as_str(), TARGET_HEAD);

    let task_uuid = task_id.as_uuid();
    let operation_uuid = operation_id.as_uuid();
    let version_one = 1_u64.to_be_bytes();
    let version_two = 2_u64.to_be_bytes();
    let generation = 7_u64.to_be_bytes();
    let expected = [
        reference_command_hash(
            "preflight",
            &[
                ("task_id", task_uuid.as_bytes()),
                ("target_branch", TARGET_BRANCH.as_bytes()),
                ("expected_target_head", TARGET_HEAD.as_bytes()),
            ],
        ),
        reference_command_hash(
            "accept_merge",
            &[
                ("task_id", task_uuid.as_bytes()),
                ("preflight_operation_id", operation_uuid.as_bytes()),
                ("expected_operation_version", &version_two),
                ("expected_review_generation", &generation),
                (
                    "expected_workspace_fingerprint",
                    WORKSPACE_FINGERPRINT.as_bytes(),
                ),
                ("target_branch", TARGET_BRANCH.as_bytes()),
                ("expected_target_head", TARGET_HEAD.as_bytes()),
            ],
        ),
        reference_command_hash(
            "remove_worktree",
            &[
                ("task_id", task_uuid.as_bytes()),
                ("expected_disposition_version", &version_one),
                ("expected_merge_operation_id", operation_uuid.as_bytes()),
                ("expected_source_ref", SOURCE_REF.as_bytes()),
                ("expected_source_oid", SOURCE_OID.as_bytes()),
            ],
        ),
        reference_command_hash(
            "delete_branch",
            &[
                ("task_id", task_uuid.as_bytes()),
                ("expected_disposition_version", &version_one),
                ("expected_merge_operation_id", operation_uuid.as_bytes()),
                ("expected_source_ref", SOURCE_REF.as_bytes()),
                ("expected_source_oid", SOURCE_OID.as_bytes()),
                ("target_branch", TARGET_BRANCH.as_bytes()),
                ("target_head", TARGET_HEAD.as_bytes()),
            ],
        ),
    ];
    let golden = [
        "dc9aae680e660af1d5616e7990f746bbac63b61b9cc03a6a96ecb3fd8aab7434",
        "b0de4a22b45c42cf80b93c2799f8ae1dc25c00f8e7c699125ff9de2f03170440",
        "81d30b3abb2a21e655f75014d8902a9b7a5f70dbc2058be422a423ebd17e767d",
        "04cd953df9bfa4e2aa4c5627f4865c410bcaa2d745d5face8344e73064fad957",
    ];
    let actual = [
        preflight.canonical_request_hash(),
        accept.canonical_request_hash(),
        remove.canonical_request_hash(),
        delete.canonical_request_hash(),
    ];
    for ((actual, expected), golden) in actual.iter().zip(&expected).zip(golden) {
        assert_eq!(expected, golden);
        assert_eq!(actual.as_str(), expected);
    }
    assert_eq!(
        actual
            .iter()
            .map(Sha256Digest::as_str)
            .collect::<HashSet<_>>()
            .len(),
        4
    );
}

#[test]
fn preflight_hash_matches_an_independent_tlv_reference_encoder() {
    let client_request_id = ClientRequestId::new();
    let task_id = TaskId::from_str(TASK_ID).unwrap();
    let branch = GitBranchRef::from_str(TARGET_BRANCH).unwrap();
    let head = GitCommitOid::from_str(TARGET_HEAD).unwrap();
    let request =
        PreflightCommandRequest::try_new(client_request_id, task_id, branch, head).unwrap();
    let expected = reference_command_hash(
        "preflight",
        &[
            ("task_id", task_id.as_uuid().as_bytes()),
            ("target_branch", TARGET_BRANCH.as_bytes()),
            ("expected_target_head", TARGET_HEAD.as_bytes()),
        ],
    );

    assert_eq!(request.canonical_request_hash().as_str(), expected);
}

#[test]
fn every_accept_merge_safety_field_changes_the_hash() {
    let client_request_id = ClientRequestId::new();
    let task_id = TaskId::from_str(TASK_ID).unwrap();
    let operation_id = DeliveryOperationId::from_str(PREFLIGHT_OPERATION_ID).unwrap();
    let version = DeliveryVersion::try_new(2).unwrap();
    let fingerprint = Sha256Digest::from_str(WORKSPACE_FINGERPRINT).unwrap();
    let branch = GitBranchRef::from_str(TARGET_BRANCH).unwrap();
    let head = GitCommitOid::from_str(TARGET_HEAD).unwrap();
    let build = |client, task, operation, version, generation, fingerprint, branch, head| {
        AcceptMergeCommandRequest::try_new(
            client,
            task,
            operation,
            version,
            generation,
            fingerprint,
            branch,
            head,
        )
        .unwrap()
        .canonical_request_hash()
    };
    let baseline = build(
        client_request_id,
        task_id,
        operation_id,
        version,
        7,
        fingerprint.clone(),
        branch.clone(),
        head.clone(),
    );
    assert_eq!(
        baseline,
        build(
            ClientRequestId::new(),
            task_id,
            operation_id,
            version,
            7,
            fingerprint.clone(),
            branch.clone(),
            head.clone(),
        )
    );
    let changed = [
        build(
            client_request_id,
            TaskId::new(),
            operation_id,
            version,
            7,
            fingerprint.clone(),
            branch.clone(),
            head.clone(),
        ),
        build(
            client_request_id,
            task_id,
            DeliveryOperationId::new(),
            version,
            7,
            fingerprint.clone(),
            branch.clone(),
            head.clone(),
        ),
        build(
            client_request_id,
            task_id,
            operation_id,
            DeliveryVersion::try_new(3).unwrap(),
            7,
            fingerprint.clone(),
            branch.clone(),
            head.clone(),
        ),
        build(
            client_request_id,
            task_id,
            operation_id,
            version,
            8,
            fingerprint.clone(),
            branch.clone(),
            head.clone(),
        ),
        build(
            client_request_id,
            task_id,
            operation_id,
            version,
            7,
            Sha256Digest::from_str(&"b".repeat(64)).unwrap(),
            branch.clone(),
            head.clone(),
        ),
        build(
            client_request_id,
            task_id,
            operation_id,
            version,
            7,
            fingerprint.clone(),
            GitBranchRef::from_str("refs/heads/release").unwrap(),
            head.clone(),
        ),
        build(
            client_request_id,
            task_id,
            operation_id,
            version,
            7,
            fingerprint,
            branch,
            GitCommitOid::from_str(&"e".repeat(40)).unwrap(),
        ),
    ];
    assert!(changed.iter().all(|hash| hash != &baseline));
}

#[test]
fn accept_merge_rejects_a_version_that_cannot_advance_before_hash_or_receipt_lookup() {
    let result = AcceptMergeCommandRequest::try_new(
        ClientRequestId::new(),
        TaskId::from_str(TASK_ID).unwrap(),
        DeliveryOperationId::from_str(PREFLIGHT_OPERATION_ID).unwrap(),
        DeliveryVersion::try_new(DeliveryVersion::MAX).unwrap(),
        7,
        Sha256Digest::from_str(WORKSPACE_FINGERPRINT).unwrap(),
        GitBranchRef::from_str(TARGET_BRANCH).unwrap(),
        GitCommitOid::from_str(TARGET_HEAD).unwrap(),
    );

    assert!(result.is_err());
}

#[test]
fn every_cleanup_safety_field_changes_the_hash() {
    let client_request_id = ClientRequestId::new();
    let task_id = TaskId::from_str(TASK_ID).unwrap();
    let operation_id = DeliveryOperationId::from_str(PREFLIGHT_OPERATION_ID).unwrap();
    let version = DeliveryVersion::initial();
    let source_ref = GitBranchRef::from_str(SOURCE_REF).unwrap();
    let source_oid = GitCommitOid::from_str(SOURCE_OID).unwrap();
    let target_branch = GitBranchRef::from_str(TARGET_BRANCH).unwrap();
    let target_head = GitCommitOid::from_str(TARGET_HEAD).unwrap();
    let remove = |task, version, operation, source_ref, source_oid| {
        RemoveWorktreeCommandRequest::try_new(
            client_request_id,
            task,
            version,
            operation,
            source_ref,
            source_oid,
        )
        .unwrap()
        .canonical_request_hash()
    };
    let delete = |task, version, operation, source_ref, source_oid, target_branch, target_head| {
        DeleteBranchCommandRequest::try_new(
            client_request_id,
            task,
            version,
            operation,
            source_ref,
            source_oid,
            target_branch,
            target_head,
        )
        .unwrap()
        .canonical_request_hash()
    };
    let remove_baseline = remove(
        task_id,
        version,
        operation_id,
        source_ref.clone(),
        source_oid.clone(),
    );
    let delete_baseline = delete(
        task_id,
        version,
        operation_id,
        source_ref.clone(),
        source_oid.clone(),
        target_branch.clone(),
        target_head.clone(),
    );
    let other_ref = GitBranchRef::from_str("refs/heads/codex/other").unwrap();
    let other_oid = GitCommitOid::from_str(&"2".repeat(40)).unwrap();
    let common_remove_changes = [
        remove(
            TaskId::new(),
            version,
            operation_id,
            source_ref.clone(),
            source_oid.clone(),
        ),
        remove(
            task_id,
            DeliveryVersion::try_new(2).unwrap(),
            operation_id,
            source_ref.clone(),
            source_oid.clone(),
        ),
        remove(
            task_id,
            version,
            DeliveryOperationId::new(),
            source_ref.clone(),
            source_oid.clone(),
        ),
        remove(
            task_id,
            version,
            operation_id,
            other_ref.clone(),
            source_oid.clone(),
        ),
        remove(
            task_id,
            version,
            operation_id,
            source_ref.clone(),
            other_oid.clone(),
        ),
    ];
    assert!(
        common_remove_changes
            .iter()
            .all(|hash| hash != &remove_baseline)
    );

    let delete_changes = [
        delete(
            TaskId::new(),
            version,
            operation_id,
            source_ref.clone(),
            source_oid.clone(),
            target_branch.clone(),
            target_head.clone(),
        ),
        delete(
            task_id,
            DeliveryVersion::try_new(2).unwrap(),
            operation_id,
            source_ref.clone(),
            source_oid.clone(),
            target_branch.clone(),
            target_head.clone(),
        ),
        delete(
            task_id,
            version,
            DeliveryOperationId::new(),
            source_ref.clone(),
            source_oid.clone(),
            target_branch.clone(),
            target_head.clone(),
        ),
        delete(
            task_id,
            version,
            operation_id,
            other_ref,
            source_oid.clone(),
            target_branch.clone(),
            target_head.clone(),
        ),
        delete(
            task_id,
            version,
            operation_id,
            source_ref.clone(),
            other_oid,
            target_branch.clone(),
            target_head.clone(),
        ),
        delete(
            task_id,
            version,
            operation_id,
            source_ref.clone(),
            source_oid.clone(),
            GitBranchRef::from_str("refs/heads/release").unwrap(),
            target_head,
        ),
        delete(
            task_id,
            version,
            operation_id,
            source_ref,
            source_oid,
            target_branch,
            GitCommitOid::from_str(&"e".repeat(40)).unwrap(),
        ),
    ];
    assert!(delete_changes.iter().all(|hash| hash != &delete_baseline));
    assert_ne!(remove_baseline, delete_baseline);
}

#[test]
fn request_serde_rejects_unknown_duplicate_nil_and_noncanonical_values() {
    let valid = format!(
        "{{\"client_request_id\":\"33333333-3333-4333-8333-333333333333\",\
          \"task_id\":\"{TASK_ID}\",\"target_branch\":\"{TARGET_BRANCH}\",\
          \"expected_target_head\":\"{TARGET_HEAD}\"}}"
    );
    let invalid = [
        valid.replace("}", ",\"unknown\":true}"),
        valid.replace(
            "\"task_id\":",
            &format!("\"task_id\":\"{TASK_ID}\",\"task_id\":"),
        ),
        valid.replace(
            "33333333-3333-4333-8333-333333333333",
            "00000000-0000-0000-0000-000000000000",
        ),
        valid.replace(
            "33333333-3333-4333-8333-333333333333",
            "33333333333343338333333333333333",
        ),
        valid.replace(TASK_ID, "22222222222242228222222222222222"),
        valid.replace(TARGET_BRANCH, "refs/heads/-invalid"),
        valid.replace(TARGET_HEAD, &"D".repeat(40)),
        valid.replace(TARGET_HEAD, &"0".repeat(40)),
    ];
    for json in invalid {
        assert!(serde_json::from_str::<PreflightCommandRequest>(&json).is_err());
    }
    for id in [
        "00000000-0000-0000-0000-000000000000",
        "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA",
        "aaaaaaaaaaaa4aaa8aaaaaaaaaaaaaaa",
    ] {
        assert!(DeliveryCommandId::from_str(id).is_err());
    }
}

#[test]
fn canonical_refs_are_hashed_as_exact_utf8_bytes() {
    let task_id = TaskId::from_str(TASK_ID).unwrap();
    let head = GitCommitOid::from_str(TARGET_HEAD).unwrap();
    let composed = PreflightCommandRequest::try_new(
        ClientRequestId::new(),
        task_id,
        GitBranchRef::from_str("refs/heads/caf\u{00e9}").unwrap(),
        head.clone(),
    )
    .unwrap();
    let decomposed = PreflightCommandRequest::try_new(
        ClientRequestId::new(),
        task_id,
        GitBranchRef::from_str("refs/heads/cafe\u{0301}").unwrap(),
        head,
    )
    .unwrap();

    assert_ne!(
        composed.canonical_request_hash(),
        decomposed.canonical_request_hash()
    );
}

#[test]
fn typed_json_semantics_not_raw_json_bytes_drive_the_hash() {
    let compact = format!(
        "{{\"client_request_id\":\"33333333-3333-4333-8333-333333333333\",\
          \"task_id\":\"{TASK_ID}\",\"target_branch\":\"{TARGET_BRANCH}\",\
          \"expected_target_head\":\"{TARGET_HEAD}\"}}"
    );
    let reordered = format!(
        "{{\n  \"expected_target_head\": \"{TARGET_HEAD}\",\n  \
          \"target_branch\": \"{TARGET_BRANCH}\",\n  \"task_id\": \"{TASK_ID}\",\n  \
          \"client_request_id\": \"33333333-3333-4333-8333-333333333333\"\n}}"
    );
    let first: PreflightCommandRequest = serde_json::from_str(&compact).unwrap();
    let second: PreflightCommandRequest = serde_json::from_str(&reordered).unwrap();

    assert_eq!(
        first.canonical_request_hash(),
        second.canonical_request_hash()
    );
}

#[test]
fn client_request_id_is_excluded_but_each_safety_field_is_bound() {
    let task_id = TaskId::from_str(TASK_ID).unwrap();
    let branch = GitBranchRef::from_str(TARGET_BRANCH).unwrap();
    let head = GitCommitOid::from_str(TARGET_HEAD).unwrap();
    let request = |client_request_id| {
        PreflightCommandRequest::try_new(client_request_id, task_id, branch.clone(), head.clone())
            .unwrap()
    };
    let first = request(ClientRequestId::new());
    let second = request(ClientRequestId::new());
    assert_eq!(
        first.canonical_request_hash(),
        second.canonical_request_hash()
    );

    let other_task = PreflightCommandRequest::try_new(
        ClientRequestId::new(),
        TaskId::new(),
        branch.clone(),
        head.clone(),
    )
    .unwrap();
    let other_branch = PreflightCommandRequest::try_new(
        ClientRequestId::new(),
        task_id,
        GitBranchRef::from_str("refs/heads/release").unwrap(),
        head.clone(),
    )
    .unwrap();
    let other_head = PreflightCommandRequest::try_new(
        ClientRequestId::new(),
        task_id,
        branch,
        GitCommitOid::from_str("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee").unwrap(),
    )
    .unwrap();
    for changed in [other_task, other_branch, other_head] {
        assert_ne!(
            first.canonical_request_hash(),
            changed.canonical_request_hash()
        );
    }
}

fn reference_hash(fields: &[(&str, &[u8])]) -> String {
    let mut bytes = Vec::new();
    for (tag, value) in fields {
        bytes.extend_from_slice(&u16::try_from(tag.len()).unwrap().to_be_bytes());
        bytes.extend_from_slice(tag.as_bytes());
        bytes.extend_from_slice(&u64::try_from(value.len()).unwrap().to_be_bytes());
        bytes.extend_from_slice(value);
    }
    format!("{:x}", Sha256::digest(bytes))
}

fn reference_command_hash(action: &str, fields: &[(&str, &[u8])]) -> String {
    let version = 1_u16.to_be_bytes();
    let mut frames = vec![
        (
            "domain",
            b"coding-agent-delivery-command-request".as_slice(),
        ),
        ("version", version.as_slice()),
        ("action", action.as_bytes()),
    ];
    frames.extend_from_slice(fields);
    reference_hash(&frames)
}
