mod support;

use coding_agent_domain::{CanonicalPath, ClientRequestId, NewTask};
use coding_agent_store::{
    AttemptArtifactIdentity, AttemptArtifactState, ReserveAttemptArtifact,
    ReserveAttemptArtifactOutcome, StoreError, UpdateAttemptArtifactOutcome,
};

const BASE_COMMIT: &str = "0123456789abcdef0123456789abcdef01234567";

#[tokio::test]
async fn reservation_and_terminal_updates_are_idempotent_and_one_way() {
    let store = support::seeded_store().await;
    let task = support::queued_task(&store).await;
    let repository = store.list_repositories().await.unwrap().remove(0);
    let identity = AttemptArtifactIdentity {
        task_id: task.id,
        repository_id: repository.id,
        attempt: task.attempt,
    };
    let input = reservation(
        identity,
        "codex/task-one-attempt-1",
        canonical_artifact_path(&repository, "one"),
    );

    let created = store.reserve_attempt_artifact(input.clone()).await.unwrap();
    let ReserveAttemptArtifactOutcome::Created(created_artifact) = created else {
        panic!("first reservation must create");
    };
    assert_eq!(created_artifact.state, AttemptArtifactState::Reserved);
    assert_eq!(created_artifact.identity, identity);
    assert!(created_artifact.failure_code.is_none());

    let repeated = store.reserve_attempt_artifact(input.clone()).await.unwrap();
    let ReserveAttemptArtifactOutcome::Existing(repeated_artifact) = repeated else {
        panic!("identical reservation must be idempotent");
    };
    assert_eq!(repeated_artifact, created_artifact);

    let mut changed = input.clone();
    changed.base_commit = "abcdef0123456789abcdef0123456789abcdef01".to_owned();
    assert!(matches!(
        store.reserve_attempt_artifact(changed).await.unwrap_err(),
        StoreError::ArtifactIdentityConflict
    ));

    let ready = store.mark_attempt_artifact_ready(identity).await.unwrap();
    let UpdateAttemptArtifactOutcome::Applied(ready_artifact) = ready else {
        panic!("reserved -> ready must apply");
    };
    assert_eq!(ready_artifact.state, AttemptArtifactState::Ready);
    assert!(ready_artifact.updated_at >= ready_artifact.created_at);

    let repeated_ready = store.mark_attempt_artifact_ready(identity).await.unwrap();
    assert!(matches!(
        repeated_ready,
        UpdateAttemptArtifactOutcome::Unchanged(ref artifact) if artifact == &ready_artifact
    ));
    assert!(matches!(
        store.reserve_attempt_artifact(input).await.unwrap(),
        ReserveAttemptArtifactOutcome::Existing(ref artifact) if artifact == &ready_artifact
    ));
    assert!(matches!(
        store
            .mark_attempt_artifact_inconsistent(identity, "LATE_FAILURE")
            .await
            .unwrap_err(),
        StoreError::ArtifactStateConflict
    ));
    assert!(
        store
            .list_reserved_attempt_artifacts()
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        store.load_attempt_artifact(task.id).await.unwrap(),
        Some(ready_artifact)
    );
}

#[tokio::test]
async fn branch_path_and_task_identity_conflicts_are_rejected_atomically() {
    let store = support::seeded_store().await;
    let repository = store.list_repositories().await.unwrap().remove(0);
    let first = support::queued_task(&store).await;
    let second = store
        .create_task(
            NewTask::try_new(ClientRequestId::new(), repository.id, "second task").unwrap(),
        )
        .await
        .unwrap()
        .task()
        .clone();
    let first_identity = AttemptArtifactIdentity {
        task_id: first.id,
        repository_id: repository.id,
        attempt: first.attempt,
    };
    let second_identity = AttemptArtifactIdentity {
        task_id: second.id,
        repository_id: repository.id,
        attempt: second.attempt,
    };
    let first_path = canonical_artifact_path(&repository, "first");
    store
        .reserve_attempt_artifact(reservation(
            first_identity,
            "codex/shared-branch",
            first_path.clone(),
        ))
        .await
        .unwrap();

    for conflicting in [
        reservation(
            second_identity,
            "codex/shared-branch",
            canonical_artifact_path(&repository, "second"),
        ),
        reservation(second_identity, "codex/second-branch", first_path),
    ] {
        assert!(matches!(
            store
                .reserve_attempt_artifact(conflicting)
                .await
                .unwrap_err(),
            StoreError::ArtifactIdentityConflict
        ));
    }
    assert!(
        store
            .load_attempt_artifact(second.id)
            .await
            .unwrap()
            .is_none()
    );

    let wrong_attempt = ReserveAttemptArtifact {
        identity: AttemptArtifactIdentity {
            attempt: second.attempt + 1,
            ..second_identity
        },
        ..reservation(
            second_identity,
            "codex/unique-branch",
            canonical_artifact_path(&repository, "unique"),
        )
    };
    assert!(matches!(
        store
            .reserve_attempt_artifact(wrong_attempt)
            .await
            .unwrap_err(),
        StoreError::ArtifactIdentityConflict
    ));
}

#[tokio::test]
async fn inconsistent_is_durable_idempotent_and_preserves_failure_identity() {
    let store = support::seeded_store().await;
    let repository = store.list_repositories().await.unwrap().remove(0);
    let task = support::queued_task(&store).await;
    let identity = AttemptArtifactIdentity {
        task_id: task.id,
        repository_id: repository.id,
        attempt: task.attempt,
    };
    store
        .reserve_attempt_artifact(reservation(
            identity,
            "codex/inconsistent",
            canonical_artifact_path(&repository, "inconsistent"),
        ))
        .await
        .unwrap();

    let applied = store
        .mark_attempt_artifact_inconsistent(identity, "WORKTREE_STATE_INCONSISTENT")
        .await
        .unwrap();
    let UpdateAttemptArtifactOutcome::Applied(artifact) = applied else {
        panic!("reserved -> inconsistent must apply");
    };
    assert_eq!(artifact.state, AttemptArtifactState::Inconsistent);
    assert_eq!(
        artifact.failure_code.as_deref(),
        Some("WORKTREE_STATE_INCONSISTENT")
    );

    assert!(matches!(
        store
            .mark_attempt_artifact_inconsistent(identity, "WORKTREE_STATE_INCONSISTENT")
            .await
            .unwrap(),
        UpdateAttemptArtifactOutcome::Unchanged(ref current) if current == &artifact
    ));
    assert!(matches!(
        store
            .mark_attempt_artifact_inconsistent(identity, "DIFFERENT_FAILURE")
            .await
            .unwrap_err(),
        StoreError::ArtifactStateConflict
    ));
    assert!(matches!(
        store
            .mark_attempt_artifact_ready(identity)
            .await
            .unwrap_err(),
        StoreError::ArtifactStateConflict
    ));
}

fn reservation(
    identity: AttemptArtifactIdentity,
    branch_name: &str,
    worktree_path: CanonicalPath,
) -> ReserveAttemptArtifact {
    ReserveAttemptArtifact {
        identity,
        base_commit: BASE_COMMIT.to_owned(),
        branch_name: branch_name.to_owned(),
        worktree_path,
    }
}

fn canonical_artifact_path(
    repository: &coding_agent_domain::Repository,
    name: &str,
) -> CanonicalPath {
    CanonicalPath::try_from_canonical(repository.git_root.as_path().join("artifacts").join(name))
        .unwrap()
}
