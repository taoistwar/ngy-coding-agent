#[path = "shape_corruption/support.rs"]
mod shape_support;

use coding_agent_store::{Store, StoreError};

use self::shape_support::{
    GitOidField, MergeShape, MetadataCorruption, SourceInvariantCorruption, SourceShape,
    corrupt_git_oid_algorithm, corrupt_merge_shape, corrupt_metadata, corrupt_source_invariant,
    corrupt_source_shape, merge_fixture, metadata_fixture, source_fixture,
};

#[tokio::test]
async fn source_current_row_shapes_are_revalidated_without_sqlite_guards() {
    for shape in SourceShape::ALL {
        let (store, task, _) = source_fixture(shape).await;
        let before = store
            .delivery_ownership_snapshot(task.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(before.source.as_ref().unwrap().state, shape.state());

        corrupt_source_shape(&store, &task, shape).await;
        assert_invariant(snapshot_error(&store, task.id, format!("source {shape:?}")).await);
    }
}

#[tokio::test]
async fn every_merge_current_row_state_shape_is_revalidated_without_sqlite_guards() {
    for shape in MergeShape::ALL {
        let (store, task, operation_id) = merge_fixture(shape).await;
        let before = store
            .delivery_ownership_snapshot(task.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(before.merge_operations[0].state, shape.state());

        corrupt_merge_shape(&store, &task, operation_id, shape).await;
        assert_invariant(snapshot_error(&store, task.id, format!("merge {shape:?}")).await);
    }
}

#[tokio::test]
async fn delivery_git_oids_must_use_the_artifact_object_algorithm() {
    for field in GitOidField::ALL {
        let (store, task, operation_id) = merge_fixture(field.fixture_shape()).await;
        corrupt_git_oid_algorithm(&store, &task, operation_id, field).await;
        assert_invariant(snapshot_error(&store, task.id, format!("OID {field:?}")).await);
    }
}

#[tokio::test]
async fn merge_pending_cannot_clear_its_source_link_after_database_guards_are_disabled() {
    let (store, task, operation_id) = merge_fixture(MergeShape::MergePending).await;
    corrupt_merge_shape(&store, &task, operation_id, MergeShape::MergePending).await;

    assert_invariant(snapshot_error(&store, task.id, "merge source link".to_owned()).await);
}

#[tokio::test]
async fn commit_metadata_shape_is_revalidated_after_typed_decoding() {
    for corruption in MetadataCorruption::ALL {
        let (store, task, operation_id) = metadata_fixture(corruption).await;
        corrupt_metadata(&store, &task, operation_id, corruption).await;
        assert_invariant(snapshot_error(&store, task.id, format!("metadata {corruption:?}")).await);
    }
}

#[tokio::test]
async fn source_failure_codes_and_reconciliation_pair_are_revalidated() {
    for corruption in SourceInvariantCorruption::ALL {
        let (store, task, operation_id) = source_fixture(corruption.fixture_shape()).await;
        corrupt_source_invariant(&store, &task, operation_id, corruption).await;
        assert_invariant(
            snapshot_error(&store, task.id, format!("source invariant {corruption:?}")).await,
        );
    }
}

async fn snapshot_error(
    store: &Store,
    task_id: coding_agent_domain::TaskId,
    context: String,
) -> StoreError {
    match store.delivery_eligibility_snapshot(task_id).await {
        Err(error) => error,
        Ok(_) => panic!("{context} corruption was accepted"),
    }
}

fn assert_invariant(error: StoreError) {
    match error {
        StoreError::InvariantViolation(message) => {
            assert_eq!(message, "delivery eligibility snapshot is inconsistent");
        }
        other => panic!("expected ownership invariant, got {other}"),
    }
}
