use coding_agent_store::{
    AdvanceDeliverySourceObjectRequest, CommitDeliverySourceRequest, CreateDeliverySourceOutcome,
    CreateDeliverySourceRequest, DeliverySourceReconciliationReason, DeliverySourceRetryReason,
    DeliverySourceState, DeliverySourceTransitionOutcome, DeliveryVersion, MergeOperationState,
    ReconcileDeliverySourceRequest, RecordDeliverySourceRetryRequest, Store, StoreError,
};

use super::fixtures::{
    accepted_fixture, commit_pending_fixture, created_source, file_backed_accepted_fixture,
    object_proof, source_anchor, source_journal_count,
};

#[tokio::test]
async fn create_rolls_back_when_the_source_insert_faults() {
    let (store, command) = accepted_fixture().await;
    sqlx::raw_sql(
        "CREATE TRIGGER task5_create_source_fault \
         BEFORE INSERT ON task_delivery_sources \
         BEGIN SELECT RAISE(ABORT, 'task5 create source fault'); END;",
    )
    .execute(store.pool())
    .await
    .unwrap();
    let request = CreateDeliverySourceRequest::try_new(command.clone()).unwrap();
    assert!(matches!(
        store.create_delivery_source(request.clone()).await,
        Err(StoreError::Database(_))
    ));
    assert_eq!(source_journal_count(&store, command.task_id()).await, 0);
    let snapshot = store
        .delivery_ownership_snapshot(command.task_id())
        .await
        .unwrap()
        .unwrap();
    assert!(snapshot.source.is_none());
    assert_eq!(
        snapshot.merge_operations[0].state,
        MergeOperationState::Accepted
    );

    sqlx::raw_sql("DROP TRIGGER task5_create_source_fault;")
        .execute(store.pool())
        .await
        .unwrap();
    assert!(matches!(
        store.create_delivery_source(request).await.unwrap(),
        CreateDeliverySourceOutcome::Created(_)
    ));
}

#[tokio::test]
async fn paired_reconcile_rolls_back_when_merge_update_faults_after_source_update() {
    assert_reconcile_fault_rolls_back(
        "CREATE TRIGGER task5_reconcile_before_merge_fault \
         BEFORE UPDATE OF state ON task_merge_operations \
         WHEN NEW.state = 'reconciliation_required' \
         BEGIN SELECT RAISE(ABORT, 'task5 before merge fault'); END;",
    )
    .await;
}

#[tokio::test]
async fn paired_reconcile_rolls_back_when_a_post_merge_fault_fires() {
    assert_reconcile_fault_rolls_back(
        "CREATE TRIGGER task5_reconcile_after_merge_fault \
         AFTER UPDATE OF state ON task_merge_operations \
         WHEN NEW.state = 'reconciliation_required' \
         BEGIN SELECT RAISE(ABORT, 'task5 after merge fault'); END;",
    )
    .await;
}

#[tokio::test]
async fn paired_reconcile_rolls_back_when_deferred_commit_validation_fails() {
    assert_reconcile_fault_rolls_back(
        "CREATE TABLE task5_fault_parent (id INTEGER PRIMARY KEY) STRICT; \
         CREATE TABLE task5_fault_child ( \
             parent_id INTEGER NOT NULL, \
             FOREIGN KEY (parent_id) REFERENCES task5_fault_parent(id) \
                 DEFERRABLE INITIALLY DEFERRED \
         ) STRICT; \
         CREATE TRIGGER task5_reconcile_deferred_fault \
         AFTER UPDATE OF state ON task_merge_operations \
         WHEN NEW.state = 'reconciliation_required' \
         BEGIN INSERT INTO task5_fault_child(parent_id) VALUES (1); END;",
    )
    .await;
}

#[tokio::test]
async fn closed_pool_returns_a_typed_database_error() {
    // Task 5 calls Store directly. StoreWriter/oneshot channel closure belongs to
    // the later runtime-integration task; this boundary covers the closed DB pool.
    let (store, command) = accepted_fixture().await;
    let source = created_source(&store, command.clone()).await;
    let request = ReconcileDeliverySourceRequest::try_new(
        source_anchor(&command),
        DeliverySourceState::ObjectPending,
        source.version,
        DeliveryVersion::try_new(3).unwrap(),
        DeliverySourceReconciliationReason::SourceInconsistent,
    )
    .unwrap();
    store.close().await;
    assert!(matches!(
        store.reconcile_delivery_source(request).await,
        Err(StoreError::Database(_))
    ));
}

#[tokio::test]
async fn object_advance_rolls_back_when_its_transition_journal_insert_faults() {
    let (store, command) = accepted_fixture().await;
    let source = created_source(&store, command.clone()).await;
    install_source_journal_fault(&store, 2).await;
    let request = AdvanceDeliverySourceObjectRequest::try_new(
        source_anchor(&command),
        source.version,
        object_proof(&source),
    )
    .unwrap();
    assert!(matches!(
        store.advance_delivery_source_object(request.clone()).await,
        Err(StoreError::Database(_))
    ));
    assert_source_state(
        &store,
        command.task_id(),
        DeliverySourceState::ObjectPending,
        DeliveryVersion::initial(),
        1,
    )
    .await;
    remove_source_journal_fault(&store).await;
    assert!(matches!(
        store.advance_delivery_source_object(request).await.unwrap(),
        DeliverySourceTransitionOutcome::Applied(_)
    ));
}

#[tokio::test]
async fn retry_rolls_back_when_its_transition_journal_insert_faults() {
    let (store, command) = accepted_fixture().await;
    let source = created_source(&store, command.clone()).await;
    install_source_journal_fault(&store, 2).await;
    let request = RecordDeliverySourceRetryRequest::try_new(
        source_anchor(&command),
        DeliverySourceState::ObjectPending,
        source.version,
        DeliverySourceRetryReason::CommandTimedOut,
    )
    .unwrap();
    assert!(matches!(
        store.record_delivery_source_retry(request.clone()).await,
        Err(StoreError::Database(_))
    ));
    assert_source_state(
        &store,
        command.task_id(),
        DeliverySourceState::ObjectPending,
        DeliveryVersion::initial(),
        1,
    )
    .await;
    remove_source_journal_fault(&store).await;
    assert!(matches!(
        store.record_delivery_source_retry(request).await.unwrap(),
        DeliverySourceTransitionOutcome::Applied(_)
    ));
}

#[tokio::test]
async fn commit_rolls_back_when_its_transition_journal_insert_faults() {
    let (store, command) = accepted_fixture().await;
    let (source, anchor, _object, proof) = commit_pending_fixture(&store, &command).await;
    install_source_journal_fault(&store, 3).await;
    let request = CommitDeliverySourceRequest::try_new(anchor, source.version, proof).unwrap();
    assert!(matches!(
        store.commit_delivery_source(request.clone()).await,
        Err(StoreError::Database(_))
    ));
    assert_source_state(
        &store,
        command.task_id(),
        DeliverySourceState::CommitPending,
        DeliveryVersion::try_new(2).unwrap(),
        2,
    )
    .await;
    remove_source_journal_fault(&store).await;
    assert!(matches!(
        store.commit_delivery_source(request).await.unwrap(),
        DeliverySourceTransitionOutcome::Applied(_)
    ));
}

#[tokio::test]
async fn real_database_busy_is_typed_and_writes_nothing() {
    let (fixture, command) = file_backed_accepted_fixture().await;
    let store = fixture.store.clone();
    let source = created_source(&store, command.clone()).await;
    let request = AdvanceDeliverySourceObjectRequest::try_new(
        source_anchor(&command),
        source.version,
        object_proof(&source),
    )
    .unwrap();

    let mut connections = Vec::new();
    for _ in 0..5 {
        connections.push(store.pool().acquire().await.unwrap());
    }
    for connection in &mut connections {
        sqlx::query("PRAGMA busy_timeout = 0")
            .execute(&mut **connection)
            .await
            .unwrap();
    }
    let mut writer = connections.pop().unwrap();
    drop(connections);
    sqlx::query("BEGIN IMMEDIATE")
        .execute(&mut *writer)
        .await
        .unwrap();
    assert!(matches!(
        store.advance_delivery_source_object(request.clone()).await,
        Err(StoreError::Database(_))
    ));
    sqlx::query("ROLLBACK").execute(&mut *writer).await.unwrap();
    drop(writer);
    assert_source_state(
        &store,
        command.task_id(),
        DeliverySourceState::ObjectPending,
        DeliveryVersion::initial(),
        1,
    )
    .await;
    assert!(matches!(
        store.advance_delivery_source_object(request).await.unwrap(),
        DeliverySourceTransitionOutcome::Applied(_)
    ));
}

async fn assert_reconcile_fault_rolls_back(fault_sql: &'static str) {
    let (store, command) = accepted_fixture().await;
    let source = created_source(&store, command.clone()).await;
    sqlx::raw_sql(fault_sql)
        .execute(store.pool())
        .await
        .unwrap();
    let request = ReconcileDeliverySourceRequest::try_new(
        source_anchor(&command),
        DeliverySourceState::ObjectPending,
        source.version,
        DeliveryVersion::try_new(3).unwrap(),
        DeliverySourceReconciliationReason::SourceInconsistent,
    )
    .unwrap();
    assert!(matches!(
        store.reconcile_delivery_source(request).await,
        Err(StoreError::Database(_))
    ));
    assert_pristine_pair(&store, command.task_id(), command.preflight_operation_id()).await;
}

async fn assert_pristine_pair(
    store: &Store,
    task_id: coding_agent_domain::TaskId,
    operation_id: coding_agent_store::DeliveryOperationId,
) {
    assert_eq!(source_journal_count(store, task_id).await, 1);
    let snapshot = store
        .delivery_ownership_snapshot(task_id)
        .await
        .unwrap()
        .unwrap();
    let source = snapshot.source.unwrap();
    assert_eq!(source.state, DeliverySourceState::ObjectPending);
    assert_eq!(source.version, DeliveryVersion::initial());
    assert_eq!(source.failure_code, None);
    let operation = snapshot
        .merge_operations
        .into_iter()
        .find(|operation| operation.operation_id == operation_id)
        .unwrap();
    assert_eq!(operation.state, MergeOperationState::Accepted);
    assert_eq!(operation.version, DeliveryVersion::try_new(3).unwrap());
    assert_eq!(operation.failure_code, None);
}

async fn install_source_journal_fault(store: &Store, version: i64) {
    let sql = match version {
        2 => {
            "CREATE TRIGGER task5_source_journal_fault \
             BEFORE INSERT ON task_delivery_operation_transitions \
             WHEN NEW.entity_kind = 'delivery_source' AND NEW.entity_version = 2 \
             BEGIN SELECT RAISE(ABORT, 'task5 source journal v2 fault'); END;"
        }
        3 => {
            "CREATE TRIGGER task5_source_journal_fault \
             BEFORE INSERT ON task_delivery_operation_transitions \
             WHEN NEW.entity_kind = 'delivery_source' AND NEW.entity_version = 3 \
             BEGIN SELECT RAISE(ABORT, 'task5 source journal v3 fault'); END;"
        }
        _ => unreachable!(),
    };
    sqlx::raw_sql(sql).execute(store.pool()).await.unwrap();
}

async fn remove_source_journal_fault(store: &Store) {
    sqlx::raw_sql("DROP TRIGGER task5_source_journal_fault;")
        .execute(store.pool())
        .await
        .unwrap();
}

async fn assert_source_state(
    store: &Store,
    task_id: coding_agent_domain::TaskId,
    expected_state: DeliverySourceState,
    expected_version: DeliveryVersion,
    expected_journals: i64,
) {
    assert_eq!(
        source_journal_count(store, task_id).await,
        expected_journals
    );
    let source = store
        .delivery_ownership_snapshot(task_id)
        .await
        .unwrap()
        .unwrap()
        .source
        .unwrap();
    assert_eq!(source.state, expected_state);
    assert_eq!(source.version, expected_version);
}
