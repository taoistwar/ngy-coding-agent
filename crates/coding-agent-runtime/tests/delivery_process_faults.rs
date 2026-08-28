#![cfg(feature = "test-support")]

mod delivery_source_support;

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use coding_agent_runtime::{
    DeliveryCandidateTree, DeliveryExpectedMerge, DeliveryMergeError, DeliveryMergeInput,
    DeliveryPreflightResult, DeliveryPreflightSource, DeliverySourceCapability,
    DeliverySourceCommit, DeliverySourceCommitInput, DeliverySourcePendingState,
    DeliverySourceProvisioner, DeliverySourceRecoveryCapability, DeliverySourceRecoveryDisposition,
    DeliverySourceRecoveryIntent, DeliveryTargetCapability, DeliveryTargetProvisioner,
    DeliveryTargetRequest, ProcessCleanupProof, ProcessFault, ProcessFaultController,
    ProcessFaultEventKind, ProcessLimits, ProcessLivenessScope, WorktreeProvisioner,
    build_expected_delivery_merge, preflight_delivery_merge,
};
use tokio_util::sync::CancellationToken;

use delivery_source_support::{
    Fixture, RepositorySnapshot, ReviewedDirtySource, delivery_source_limits, git_line,
};

const ZERO_LIVE_TIMEOUT: Duration = Duration::from_secs(5);
const EXPECTED_MERGE_EPOCH_SECONDS: i64 = 1_700_000_100;
const SOURCE_PROCESS_FAULTS: [ProcessFault; 8] = [
    ProcessFault::BeforeSpawn,
    ProcessFault::AfterSpawnUnknown,
    ProcessFault::StdoutOverflow,
    ProcessFault::Deadline,
    ProcessFault::WaitUnknown,
    ProcessFault::ChannelUnknown,
    ProcessFault::KillFailure,
    ProcessFault::CleanupFailure,
];

/// Reviewed source state before the first P4-B authentication call. Keeping
/// this separate lets spawn-before tests install a controller after all
/// fixture/toolchain setup has completed.
struct UnopenedSourcePhase {
    fixture: Fixture,
    source: ReviewedDirtySource,
}

impl UnopenedSourcePhase {
    async fn new(name: &str, task_id: &str) -> Self {
        let fixture = Fixture::new(name).await;
        let source = fixture.reviewed_dirty_source(task_id).await;
        Self { fixture, source }
    }

    fn snapshot(&self) -> RepositorySnapshot {
        self.source.snapshot(&self.fixture.repository)
    }

    fn provisioner(&self) -> DeliverySourceProvisioner {
        self.fixture
            .delivery_source(&self.source.worktrees)
            .unwrap()
    }

    async fn open(self) -> OpenedSourcePhase {
        let provisioner = self.provisioner();
        let opened = provisioner
            .open_delivery_source(
                &self.source.reservation,
                self.source.approved_fingerprint,
                CancellationToken::new(),
            )
            .await
            .unwrap();
        OpenedSourcePhase {
            fixture: self.fixture,
            source: self.source,
            provisioner,
            opened,
        }
    }
}

/// A freshly authenticated source whose candidate/object work has not crossed
/// a durable boundary yet. Fault tests install their controller only around
/// the one production call under test, so fixture setup children never consume
/// an injected child ordinal.
struct OpenedSourcePhase {
    fixture: Fixture,
    source: ReviewedDirtySource,
    provisioner: DeliverySourceProvisioner,
    opened: DeliverySourceCapability,
}

impl OpenedSourcePhase {
    async fn new(name: &str, task_id: &str) -> Self {
        UnopenedSourcePhase::new(name, task_id).await.open().await
    }

    fn snapshot(&self) -> RepositorySnapshot {
        self.source.snapshot(&self.fixture.repository)
    }

    async fn into_object_pending(self) -> ObjectPendingPhase {
        let candidate = self
            .provisioner
            .build_candidate_tree(&self.opened, CancellationToken::new())
            .await
            .unwrap();
        let input = DeliverySourceCommitInput::try_new(
            self.source.reservation.identity().task_id(),
            u64::from(self.source.reservation.identity().attempt()),
            1_700_000_000,
        )
        .unwrap();
        ObjectPendingPhase {
            fixture: self.fixture,
            source: self.source,
            provisioner: self.provisioner,
            opened: self.opened,
            candidate,
            input,
        }
    }
}

/// Runtime inputs available after the application has durably persisted
/// ObjectPending but before the deterministic source object is known.
struct ObjectPendingPhase {
    fixture: Fixture,
    source: ReviewedDirtySource,
    provisioner: DeliverySourceProvisioner,
    opened: DeliverySourceCapability,
    candidate: DeliveryCandidateTree,
    input: DeliverySourceCommitInput,
}

impl ObjectPendingPhase {
    fn snapshot(&self) -> RepositorySnapshot {
        self.source.snapshot(&self.fixture.repository)
    }

    async fn into_commit_pending(self) -> CommitPendingPhase {
        let expected = self
            .provisioner
            .build_source_commit(
                &self.opened,
                &self.candidate,
                &self.input,
                CancellationToken::new(),
            )
            .await
            .unwrap();
        let object_intent = DeliverySourceRecoveryIntent::from_source(
            DeliverySourcePendingState::ObjectPending,
            &self.opened,
            &self.candidate,
            None,
            self.input.clone(),
        )
        .unwrap();
        let commit_intent = DeliverySourceRecoveryIntent::from_source(
            DeliverySourcePendingState::CommitPending,
            &self.opened,
            &self.candidate,
            Some(&expected),
            self.input,
        )
        .unwrap();
        drop(self.opened);
        drop(self.provisioner);
        CommitPendingPhase {
            fixture: self.fixture,
            source: self.source,
            object_intent,
            commit_intent,
            expected,
        }
    }
}

/// Both persisted recovery intents rebuilt without retaining the pre-crash
/// capability. Tests can therefore inject a fault into observation, replay,
/// or real-index/ref application without weakening the recovery boundary.
struct CommitPendingPhase {
    fixture: Fixture,
    source: ReviewedDirtySource,
    object_intent: DeliverySourceRecoveryIntent,
    commit_intent: DeliverySourceRecoveryIntent,
    expected: DeliverySourceCommit,
}

impl CommitPendingPhase {
    fn snapshot(&self) -> RepositorySnapshot {
        self.source.snapshot(&self.fixture.repository)
    }

    async fn open_recovery(
        &self,
        pending: DeliverySourcePendingState,
    ) -> (DeliverySourceProvisioner, DeliverySourceRecoveryCapability) {
        let intent = match pending {
            DeliverySourcePendingState::ObjectPending => &self.object_intent,
            DeliverySourcePendingState::CommitPending => &self.commit_intent,
        };
        let provisioner = self
            .fixture
            .delivery_source(&self.source.worktrees)
            .unwrap();
        let recovery = provisioner
            .open_delivery_source_for_recovery(
                &self.source.reservation,
                intent,
                CancellationToken::new(),
            )
            .await
            .unwrap();
        (provisioner, recovery)
    }
}

#[derive(Debug, PartialEq, Eq)]
struct TargetCheckoutSnapshot {
    refs: Vec<u8>,
    index: Vec<u8>,
    head: Vec<u8>,
    status: Vec<u8>,
    worktree: BTreeMap<PathBuf, Option<Vec<u8>>>,
}

fn target_process_limits() -> ProcessLimits {
    ProcessLimits::try_new(
        512 * 1024,
        512 * 1024,
        Duration::from_secs(30),
        Duration::from_secs(5),
    )
    .unwrap()
}

fn target_provisioner(
    fixture: &Fixture,
    worktrees: &WorktreeProvisioner,
    process_scope: ProcessLivenessScope,
) -> DeliveryTargetProvisioner {
    DeliveryTargetProvisioner::from_worktree_provisioner(
        worktrees,
        Arc::clone(&fixture.delivery_git),
        &fixture.runtime_directory,
        process_scope,
        target_process_limits(),
        delivery_source_limits(),
    )
    .unwrap()
}

fn target_request(fixture: &Fixture) -> DeliveryTargetRequest {
    DeliveryTargetRequest::try_new(
        git_line(&fixture.repository, &["symbolic-ref", "--short", "HEAD"]),
        git_line(&fixture.repository, &["rev-parse", "HEAD"]),
    )
    .unwrap()
}

fn target_checkout_snapshot(fixture: &Fixture) -> TargetCheckoutSnapshot {
    TargetCheckoutSnapshot {
        refs: fixture_git_output(
            &fixture.repository,
            &[
                "for-each-ref",
                "--format=%(refname)%00%(objectname)",
                "refs",
            ],
        ),
        index: std::fs::read(fixture.repository.join(".git/index")).unwrap(),
        head: std::fs::read(fixture.repository.join(".git/HEAD")).unwrap(),
        status: fixture_git_output(
            &fixture.repository,
            &["status", "--porcelain=v2", "-z", "--untracked-files=all"],
        ),
        worktree: snapshot_target_worktree(&fixture.repository),
    }
}

fn snapshot_target_worktree(root: &Path) -> BTreeMap<PathBuf, Option<Vec<u8>>> {
    fn visit(root: &Path, directory: &Path, entries: &mut BTreeMap<PathBuf, Option<Vec<u8>>>) {
        let mut children = std::fs::read_dir(directory)
            .unwrap()
            .map(|entry| entry.unwrap())
            .collect::<Vec<_>>();
        children.sort_by_key(|entry| entry.file_name());
        for entry in children {
            if entry.file_name() == ".git" {
                continue;
            }
            let path = entry.path();
            let relative = path.strip_prefix(root).unwrap().to_owned();
            if entry.file_type().unwrap().is_dir() {
                entries.insert(relative, None);
                visit(root, &path, entries);
            } else {
                entries.insert(relative, Some(std::fs::read(path).unwrap()));
            }
        }
    }

    let mut entries = BTreeMap::new();
    visit(root, root, &mut entries);
    entries
}

fn fixture_git_output(repository: &Path, arguments: &[&str]) -> Vec<u8> {
    let output = Command::new("git")
        .arg("-C")
        .arg(repository)
        .args(arguments)
        .output()
        .unwrap();
    assert!(output.status.success(), "fixture Git command failed");
    output.stdout
}

fn object_inventory(repository: &Path) -> BTreeMap<String, String> {
    String::from_utf8(fixture_git_output(
        repository,
        &[
            "cat-file",
            "--batch-all-objects",
            "--batch-check=%(objectname) %(objecttype)",
        ],
    ))
    .unwrap()
    .lines()
    .map(|line| {
        let (object_id, object_type) = line.split_once(' ').expect("typed object inventory line");
        (object_id.to_owned(), object_type.to_owned())
    })
    .collect()
}

fn reachable_object_ids(repository: &Path) -> BTreeSet<String> {
    String::from_utf8(fixture_git_output(
        repository,
        &["rev-list", "--objects", "--all"],
    ))
    .unwrap()
    .lines()
    .map(|line| {
        line.split_ascii_whitespace()
            .next()
            .expect("reachable object ID")
            .to_owned()
    })
    .collect()
}

fn assert_only_one_unreachable_expected_commit_may_be_added(
    repository: &Path,
    baseline: &BTreeMap<String, String>,
    observed: &BTreeMap<String, String>,
    context: &str,
) {
    for (object_id, object_type) in baseline {
        assert_eq!(
            observed.get(object_id),
            Some(object_type),
            "existing object inventory changed: {context}",
        );
    }
    let created = observed
        .iter()
        .filter(|(object_id, _)| !baseline.contains_key(*object_id))
        .collect::<Vec<_>>();
    assert!(
        created.len() <= 1,
        "expected-merge fault created unexpected objects: {context}: {created:?}",
    );
    let reachable = reachable_object_ids(repository);
    for (object_id, object_type) in created {
        assert_eq!(
            object_type, "commit",
            "expected-merge fault created a non-commit object: {context}",
        );
        assert!(
            !reachable.contains(object_id),
            "expected-merge object became reachable: {context}",
        );
    }
}

struct PreparedPreflightPhase {
    fixture: Fixture,
    source: ReviewedDirtySource,
    source_provisioner: DeliverySourceProvisioner,
    opened_source: DeliverySourceCapability,
    candidate: DeliveryCandidateTree,
    target_provisioner: DeliveryTargetProvisioner,
    target: DeliveryTargetCapability,
}

impl PreparedPreflightPhase {
    async fn new(name: &str, task_id: &str) -> Self {
        let fixture = Fixture::new(name).await;
        let source = fixture.reviewed_dirty_source(task_id).await;
        let source_provisioner = fixture.delivery_source(&source.worktrees).unwrap();
        let opened_source = source_provisioner
            .open_delivery_source(
                &source.reservation,
                source.approved_fingerprint,
                CancellationToken::new(),
            )
            .await
            .unwrap();
        let candidate = source_provisioner
            .build_candidate_tree(&opened_source, CancellationToken::new())
            .await
            .unwrap();
        let target_provisioner = target_provisioner(
            &fixture,
            &source.worktrees,
            source.worker_process_scope.clone(),
        );
        let target = target_provisioner
            .open_delivery_target(&target_request(&fixture), CancellationToken::new())
            .await
            .unwrap();
        Self {
            fixture,
            source,
            source_provisioner,
            opened_source,
            candidate,
            target_provisioner,
            target,
        }
    }

    fn source_snapshot(&self) -> RepositorySnapshot {
        self.source.snapshot(&self.fixture.repository)
    }

    fn target_snapshot(&self) -> TargetCheckoutSnapshot {
        target_checkout_snapshot(&self.fixture)
    }
}

/// Fully committed source and ready target/preflight inputs for faulting only
/// the deterministic expected-merge object path. Fixture setup, source apply,
/// and initial preflight all finish before a controller is installed.
struct PreparedExpectedMergePhase {
    source: ReviewedDirtySource,
    source_provisioner: DeliverySourceProvisioner,
    opened_source: DeliverySourceCapability,
    candidate: DeliveryCandidateTree,
    source_commit: DeliverySourceCommit,
    source_input: DeliverySourceCommitInput,
    target_process_scope: ProcessLivenessScope,
    target_provisioner: DeliveryTargetProvisioner,
    target: DeliveryTargetCapability,
    preflight: DeliveryPreflightResult,
    merge_input: DeliveryMergeInput,
    // Drop the fixture last so its shared ProcessScopeTracker proves every
    // retained source/target scope after the provisioners have been released.
    fixture: Fixture,
}

impl PreparedExpectedMergePhase {
    async fn new(name: &str, task_id: &str) -> Self {
        let fixture = Fixture::new(name).await;
        let source = fixture.reviewed_dirty_source(task_id).await;
        let source_provisioner = fixture.delivery_source(&source.worktrees).unwrap();
        let opened_source = source_provisioner
            .open_delivery_source(
                &source.reservation,
                source.approved_fingerprint,
                CancellationToken::new(),
            )
            .await
            .unwrap();
        let candidate = source_provisioner
            .build_candidate_tree(&opened_source, CancellationToken::new())
            .await
            .unwrap();
        let source_input =
            DeliverySourceCommitInput::try_new(task_id, 1, EXPECTED_MERGE_EPOCH_SECONDS).unwrap();
        let source_commit = source_provisioner
            .build_source_commit(
                &opened_source,
                &candidate,
                &source_input,
                CancellationToken::new(),
            )
            .await
            .unwrap();
        let commit_intent = DeliverySourceRecoveryIntent::from_source(
            DeliverySourcePendingState::CommitPending,
            &opened_source,
            &candidate,
            Some(&source_commit),
            source_input.clone(),
        )
        .unwrap();
        let recovery = source_provisioner
            .open_delivery_source_for_recovery(
                &source.reservation,
                &commit_intent,
                CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(
            source_provisioner
                .apply_source_commit(&recovery, CancellationToken::new())
                .await
                .unwrap(),
            DeliverySourceRecoveryDisposition::Applied,
        );
        drop(recovery);

        let target_process_scope = fixture.task_process_scope();
        let target_provisioner =
            target_provisioner(&fixture, &source.worktrees, target_process_scope.clone());
        let target = target_provisioner
            .open_delivery_target(&target_request(&fixture), CancellationToken::new())
            .await
            .unwrap();
        let preflight = preflight_delivery_merge(
            &source_provisioner,
            &target_provisioner,
            &target,
            DeliveryPreflightSource::committed(
                &opened_source,
                &candidate,
                &source_commit,
                &source_input,
            ),
            CancellationToken::new(),
        )
        .await
        .unwrap();
        assert!(preflight.is_ready());
        let merge_input =
            DeliveryMergeInput::try_new(task_id, 1, EXPECTED_MERGE_EPOCH_SECONDS).unwrap();

        Self {
            source,
            source_provisioner,
            opened_source,
            candidate,
            source_commit,
            source_input,
            target_process_scope,
            target_provisioner,
            target,
            preflight,
            merge_input,
            fixture,
        }
    }

    async fn build_expected(&self) -> Result<DeliveryExpectedMerge, DeliveryMergeError> {
        build_expected_delivery_merge(
            &self.source_provisioner,
            &self.target_provisioner,
            &self.opened_source,
            &self.target,
            &self.candidate,
            &self.source_commit,
            &self.source_input,
            &self.preflight,
            &self.merge_input,
            CancellationToken::new(),
        )
        .await
    }

    async fn reprove_preflight(&self) -> DeliveryPreflightResult {
        preflight_delivery_merge(
            &self.source_provisioner,
            &self.target_provisioner,
            &self.target,
            DeliveryPreflightSource::committed(
                &self.opened_source,
                &self.candidate,
                &self.source_commit,
                &self.source_input,
            ),
            CancellationToken::new(),
        )
        .await
        .unwrap()
    }

    fn source_snapshot(&self) -> RepositorySnapshot {
        self.source.snapshot(&self.fixture.repository)
    }

    fn target_snapshot(&self) -> TargetCheckoutSnapshot {
        target_checkout_snapshot(&self.fixture)
    }

    fn object_inventory(&self) -> BTreeMap<String, String> {
        object_inventory(&self.fixture.repository)
    }
}

async fn assert_zero_live_children(source: &ReviewedDirtySource) {
    assert_zero_live_scope(&source.worker_process_scope).await;
}

async fn assert_zero_live_scope(scope: &ProcessLivenessScope) {
    tokio::time::timeout(ZERO_LIVE_TIMEOUT, async {
        loop {
            let proof = scope.cleanup_proof().expect("read process cleanup proof");
            if scope.active_tree_count() == 0 && proof == ProcessCleanupProof::Confirmed {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("Git children must be gone with confirmed cleanup proof");
}

async fn assert_controlled_child_was_reaped(
    controller: &ProcessFaultController,
    fault_child_ordinal: u64,
    expected_observed_children: Option<u64>,
    fault: ProcessFault,
) {
    let proof = controller
        .prove_zero_live(ZERO_LIVE_TIMEOUT)
        .await
        .unwrap_or_else(|error| panic!("{} after {fault:?}", error.code()));
    let observed_children = proof.observed_children();
    assert!(observed_children >= fault_child_ordinal, "{fault:?}");
    if let Some(expected) = expected_observed_children {
        assert_eq!(observed_children, expected, "{fault:?}");
    }
    assert_eq!(
        proof.checked_scopes(),
        usize::try_from(observed_children).unwrap(),
        "{fault:?}",
    );
    let mut expected_events = Vec::new();
    for ordinal in 1..=observed_children {
        expected_events.push((ordinal, ProcessFaultEventKind::Admitted));
        if ordinal == fault_child_ordinal {
            expected_events.push((ordinal, ProcessFaultEventKind::Injected(fault)));
        }
        expected_events.push((ordinal, ProcessFaultEventKind::Returned));
    }
    assert_eq!(
        controller
            .events()
            .into_iter()
            .map(|event| (event.child_ordinal(), event.kind()))
            .collect::<Vec<_>>(),
        expected_events,
        "{fault:?}",
    );
}

fn pre_mutation_error_code(fault: ProcessFault) -> &'static str {
    match fault {
        ProcessFault::BeforeSpawn => "DELIVERY_SOURCE_COMMAND_FAILED",
        ProcessFault::AfterSpawnUnknown
        | ProcessFault::WaitUnknown
        | ProcessFault::ChannelUnknown => "DELIVERY_RECONCILIATION_REQUIRED",
        ProcessFault::StdoutOverflow => "DELIVERY_SOURCE_BOUNDS_EXCEEDED",
        ProcessFault::Deadline => "COMMAND_TIMED_OUT",
        ProcessFault::KillFailure | ProcessFault::CleanupFailure => "PROCESS_TREE_CLEANUP_FAILED",
    }
}

fn target_observation_error_code(fault: ProcessFault) -> &'static str {
    match fault {
        ProcessFault::BeforeSpawn => "DELIVERY_TARGET_INVALID",
        ProcessFault::AfterSpawnUnknown
        | ProcessFault::WaitUnknown
        | ProcessFault::ChannelUnknown => "DELIVERY_RECONCILIATION_REQUIRED",
        ProcessFault::StdoutOverflow => "DELIVERY_SOURCE_BOUNDS_EXCEEDED",
        ProcessFault::Deadline => "COMMAND_TIMED_OUT",
        ProcessFault::KillFailure | ProcessFault::CleanupFailure => "PROCESS_TREE_CLEANUP_FAILED",
    }
}

fn preflight_observation_error_code(fault: ProcessFault) -> &'static str {
    match fault {
        ProcessFault::BeforeSpawn => "TARGET_BRANCH_DETACHED",
        _ => target_observation_error_code(fault),
    }
}

fn admitted_child_count(controller: &ProcessFaultController) -> u64 {
    controller
        .events()
        .into_iter()
        .filter(|event| event.kind() == ProcessFaultEventKind::Admitted)
        .map(|event| event.child_ordinal())
        .max()
        .unwrap_or(0)
}

async fn discover_candidate_write_tree_ordinal() -> (u64, String) {
    let mut phase = OpenedSourcePhase::new(
        "process-fault-candidate-discovery",
        "123e4567-e89b-12d3-a456-426614174033",
    )
    .await;
    let controller =
        ProcessFaultController::for_child(u64::MAX, ProcessFault::BeforeSpawn).unwrap();
    let boundaries = Arc::new(Mutex::new((None, None)));
    phase
        .provisioner
        .set_authentication_boundary_hook_for_tests({
            let controller = controller.clone();
            let boundaries = Arc::clone(&boundaries);
            move |boundary| {
                let admitted = admitted_child_count(&controller);
                let mut observed = boundaries.lock().unwrap();
                match boundary {
                    "after-candidate-revalidation-before-tree-build" => observed.0 = Some(admitted),
                    "after-write-tree-before-fresh-fingerprint" => observed.1 = Some(admitted),
                    _ => {}
                }
            }
        });

    let candidate = controller
        .scope(
            phase
                .provisioner
                .build_candidate_tree(&phase.opened, CancellationToken::new()),
        )
        .await
        .unwrap();
    let (before_tree_build, after_write_tree) = *boundaries.lock().unwrap();
    let before_tree_build = before_tree_build.expect("candidate pre-build boundary");
    let write_tree_ordinal = after_write_tree.expect("candidate post-write-tree boundary");
    assert!(write_tree_ordinal > before_tree_build);
    assert!(admitted_child_count(&controller) > write_tree_ordinal);
    assert!(
        controller
            .events()
            .iter()
            .all(|event| !matches!(event.kind(), ProcessFaultEventKind::Injected(_)))
    );
    assert_zero_live_children(&phase.source).await;
    (write_tree_ordinal, candidate.object_id().to_owned())
}

#[derive(Debug, Clone, Copy)]
struct ApplyChildOrdinals {
    stage_candidate: u64,
    refresh_index: u64,
    source_cas: u64,
}

static APPLY_CHILD_ORDINALS: tokio::sync::OnceCell<ApplyChildOrdinals> =
    tokio::sync::OnceCell::const_new();

async fn shared_apply_child_ordinals() -> ApplyChildOrdinals {
    *APPLY_CHILD_ORDINALS
        .get_or_init(discover_apply_child_ordinals)
        .await
}

async fn discover_apply_child_ordinals() -> ApplyChildOrdinals {
    let phase = OpenedSourcePhase::new(
        "process-fault-apply-discovery",
        "123e4567-e89b-12d3-a456-426614174036",
    )
    .await
    .into_object_pending()
    .await
    .into_commit_pending()
    .await;
    let (mut provisioner, recovery) = phase
        .open_recovery(DeliverySourcePendingState::CommitPending)
        .await;
    let controller =
        ProcessFaultController::for_child(u64::MAX, ProcessFault::BeforeSpawn).unwrap();
    let boundaries = Arc::new(Mutex::new((None, None, None)));
    provisioner.set_authentication_boundary_hook_for_tests({
        let controller = controller.clone();
        let boundaries = Arc::clone(&boundaries);
        move |boundary| {
            let admitted = admitted_child_count(&controller);
            let mut observed = boundaries.lock().unwrap();
            match boundary {
                "after-real-index-stage-before-source-object-reverify" => {
                    observed.0 = Some(admitted)
                }
                "after-source-object-reverify-before-cas" => observed.1 = Some(admitted),
                "after-source-cas-before-postverify" => observed.2 = Some(admitted),
                _ => {}
            }
        }
    });

    assert_eq!(
        controller
            .scope(provisioner.apply_source_commit(&recovery, CancellationToken::new()))
            .await
            .unwrap(),
        DeliverySourceRecoveryDisposition::Applied,
    );
    let (after_refresh, before_cas, after_cas) = *boundaries.lock().unwrap();
    let refresh_index = after_refresh.expect("post-refresh boundary");
    let stage_candidate = refresh_index
        .checked_sub(1)
        .expect("stage child before refresh");
    let before_cas = before_cas.expect("pre-CAS boundary");
    let source_cas = after_cas.expect("post-CAS boundary");
    assert_eq!(source_cas, before_cas + 1);
    assert!(stage_candidate > 0);
    assert!(admitted_child_count(&controller) > source_cas);
    assert!(
        controller
            .events()
            .iter()
            .all(|event| !matches!(event.kind(), ProcessFaultEventKind::Injected(_)))
    );
    assert_zero_live_children(&phase.source).await;
    ApplyChildOrdinals {
        stage_candidate,
        refresh_index,
        source_cas,
    }
}

fn post_real_index_mutation_error_code(fault: ProcessFault) -> &'static str {
    match fault {
        ProcessFault::BeforeSpawn => "DELIVERY_SOURCE_COMMAND_FAILED",
        ProcessFault::KillFailure | ProcessFault::CleanupFailure => "PROCESS_TREE_CLEANUP_FAILED",
        ProcessFault::AfterSpawnUnknown
        | ProcessFault::StdoutOverflow
        | ProcessFault::Deadline
        | ProcessFault::WaitUnknown
        | ProcessFault::ChannelUnknown => "DELIVERY_RECONCILIATION_REQUIRED",
    }
}

fn apply_fault_ordinal(ordinals: ApplyChildOrdinals, fault: ProcessFault) -> u64 {
    match fault {
        ProcessFault::BeforeSpawn | ProcessFault::AfterSpawnUnknown => ordinals.stage_candidate,
        ProcessFault::StdoutOverflow | ProcessFault::Deadline | ProcessFault::KillFailure => {
            ordinals.refresh_index
        }
        ProcessFault::WaitUnknown | ProcessFault::ChannelUnknown | ProcessFault::CleanupFailure => {
            ordinals.source_cas
        }
    }
}

fn expected_apply_recovery_dispositions(
    fault: ProcessFault,
) -> &'static [DeliverySourceRecoveryDisposition] {
    match fault {
        ProcessFault::BeforeSpawn => &[DeliverySourceRecoveryDisposition::Continue],
        ProcessFault::AfterSpawnUnknown => &[
            DeliverySourceRecoveryDisposition::Continue,
            DeliverySourceRecoveryDisposition::ReconciliationRequired,
        ],
        ProcessFault::StdoutOverflow => &[DeliverySourceRecoveryDisposition::StageComplete],
        ProcessFault::Deadline => &[
            DeliverySourceRecoveryDisposition::ReconciliationRequired,
            DeliverySourceRecoveryDisposition::StageComplete,
        ],
        ProcessFault::KillFailure => &[DeliverySourceRecoveryDisposition::StageComplete],
        ProcessFault::WaitUnknown => &[
            DeliverySourceRecoveryDisposition::StageComplete,
            DeliverySourceRecoveryDisposition::Applied,
        ],
        ProcessFault::ChannelUnknown | ProcessFault::CleanupFailure => {
            &[DeliverySourceRecoveryDisposition::Applied]
        }
    }
}

fn fault_name(fault: ProcessFault) -> &'static str {
    match fault {
        ProcessFault::BeforeSpawn => "before-spawn",
        ProcessFault::AfterSpawnUnknown => "after-spawn",
        ProcessFault::StdoutOverflow => "stdout-overflow",
        ProcessFault::Deadline => "deadline",
        ProcessFault::WaitUnknown => "wait-unknown",
        ProcessFault::ChannelUnknown => "channel-unknown",
        ProcessFault::KillFailure => "kill-failure",
        ProcessFault::CleanupFailure => "cleanup-failure",
    }
}

#[derive(Debug, Clone, Copy)]
struct ExpectedMergeChildOrdinals {
    commit_tree: u64,
    inspect_commit: u64,
}

static EXPECTED_MERGE_CHILD_ORDINALS: tokio::sync::OnceCell<ExpectedMergeChildOrdinals> =
    tokio::sync::OnceCell::const_new();

async fn shared_expected_merge_child_ordinals() -> ExpectedMergeChildOrdinals {
    *EXPECTED_MERGE_CHILD_ORDINALS
        .get_or_init(discover_expected_merge_child_ordinals)
        .await
}

async fn discover_expected_merge_child_ordinals() -> ExpectedMergeChildOrdinals {
    let phase = PreparedExpectedMergePhase::new(
        "process-fault-expected-merge-discovery",
        "123e4567-e89b-12d3-a456-426614174060",
    )
    .await;
    let controller =
        ProcessFaultController::for_child(u64::MAX, ProcessFault::BeforeSpawn).unwrap();
    let observed = controller.scope(phase.reprove_preflight()).await;
    assert_eq!(observed, phase.preflight);
    let preflight_children = admitted_child_count(&controller);
    assert!(preflight_children > 0);
    assert!(
        controller
            .events()
            .iter()
            .all(|event| !matches!(event.kind(), ProcessFaultEventKind::Injected(_)))
    );
    assert_zero_live_children(&phase.source).await;
    assert_zero_live_scope(&phase.target_process_scope).await;

    // `build_expected_delivery_merge` starts with exactly the same committed
    // preflight call measured above, followed immediately by commit-tree and
    // the single cat-file inspection child.
    ExpectedMergeChildOrdinals {
        commit_tree: preflight_children + 1,
        inspect_commit: preflight_children + 2,
    }
}

async fn assert_expected_merge_child_fault_matrix(
    name: &str,
    task_id: &str,
    fault_child_ordinal: u64,
) {
    let phase = PreparedExpectedMergePhase::new(name, task_id).await;
    let approved_source = phase.source_snapshot();
    let approved_target = phase.target_snapshot();
    let baseline_objects = phase.object_inventory();
    let mut deterministic_expected = None;

    for fault in SOURCE_PROCESS_FAULTS {
        assert_expected_merge_child_fault_case(
            &phase,
            &approved_source,
            &approved_target,
            &baseline_objects,
            fault_child_ordinal,
            fault,
            &mut deterministic_expected,
        )
        .await;
    }
}

async fn assert_one_expected_merge_child_fault(
    name: &str,
    task_id: &str,
    fault_child_ordinal: u64,
    fault: ProcessFault,
) {
    let phase = PreparedExpectedMergePhase::new(name, task_id).await;
    let approved_source = phase.source_snapshot();
    let approved_target = phase.target_snapshot();
    let baseline_objects = phase.object_inventory();
    let mut deterministic_expected = None;
    assert_expected_merge_child_fault_case(
        &phase,
        &approved_source,
        &approved_target,
        &baseline_objects,
        fault_child_ordinal,
        fault,
        &mut deterministic_expected,
    )
    .await;
}

#[allow(clippy::too_many_arguments)]
async fn assert_expected_merge_child_fault_case(
    phase: &PreparedExpectedMergePhase,
    approved_source: &RepositorySnapshot,
    approved_target: &TargetCheckoutSnapshot,
    baseline_objects: &BTreeMap<String, String>,
    fault_child_ordinal: u64,
    fault: ProcessFault,
    deterministic_expected: &mut Option<String>,
) {
    let controller = ProcessFaultController::for_child(fault_child_ordinal, fault).unwrap();
    let error = controller.scope(phase.build_expected()).await.unwrap_err();
    assert_eq!(error.code(), pre_mutation_error_code(fault), "{fault:?}");
    assert_eq!(&phase.source_snapshot(), approved_source, "{fault:?}");
    assert_eq!(&phase.target_snapshot(), approved_target, "{fault:?}");
    assert_only_one_unreachable_expected_commit_may_be_added(
        &phase.fixture.repository,
        baseline_objects,
        &phase.object_inventory(),
        &format!("child {fault_child_ordinal} {fault:?} error"),
    );
    assert_controlled_child_was_reaped(
        &controller,
        fault_child_ordinal,
        Some(fault_child_ordinal),
        fault,
    )
    .await;

    let retried = phase.build_expected().await.unwrap_or_else(|retry_error| {
        panic!(
            "expected-merge retry after child {fault_child_ordinal} {fault:?}: {}",
            retry_error.code(),
        )
    });
    if let Some(expected) = deterministic_expected.as_deref() {
        assert_eq!(retried.object_id(), expected, "{fault:?}");
    } else {
        *deterministic_expected = Some(retried.object_id().to_owned());
    }
    assert_eq!(&phase.source_snapshot(), approved_source, "retry {fault:?}");
    assert_eq!(&phase.target_snapshot(), approved_target, "retry {fault:?}");
    assert_only_one_unreachable_expected_commit_may_be_added(
        &phase.fixture.repository,
        baseline_objects,
        &phase.object_inventory(),
        &format!("child {fault_child_ordinal} {fault:?} retry"),
    );
    assert_zero_live_children(&phase.source).await;
    assert_zero_live_scope(&phase.target_process_scope).await;
}

#[tokio::test]
async fn source_open_fault_matrix_is_read_only_retryable_and_leaves_no_live_child() {
    let phase = UnopenedSourcePhase::new(
        "process-fault-source-open",
        "123e4567-e89b-12d3-a456-426614174031",
    )
    .await;
    let approved = phase.snapshot();

    for fault in SOURCE_PROCESS_FAULTS {
        let provisioner = phase.provisioner();
        let controller = ProcessFaultController::for_child(1, fault).unwrap();
        let error = controller
            .scope(provisioner.open_delivery_source(
                &phase.source.reservation,
                phase.source.approved_fingerprint,
                CancellationToken::new(),
            ))
            .await
            .unwrap_err();

        assert_eq!(error.code(), pre_mutation_error_code(fault), "{fault:?}");
        assert_eq!(phase.snapshot(), approved, "{fault:?}");
        assert_controlled_child_was_reaped(&controller, 1, Some(1), fault).await;
        drop(provisioner);

        let retry = phase.provisioner();
        let opened = retry
            .open_delivery_source(
                &phase.source.reservation,
                phase.source.approved_fingerprint,
                CancellationToken::new(),
            )
            .await
            .unwrap_or_else(|error| panic!("source retry after {fault:?}: {}", error.code()));
        drop(opened);
        drop(retry);
        assert_eq!(phase.snapshot(), approved, "retry after {fault:?}");
        assert_zero_live_children(&phase.source).await;
    }
}

#[tokio::test]
async fn target_open_fault_matrix_is_read_only_retryable_and_leaves_no_live_child() {
    let fixture = Fixture::new("process-fault-target-open").await;
    let worktrees = fixture.fresh_worktree_provisioner();
    let process_scope = fixture.task_process_scope();
    let provisioner = target_provisioner(&fixture, &worktrees, process_scope.clone());
    let request = target_request(&fixture);
    let approved = target_checkout_snapshot(&fixture);

    for fault in SOURCE_PROCESS_FAULTS {
        let controller = ProcessFaultController::for_child(1, fault).unwrap();
        let error = controller
            .scope(provisioner.open_delivery_target(&request, CancellationToken::new()))
            .await
            .unwrap_err();

        assert_eq!(
            error.code(),
            target_observation_error_code(fault),
            "{fault:?}"
        );
        assert_eq!(target_checkout_snapshot(&fixture), approved, "{fault:?}");
        assert_controlled_child_was_reaped(&controller, 1, Some(1), fault).await;

        let opened = provisioner
            .open_delivery_target(&request, CancellationToken::new())
            .await
            .unwrap_or_else(|error| panic!("target retry after {fault:?}: {}", error.code()));
        drop(opened);
        assert_eq!(
            target_checkout_snapshot(&fixture),
            approved,
            "retry after {fault:?}",
        );
        assert_zero_live_scope(&process_scope).await;
    }
}

#[tokio::test]
async fn expected_merge_commit_tree_fault_matrix_preserves_only_a_dangling_object_and_retries() {
    let ordinals = shared_expected_merge_child_ordinals().await;
    assert_expected_merge_child_fault_matrix(
        "process-fault-expected-merge-commit-tree",
        "123e4567-e89b-12d3-a456-426614174061",
        ordinals.commit_tree,
    )
    .await;
}

macro_rules! expected_merge_cat_file_fault_test {
    ($name:ident, $fixture_name:literal, $task_id:literal, $fault:expr) => {
        #[tokio::test]
        async fn $name() {
            let ordinals = shared_expected_merge_child_ordinals().await;
            assert_one_expected_merge_child_fault(
                $fixture_name,
                $task_id,
                ordinals.inspect_commit,
                $fault,
            )
            .await;
        }
    };
}

expected_merge_cat_file_fault_test!(
    expected_merge_cat_file_fault_before_spawn_is_bounded_and_retryable,
    "process-fault-expected-cat-before-spawn",
    "123e4567-e89b-12d3-a456-426614174062",
    ProcessFault::BeforeSpawn
);
expected_merge_cat_file_fault_test!(
    expected_merge_cat_file_fault_after_spawn_unknown_is_bounded_and_retryable,
    "process-fault-expected-cat-after-spawn",
    "123e4567-e89b-12d3-a456-426614174063",
    ProcessFault::AfterSpawnUnknown
);
expected_merge_cat_file_fault_test!(
    expected_merge_cat_file_fault_stdout_overflow_is_bounded_and_retryable,
    "process-fault-expected-cat-stdout-overflow",
    "123e4567-e89b-12d3-a456-426614174064",
    ProcessFault::StdoutOverflow
);
expected_merge_cat_file_fault_test!(
    expected_merge_cat_file_fault_deadline_is_bounded_and_retryable,
    "process-fault-expected-cat-deadline",
    "123e4567-e89b-12d3-a456-426614174065",
    ProcessFault::Deadline
);
expected_merge_cat_file_fault_test!(
    expected_merge_cat_file_fault_wait_unknown_is_bounded_and_retryable,
    "process-fault-expected-cat-wait-unknown",
    "123e4567-e89b-12d3-a456-426614174066",
    ProcessFault::WaitUnknown
);
expected_merge_cat_file_fault_test!(
    expected_merge_cat_file_fault_channel_unknown_is_bounded_and_retryable,
    "process-fault-expected-cat-channel-unknown",
    "123e4567-e89b-12d3-a456-426614174067",
    ProcessFault::ChannelUnknown
);
expected_merge_cat_file_fault_test!(
    expected_merge_cat_file_fault_kill_failure_is_bounded_and_retryable,
    "process-fault-expected-cat-kill-failure",
    "123e4567-e89b-12d3-a456-426614174068",
    ProcessFault::KillFailure
);
expected_merge_cat_file_fault_test!(
    expected_merge_cat_file_fault_cleanup_failure_is_bounded_and_retryable,
    "process-fault-expected-cat-cleanup-failure",
    "123e4567-e89b-12d3-a456-426614174069",
    ProcessFault::CleanupFailure
);

#[tokio::test]
async fn preflight_fault_matrix_preserves_both_checkouts_and_retries_ready() {
    let phase = PreparedPreflightPhase::new(
        "process-fault-preflight",
        "123e4567-e89b-12d3-a456-426614174050",
    )
    .await;
    let approved_source = phase.source_snapshot();
    let approved_target = phase.target_snapshot();

    for fault in SOURCE_PROCESS_FAULTS {
        let controller = ProcessFaultController::for_child(1, fault).unwrap();
        let error = controller
            .scope(preflight_delivery_merge(
                &phase.source_provisioner,
                &phase.target_provisioner,
                &phase.target,
                DeliveryPreflightSource::candidate(&phase.opened_source, &phase.candidate),
                CancellationToken::new(),
            ))
            .await
            .unwrap_err();

        assert_eq!(
            error.code(),
            preflight_observation_error_code(fault),
            "{fault:?}"
        );
        assert_eq!(phase.source_snapshot(), approved_source, "{fault:?}");
        assert_eq!(phase.target_snapshot(), approved_target, "{fault:?}");
        assert_controlled_child_was_reaped(&controller, 1, Some(1), fault).await;

        let retried = preflight_delivery_merge(
            &phase.source_provisioner,
            &phase.target_provisioner,
            &phase.target,
            DeliveryPreflightSource::candidate(&phase.opened_source, &phase.candidate),
            CancellationToken::new(),
        )
        .await
        .unwrap_or_else(|error| panic!("preflight retry after {fault:?}: {}", error.code()));
        assert!(retried.is_ready(), "{fault:?}");
        assert_eq!(
            phase.source_snapshot(),
            approved_source,
            "retry after {fault:?}"
        );
        assert_eq!(
            phase.target_snapshot(),
            approved_target,
            "retry after {fault:?}"
        );
        assert_zero_live_children(&phase.source).await;
    }
}

#[tokio::test]
async fn object_pending_commit_children_are_deterministically_retryable_after_every_fault() {
    let phase = OpenedSourcePhase::new(
        "process-fault-source-object",
        "123e4567-e89b-12d3-a456-426614174032",
    )
    .await
    .into_object_pending()
    .await;
    let approved = phase.snapshot();
    let mut deterministic_object = None;

    // `build_source_commit` has exactly two children: commit-tree creates an
    // unreachable deterministic object, then cat-file proves its exact shape.
    for fault_child_ordinal in [1, 2] {
        for fault in SOURCE_PROCESS_FAULTS {
            let controller = ProcessFaultController::for_child(fault_child_ordinal, fault).unwrap();
            let error = controller
                .scope(phase.provisioner.build_source_commit(
                    &phase.opened,
                    &phase.candidate,
                    &phase.input,
                    CancellationToken::new(),
                ))
                .await
                .unwrap_err();

            assert_eq!(error.code(), pre_mutation_error_code(fault), "{fault:?}");
            assert_eq!(phase.snapshot(), approved, "{fault:?}");
            assert_controlled_child_was_reaped(
                &controller,
                fault_child_ordinal,
                Some(fault_child_ordinal),
                fault,
            )
            .await;

            let retried = phase
                .provisioner
                .build_source_commit(
                    &phase.opened,
                    &phase.candidate,
                    &phase.input,
                    CancellationToken::new(),
                )
                .await
                .unwrap_or_else(|error| {
                    panic!(
                        "object retry after child {fault_child_ordinal} {fault:?}: {}",
                        error.code()
                    )
                });
            if let Some(expected) = deterministic_object.as_deref() {
                assert_eq!(retried.object_id(), expected, "{fault:?}");
            } else {
                deterministic_object = Some(retried.object_id().to_owned());
            }
            assert_eq!(phase.snapshot(), approved, "retry after {fault:?}");
            assert_zero_live_children(&phase.source).await;
        }
    }
}

#[tokio::test]
async fn candidate_write_tree_faults_preserve_real_state_and_retry_the_same_tree() {
    let (write_tree_ordinal, expected_tree) = discover_candidate_write_tree_ordinal().await;
    let phase = OpenedSourcePhase::new(
        "process-fault-candidate-write-tree",
        "123e4567-e89b-12d3-a456-426614174034",
    )
    .await;
    let approved = phase.snapshot();

    for fault in SOURCE_PROCESS_FAULTS {
        let controller = ProcessFaultController::for_child(write_tree_ordinal, fault).unwrap();
        let error = controller
            .scope(
                phase
                    .provisioner
                    .build_candidate_tree(&phase.opened, CancellationToken::new()),
            )
            .await
            .unwrap_err();

        assert_eq!(error.code(), pre_mutation_error_code(fault), "{fault:?}");
        assert_eq!(phase.snapshot(), approved, "{fault:?}");
        assert_controlled_child_was_reaped(
            &controller,
            write_tree_ordinal,
            Some(write_tree_ordinal),
            fault,
        )
        .await;

        let retried = phase
            .provisioner
            .build_candidate_tree(&phase.opened, CancellationToken::new())
            .await
            .unwrap_or_else(|error| panic!("candidate retry after {fault:?}: {}", error.code()));
        assert_eq!(retried.object_id(), expected_tree, "{fault:?}");
        assert_eq!(phase.snapshot(), approved, "retry after {fault:?}");
        assert_zero_live_children(&phase.source).await;
    }
}

#[tokio::test]
async fn commit_pending_recovery_observation_faults_are_pure_and_retryable() {
    let phase = OpenedSourcePhase::new(
        "process-fault-recovery-observation",
        "123e4567-e89b-12d3-a456-426614174035",
    )
    .await
    .into_object_pending()
    .await
    .into_commit_pending()
    .await;
    let approved = phase.snapshot();
    let (provisioner, recovery) = phase
        .open_recovery(DeliverySourcePendingState::CommitPending)
        .await;

    for fault in SOURCE_PROCESS_FAULTS {
        let controller = ProcessFaultController::for_child(1, fault).unwrap();
        let observed = controller
            .scope(provisioner.classify_source_recovery(&recovery, CancellationToken::new()))
            .await;

        if fault == ProcessFault::BeforeSpawn {
            assert_eq!(
                observed.unwrap(),
                DeliverySourceRecoveryDisposition::ReconciliationRequired,
                "{fault:?}",
            );
        } else {
            assert_eq!(
                observed.unwrap_err().code(),
                pre_mutation_error_code(fault),
                "{fault:?}",
            );
        }
        assert_eq!(phase.snapshot(), approved, "{fault:?}");
        let expected_children = (fault != ProcessFault::BeforeSpawn).then_some(1);
        assert_controlled_child_was_reaped(&controller, 1, expected_children, fault).await;
        assert_eq!(
            provisioner
                .classify_source_recovery(&recovery, CancellationToken::new())
                .await
                .unwrap_or_else(|error| {
                    panic!("recovery retry after {fault:?}: {}", error.code())
                }),
            DeliverySourceRecoveryDisposition::Continue,
            "{fault:?}",
        );
        assert_eq!(phase.snapshot(), approved, "retry after {fault:?}");
        assert_zero_live_children(&phase.source).await;
    }
}

async fn assert_commit_pending_apply_fault(
    ordinals: ApplyChildOrdinals,
    fault: ProcessFault,
    case_index: u8,
) {
    let name = format!("process-fault-apply-{}", fault_name(fault));
    let task_id = format!(
        "123e4567-e89b-12d3-a456-4266141740{:02x}",
        0x40_u8 + case_index
    );
    let phase = OpenedSourcePhase::new(&name, &task_id)
        .await
        .into_object_pending()
        .await
        .into_commit_pending()
        .await;
    let (provisioner, recovery) = phase
        .open_recovery(DeliverySourcePendingState::CommitPending)
        .await;
    let fault_child_ordinal = apply_fault_ordinal(ordinals, fault);
    let controller = ProcessFaultController::for_child(fault_child_ordinal, fault).unwrap();

    let error = controller
        .scope(provisioner.apply_source_commit(&recovery, CancellationToken::new()))
        .await
        .unwrap_err();
    assert_eq!(
        error.code(),
        post_real_index_mutation_error_code(fault),
        "{fault:?}",
    );
    assert_controlled_child_was_reaped(
        &controller,
        fault_child_ordinal,
        Some(fault_child_ordinal),
        fault,
    )
    .await;
    drop(recovery);
    drop(provisioner);

    // Re-open the durable CommitPending intent after the simulated crash.
    // Classification is observational and must happen before any retry.
    let before_classification = phase.snapshot();
    let (recovery_provisioner, reopened) = phase
        .open_recovery(DeliverySourcePendingState::CommitPending)
        .await;
    let disposition = recovery_provisioner
        .classify_source_recovery(&reopened, CancellationToken::new())
        .await
        .unwrap_or_else(|error| {
            panic!("post-fault classification for {fault:?}: {}", error.code())
        });
    assert!(
        expected_apply_recovery_dispositions(fault).contains(&disposition),
        "unexpected recovery disposition {disposition:?} for {fault:?}",
    );
    assert_eq!(phase.snapshot(), before_classification, "{fault:?}");

    let retried = recovery_provisioner
        .apply_source_commit(&reopened, CancellationToken::new())
        .await
        .unwrap_or_else(|error| {
            panic!("post-classification retry for {fault:?}: {}", error.code())
        });
    if disposition == DeliverySourceRecoveryDisposition::ReconciliationRequired {
        assert_eq!(
            retried,
            DeliverySourceRecoveryDisposition::ReconciliationRequired,
            "{fault:?}",
        );
        assert_eq!(phase.snapshot(), before_classification, "{fault:?}");
    } else {
        assert_eq!(
            retried,
            DeliverySourceRecoveryDisposition::Applied,
            "{fault:?}",
        );
    }
    assert_zero_live_children(&phase.source).await;
}

macro_rules! commit_pending_apply_fault_test {
    ($name:ident, $fault:expr, $case_index:expr) => {
        #[tokio::test]
        async fn $name() {
            assert_commit_pending_apply_fault(
                shared_apply_child_ordinals().await,
                $fault,
                $case_index,
            )
            .await;
        }
    };
}

commit_pending_apply_fault_test!(
    commit_pending_before_spawn_remains_retryable_then_applies,
    ProcessFault::BeforeSpawn,
    0
);
commit_pending_apply_fault_test!(
    commit_pending_after_spawn_unknown_reconciles_from_observed_state,
    ProcessFault::AfterSpawnUnknown,
    1
);
commit_pending_apply_fault_test!(
    commit_pending_stdout_overflow_after_refresh_resumes_from_stage_complete,
    ProcessFault::StdoutOverflow,
    2
);
commit_pending_apply_fault_test!(
    commit_pending_refresh_deadline_reconciles_from_observed_state,
    ProcessFault::Deadline,
    3
);
commit_pending_apply_fault_test!(
    commit_pending_wait_unknown_at_cas_reconciles_from_observed_state,
    ProcessFault::WaitUnknown,
    4
);
commit_pending_apply_fault_test!(
    commit_pending_channel_unknown_after_cas_recovers_applied,
    ProcessFault::ChannelUnknown,
    5
);
commit_pending_apply_fault_test!(
    commit_pending_kill_failure_after_refresh_stops_then_resumes,
    ProcessFault::KillFailure,
    6
);
commit_pending_apply_fault_test!(
    commit_pending_cleanup_failure_after_cas_stops_then_recovers_applied,
    ProcessFault::CleanupFailure,
    7
);

#[tokio::test]
async fn source_fault_harness_preserves_all_three_durable_phase_boundaries() {
    let opened = OpenedSourcePhase::new(
        "process-fault-phase-harness",
        "123e4567-e89b-12d3-a456-426614174030",
    )
    .await;
    let opened_snapshot = opened.snapshot();
    assert_zero_live_children(&opened.source).await;

    let object_pending = opened.into_object_pending().await;
    assert_eq!(object_pending.snapshot(), opened_snapshot);
    assert_zero_live_children(&object_pending.source).await;

    let commit_pending = object_pending.into_commit_pending().await;
    assert_eq!(commit_pending.snapshot(), opened_snapshot);
    assert_zero_live_children(&commit_pending.source).await;

    let (provisioner, recovery) = commit_pending
        .open_recovery(DeliverySourcePendingState::CommitPending)
        .await;
    assert_eq!(
        provisioner
            .classify_source_recovery(&recovery, CancellationToken::new())
            .await
            .unwrap(),
        DeliverySourceRecoveryDisposition::Continue,
    );
    assert_eq!(commit_pending.snapshot(), opened_snapshot);
    assert!(!commit_pending.expected.object_id().is_empty());
    drop(recovery);
    drop(provisioner);
    assert_zero_live_children(&commit_pending.source).await;
}
