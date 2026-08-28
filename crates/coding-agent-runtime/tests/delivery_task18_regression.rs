mod delivery_source_support;

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use coding_agent_runtime::{
    DeliveryCandidateTree, DeliveryMergeInput, DeliveryMergeOutcome, DeliveryPreflightResult,
    DeliveryPreflightSource, DeliverySourceCapability, DeliverySourceCommit,
    DeliverySourceCommitInput, DeliverySourcePendingState, DeliverySourceProvisioner,
    DeliverySourceRecoveryDisposition, DeliverySourceRecoveryIntent, DeliveryTargetProvisioner,
    DeliveryTargetRequest, DeliveryUnlockPendingAuthorizer, DeliveryUnlockPendingDisposition,
    DeliveryWorktreeCleanupIntent, DeliveryWorktreeCleanupProvisioner, FingerprintLimits,
    ProcessLimits, ProcessLivenessScope, WorktreeProvisioner, apply_expected_delivery_merge,
    authorize_persisted_delivery_unlock, build_expected_delivery_merge, preflight_delivery_merge,
};
use delivery_source_support::{Fixture, ReviewedDirtySource, delivery_source_limits, git_line};
use tokio_util::sync::CancellationToken;

const SHA256_TASK_ID: &str = "123e4567-e89b-12d3-a456-426614174218";
const EPOCH_SECONDS: i64 = 1_700_000_018;
const REPEATED_PREFLIGHTS: usize = 8;

struct PreparedCandidate {
    source: ReviewedDirtySource,
    provisioner: DeliverySourceProvisioner,
    opened: DeliverySourceCapability,
    candidate: DeliveryCandidateTree,
    delivery_process_scope: ProcessLivenessScope,
}

impl PreparedCandidate {
    async fn new(fixture: &Fixture, task_id: &str) -> Self {
        let source = fixture.reviewed_dirty_source(task_id).await;
        let delivery_process_scope = delivery_process_scope(&source.worker_process_scope);
        let provisioner =
            source_provisioner(fixture, &source.worktrees, delivery_process_scope.clone());
        let opened = provisioner
            .open_delivery_source(
                &source.reservation,
                source.approved_fingerprint,
                CancellationToken::new(),
            )
            .await
            .unwrap();
        let candidate = provisioner
            .build_candidate_tree(&opened, CancellationToken::new())
            .await
            .unwrap();
        Self {
            source,
            provisioner,
            opened,
            candidate,
            delivery_process_scope,
        }
    }
}

struct PreparedCommittedSource {
    source: ReviewedDirtySource,
    provisioner: DeliverySourceProvisioner,
    opened: DeliverySourceCapability,
    candidate: DeliveryCandidateTree,
    commit: DeliverySourceCommit,
    input: DeliverySourceCommitInput,
    delivery_process_scope: ProcessLivenessScope,
}

impl PreparedCommittedSource {
    async fn new(fixture: &Fixture, task_id: &str) -> Self {
        let source = fixture.reviewed_dirty_source(task_id).await;
        let delivery_process_scope = delivery_process_scope(&source.worker_process_scope);
        let provisioner =
            source_provisioner(fixture, &source.worktrees, delivery_process_scope.clone());
        let opened = provisioner
            .open_delivery_source(
                &source.reservation,
                source.approved_fingerprint,
                CancellationToken::new(),
            )
            .await
            .unwrap();
        let candidate = provisioner
            .build_candidate_tree(&opened, CancellationToken::new())
            .await
            .unwrap();
        let input = DeliverySourceCommitInput::try_new(task_id, 1, EPOCH_SECONDS).unwrap();
        let commit = provisioner
            .build_source_commit(&opened, &candidate, &input, CancellationToken::new())
            .await
            .unwrap();
        let intent = DeliverySourceRecoveryIntent::from_source(
            DeliverySourcePendingState::CommitPending,
            &opened,
            &candidate,
            Some(&commit),
            input.clone(),
        )
        .unwrap();
        let recovery = provisioner
            .open_delivery_source_for_recovery(
                &source.reservation,
                &intent,
                CancellationToken::new(),
            )
            .await
            .unwrap();

        assert_eq!(
            provisioner
                .apply_source_commit(&recovery, CancellationToken::new())
                .await
                .unwrap(),
            DeliverySourceRecoveryDisposition::Applied,
        );
        drop(recovery);

        Self {
            source,
            provisioner,
            opened,
            candidate,
            commit,
            input,
            delivery_process_scope,
        }
    }
}

#[tokio::test]
async fn real_sha256_repository_runs_source_preflight_and_merge_with_exact_oid_width() {
    let fixture = match Fixture::try_new_sha256("task18-sha256-pipeline").await {
        Ok(fixture) => fixture,
        Err(error) if error.explicitly_reports_unsupported_object_format() => {
            eprintln!(
                "SKIP[git-sha256-unavailable]: the exact Git SHA-256 init probe failed: {error}"
            );
            return;
        }
        Err(error) => panic!(
            "Git SHA-256 initialization failed without an explicit unsupported-capability diagnostic: {error}"
        ),
    };
    assert_eq!(
        git_line(&fixture.repository, &["rev-parse", "--show-object-format"]),
        "sha256",
        "the fixture must not silently fall back to SHA-1"
    );

    let prepared = PreparedCommittedSource::new(&fixture, SHA256_TASK_ID).await;
    assert_sha256_oid(prepared.candidate.object_id());
    assert_sha256_oid(prepared.commit.object_id());
    assert_eq!(
        git_line(prepared.source.worktree_path(), &["rev-parse", "HEAD"]),
        prepared.commit.object_id(),
    );

    let target_provisioner = target_provisioner(
        &fixture,
        &prepared.source.worktrees,
        prepared.delivery_process_scope.clone(),
    );
    let target = target_provisioner
        .open_delivery_target(&target_request(&fixture), CancellationToken::new())
        .await
        .unwrap();
    assert_sha256_oid(target.head_id());

    let preflight = preflight_delivery_merge(
        &prepared.provisioner,
        &target_provisioner,
        &target,
        DeliveryPreflightSource::committed(
            &prepared.opened,
            &prepared.candidate,
            &prepared.commit,
            &prepared.input,
        ),
        CancellationToken::new(),
    )
    .await
    .unwrap_or_else(|error| panic!("SHA-256 preflight failed: {}", error.code()));

    assert!(preflight.is_ready());
    assert_eq!(preflight.source_commit_id(), prepared.commit.object_id());
    assert_sha256_oid(preflight.source_commit_id());
    assert_sha256_oid(preflight.merge_base_id());
    assert_sha256_oid(preflight.candidate_merge_tree_id());

    let merge_input = DeliveryMergeInput::try_new(SHA256_TASK_ID, 1, EPOCH_SECONDS).unwrap();
    let expected = build_expected_delivery_merge(
        &prepared.provisioner,
        &target_provisioner,
        &prepared.opened,
        &target,
        &prepared.candidate,
        &prepared.commit,
        &prepared.input,
        &preflight,
        &merge_input,
        CancellationToken::new(),
    )
    .await
    .unwrap();
    assert_sha256_oid(expected.object_id());

    assert_eq!(
        apply_expected_delivery_merge(
            &prepared.provisioner,
            &target_provisioner,
            &prepared.opened,
            &target,
            &prepared.candidate,
            &prepared.commit,
            &prepared.input,
            &preflight,
            &expected,
            CancellationToken::new(),
        )
        .await
        .unwrap(),
        DeliveryMergeOutcome::Applied,
    );
    assert_eq!(
        git_line(&fixture.repository, &["rev-parse", "HEAD"]),
        expected.object_id(),
    );
    assert_eq!(
        git_line(
            &fixture.repository,
            &["rev-list", "--parents", "-n", "1", expected.object_id()],
        ),
        format!(
            "{} {} {}",
            expected.object_id(),
            target.head_id(),
            prepared.commit.object_id()
        ),
    );

    drop(target);
    drop(target_provisioner);
    let PreparedCommittedSource {
        source,
        provisioner,
        opened,
        candidate,
        commit,
        input,
        delivery_process_scope,
    } = prepared;
    assert_eq!(
        git_line(
            source.worktree_path(),
            &["status", "--porcelain=v2", "--untracked-files=all"],
        ),
        "",
    );
    let cleanup = DeliveryWorktreeCleanupProvisioner::from_worktree_provisioner(
        &source.worktrees,
        Arc::clone(&fixture.delivery_git),
        &fixture.runtime_directory,
        delivery_process_scope,
        process_limits(),
        delivery_source_limits(),
    )
    .unwrap();
    let sealed_worker = source
        .worker_process_scope
        .seal_task_scope(worker_task_id())
        .unwrap();
    let cleanup_intent = cleanup
        .capture_intent(
            &provisioner,
            &source.reservation,
            opened,
            &candidate,
            &commit,
            &input,
            &sealed_worker,
            CancellationToken::new(),
        )
        .await
        .unwrap();
    let unlock = authorize_persisted_delivery_unlock(&AllowUnlock, cleanup_intent)
        .await
        .unwrap();
    assert_eq!(
        cleanup
            .classify_delivery_unlock_pending(
                &provisioner,
                &unlock,
                &sealed_worker,
                CancellationToken::new(),
            )
            .await
            .unwrap(),
        DeliveryUnlockPendingDisposition::RetryExactUnlock,
        "SHA-256 cleanup classification must retain the authenticated repository format",
    );
}

#[tokio::test]
async fn repeated_candidate_preflight_only_adds_stable_unreachable_objects_without_auto_gc() {
    let fixture = Fixture::new("task18-repeated-preflight").await;
    let prepared = PreparedCandidate::new(&fixture, "123e4567-e89b-12d3-a456-426614174219").await;
    let target_provisioner = target_provisioner(
        &fixture,
        &prepared.source.worktrees,
        prepared.delivery_process_scope.clone(),
    );
    let target = target_provisioner
        .open_delivery_target(&target_request(&fixture), CancellationToken::new())
        .await
        .unwrap();
    let before_source = prepared.source.snapshot(&fixture.repository);
    let before_target = target_checkout_snapshot(&fixture.repository);
    let before_objects = object_inventory(&fixture.repository);
    let before_maintenance = maintenance_snapshot(&fixture.repository);

    let first = candidate_preflight(&prepared, &target_provisioner, &target).await;
    assert!(first.is_ready());
    let after_first_objects = object_inventory(&fixture.repository);
    let created = after_first_objects
        .iter()
        .filter(|(object_id, _)| !before_objects.contains_key(*object_id))
        .map(|(object_id, object_type)| (object_id.clone(), object_type.clone()))
        .collect::<BTreeMap<_, _>>();
    assert!(
        !created.is_empty(),
        "candidate preflight must materialize its unreachable source/merge objects"
    );
    let reachable = reachable_object_ids(&fixture.repository);
    for (object_id, object_type) in &created {
        assert!(
            matches!(object_type.as_str(), "commit" | "tree"),
            "preflight created an unexpected {object_type} object"
        );
        assert!(
            !reachable.contains(object_id),
            "preflight-only object unexpectedly became reachable: {object_id}"
        );
    }

    for iteration in 1..REPEATED_PREFLIGHTS {
        let repeated = candidate_preflight(&prepared, &target_provisioner, &target).await;
        assert_eq!(
            repeated, first,
            "preflight result drifted on repetition {iteration}"
        );
    }

    assert_eq!(
        object_inventory(&fixture.repository),
        after_first_objects,
        "deterministic repeated preflight must not accumulate new loose objects"
    );
    assert_eq!(prepared.source.snapshot(&fixture.repository), before_source);
    assert_eq!(target_checkout_snapshot(&fixture.repository), before_target);
    assert_eq!(
        maintenance_snapshot(&fixture.repository),
        before_maintenance,
        "preflight must not invoke automatic GC, repack, or commit-graph maintenance"
    );
}

#[tokio::test]
async fn temporary_index_cleanup_preserves_a_foreign_same_namespace_directory() {
    let fixture = Fixture::new("task18-temp-index-foreign").await;
    let source = fixture
        .reviewed_dirty_source("123e4567-e89b-12d3-a456-426614174220")
        .await;
    let delivery_process_scope = delivery_process_scope(&source.worker_process_scope);
    let provisioner = source_provisioner(&fixture, &source.worktrees, delivery_process_scope);
    let opened = provisioner
        .open_delivery_source(
            &source.reservation,
            source.approved_fingerprint,
            CancellationToken::new(),
        )
        .await
        .unwrap();
    let foreign = fixture
        .runtime_directory
        .join("delivery-temp-index-foreign-owned");
    std::fs::create_dir(&foreign).unwrap();
    std::fs::write(foreign.join("index"), b"foreign index bytes\n").unwrap();
    std::fs::write(foreign.join("owner-note"), b"not owned by delivery\n").unwrap();
    let before_foreign = snapshot_tree(&foreign);
    let before_namespace = temporary_index_namespace(&fixture.runtime_directory);

    let candidate = provisioner
        .build_candidate_tree(&opened, CancellationToken::new())
        .await
        .unwrap();

    assert_eq!(candidate.object_id().len(), 40);
    assert_eq!(snapshot_tree(&foreign), before_foreign);
    assert_eq!(
        temporary_index_namespace(&fixture.runtime_directory),
        before_namespace,
        "cleanup must remove only its retained temporary-index directory"
    );
}

struct AllowUnlock;

#[async_trait]
impl DeliveryUnlockPendingAuthorizer for AllowUnlock {
    type Error = ();

    async fn authorize_persisted_unlock_pending(
        &self,
        _intent: &DeliveryWorktreeCleanupIntent,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
}

async fn candidate_preflight(
    prepared: &PreparedCandidate,
    target_provisioner: &DeliveryTargetProvisioner,
    target: &coding_agent_runtime::DeliveryTargetCapability,
) -> DeliveryPreflightResult {
    preflight_delivery_merge(
        &prepared.provisioner,
        target_provisioner,
        target,
        DeliveryPreflightSource::candidate(&prepared.opened, &prepared.candidate),
        CancellationToken::new(),
    )
    .await
    .unwrap_or_else(|error| panic!("candidate preflight failed: {}", error.code()))
}

#[derive(Debug, PartialEq, Eq)]
struct TargetCheckoutSnapshot {
    refs: Vec<u8>,
    index: Vec<u8>,
    head: Vec<u8>,
    status: Vec<u8>,
    worktree: BTreeMap<PathBuf, Option<Vec<u8>>>,
}

fn target_checkout_snapshot(repository: &Path) -> TargetCheckoutSnapshot {
    TargetCheckoutSnapshot {
        refs: git_command_output(
            repository,
            &[
                "for-each-ref",
                "--format=%(refname)%00%(objectname)",
                "refs",
            ],
        ),
        index: std::fs::read(repository.join(".git/index")).unwrap(),
        head: std::fs::read(repository.join(".git/HEAD")).unwrap(),
        status: git_command_output(
            repository,
            &["status", "--porcelain=v2", "-z", "--untracked-files=all"],
        ),
        worktree: snapshot_target_worktree(repository),
    }
}

fn object_inventory(repository: &Path) -> BTreeMap<String, String> {
    let output = git_command_output(
        repository,
        &[
            "cat-file",
            "--batch-all-objects",
            "--batch-check=%(objectname) %(objecttype)",
        ],
    );
    String::from_utf8(output)
        .unwrap()
        .lines()
        .map(|line| {
            let (object_id, object_type) = line.split_once(' ').unwrap();
            (object_id.to_owned(), object_type.to_owned())
        })
        .collect()
}

fn reachable_object_ids(repository: &Path) -> BTreeSet<String> {
    String::from_utf8(git_command_output(
        repository,
        &["rev-list", "--objects", "--all"],
    ))
    .unwrap()
    .lines()
    .map(|line| {
        line.split_once(' ')
            .map_or(line, |(object_id, _)| object_id)
    })
    .map(ToOwned::to_owned)
    .collect()
}

#[derive(Debug, PartialEq, Eq)]
struct MaintenanceSnapshot {
    pack_directory: BTreeMap<PathBuf, Option<Vec<u8>>>,
    gc_log: Option<Vec<u8>>,
    commit_graph: Option<Vec<u8>>,
    multi_pack_index: Option<Vec<u8>>,
    gc_pid_present: bool,
}

fn maintenance_snapshot(repository: &Path) -> MaintenanceSnapshot {
    let git = repository.join(".git");
    MaintenanceSnapshot {
        pack_directory: snapshot_tree(&git.join("objects/pack")),
        gc_log: read_optional(&git.join("gc.log")),
        commit_graph: read_optional(&git.join("objects/info/commit-graph")),
        multi_pack_index: read_optional(&git.join("objects/pack/multi-pack-index")),
        gc_pid_present: git.join("gc.pid").exists(),
    }
}

fn read_optional(path: &Path) -> Option<Vec<u8>> {
    path.is_file().then(|| std::fs::read(path).unwrap())
}

fn temporary_index_namespace(
    runtime_directory: &Path,
) -> BTreeMap<PathBuf, BTreeMap<PathBuf, Option<Vec<u8>>>> {
    let mut namespace = BTreeMap::new();
    for entry in std::fs::read_dir(runtime_directory).unwrap() {
        let entry = entry.unwrap();
        let name = PathBuf::from(entry.file_name());
        if entry.file_type().unwrap().is_dir()
            && entry
                .file_name()
                .to_string_lossy()
                .starts_with("delivery-temp-index-")
        {
            namespace.insert(name, snapshot_tree(&entry.path()));
        }
    }
    namespace
}

fn snapshot_target_worktree(root: &Path) -> BTreeMap<PathBuf, Option<Vec<u8>>> {
    snapshot_tree_filtered(root, true)
}

fn snapshot_tree(root: &Path) -> BTreeMap<PathBuf, Option<Vec<u8>>> {
    snapshot_tree_filtered(root, false)
}

fn snapshot_tree_filtered(
    root: &Path,
    exclude_git_directory: bool,
) -> BTreeMap<PathBuf, Option<Vec<u8>>> {
    fn visit(
        root: &Path,
        directory: &Path,
        exclude_git_directory: bool,
        entries: &mut BTreeMap<PathBuf, Option<Vec<u8>>>,
    ) {
        let mut children = std::fs::read_dir(directory)
            .unwrap()
            .map(|entry| entry.unwrap())
            .collect::<Vec<_>>();
        children.sort_by_key(|entry| entry.file_name());
        for entry in children {
            if exclude_git_directory && entry.file_name() == ".git" {
                continue;
            }
            let path = entry.path();
            let relative = path.strip_prefix(root).unwrap().to_owned();
            if entry.file_type().unwrap().is_dir() {
                entries.insert(relative, None);
                visit(root, &path, exclude_git_directory, entries);
            } else {
                entries.insert(relative, Some(std::fs::read(path).unwrap()));
            }
        }
    }

    let mut entries = BTreeMap::new();
    visit(root, root, exclude_git_directory, &mut entries);
    entries
}

fn git_command_output(repository: &Path, arguments: &[&str]) -> Vec<u8> {
    let output = std::process::Command::new("git")
        .arg("-C")
        .arg(repository)
        .args(arguments)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "git fixture command failed: git -C {} {}\nstdout: {}\nstderr: {}",
        repository.display(),
        arguments.join(" "),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    output.stdout
}

fn assert_sha256_oid(object_id: &str) {
    assert_eq!(object_id.len(), 64, "expected a full SHA-256 object ID");
    assert!(
        object_id
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
        "expected a canonical lower-case hexadecimal object ID"
    );
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
        process_limits(),
        delivery_source_limits(),
    )
    .unwrap()
}

fn source_provisioner(
    fixture: &Fixture,
    worktrees: &WorktreeProvisioner,
    process_scope: ProcessLivenessScope,
) -> DeliverySourceProvisioner {
    DeliverySourceProvisioner::from_worktree_provisioner(
        worktrees,
        Arc::clone(&fixture.delivery_git),
        &fixture.runtime_directory,
        process_scope,
        process_limits(),
        delivery_source_limits(),
        fingerprint_limits(),
    )
    .unwrap()
}

fn delivery_process_scope(worker_process_scope: &ProcessLivenessScope) -> ProcessLivenessScope {
    let mut task_id = [0x35; 16];
    task_id[6] = 0x45;
    task_id[8] = 0xb5;
    worker_process_scope.sibling_task_scope(task_id).unwrap()
}

fn worker_task_id() -> [u8; 16] {
    let mut task_id = [0x25; 16];
    task_id[6] = 0x45;
    task_id[8] = 0xa5;
    task_id
}

fn target_request(fixture: &Fixture) -> DeliveryTargetRequest {
    DeliveryTargetRequest::try_new(
        git_line(&fixture.repository, &["symbolic-ref", "--short", "HEAD"]),
        git_line(&fixture.repository, &["rev-parse", "HEAD"]),
    )
    .unwrap()
}

fn process_limits() -> ProcessLimits {
    ProcessLimits::try_new(
        512 * 1024,
        512 * 1024,
        std::time::Duration::from_secs(30),
        std::time::Duration::from_secs(5),
    )
    .unwrap()
}

fn fingerprint_limits() -> FingerprintLimits {
    FingerprintLimits::try_new(
        std::time::Duration::from_secs(10),
        4_096,
        2 * 1024 * 1024,
        32 * 1024 * 1024,
    )
    .unwrap()
}
