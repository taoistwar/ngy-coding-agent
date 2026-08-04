#![cfg(feature = "test-support")]

mod support;

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use coding_agent_app::{InstanceLock, StartupOutcome, launch};
use coding_agent_runtime::ProcessLivenessDirectory;

const HELPER_TEST: &str = "independent_process_holds_liveness_sentinel_until_released";
const RUNTIME_ENV: &str = "CODING_AGENT_SENTINEL_HELPER_RUNTIME";
const READY_ENV: &str = "CODING_AGENT_SENTINEL_HELPER_READY";
const RELEASE_ENV: &str = "CODING_AGENT_SENTINEL_HELPER_RELEASE";

#[test]
#[ignore = "spawned by the startup liveness integration test"]
fn independent_process_holds_liveness_sentinel_until_released() {
    let runtime = required_helper_path(RUNTIME_ENV);
    let ready = required_helper_path(READY_ENV);
    let release = required_helper_path(RELEASE_ENV);
    let directory = ProcessLivenessDirectory::open(runtime)
        .expect("helper opens the shared process-liveness namespace");
    let scope = directory
        .instance_scope(*uuid::Uuid::new_v4().as_bytes())
        .expect("helper creates a valid independent instance scope");
    let _held = scope
        .hold_tree_for_test()
        .expect("helper holds an exclusive process-tree sentinel");
    std::fs::write(&ready, b"held\n").expect("helper publishes held-sentinel readiness");

    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    while !release.is_file() {
        assert!(
            std::time::Instant::now() < deadline,
            "helper timed out waiting for the release signal"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[tokio::test]
async fn primary_waits_for_an_independent_held_sentinel_before_db_recovery_or_admission() {
    let fixture = support::StartupFixture::new();
    fixture.prepare();
    let ready = fixture.paths.runtime_dir.join("sentinel-helper.ready");
    let release = fixture.paths.runtime_dir.join("sentinel-helper.release");

    let mut command = tokio::process::Command::new(
        std::env::current_exe().expect("resolve the current integration-test executable"),
    );
    command
        .args(["--exact", HELPER_TEST, "--ignored", "--nocapture"])
        .env(RUNTIME_ENV, &fixture.paths.runtime_dir)
        .env(READY_ENV, &ready)
        .env(RELEASE_ENV, &release)
        .stdin(Stdio::null())
        .kill_on_drop(true);
    let mut helper = command.spawn().expect("spawn independent sentinel helper");
    wait_for_file(&ready, "independent helper to hold the sentinel").await;

    let launch_task = tokio::spawn(launch(
        fixture.dependencies(support::StartupBehavior::default()),
    ));
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if InstanceLock::try_acquire(&fixture.paths.instance_lock)
                .expect("probe startup primary lock")
                .is_none()
            {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the blocked startup owns and retains the primary lock");

    assert_eq!(
        fixture.calls.store_opens(),
        0,
        "DB open, migration, and cold recovery must remain behind liveness proof"
    );
    assert_eq!(
        fixture.calls.listener_binds(),
        0,
        "listener publication and scheduler admission must remain behind liveness proof"
    );
    assert!(!launch_task.is_finished());

    std::fs::write(&release, b"release\n").expect("release independent sentinel helper");
    let helper_status = tokio::time::timeout(Duration::from_secs(5), helper.wait())
        .await
        .expect("independent helper exits after release")
        .expect("join independent sentinel helper");
    assert!(
        helper_status.success(),
        "sentinel helper failed: {helper_status}"
    );

    let outcome = tokio::time::timeout(Duration::from_secs(10), launch_task)
        .await
        .expect("startup resumes after independent cleanup proof")
        .expect("join resumed primary startup")
        .expect("resumed primary startup succeeds");
    let StartupOutcome::Primary(primary) = outcome else {
        panic!("the retained lock owner must continue as primary");
    };
    assert_eq!(fixture.calls.store_opens(), 1);
    assert_eq!(fixture.calls.listener_binds(), 1);
    let _ = primary.shutdown().await;
}

async fn wait_for_file(path: &Path, label: &str) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !path.is_file() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for {label}"));
}

fn required_helper_path(name: &'static str) -> PathBuf {
    std::env::var_os(name)
        .map(PathBuf::from)
        .unwrap_or_else(|| panic!("missing helper environment variable {name}"))
}
