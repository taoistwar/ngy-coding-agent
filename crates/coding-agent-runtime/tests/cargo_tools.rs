mod support;

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use coding_agent_core::RequiredCheckKind;
use coding_agent_runtime::{
    CargoRunStatus, CargoToolLimits, CargoTools, ExecutionDirectory, ProcessLimits,
    discover_toolchain,
};
use tokio_util::sync::CancellationToken;

#[tokio::test]
async fn typed_offline_metadata_check_and_test_use_only_catalogued_selectors() {
    let test_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target");
    let temporary = tempfile::Builder::new()
        .prefix("typed-cargo-")
        .tempdir_in(test_root)
        .unwrap();
    let root = temporary.path().canonicalize().unwrap();
    let runtime_directory = root.join("runtime");
    let workspace = root.join("workspace");
    let source = workspace.join("src");
    let tests = workspace.join("tests");
    let target = workspace.join("target");
    for directory in [&runtime_directory, &source, &tests, &target] {
        std::fs::create_dir_all(directory).unwrap();
    }
    std::fs::write(
        workspace.join("Cargo.toml"),
        b"[workspace]\n\n[package]\nname = \"typed_fixture\"\nversion = \"0.1.0\"\nedition = \"2024\"\n",
    )
    .unwrap();
    std::fs::write(source.join("lib.rs"), b"pub fn answer() -> u32 { 42 }\n").unwrap();
    std::fs::write(
        tests.join("passing.rs"),
        b"#[test]\nfn passes() { assert_eq!(typed_fixture::answer(), 42); }\n",
    )
    .unwrap();
    std::fs::write(
        tests.join("slow.rs"),
        b"#[test]\nfn waits() { std::thread::sleep(std::time::Duration::from_secs(30)); }\n",
    )
    .unwrap();

    let rustc = concrete_rustc();
    let git = path_executable(if cfg!(windows) { "git.exe" } else { "git" });
    let toolchain = discover_toolchain(&runtime_directory, Some(&rustc), Some(&git))
        .await
        .unwrap();
    let tools = CargoTools::from_trusted_capabilities(
        &toolchain,
        Arc::new(ExecutionDirectory::open(&workspace).unwrap()),
        Arc::new(ExecutionDirectory::open(&target).unwrap()),
        &runtime_directory,
        ProcessLimits::try_new(
            512 * 1024,
            512 * 1024,
            Duration::from_secs(20),
            Duration::from_secs(3),
        )
        .unwrap(),
        CargoToolLimits::try_new(Duration::from_secs(5), 8, 32, 128).unwrap(),
    )
    .unwrap();

    let catalog = tools.catalog(CancellationToken::new()).await.unwrap();
    assert_eq!(catalog.packages().len(), 1);
    assert_eq!(catalog.packages()[0].name(), "typed_fixture");
    assert_eq!(
        catalog.packages()[0].integration_tests(),
        &["passing".to_owned(), "slow".to_owned()]
    );
    let selectors = catalog.required_check_selectors().unwrap();
    assert!(selectors.iter().any(|selector| {
        selector.kind() == RequiredCheckKind::CargoCheck && selector.package().is_none()
    }));
    assert!(selectors.iter().any(|selector| {
        selector.kind() == RequiredCheckKind::CargoTest
            && selector.package().is_none()
            && selector.integration_test().is_none()
    }));
    assert!(selectors.iter().any(|selector| {
        selector.package() == Some("typed_fixture")
            && selector.integration_test() == Some("passing")
    }));

    let check = tools
        .check(
            Some("typed_fixture"),
            Duration::from_secs(10),
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(check.status, CargoRunStatus::Passed, "{check:?}");

    let passing = tools
        .test(
            Some("typed_fixture"),
            Some("passing"),
            Duration::from_secs(10),
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(passing.status, CargoRunStatus::Passed, "{passing:?}");

    let package_required = tools
        .test(
            None,
            Some("passing"),
            Duration::from_secs(10),
            CancellationToken::new(),
        )
        .await
        .unwrap_err();
    assert_eq!(package_required.code(), "COMMAND_NOT_ALLOWED");

    let workspace_test = tools
        .test(
            None,
            None,
            Duration::from_secs(10),
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(workspace_test.status, CargoRunStatus::TimedOut);

    std::fs::write(
        tests.join("passing.rs"),
        b"#[test]\nfn fails() { assert_eq!(typed_fixture::answer(), 0); }\n",
    )
    .unwrap();
    let failing = tools
        .test(
            Some("typed_fixture"),
            Some("passing"),
            Duration::from_secs(10),
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(failing.status, CargoRunStatus::Failed, "{failing:?}");

    let timed_out = tools
        .test(
            Some("typed_fixture"),
            Some("slow"),
            Duration::from_secs(10),
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(timed_out.status, CargoRunStatus::TimedOut, "{timed_out:?}");

    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let cancelled = tools
        .test(None, None, Duration::from_secs(1), cancellation)
        .await
        .unwrap_err();
    assert_eq!(cancelled.code(), "COMMAND_CANCELLED");
}

fn concrete_rustc() -> PathBuf {
    let output =
        support::command_output(Command::new("rustc").args(["--print", "sysroot"])).unwrap();
    assert!(output.status.success());
    PathBuf::from(String::from_utf8(output.stdout).unwrap().trim())
        .join("bin")
        .join(if cfg!(windows) { "rustc.exe" } else { "rustc" })
        .canonicalize()
        .unwrap()
}

fn path_executable(name: &str) -> PathBuf {
    std::env::split_paths(&std::env::var_os("PATH").unwrap())
        .map(|directory| directory.join(name))
        .find(|candidate| candidate.is_file())
        .unwrap()
        .canonicalize()
        .unwrap()
}
