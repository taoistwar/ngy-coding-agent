#![cfg(feature = "test-support")]

use std::fs;
use std::num::{NonZeroU32, NonZeroUsize};
use std::path::Path;
use std::sync::{Arc, Mutex};

use coding_agent_app::{
    ActorPausePoint, FakeScenario, FakeTaskRunner, FixedStartupRunnerFactory, LegacyV2Seed,
    PreActorStartupRunnerContext, ProcessRunnerMode, ProcessRuntimeConfig,
    ProcessRuntimeStorageConfig, ProcessStorageSample, ProcessTestConfig, ProcessTestEnvironment,
    RunContext, ShutdownOutcome, StartupDependencies, StartupOutcome, StartupRunnerContext,
    StartupRunnerFactory, StartupRunnerFactoryError, StartupRunnerSelection, StoreWriterFaultPoint,
    StoreWriterOperationKind, TEST_PICKER_PROBE_FILE, VirtualReleaseTarget, launch,
    load_runtime_config_for_test,
};
use coding_agent_runtime::ProcessLivenessScope;

struct CapturedStartupProcessOwnership {
    observed: Mutex<Option<(uuid::Uuid, ProcessLivenessScope)>>,
    inner: FixedStartupRunnerFactory,
}

impl Default for CapturedStartupProcessOwnership {
    fn default() -> Self {
        Self {
            observed: Mutex::new(None),
            inner: FixedStartupRunnerFactory::new(
                Arc::new(FakeTaskRunner::default()),
                NonZeroU32::new(1).unwrap(),
            ),
        }
    }
}

#[async_trait::async_trait]
impl StartupRunnerFactory for CapturedStartupProcessOwnership {
    async fn prepare_before_actors(
        &self,
        context: &PreActorStartupRunnerContext,
    ) -> Result<Arc<dyn std::any::Any + Send + Sync>, StartupRunnerFactoryError> {
        self.inner.prepare_before_actors(context).await
    }

    async fn create(
        &self,
        context: StartupRunnerContext,
    ) -> Result<StartupRunnerSelection, StartupRunnerFactoryError> {
        let observed = (
            context.instance_id(),
            context.process_liveness_scope().clone(),
        );
        *self.observed.lock().expect("capture startup ownership") = Some(observed);
        self.inner.create(context).await
    }
}

#[test]
fn complete_process_scenario_is_closed_validated_and_consumed_once() {
    let fixture = tempfile::tempdir().expect("create process-support fixture");
    let signal_path = fixture.path().join("runner-0.release");
    let scenario_path = fixture.path().join("scenario.json");
    write_scenario(&scenario_path, &signal_path, "");

    let config = ProcessTestConfig::load(&scenario_path).expect("load complete scenario");

    assert_eq!(
        config.fake_scenarios,
        vec![FakeScenario::Success, FakeScenario::Blocking]
    );
    assert_eq!(config.storage_samples, vec![ProcessStorageSample::Native]);
    assert_eq!(config.store_writer_faults.len(), 1);
    assert_eq!(
        config.store_writer_faults[0].point,
        StoreWriterFaultPoint::FailBeforeExecute
    );
    assert_eq!(
        config.store_writer_faults[0].operation,
        Some(StoreWriterOperationKind::CreateTask)
    );
    assert_eq!(config.store_writer_faults[0].count, 2);
    assert_eq!(
        config.actor_pauses,
        vec![ActorPausePoint::ClaimPermitAcquired]
    );
    assert_eq!(config.virtual_release_signals.len(), 1);
    assert_eq!(config.virtual_release_signals[0].name, "claim-permit");
    assert_eq!(config.virtual_release_signals[0].path, signal_path);
    assert_eq!(
        config.virtual_release_signals[0].target,
        VirtualReleaseTarget::ActorClaimPermitAcquired
    );
    assert!(config.marker_write_failure);
    assert_eq!(config.legacy_v2_seed, LegacyV2Seed::None);
    assert_eq!(config.runtime_config, None);
    assert_eq!(config.runner_mode, ProcessRunnerMode::ScriptedFake {});

    assert!(
        !scenario_path.exists()
            || fs::metadata(&scenario_path)
                .expect("read a retained zero-length source")
                .len()
                == 0,
        "a successfully consumed scenario must leave no source bytes"
    );
    assert!(
        ProcessTestConfig::load(&scenario_path).is_err(),
        "the same scenario source cannot be consumed twice"
    );
    assert_eq!(
        fs::read_dir(fixture.path())
            .expect("scan consumed scenario directory")
            .count(),
        0,
        "no claimed scenario copy may retain source bytes"
    );
}

#[test]
fn runtime_config_is_required_and_cannot_silently_default() {
    let fixture = tempfile::tempdir().expect("create process-support fixture");
    let scenario_path = fixture.path().join("scenario.json");
    write_scenario(
        &scenario_path,
        &fixture.path().join("claim-permit.release"),
        "",
    );
    let scenario = fs::read_to_string(&scenario_path)
        .expect("read scenario")
        .replace("  \"runtime_config\": null,\n", "");
    fs::write(&scenario_path, scenario).expect("remove required runtime config field");

    let error = ProcessTestConfig::load(&scenario_path)
        .expect_err("a missing runtime config field must be rejected");

    assert!(error.to_string().contains("runtime_config"));
    assert!(scenario_path.exists(), "invalid input is not consumed");
}

#[test]
fn runner_mode_is_required_and_nested_unknown_fields_are_denied() {
    for mutation in [
        Box::new(|scenario: String| {
            scenario.replace("  \"runner_mode\": { \"kind\": \"scripted_fake\" },\n", "")
        }) as Box<dyn FnOnce(String) -> String>,
        Box::new(|scenario: String| {
            scenario.replace(
                "{ \"kind\": \"scripted_fake\" }",
                "{ \"kind\": \"scripted_fake\", \"unexpected\": true }",
            )
        }),
    ] {
        let fixture = tempfile::tempdir().expect("create strict runner-mode fixture");
        let scenario_path = fixture.path().join("scenario.json");
        write_scenario(
            &scenario_path,
            &fixture.path().join("claim-permit.release"),
            "",
        );
        let scenario = mutation(fs::read_to_string(&scenario_path).expect("read scenario"));
        fs::write(&scenario_path, scenario).expect("write malformed runner mode");

        let error = ProcessTestConfig::load(&scenario_path)
            .expect_err("missing or unknown runner-mode fields must be rejected");

        assert!(
            error.to_string().contains("runner_mode")
                || error.to_string().contains("unknown field")
        );
        assert!(scenario_path.exists(), "invalid input is not consumed");
    }
}

#[test]
fn production_offline_runner_rejects_fake_runner_controls() {
    let fixture = tempfile::tempdir().expect("create production runner validation fixture");
    let repository = fixture.path().join("repository");
    fs::create_dir_all(repository.join(".git")).expect("create git metadata directory");
    fs::create_dir_all(repository.join("src")).expect("create source directory");
    fs::write(
        repository.join("Cargo.toml"),
        "[package]\nname = \"e2e-fixture\"\nversion = \"0.1.0\"\nedition = \"2024\"\n",
    )
    .expect("write fixture manifest");
    fs::write(
        repository.join("src").join("lib.rs"),
        "pub fn fixture_value() -> u32 { 42 }\n",
    )
    .expect("write fixture source");
    let repository = repository.canonicalize().expect("canonical repository");
    let scenario_path = fixture.path().join("scenario.json");
    write_scenario(
        &scenario_path,
        &fixture.path().join("claim-permit.release"),
        "",
    );
    let mut scenario: serde_json::Value =
        serde_json::from_slice(&fs::read(&scenario_path).expect("read production runner scenario"))
            .expect("parse production runner scenario");
    scenario["runner_mode"] = serde_json::json!({
        "kind": "production_offline_delivery",
        "repository_path": repository,
        "provider_scenario": "approve",
        "process_fault": "none"
    });
    fs::write(
        &scenario_path,
        serde_json::to_vec(&scenario).expect("encode production runner scenario"),
    )
    .expect("write production runner scenario");

    let error = ProcessTestConfig::load(&scenario_path)
        .expect_err("production runner cannot silently consume fake scenarios");

    assert!(error.to_string().contains("fake_scenarios"));
    assert!(scenario_path.exists(), "invalid input is not consumed");
}

#[test]
fn production_offline_process_fault_is_required_and_closed() {
    let fixture = tempfile::tempdir().expect("create production fault config fixture");
    let repository = fixture.path().join("repository");
    fs::create_dir_all(repository.join(".git")).expect("create git metadata directory");
    fs::create_dir_all(repository.join("src")).expect("create source directory");
    fs::write(
        repository.join("Cargo.toml"),
        "[package]\nname = \"e2e-fixture\"\nversion = \"0.1.0\"\nedition = \"2024\"\n",
    )
    .expect("write fixture manifest");
    fs::write(
        repository.join("src").join("lib.rs"),
        "pub fn fixture_value() -> u32 { 42 }\n",
    )
    .expect("write fixture source");
    let repository = repository.canonicalize().expect("canonical repository");

    let scenario = |process_fault: Option<&str>| {
        let scenario_path = fixture.path().join(format!(
            "scenario-{}.json",
            process_fault.unwrap_or("missing")
        ));
        write_scenario(&scenario_path, &fixture.path().join("unused.release"), "");
        let mut scenario: serde_json::Value = serde_json::from_slice(
            &fs::read(&scenario_path).expect("read production fault scenario"),
        )
        .expect("parse production fault scenario");
        scenario["runner_mode"] = serde_json::json!({
            "kind": "production_offline_delivery",
            "repository_path": repository,
            "provider_scenario": "approve",
        });
        if let Some(process_fault) = process_fault {
            scenario["runner_mode"]["process_fault"] = serde_json::json!(process_fault);
        }
        scenario["fake_scenarios"] = serde_json::json!([]);
        scenario["actor_pauses"] = serde_json::json!([]);
        scenario["virtual_release_signals"] = serde_json::json!([]);
        fs::write(
            &scenario_path,
            serde_json::to_vec(&scenario).expect("encode production fault scenario"),
        )
        .expect("write production fault scenario");
        scenario_path
    };

    let missing = scenario(None);
    ProcessTestConfig::load(&missing)
        .expect_err("production process fault must be stated explicitly");
    assert!(missing.exists(), "invalid missing fault is not consumed");

    let unknown = scenario(Some("arbitrary_process_fault"));
    ProcessTestConfig::load(&unknown).expect_err("process fault enum is closed");
    assert!(unknown.exists(), "invalid unknown fault is not consumed");

    let none = scenario(Some("none"));
    ProcessTestConfig::load(&none).expect("explicit no-fault production scenario is valid");
    assert!(!none.exists(), "valid strict scenario is consumed");
}

#[tokio::test]
async fn production_offline_runner_starts_loopback_and_seeds_the_real_repository() {
    let fixture = tempfile::tempdir().expect("create production runner fixture");
    let data_dir = fixture.path().join("data");
    let runtime_dir = fixture.path().join("runtime");
    let repository = fixture.path().join("repository");
    fs::create_dir(&data_dir).expect("create isolated data root");
    fs::create_dir(&runtime_dir).expect("create isolated runtime root");
    fs::create_dir_all(repository.join(".git")).expect("create git metadata directory");
    fs::create_dir_all(repository.join("src")).expect("create source directory");
    fs::write(
        repository.join("Cargo.toml"),
        "[package]\nname = \"e2e-fixture\"\nversion = \"0.1.0\"\nedition = \"2024\"\n",
    )
    .expect("write fixture manifest");
    fs::write(
        repository.join("src").join("lib.rs"),
        "pub fn fixture_value() -> u32 { 42 }\n",
    )
    .expect("write fixture source");
    let repository = repository.canonicalize().expect("canonical repository");
    let scenario_path = data_dir.join("scenario.json");
    let signal_path = runtime_dir.join("signals").join("unused.release");
    write_scenario(&scenario_path, &signal_path, "");
    let mut scenario: serde_json::Value =
        serde_json::from_slice(&fs::read(&scenario_path).expect("read production runner scenario"))
            .expect("parse production runner scenario");
    scenario["runner_mode"] = serde_json::json!({
        "kind": "production_offline_delivery",
        "repository_path": repository,
        "provider_scenario": "approve",
        "process_fault": "none"
    });
    scenario["fake_scenarios"] = serde_json::json!([]);
    scenario["actor_pauses"] = serde_json::json!([]);
    scenario["virtual_release_signals"] = serde_json::json!([]);
    fs::write(
        &scenario_path,
        serde_json::to_vec(&scenario).expect("encode production runner scenario"),
    )
    .expect("write production runner scenario");

    let environment = ProcessTestEnvironment::load(&data_dir, &runtime_dir, &scenario_path)
        .expect("load strict production runner environment");
    let database_path = environment.paths().database_path.clone();
    let dependencies = environment
        .apply(StartupDependencies::production(None))
        .expect("start loopback provider and install production factory");
    let store = dependencies
        .stores
        .open(&database_path)
        .await
        .expect("open seeded process store");
    store.migrate().await.expect("migrate seeded process store");
    let repositories = store
        .list_repositories()
        .await
        .expect("load production repository seed");

    assert_eq!(repositories.len(), 1);
    assert_eq!(repositories[0].git_root.as_path(), repository);
    store.close().await;
    drop(dependencies);
}

#[test]
fn process_environment_writes_a_private_typed_runtime_config_before_launch() {
    let fixture = tempfile::tempdir().expect("create runtime-config process fixture");
    let data_dir = fixture.path().join("data");
    let runtime_dir = fixture.path().join("runtime");
    fs::create_dir(&data_dir).expect("create isolated data root");
    fs::create_dir(&runtime_dir).expect("create isolated runtime root");
    let scenario_path = data_dir.join("scenario.json");
    let signal_path = runtime_dir.join("signals").join("claim-permit.release");
    write_scenario(&scenario_path, &signal_path, "");
    let expected = ProcessRuntimeConfig {
        schema_version: 1,
        max_concurrent_tasks: 4,
        max_concurrent_tasks_per_repository: 4,
        max_queued_tasks: 32,
        storage: ProcessRuntimeStorageConfig {
            data_control_reserve_bytes: 1,
            data_task_reservation_bytes: 1,
        },
    };
    let mut scenario: serde_json::Value =
        serde_json::from_slice(&fs::read(&scenario_path).expect("read runtime-config scenario"))
            .expect("parse runtime-config scenario");
    scenario["runtime_config"] =
        serde_json::to_value(&expected).expect("serialize typed runtime config");
    fs::write(
        &scenario_path,
        serde_json::to_vec(&scenario).expect("encode runtime-config scenario"),
    )
    .expect("write runtime-config scenario");

    let environment = ProcessTestEnvironment::load(&data_dir, &runtime_dir, &scenario_path)
        .expect("load process environment with runtime config");
    assert_eq!(
        environment.config().runtime_config.as_ref(),
        Some(&expected)
    );
    let loaded =
        load_runtime_config_for_test(environment.paths(), Some(NonZeroUsize::new(8).unwrap()))
            .expect("production loader accepts the protected runtime config");
    assert_eq!(loaded.max_concurrent_tasks().get(), 4);
    assert_eq!(loaded.max_concurrent_tasks_per_repository().get(), 4);
    assert_eq!(loaded.max_queued_tasks().get(), 32);
    assert_eq!(loaded.storage().data_control_reserve_bytes().get(), 1);
    assert_eq!(loaded.storage().data_task_reservation_bytes().get(), 1);
}

#[test]
fn every_actor_pause_requires_one_matching_release_target() {
    let fixture = tempfile::tempdir().expect("create process-support fixture");
    let signal_path = fixture.path().join("claim-permit.release");
    let scenario_path = fixture.path().join("scenario.json");
    write_scenario(&scenario_path, &signal_path, "");
    let scenario = fs::read_to_string(&scenario_path)
        .expect("read scenario")
        .replace("actor_claim_permit_acquired", "runner_next");
    fs::write(&scenario_path, scenario).expect("replace actor release target");

    let error = ProcessTestConfig::load(&scenario_path)
        .expect_err("reject an actor pause without its release target");

    assert!(error.to_string().contains("has no virtual release"));
    assert!(scenario_path.exists(), "invalid input is not consumed");
}

#[test]
fn actor_release_targets_cannot_name_an_unconfigured_pause() {
    let fixture = tempfile::tempdir().expect("create process-support fixture");
    let signal_path = fixture.path().join("claim-permit.release");
    let scenario_path = fixture.path().join("scenario.json");
    write_scenario(&scenario_path, &signal_path, "");
    let scenario = fs::read_to_string(&scenario_path)
        .expect("read scenario")
        .replace(
            "\"actor_pauses\": [\"claim_permit_acquired\"]",
            "\"actor_pauses\": []",
        );
    fs::write(&scenario_path, scenario).expect("remove configured actor pause");

    let error = ProcessTestConfig::load(&scenario_path)
        .expect_err("reject a release target for an unconfigured actor pause");

    assert!(error.to_string().contains("is not configured"));
    assert!(scenario_path.exists(), "invalid input is not consumed");
}

#[test]
fn actor_reached_path_cannot_collide_with_any_release_path() {
    let fixture = tempfile::tempdir().expect("create process-support fixture");
    let signal_path = fixture.path().join("claim-permit.release");
    let reached_path = fixture.path().join("claim-permit.release.reached");
    let scenario_path = fixture.path().join("scenario.json");
    write_scenario(&scenario_path, &signal_path, "");
    let mut scenario: serde_json::Value = serde_json::from_slice(
        &fs::read(&scenario_path).expect("read actor reached-conflict scenario"),
    )
    .expect("parse actor reached-conflict scenario");
    scenario["virtual_release_signals"]
        .as_array_mut()
        .expect("release signals are an array")
        .push(serde_json::json!({
            "name": "runner-conflict",
            "path": reached_path,
            "target": "runner_next"
        }));
    fs::write(
        &scenario_path,
        serde_json::to_vec(&scenario).expect("encode reached-conflict scenario"),
    )
    .expect("write reached-conflict scenario");

    let error = ProcessTestConfig::load(&scenario_path)
        .expect_err("reject a reached marker colliding with a release path");

    assert!(error.to_string().contains("reached marker path conflicts"));
    assert!(scenario_path.exists(), "invalid input is not consumed");
}

#[test]
fn actor_reached_path_must_not_exist_before_startup() {
    let fixture = tempfile::tempdir().expect("create process-support fixture");
    let signal_path = fixture.path().join("claim-permit.release");
    let reached_path = fixture.path().join("claim-permit.release.reached");
    let scenario_path = fixture.path().join("scenario.json");
    fs::write(&reached_path, b"stale").expect("create stale reached marker");
    write_scenario(&scenario_path, &signal_path, "");

    let error = ProcessTestConfig::load(&scenario_path)
        .expect_err("reject a preexisting actor reached marker");

    assert!(error.to_string().contains("reached marker path is invalid"));
    assert!(scenario_path.exists(), "invalid input is not consumed");
}

#[test]
fn actor_pause_rejects_duplicate_release_targets() {
    let fixture = tempfile::tempdir().expect("create process-support fixture");
    let signal_path = fixture.path().join("claim-permit.release");
    let duplicate_path = fixture.path().join("claim-permit-second.release");
    let scenario_path = fixture.path().join("scenario.json");
    write_scenario(&scenario_path, &signal_path, "");
    let mut scenario: serde_json::Value = serde_json::from_slice(
        &fs::read(&scenario_path).expect("read duplicate actor release scenario"),
    )
    .expect("parse duplicate actor release scenario");
    scenario["virtual_release_signals"]
        .as_array_mut()
        .expect("release signals are an array")
        .push(serde_json::json!({
            "name": "claim-permit-second",
            "path": duplicate_path,
            "target": "actor_claim_permit_acquired"
        }));
    fs::write(
        &scenario_path,
        serde_json::to_vec(&scenario).expect("encode duplicate actor release scenario"),
    )
    .expect("write duplicate actor release scenario");

    let error = ProcessTestConfig::load(&scenario_path)
        .expect_err("reject two releases for one actor pause");

    assert!(error.to_string().contains("more than one virtual release"));
    assert!(scenario_path.exists(), "invalid input is not consumed");
}

#[test]
fn unknown_fields_are_rejected_without_consuming_the_source() {
    let fixture = tempfile::tempdir().expect("create process-support fixture");
    let signal_path = fixture.path().join("runner-0.release");
    let scenario_path = fixture.path().join("scenario.json");
    write_scenario(&scenario_path, &signal_path, ",\n  \"unexpected\": true");

    let error = ProcessTestConfig::load(&scenario_path).expect_err("reject unknown field");

    assert!(error.to_string().contains("unknown field"));
    assert!(scenario_path.exists(), "invalid input is not consumed");
}

#[test]
fn legacy_v2_seed_is_required_and_cannot_silently_default() {
    let fixture = tempfile::tempdir().expect("create process-support fixture");
    let scenario_path = fixture.path().join("scenario.json");
    write_scenario(
        &scenario_path,
        &fixture.path().join("claim-permit.release"),
        "",
    );
    let scenario = fs::read_to_string(&scenario_path)
        .expect("read scenario")
        .replace("  \"legacy_v2_seed\": { \"kind\": \"none\" },\n", "");
    fs::write(&scenario_path, scenario).expect("remove required legacy seed field");

    let error = ProcessTestConfig::load(&scenario_path)
        .expect_err("a missing legacy seed field must be rejected");

    assert!(error.to_string().contains("legacy_v2_seed"));
    assert!(scenario_path.exists(), "invalid input is not consumed");
}

#[test]
fn storage_samples_are_required_and_cannot_silently_default() {
    let fixture = tempfile::tempdir().expect("create process-support fixture");
    let scenario_path = fixture.path().join("scenario.json");
    write_scenario(
        &scenario_path,
        &fixture.path().join("claim-permit.release"),
        "",
    );
    let scenario = fs::read_to_string(&scenario_path)
        .expect("read scenario")
        .replace("  \"storage_samples\": [{ \"kind\": \"native\" }],\n", "");
    fs::write(&scenario_path, scenario).expect("remove required storage samples field");

    let error = ProcessTestConfig::load(&scenario_path)
        .expect_err("a missing storage samples field must be rejected");

    assert!(error.to_string().contains("storage_samples"));
    assert!(scenario_path.exists(), "invalid input is not consumed");
}

#[test]
fn storage_script_requires_one_private_release_per_transition() {
    let fixture = tempfile::tempdir().expect("create process-support fixture");
    let signal_path = fixture.path().join("claim-permit.release");
    let scenario_path = fixture.path().join("scenario.json");
    write_scenario(&scenario_path, &signal_path, "");
    let scenario = fs::read_to_string(&scenario_path)
        .expect("read scenario")
        .replace(
            "[{ \"kind\": \"native\" }]",
            "[{ \"kind\": \"native\" }, { \"kind\": \"unavailable\" }]",
        );
    fs::write(&scenario_path, scenario).expect("add storage transition without release");

    let error = ProcessTestConfig::load(&scenario_path)
        .expect_err("a storage transition without a private release must be rejected");

    assert!(error.to_string().contains("requires 1 storage release"));
    assert!(scenario_path.exists(), "invalid input is not consumed");
}

#[test]
fn every_configured_path_is_validated_before_consumption() {
    let fixture = tempfile::tempdir().expect("create process-support fixture");
    let scenario_path = fixture.path().join("scenario.json");
    write_scenario(&scenario_path, Path::new("relative-release-signal"), "");

    let error = ProcessTestConfig::load(&scenario_path).expect_err("reject relative path");

    assert!(error.to_string().contains("absolute"));
    assert!(scenario_path.exists(), "path-invalid input is not consumed");
}

#[tokio::test]
async fn isolated_roots_are_applied_before_startup_and_disable_native_side_effects() {
    let fixture = tempfile::tempdir().expect("create process-support fixture");
    let data_dir = fixture.path().join("data");
    let runtime_dir = fixture.path().join("runtime");
    fs::create_dir(&data_dir).expect("create isolated data root");
    fs::create_dir(&runtime_dir).expect("create isolated runtime root");
    let scenario_path = runtime_dir.join("scenario.json");
    let signal_path = runtime_dir.join("signals").join("runner-0.release");
    write_scenario(&scenario_path, &signal_path, "");

    let environment = ProcessTestEnvironment::load(&data_dir, &runtime_dir, &scenario_path)
        .expect("load isolated process environment");
    assert_eq!(
        environment.paths().data_dir,
        data_dir.canonicalize().unwrap()
    );
    assert_eq!(
        environment.paths().runtime_dir,
        runtime_dir.canonicalize().unwrap()
    );

    let dependencies = environment
        .apply(StartupDependencies::production(None))
        .expect("apply test-only startup dependencies");
    assert_eq!(
        dependencies.paths.discover().unwrap().data_dir,
        data_dir.canonicalize().unwrap()
    );
    assert!(
        dependencies.browser.open(0, "").is_ok(),
        "real-process tests never open a system browser"
    );
    assert!(
        dependencies.messages.publish_degraded_shutdown().is_ok(),
        "real-process tests never spawn a native warning helper"
    );
    let picker_probe = runtime_dir.join(TEST_PICKER_PROBE_FILE);
    assert!(!picker_probe.exists());
    assert_eq!(
        dependencies
            .dialog
            .as_ref()
            .expect("test support installs a picker probe")
            .pick_repository()
            .await
            .expect("probe picker returns a cancellation"),
        None
    );
    assert!(
        picker_probe.is_file(),
        "a real picker dispatch must be observable without opening a native dialog"
    );
}

#[tokio::test]
async fn primary_uuid_and_instance_process_scope_are_created_once_after_lock_acquisition() {
    let fixture = tempfile::tempdir().expect("create process-liveness startup fixture");
    let data_dir = fixture.path().join("data");
    let runtime_dir = fixture.path().join("runtime");
    fs::create_dir(&data_dir).expect("create isolated data root");
    fs::create_dir(&runtime_dir).expect("create isolated runtime root");
    let scenario_path = runtime_dir.join("scenario.json");
    let signal_path = runtime_dir.join("signals").join("runner-0.release");
    write_scenario(&scenario_path, &signal_path, "");

    let environment = ProcessTestEnvironment::load(&data_dir, &runtime_dir, &scenario_path)
        .expect("load isolated process environment");
    let capture = Arc::new(CapturedStartupProcessOwnership::default());
    let mut dependencies = environment
        .apply(StartupDependencies::production(None))
        .expect("apply process-test startup dependencies");
    dependencies.runner_factory = capture.clone();

    let primary = match launch(dependencies).await.expect("launch scoped primary") {
        StartupOutcome::Primary(primary) => primary,
        StartupOutcome::Secondary(_) => panic!("isolated runtime must acquire the primary lock"),
    };
    let (startup_instance_id, startup_scope) = capture
        .observed
        .lock()
        .expect("read captured startup ownership")
        .take()
        .expect("runner startup must receive process ownership");

    assert_eq!(startup_instance_id, primary.instance_id());
    assert_eq!(startup_scope.active_tree_count(), 0);
    assert_eq!(
        format!("{startup_scope:?}"),
        "ProcessLivenessScope(<opaque>)"
    );
    assert!(
        runtime_dir.join("process-liveness").is_dir(),
        "the primary must prepare its fixed private sentinel directory before runner startup"
    );

    assert_eq!(primary.shutdown().await, ShutdownOutcome::Clean);
    drop(primary);
    drop(startup_scope);
    drop(capture);
    let fixture_root = fixture.path().to_path_buf();
    fixture
        .close()
        .expect("remove process-liveness startup fixture after clean shutdown");
    assert!(
        !fixture_root.exists(),
        "process-liveness startup fixture leaked after clean shutdown"
    );
}

#[test]
fn run_context_exposes_only_opaque_process_ownership() {
    let accessor: for<'a> fn(&'a RunContext) -> &'a ProcessLivenessScope =
        RunContext::process_liveness_scope;
    let _ = accessor;
}

#[test]
fn scenario_outside_isolated_roots_is_rejected_before_consumption() {
    let fixture = tempfile::tempdir().expect("create process-support fixture");
    let data_dir = fixture.path().join("data");
    let runtime_dir = fixture.path().join("runtime");
    fs::create_dir(&data_dir).expect("create isolated data root");
    fs::create_dir(&runtime_dir).expect("create isolated runtime root");
    let scenario_path = fixture.path().join("outside.json");
    write_scenario(&scenario_path, &runtime_dir.join("runner-0.release"), "");

    let error = ProcessTestEnvironment::load(&data_dir, &runtime_dir, &scenario_path)
        .expect_err("reject scenario outside isolated roots");

    assert!(error.to_string().contains("outside"));
    assert!(scenario_path.exists(), "invalid source is not consumed");
}

#[test]
fn hard_linked_scenario_is_rejected_without_clearing_the_other_link() {
    let fixture = tempfile::tempdir().expect("create hard-link scenario fixture");
    let original_path = fixture.path().join("original.json");
    let scenario_path = fixture.path().join("scenario.json");
    let signal_path = fixture.path().join("claim-permit.release");
    write_scenario(&original_path, &signal_path, "");
    let original_bytes = fs::read(&original_path).expect("read original scenario bytes");
    fs::hard_link(&original_path, &scenario_path).expect("hard-link scenario source");

    ProcessTestConfig::load(&scenario_path)
        .expect_err("a multiply-linked scenario source must be rejected");

    assert_eq!(
        fs::read(&original_path).expect("read untouched original link"),
        original_bytes,
        "rejecting the scenario must never clear bytes through another hard link"
    );
}

#[test]
fn release_signals_cannot_alias_product_runtime_files() {
    for reserved_name in ["instance.lock", "instance.json"] {
        let fixture = tempfile::tempdir().expect("create reserved-signal fixture");
        let data_dir = fixture.path().join("data");
        let runtime_dir = fixture.path().join("runtime");
        fs::create_dir(&data_dir).expect("create isolated data root");
        fs::create_dir(&runtime_dir).expect("create isolated runtime root");
        let scenario_path = data_dir.join("scenario.json");
        write_scenario(&scenario_path, &runtime_dir.join(reserved_name), "");

        let error = ProcessTestEnvironment::load(&data_dir, &runtime_dir, &scenario_path)
            .expect_err("product runtime files cannot be virtual release signals");

        assert!(
            error.to_string().contains("signals"),
            "reserved signal rejection should name the dedicated signals directory: {error}"
        );
        assert!(scenario_path.exists(), "invalid source is not consumed");
    }
}

fn write_scenario(path: &Path, signal_path: &Path, extra_field: &str) {
    let signal_path = serde_json::to_string(signal_path).expect("serialize signal path");
    let bytes = format!(
        r#"{{
  "runner_mode": {{ "kind": "scripted_fake" }},
  "runtime_config": null,
  "fake_scenarios": ["success", "blocking"],
  "storage_samples": [{{ "kind": "native" }}],
  "store_writer_faults": [{{ "point": "fail_before_execute", "operation": "create_task", "count": 2 }}],
  "actor_pauses": ["claim_permit_acquired"],
  "virtual_release_signals": [{{ "name": "claim-permit", "path": {signal_path}, "target": "actor_claim_permit_acquired" }}],
  "legacy_v2_seed": {{ "kind": "none" }},
  "marker_write_failure": true{extra_field}
}}"#
    );
    fs::write(path, bytes).expect("write process scenario");
}
