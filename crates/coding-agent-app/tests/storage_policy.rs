#![cfg(feature = "test-support")]

use std::num::{NonZeroU32, NonZeroU64};
use std::str::FromStr;
use std::time::Duration;

use coding_agent_app::{
    ActiveTaskStorage, DATA_CRITICAL_BYTES, DATA_RECOVERY_MARGIN_BYTES,
    GIT_RUNTIME_ADMISSION_BYTES, GIT_RUNTIME_CRITICAL_BYTES, GIT_RUNTIME_RECOVERY_MARGIN_BYTES,
    ScopeStorageClassification, StorageObservation, StoragePolicy, StoragePolicyError,
    StorageScope, StorageScopeBinding, StorageScopeHysteresis, StorageScopeState, StorageState,
    aggregate_storage_state, critical_affected_tasks,
};
use coding_agent_domain::TaskId;
use coding_agent_runtime::{VolumeIdentity, VolumeSample};

const MEBIBYTE: u64 = 1024 * 1024;

fn nonzero_u32(value: u32) -> NonZeroU32 {
    NonZeroU32::new(value).unwrap()
}

fn nonzero_u64(value: u64) -> NonZeroU64 {
    NonZeroU64::new(value).unwrap()
}

fn policy(max: u32, control: u64, reservation: u64) -> StoragePolicy {
    StoragePolicy::try_new(
        nonzero_u32(max),
        nonzero_u64(control),
        nonzero_u64(reservation),
    )
    .unwrap()
}

fn volume(token: u64) -> VolumeIdentity {
    VolumeIdentity::for_test(token)
}

fn binding(scope: StorageScope, volume: VolumeIdentity) -> StorageScopeBinding {
    StorageScopeBinding::new(scope, volume)
}

fn available(identity: VolumeIdentity, available_bytes: u64) -> StorageObservation {
    VolumeSample::for_test(identity, available_bytes).into()
}

fn classify(
    policy: StoragePolicy,
    binding: StorageScopeBinding,
    available_bytes: u64,
    active_task_count: u32,
) -> ScopeStorageClassification {
    policy
        .classify_scope(
            binding,
            available(binding.volume(), available_bytes),
            active_task_count,
        )
        .unwrap()
}

fn task(suffix: u32) -> TaskId {
    TaskId::from_str(&format!("20000000-0000-4000-8000-{suffix:012x}")).unwrap()
}

#[test]
fn next_candidate_data_formula_is_exhaustive_bounded_and_checked() {
    let policy = policy(4, 100, 10);
    for (active, expected_tasks, expected_bytes) in [
        (0, 1, 110),
        (1, 2, 120),
        (2, 3, 130),
        (3, 4, 140),
        (4, 4, 140),
    ] {
        assert_eq!(
            policy.next_candidate_task_count(active).unwrap(),
            expected_tasks
        );
        assert_eq!(
            policy.data_next_candidate_threshold(active).unwrap(),
            expected_bytes
        );
    }
    assert_eq!(
        policy.next_candidate_task_count(5),
        Err(StoragePolicyError::ActiveTaskCountExceedsLimit)
    );

    let base_overflow =
        StoragePolicy::try_new(nonzero_u32(1), nonzero_u64(u64::MAX), nonzero_u64(1));
    assert_eq!(base_overflow, Err(StoragePolicyError::ArithmeticOverflow));

    let margin_overflow = StoragePolicy::try_new(
        nonzero_u32(1),
        nonzero_u64(u64::MAX - DATA_RECOVERY_MARGIN_BYTES),
        nonzero_u64(1),
    );
    assert_eq!(margin_overflow, Err(StoragePolicyError::ArithmeticOverflow));
}

#[test]
fn pressure_critical_and_recovery_boundaries_use_exact_inclusive_edges() {
    let policy = policy(4, 512 * MEBIBYTE, 128 * MEBIBYTE);
    let data_volume = volume(1);
    let data = binding(StorageScope::Data, data_volume);
    let data_threshold = 640 * MEBIBYTE;
    let data_recovery = data_threshold + DATA_RECOVERY_MARGIN_BYTES;

    for (bytes, expected_state, expected_recovery) in [
        (DATA_CRITICAL_BYTES - 1, StorageState::Critical, false),
        (DATA_CRITICAL_BYTES, StorageState::Pressure, false),
        (data_threshold - 1, StorageState::Pressure, false),
        (data_threshold, StorageState::Normal, false),
        (data_recovery - 1, StorageState::Normal, false),
        (data_recovery, StorageState::Normal, true),
    ] {
        let classification = classify(policy, data, bytes, 0);
        assert_eq!(classification.state(), expected_state, "bytes={bytes}");
        assert_eq!(
            classification.recovery_margin_satisfied(),
            expected_recovery,
            "bytes={bytes}"
        );
    }

    for scope in [StorageScope::RepositoryGit, StorageScope::Runtime] {
        let fixed = binding(scope, volume(2 + scope as u64));
        let recovery = GIT_RUNTIME_ADMISSION_BYTES + GIT_RUNTIME_RECOVERY_MARGIN_BYTES;
        for (bytes, expected_state, expected_recovery) in [
            (
                GIT_RUNTIME_CRITICAL_BYTES - 1,
                StorageState::Critical,
                false,
            ),
            (GIT_RUNTIME_CRITICAL_BYTES, StorageState::Pressure, false),
            (
                GIT_RUNTIME_ADMISSION_BYTES - 1,
                StorageState::Pressure,
                false,
            ),
            (GIT_RUNTIME_ADMISSION_BYTES, StorageState::Normal, false),
            (recovery - 1, StorageState::Normal, false),
            (recovery, StorageState::Normal, true),
        ] {
            let classification = classify(policy, fixed, bytes, 0);
            assert_eq!(
                classification.state(),
                expected_state,
                "{scope:?}, bytes={bytes}"
            );
            assert_eq!(
                classification.recovery_margin_satisfied(),
                expected_recovery,
                "{scope:?}, bytes={bytes}"
            );
        }
    }
}

#[test]
fn shared_volumes_take_the_strictest_predicate_without_double_counting() {
    let policy = policy(4, 256 * MEBIBYTE, 128 * MEBIBYTE);
    let shared = volume(10);
    let other = volume(11);
    let requirements = policy
        .volume_admission_requirements(
            0,
            [
                binding(StorageScope::Data, shared),
                binding(StorageScope::Runtime, shared),
                binding(StorageScope::RepositoryGit, shared),
                binding(StorageScope::RepositoryGit, shared),
                binding(StorageScope::Runtime, other),
            ],
        )
        .unwrap();

    assert_eq!(requirements.len(), 2);
    assert_eq!(requirements.required_bytes(shared), Some(384 * MEBIBYTE));
    assert_eq!(
        requirements.required_bytes(other),
        Some(GIT_RUNTIME_ADMISSION_BYTES)
    );
    assert_eq!(
        requirements.admits(VolumeSample::for_test(shared, 384 * MEBIBYTE)),
        Some(true)
    );
    assert_eq!(
        requirements.admits(VolumeSample::for_test(shared, 384 * MEBIBYTE - 1)),
        Some(false)
    );
    assert_eq!(
        requirements.admits(VolumeSample::for_test(volume(99), u64::MAX)),
        None
    );

    assert_eq!(
        policy.classify_scope(
            binding(StorageScope::Data, shared),
            available(other, u64::MAX),
            0,
        ),
        Err(StoragePolicyError::VolumeIdentityMismatch)
    );
}

#[test]
fn aggregate_priority_and_admission_stop_semantics_are_exhaustive() {
    let states = [
        StorageState::Normal,
        StorageState::Pressure,
        StorageState::Critical,
        StorageState::Unavailable,
    ];
    assert_eq!(
        aggregate_storage_state(std::iter::empty()),
        StorageState::Normal
    );
    for left in states {
        for right in states {
            let expected = if left == StorageState::Critical || right == StorageState::Critical {
                StorageState::Critical
            } else if left == StorageState::Unavailable || right == StorageState::Unavailable {
                StorageState::Unavailable
            } else if left == StorageState::Pressure || right == StorageState::Pressure {
                StorageState::Pressure
            } else {
                StorageState::Normal
            };
            assert_eq!(aggregate_storage_state([left, right]), expected);
            assert_eq!(aggregate_storage_state([right, left]), expected);
        }
    }

    for (state, blocks_admission, requires_stop) in [
        (StorageState::Normal, false, false),
        (StorageState::Pressure, true, false),
        (StorageState::Unavailable, true, false),
        (StorageState::Critical, true, true),
    ] {
        assert_eq!(state.blocks_admission(), blocks_admission);
        assert_eq!(state.requires_critical_stop(), requires_stop);
    }
}

#[test]
fn hysteresis_has_no_implicit_first_sample_and_recovers_on_two_margin_samples() {
    let policy = policy(4, 512 * MEBIBYTE, 128 * MEBIBYTE);
    let data_binding = binding(StorageScope::Data, volume(20));
    let normal_bytes = 640 * MEBIBYTE;
    let margin_bytes = normal_bytes + DATA_RECOVERY_MARGIN_BYTES;
    let mut hysteresis = StorageScopeHysteresis::new(data_binding);

    assert_eq!(hysteresis.state(), None);
    assert!(hysteresis.blocks_admission());
    assert_eq!(
        hysteresis
            .observe(
                classify(policy, data_binding, normal_bytes, 0),
                Duration::ZERO,
            )
            .unwrap(),
        StorageState::Normal
    );
    assert!(!hysteresis.blocks_admission());

    assert_eq!(
        hysteresis
            .observe(
                classify(policy, data_binding, normal_bytes - 1, 0),
                Duration::from_secs(1),
            )
            .unwrap(),
        StorageState::Pressure
    );
    assert_eq!(
        hysteresis
            .observe(
                classify(policy, data_binding, margin_bytes, 0),
                Duration::from_secs(2),
            )
            .unwrap(),
        StorageState::Pressure
    );
    assert_eq!(
        hysteresis
            .observe(
                classify(policy, data_binding, margin_bytes, 0),
                Duration::from_secs(6),
            )
            .unwrap(),
        StorageState::Pressure,
        "an early qualifying sample must not slide the first sample forward"
    );
    assert_eq!(
        hysteresis
            .observe(
                classify(policy, data_binding, margin_bytes, 0),
                Duration::from_secs(7),
            )
            .unwrap(),
        StorageState::Normal,
        "exactly five seconds after the first sample recovers"
    );

    let wrong_binding = binding(StorageScope::Runtime, data_binding.volume());
    assert_eq!(
        hysteresis.observe(
            classify(policy, wrong_binding, GIT_RUNTIME_ADMISSION_BYTES, 0,),
            Duration::from_secs(8),
        ),
        Err(StoragePolicyError::ScopeBindingMismatch)
    );
    assert_eq!(
        hysteresis.observe(
            classify(policy, data_binding, margin_bytes, 0),
            Duration::from_secs(6),
        ),
        Err(StoragePolicyError::NonMonotonicObservationTime)
    );
    assert_eq!(hysteresis.state(), Some(StorageState::Normal));
}

#[test]
fn failed_or_sub_margin_samples_reset_recovery_and_downgrades_stay_fail_closed() {
    let policy = policy(4, 512 * MEBIBYTE, 128 * MEBIBYTE);
    let data_binding = binding(StorageScope::Data, volume(30));
    let margin_bytes = 640 * MEBIBYTE + DATA_RECOVERY_MARGIN_BYTES;
    let mut hysteresis = StorageScopeHysteresis::new(data_binding);

    assert_eq!(
        hysteresis
            .observe(
                classify(policy, data_binding, DATA_CRITICAL_BYTES - 1, 0,),
                Duration::ZERO,
            )
            .unwrap(),
        StorageState::Critical
    );
    let unavailable = policy
        .classify_scope(
            data_binding,
            StorageObservation::unavailable(data_binding.volume()),
            0,
        )
        .unwrap();
    assert_eq!(
        hysteresis
            .observe(unavailable, Duration::from_secs(1))
            .unwrap(),
        StorageState::Critical,
        "critical cannot downgrade to unavailable on a failed sample"
    );
    assert_eq!(
        hysteresis
            .observe(
                classify(policy, data_binding, margin_bytes, 0),
                Duration::from_secs(2),
            )
            .unwrap(),
        StorageState::Critical
    );
    assert_eq!(
        hysteresis
            .observe(unavailable, Duration::from_secs(7))
            .unwrap(),
        StorageState::Critical,
        "failure resets the recovery sample"
    );
    assert_eq!(
        hysteresis
            .observe(
                classify(policy, data_binding, margin_bytes, 0),
                Duration::from_secs(8),
            )
            .unwrap(),
        StorageState::Critical
    );
    assert_eq!(
        hysteresis
            .observe(
                classify(policy, data_binding, margin_bytes - 1, 0),
                Duration::from_secs(13),
            )
            .unwrap(),
        StorageState::Critical,
        "a successful but sub-margin sample also resets recovery"
    );
    assert_eq!(
        hysteresis
            .observe(
                classify(policy, data_binding, margin_bytes, 0),
                Duration::from_secs(14),
            )
            .unwrap(),
        StorageState::Critical
    );
    assert_eq!(
        hysteresis
            .observe(
                classify(policy, data_binding, margin_bytes, 0),
                Duration::from_secs(19),
            )
            .unwrap(),
        StorageState::Normal
    );

    assert_eq!(
        hysteresis
            .observe(unavailable, Duration::from_secs(20))
            .unwrap(),
        StorageState::Unavailable
    );
    assert_eq!(
        hysteresis
            .observe(
                classify(policy, data_binding, margin_bytes, 0),
                Duration::from_secs(21),
            )
            .unwrap(),
        StorageState::Unavailable,
        "one normal sample cannot recover unavailable"
    );
}

#[test]
fn critical_impact_preserves_scope_and_volume_and_deduplicates_tasks() {
    let git_a = volume(40);
    let git_b = volume(41);
    let data_volume = volume(42);
    let runtime_volume = volume(43);
    let task_a = task(1);
    let task_a_alias = task(2);
    let task_b = task(3);
    let active = [
        ActiveTaskStorage::new(task_b, git_b),
        ActiveTaskStorage::new(task_a_alias, git_a),
        ActiveTaskStorage::new(task_a, git_a),
        ActiveTaskStorage::new(task_a, git_a),
    ];

    let non_critical = [
        StorageScopeState::new(
            binding(StorageScope::Data, data_volume),
            StorageState::Pressure,
        ),
        StorageScopeState::new(
            binding(StorageScope::Runtime, runtime_volume),
            StorageState::Unavailable,
        ),
        StorageScopeState::new(
            binding(StorageScope::RepositoryGit, git_a),
            StorageState::Pressure,
        ),
    ];
    assert!(critical_affected_tasks(non_critical, active).is_empty());

    let git_only = [StorageScopeState::new(
        binding(StorageScope::RepositoryGit, git_a),
        StorageState::Critical,
    )];
    assert_eq!(
        critical_affected_tasks(git_only, active),
        vec![task_a, task_a_alias]
    );

    let overlapping = [
        StorageScopeState::new(
            binding(StorageScope::RepositoryGit, git_a),
            StorageState::Critical,
        ),
        StorageScopeState::new(
            binding(StorageScope::RepositoryGit, git_b),
            StorageState::Critical,
        ),
        StorageScopeState::new(
            binding(StorageScope::Data, data_volume),
            StorageState::Critical,
        ),
        StorageScopeState::new(
            binding(StorageScope::Runtime, runtime_volume),
            StorageState::Critical,
        ),
    ];
    assert_eq!(
        critical_affected_tasks(overlapping, active),
        vec![task_a, task_a_alias, task_b],
        "overlapping critical scopes form a canonical deduplicated union"
    );
}
