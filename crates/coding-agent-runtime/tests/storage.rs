use std::collections::HashSet;
use std::fs;
use std::hash::{Hash, Hasher};

use coding_agent_runtime::{
    NativeVolumeSampler, RootCapability, VolumeSample, VolumeSampleError, VolumeSampler,
};
use tempfile::{Builder, tempdir};

fn sample_through_port(
    sampler: &dyn VolumeSampler,
    root: &RootCapability,
) -> Result<VolumeSample, VolumeSampleError> {
    sampler.sample(root)
}

#[test]
fn logical_scope_aliases_on_the_same_volume_deduplicate() {
    let temporary = tempdir().unwrap();
    let data_path = temporary.path().join("data-scope");
    let runtime_path = temporary.path().join("runtime-scope");
    fs::create_dir(&data_path).unwrap();
    fs::create_dir(&runtime_path).unwrap();

    let data = RootCapability::open(data_path.canonicalize().unwrap()).unwrap();
    let runtime = RootCapability::open(runtime_path.canonicalize().unwrap()).unwrap();
    assert_ne!(
        data.identity_marker().unwrap(),
        runtime.identity_marker().unwrap(),
        "the fixture must contain two distinct directory objects"
    );

    let sampler = NativeVolumeSampler::new();
    let first_data_sample = sample_through_port(&sampler, &data).unwrap();
    let second_data_sample = sample_through_port(&sampler, &data).unwrap();
    let runtime_sample = sample_through_port(&sampler, &runtime).unwrap();

    assert_eq!(
        first_data_sample.identity(),
        second_data_sample.identity(),
        "a retained capability must keep a stable volume identity"
    );
    assert_eq!(
        first_data_sample.identity(),
        runtime_sample.identity(),
        "different logical roots on the same volume must share one identity"
    );

    let identities = HashSet::from([
        first_data_sample.identity(),
        second_data_sample.identity(),
        runtime_sample.identity(),
    ]);
    assert_eq!(identities.len(), 1);
}

#[test]
fn retained_capability_remains_sampleable_after_its_namespace_name_moves() {
    let temporary = tempdir().unwrap();
    let original_path = temporary.path().join("before");
    let moved_path = temporary.path().join("after");
    fs::create_dir(&original_path).unwrap();

    let root = RootCapability::open(original_path.canonicalize().unwrap()).unwrap();
    let sampler = NativeVolumeSampler::new();
    let before = sampler.sample(&root).unwrap();

    fs::rename(&original_path, &moved_path).unwrap();
    assert!(!original_path.exists());

    let after = sampler.sample(&root).unwrap();
    assert_eq!(before.identity(), after.identity());
}

#[test]
fn identity_and_sample_debug_output_are_opaque() {
    let temporary = tempdir().unwrap();
    let root = RootCapability::open(temporary.path().canonicalize().unwrap()).unwrap();
    let sample = NativeVolumeSampler::new().sample(&root).unwrap();
    let identity = sample.identity();

    assert_eq!(format!("{identity:?}"), "VolumeIdentity(<opaque>)");
    assert_eq!(format!("{sample:?}"), "VolumeSample(<opaque>)");

    let mut hasher = RecordingHasher::default();
    identity.hash(&mut hasher);
    assert_eq!(hasher.u64_writes.len(), 1);
    assert_eq!(hasher.other_writes, 0);
}

#[test]
fn unavailable_error_has_a_fixed_path_free_diagnostic() {
    let error = VolumeSampleError::Unavailable;
    assert_eq!(error.to_string(), "volume sample is unavailable");
    assert_eq!(format!("{error:?}"), "Unavailable");
    assert!(!error.to_string().contains('\\'));
    assert!(!error.to_string().contains('/'));
}

#[test]
fn explicitly_configured_second_volume_has_an_independent_identity() {
    let Some(second_volume_root) = std::env::var_os("CODING_AGENT_RUNTIME_SECOND_VOLUME_TEST_ROOT")
    else {
        return;
    };

    let primary = tempdir().unwrap();
    let secondary = Builder::new()
        .prefix("coding-agent-storage-test-")
        .tempdir_in(second_volume_root)
        .unwrap();
    let sampler = NativeVolumeSampler::new();
    let primary_root = RootCapability::open(primary.path().canonicalize().unwrap()).unwrap();
    let secondary_root = RootCapability::open(secondary.path().canonicalize().unwrap()).unwrap();

    assert_ne!(
        sampler.sample(&primary_root).unwrap().identity(),
        sampler.sample(&secondary_root).unwrap().identity(),
        "CODING_AGENT_RUNTIME_SECOND_VOLUME_TEST_ROOT must name a different physical volume"
    );
}

#[derive(Default)]
struct RecordingHasher {
    u64_writes: Vec<u64>,
    other_writes: usize,
}

impl Hasher for RecordingHasher {
    fn finish(&self) -> u64 {
        0
    }

    fn write(&mut self, _: &[u8]) {
        self.other_writes += 1;
    }

    fn write_u64(&mut self, value: u64) {
        self.u64_writes.push(value);
    }
}
