use std::sync::atomic::{AtomicUsize, Ordering};

use coding_agent_runtime::{
    NativeVolumeSampler, RootCapability, VolumeSample, VolumeSampleError, VolumeSampler,
};
use serde::{Deserialize, Serialize};

/// One path-free storage observation used by a real-process test.
///
/// Even deterministic observations retain the native volume identity obtained
/// from the authenticated root capability. Tests can therefore vary only the
/// admission signal, never redirect which filesystem object is being sampled.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ProcessStorageSample {
    Native,
    Available { available_bytes: u64 },
    Unavailable,
}

/// Test-only sampler advanced by private virtual-release signals.
pub(super) struct ProcessVolumeSampler {
    samples: Box<[ProcessStorageSample]>,
    current: AtomicUsize,
}

impl ProcessVolumeSampler {
    pub(super) fn new(samples: Vec<ProcessStorageSample>) -> Self {
        assert!(
            !samples.is_empty(),
            "validated process storage script is nonempty"
        );
        Self {
            samples: samples.into_boxed_slice(),
            current: AtomicUsize::new(0),
        }
    }

    pub(super) fn advance(&self) -> bool {
        self.current
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                current
                    .checked_add(1)
                    .filter(|next| *next < self.samples.len())
            })
            .is_ok()
    }

    fn current_sample(&self) -> ProcessStorageSample {
        self.samples[self.current.load(Ordering::Acquire)]
    }
}

impl VolumeSampler for ProcessVolumeSampler {
    fn sample(&self, root: &RootCapability) -> Result<VolumeSample, VolumeSampleError> {
        let native = NativeVolumeSampler::new().sample(root)?;
        match self.current_sample() {
            ProcessStorageSample::Native => Ok(native),
            ProcessStorageSample::Available { available_bytes } => {
                Ok(VolumeSample::for_test(native.identity(), available_bytes))
            }
            ProcessStorageSample::Unavailable => Err(VolumeSampleError::Unavailable),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{ProcessStorageSample, ProcessVolumeSampler};

    #[test]
    fn script_advances_once_per_transition_and_stops_at_the_last_sample() {
        let sampler = ProcessVolumeSampler::new(vec![
            ProcessStorageSample::Native,
            ProcessStorageSample::Unavailable,
        ]);

        assert_eq!(sampler.current_sample(), ProcessStorageSample::Native);
        assert!(sampler.advance());
        assert_eq!(sampler.current_sample(), ProcessStorageSample::Unavailable);
        assert!(!sampler.advance());
        assert_eq!(sampler.current_sample(), ProcessStorageSample::Unavailable);
    }
}
