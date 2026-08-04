#[cfg(feature = "test-support")]
use std::num::{NonZeroU32, NonZeroU64};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

use coding_agent_domain::{
    CanonicalPath, ClientRequestId, EventCursor, NewRepository, NewTask, TaskEventKind,
};
#[cfg(feature = "test-support")]
use coding_agent_domain::{
    CheckActor, CheckEvidence, CheckEvidenceStatus, DeliveryReadiness, FindingSeverity,
    ReviewDecisionSource, ReviewFinding, WorkspaceDigest,
};
use coding_agent_runtime::{DirectoryIdentityMarker, ProcessLivenessDirectory, RootCapability};
use coding_agent_store::{CreateTaskOutcome, RegisterRepositoryOutcome, RepositoryIdentityLookup};
use tokio::sync::mpsc::error::TryRecvError;

use super::*;
#[cfg(feature = "test-support")]
use crate::{FakeRunnerConfig, FakeTaskRunner};
#[cfg(feature = "test-support")]
use crate::{
    KnownNotAppliedError, StoreWriterFaultPoint, StoreWriterFaultSpec, StoreWriterOperationKind,
    StoreWriterTestController,
};

struct FixedMarkerResolver(DirectoryIdentityMarker);

impl crate::RepositoryIdentityResolver for FixedMarkerResolver {
    fn resolve(
        &self,
        _identity: &RepositoryIdentityLookup,
    ) -> Result<DirectoryIdentityMarker, crate::RepositoryIdentityResolutionError> {
        Ok(self.0)
    }
}

#[cfg(feature = "test-support")]
include!("support/fixture_builders.rs");
#[cfg(feature = "test-support")]
include!("support/replay_helpers.rs");
include!("support/repository.rs");
#[cfg(feature = "test-support")]
include!("support/review_runners.rs");
#[cfg(feature = "test-support")]
include!("support/stop_helpers.rs");
include!("support/types_and_runners.rs");
