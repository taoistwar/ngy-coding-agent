use std::fmt;
use std::sync::Arc;

use coding_agent_core::{
    ContextRedactor, ReviewDiffBundle, ReviewDiffCheckpoint, ReviewDiffChunkBatch,
    ReviewDiffChunkRequest, ReviewDiffError, ReviewDiffManifest, ReviewDiffRuntime, RuntimeError,
};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::runtime_session::RuntimeSession;
use crate::{DiffError, FingerprintError};

#[derive(Debug, Default)]
pub(crate) struct ReviewDiffState {
    cache: Mutex<Option<CachedReviewDiff>>,
}

struct CachedReviewDiff {
    checkpoint: ReviewDiffCheckpoint,
    bundle: ReviewDiffBundle,
    redactor: Arc<dyn ContextRedactor>,
}

impl fmt::Debug for CachedReviewDiff {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CachedReviewDiff")
            .field("checkpoint", &self.checkpoint)
            .field("bundle", &self.bundle)
            .field("redactor", &"<context-redactor>")
            .finish()
    }
}

impl CachedReviewDiff {
    fn matches(
        &self,
        checkpoint: &ReviewDiffCheckpoint,
        redactor: &Arc<dyn ContextRedactor>,
    ) -> bool {
        self.checkpoint == *checkpoint && Arc::ptr_eq(&self.redactor, redactor)
    }
}

#[async_trait::async_trait]
impl ReviewDiffRuntime for RuntimeSession {
    async fn review_diff_manifest(
        &self,
        checkpoint: ReviewDiffCheckpoint,
        redactor: Arc<dyn ContextRedactor>,
        cancellation: CancellationToken,
    ) -> Result<ReviewDiffManifest, RuntimeError> {
        let mut cache = self.review_diff_state.cache.lock().await;
        if cache
            .as_ref()
            .is_some_and(|cached| cached.matches(&checkpoint, &redactor))
        {
            let result = self
                .serve_cached_manifest(
                    cache.as_ref().expect("matching cache exists"),
                    &checkpoint,
                    cancellation,
                )
                .await;
            if result.is_err() {
                *cache = None;
            }
            return result;
        }

        // A different generation/digest is never reusable, even if its raw
        // fingerprint later returns to a previous value.
        *cache = None;
        let bundle = match self
            .collect_review_bundle(&checkpoint, redactor.as_ref(), cancellation)
            .await
        {
            Ok(bundle) => bundle,
            Err(error) => return Err(error),
        };
        let manifest = bundle.manifest().clone();
        *cache = Some(CachedReviewDiff {
            checkpoint,
            bundle,
            redactor,
        });
        Ok(manifest)
    }

    async fn review_diff_chunks(
        &self,
        request: ReviewDiffChunkRequest,
        cancellation: CancellationToken,
    ) -> Result<ReviewDiffChunkBatch, RuntimeError> {
        let mut cache = self.review_diff_state.cache.lock().await;
        let Some(cached) = cache.as_ref() else {
            return Err(cache_miss_error());
        };
        if request.generation() != cached.checkpoint.generation()
            || request.workspace_digest() != cached.checkpoint.workspace_digest()
        {
            *cache = None;
            return Err(request_mismatch_error());
        }

        let before = match self.fingerprint.collect(cancellation.clone()).await {
            Ok(fingerprint) => fingerprint,
            Err(error) => {
                *cache = None;
                return Err(fingerprint_error(error));
            }
        };
        if before != cached.checkpoint.fingerprint() {
            *cache = None;
            return Err(workspace_changed_error());
        }
        let batch = match cached.bundle.chunk_batch(&request) {
            Ok(batch) => batch,
            Err(error) => {
                if error == ReviewDiffError::ManifestMismatch {
                    *cache = None;
                }
                return Err(review_diff_error(error));
            }
        };
        let after = match self.fingerprint.collect(cancellation).await {
            Ok(fingerprint) => fingerprint,
            Err(error) => {
                *cache = None;
                return Err(fingerprint_error(error));
            }
        };
        if before != after || after != cached.checkpoint.fingerprint() {
            *cache = None;
            return Err(workspace_changed_error());
        }
        Ok(batch)
    }

    async fn terminal_review_diff_manifest(
        &self,
        checkpoint: ReviewDiffCheckpoint,
        redactor: Arc<dyn ContextRedactor>,
        cancellation: CancellationToken,
    ) -> Result<ReviewDiffManifest, RuntimeError> {
        let mut cache = self.review_diff_state.cache.lock().await;
        let Some(cached) = cache.as_ref() else {
            return Err(cache_miss_error());
        };
        if cached.checkpoint != checkpoint {
            *cache = None;
            return Err(request_mismatch_error());
        }
        if !Arc::ptr_eq(&cached.redactor, &redactor) {
            *cache = None;
            return Err(redactor_mismatch_error());
        }
        // Final approval always recollects; a prior Reviewer cache is not an
        // admissible terminal observation.
        *cache = None;
        self.collect_review_bundle(&checkpoint, redactor.as_ref(), cancellation)
            .await
            .map(|bundle| bundle.manifest().clone())
    }
}

impl RuntimeSession {
    async fn serve_cached_manifest(
        &self,
        cached: &CachedReviewDiff,
        checkpoint: &ReviewDiffCheckpoint,
        cancellation: CancellationToken,
    ) -> Result<ReviewDiffManifest, RuntimeError> {
        let before = self
            .fingerprint
            .collect(cancellation.clone())
            .await
            .map_err(fingerprint_error)?;
        if before != checkpoint.fingerprint() {
            return Err(workspace_changed_error());
        }
        let manifest = cached.bundle.manifest().clone();
        let after = self
            .fingerprint
            .collect(cancellation)
            .await
            .map_err(fingerprint_error)?;
        if before != after || after != checkpoint.fingerprint() {
            return Err(workspace_changed_error());
        }
        Ok(manifest)
    }

    async fn collect_review_bundle(
        &self,
        checkpoint: &ReviewDiffCheckpoint,
        redactor: &dyn ContextRedactor,
        cancellation: CancellationToken,
    ) -> Result<ReviewDiffBundle, RuntimeError> {
        let before = self
            .fingerprint
            .collect(cancellation.clone())
            .await
            .map_err(fingerprint_error)?;
        if before != checkpoint.fingerprint() {
            return Err(workspace_changed_error());
        }
        let files = self
            .diff
            .collect_review_inputs(cancellation.clone())
            .await
            .map_err(diff_error)?;
        let bundle =
            ReviewDiffBundle::try_new(checkpoint, files, redactor).map_err(review_diff_error)?;
        let after = self
            .fingerprint
            .collect(cancellation)
            .await
            .map_err(fingerprint_error)?;
        if before != after || after != checkpoint.fingerprint() {
            return Err(workspace_changed_error());
        }
        Ok(bundle)
    }
}

fn review_diff_error(error: ReviewDiffError) -> RuntimeError {
    match error {
        ReviewDiffError::InvalidChunkRequest | ReviewDiffError::ManifestMismatch => {
            request_mismatch_error()
        }
        ReviewDiffError::InvalidCheckpoint => RuntimeError::new(
            "REVIEW_DIFF_EVIDENCE_INVALID",
            "review diff checkpoint authority is invalid",
            false,
        ),
        ReviewDiffError::InvalidPath
        | ReviewDiffError::DuplicatePath
        | ReviewDiffError::InvalidPatch
        | ReviewDiffError::InvalidFileMetadata
        | ReviewDiffError::UnsafeRedaction
        | ReviewDiffError::ManifestTooLarge
        | ReviewDiffError::TooManyChunks
        | ReviewDiffError::CoverageMismatch
        | ReviewDiffError::IncompleteCoverage => RuntimeError::new(
            "REVIEW_DIFF_COVERAGE_LIMIT",
            "review diff cannot be represented within authoritative coverage bounds",
            false,
        ),
    }
}

fn diff_error(error: DiffError) -> RuntimeError {
    let code = error.code();
    RuntimeError::new(
        code,
        "authoritative review diff collection failed",
        matches!(
            code,
            "COMMAND_TIMED_OUT" | "WORKSPACE_CHANGED" | "WORKTREE_CHANGED_DURING_DIFF"
        ),
    )
}

fn fingerprint_error(error: FingerprintError) -> RuntimeError {
    let code = error.code();
    RuntimeError::new(
        code,
        "workspace fingerprint failed during review diff collection",
        matches!(code, "COMMAND_TIMED_OUT" | "WORKSPACE_CHANGED"),
    )
}

fn cache_miss_error() -> RuntimeError {
    RuntimeError::new(
        "REVIEW_DIFF_CACHE_MISS",
        "review diff chunks require the current authoritative manifest",
        false,
    )
}

fn request_mismatch_error() -> RuntimeError {
    RuntimeError::new(
        "REVIEW_DIFF_REQUEST_MISMATCH",
        "review diff chunk request does not exactly match the current manifest",
        false,
    )
}

fn redactor_mismatch_error() -> RuntimeError {
    RuntimeError::new(
        "REVIEW_DIFF_REDACTOR_MISMATCH",
        "terminal review diff requires the same task redactor instance as the Reviewer manifest",
        false,
    )
}

fn workspace_changed_error() -> RuntimeError {
    RuntimeError::new(
        "WORKSPACE_CHANGED",
        "workspace changed during authoritative review diff collection",
        true,
    )
}
