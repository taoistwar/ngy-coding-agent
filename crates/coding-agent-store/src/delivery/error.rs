/// Stable, redacted validation failures for durable delivery values and states.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum DeliveryError {
    #[error("delivery UUID is invalid")]
    InvalidUuid,
    #[error("delivery identity is invalid")]
    InvalidIdentity,
    #[error("Git object ID is invalid")]
    InvalidGitOid,
    #[error("SHA-256 digest is invalid")]
    InvalidSha256Digest,
    #[error("Git branch reference is invalid")]
    InvalidGitBranchRef,
    #[error("delivery timestamp is invalid")]
    InvalidTimestamp,
    #[error("delivery version is invalid")]
    InvalidVersion,
    #[error("delivery failure code is invalid")]
    InvalidFailureCode,
    #[error("delivery evidence identity is invalid")]
    InvalidEvidenceIdentity,
    #[error("directory identity is invalid")]
    InvalidDirectoryIdentity,
    #[error("delivery command request is invalid")]
    InvalidCommandRequest,
    #[error("delivery state value is invalid")]
    InvalidState,
    #[error("delivery state transition is not allowed")]
    IllegalTransition,
    #[error("delivery states are inconsistent")]
    InvalidStateCombination,
}
