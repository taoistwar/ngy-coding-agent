mod abort;
mod applied;
mod object;

pub use abort::{MergeAbortAppliedProof, MergeAbortProof};
pub use applied::MergeAppliedProof;
pub use object::MergeCommitObjectProof;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MergeAutostashObservation {
    Absent,
    Present,
    Unobservable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OtherGitOperationObservation {
    Clear,
    Present,
    Unobservable,
}
