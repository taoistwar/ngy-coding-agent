use std::fmt;

use crate::delivery::{DeliveryCommitMetadata, DeliveryError, GitCommitOid, GitTreeOid};

#[derive(Clone, PartialEq, Eq)]
pub struct MergeCommitObjectProof {
    pub(in crate::delivery::merges) expected_merge_commit: GitCommitOid,
    pub(in crate::delivery::merges) tree: GitTreeOid,
    pub(in crate::delivery::merges) parents: [GitCommitOid; 2],
    pub(in crate::delivery::merges) metadata: DeliveryCommitMetadata,
}

impl MergeCommitObjectProof {
    pub fn try_new(
        expected_merge_commit: GitCommitOid,
        tree: GitTreeOid,
        parents: Vec<GitCommitOid>,
        metadata: DeliveryCommitMetadata,
    ) -> Result<Self, DeliveryError> {
        let parents: [GitCommitOid; 2] = parents
            .try_into()
            .map_err(|_| DeliveryError::InvalidCommandRequest)?;
        let algorithm = expected_merge_commit.algorithm();
        if tree.algorithm() != algorithm
            || parents.iter().any(|parent| parent.algorithm() != algorithm)
            || parents[0] == parents[1]
            || parents.contains(&expected_merge_commit)
        {
            return Err(DeliveryError::InvalidCommandRequest);
        }
        Ok(Self {
            expected_merge_commit,
            tree,
            parents,
            metadata,
        })
    }
}

impl fmt::Debug for MergeCommitObjectProof {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MergeCommitObjectProof")
            .field("object_shape", &"<redacted>")
            .finish()
    }
}
