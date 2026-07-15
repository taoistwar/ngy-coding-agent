use crate::{CanonicalPath, RepositoryId, UtcTimestamp};

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct NewRepository {
    pub selected_path: CanonicalPath,
    pub display_name: String,
    pub git_root: CanonicalPath,
    pub cargo_workspace_root: CanonicalPath,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Repository {
    pub id: RepositoryId,
    pub selected_path: CanonicalPath,
    pub display_name: String,
    pub git_root: CanonicalPath,
    pub cargo_workspace_root: CanonicalPath,
    pub created_at: UtcTimestamp,
    pub last_opened_at: UtcTimestamp,
}
