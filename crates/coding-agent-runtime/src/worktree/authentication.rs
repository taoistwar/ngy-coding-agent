mod cleanup;
mod identity;
mod linked;
mod metadata;
mod target;

pub(crate) use cleanup::{
    CleanupAbsentAuthentication, CleanupPresentAuthentication, CleanupTopologyIntentV1,
    CleanupTopologyObservation, CleanupWorktreeAuthenticator, CleanupWorktreeTarget,
};
pub(crate) use identity::RetainedDirectory;
pub(crate) use linked::{
    LinkedWorktreeAuthentication, LinkedWorktreeAuthenticator, LinkedWorktreeCommandContext,
};
pub(super) use metadata::{
    admin_commondir_matches, find_reserved_git_directory, list_worktree_admin_entries,
    read_admin_gitdir, read_admin_line,
};
