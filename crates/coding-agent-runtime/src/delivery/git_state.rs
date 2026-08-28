use std::ffi::OsStr;
use std::fs::File;
use std::io;

use crate::native_fs::child_entry_exists;

pub(super) const IN_PROGRESS_GIT_STATE_ENTRIES: [&str; 15] = [
    "AUTO_MERGE",
    "BISECT_LOG",
    "BISECT_START",
    "CHERRY_PICK_HEAD",
    "MERGE_AUTOSTASH",
    "MERGE_HEAD",
    "MERGE_MODE",
    "MERGE_MSG",
    "MERGE_RR",
    "REBASE_HEAD",
    "REVERT_HEAD",
    "SQUASH_MSG",
    "index.lock",
    "rebase-apply",
    "rebase-merge",
];

// Kept separate so reviews can compare the fixed list above directly with
// Git's top-level state-file spellings.
pub(super) const SEQUENCER_DIRECTORY: &str = "sequencer";

pub(super) fn has_in_progress_git_state(root: &File) -> io::Result<bool> {
    has_disallowed_git_state(root, &[])
}

pub(super) fn has_disallowed_git_state(root: &File, allowed: &[&str]) -> io::Result<bool> {
    for name in IN_PROGRESS_GIT_STATE_ENTRIES
        .into_iter()
        .filter(|name| !allowed.contains(name))
        .chain(std::iter::once(SEQUENCER_DIRECTORY))
    {
        if child_entry_exists(root, OsStr::new(name))? {
            return Ok(true);
        }
    }
    Ok(false)
}
