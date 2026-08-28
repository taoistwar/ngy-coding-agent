use std::ffi::{OsStr, OsString};
use std::fs::File;
use std::io;

use super::{DeliveryCommandSandbox, DeliverySourceError, SandboxStage};
use crate::native_fs::{
    child_directory_matches, child_entry_exists, quarantine_child_entry_no_replace,
    read_directory_names, remove_child_directory, reopen_directory,
    reopen_directory_for_child_directory, reopen_directory_for_delete,
};

const MAX_QUARANTINE_ATTEMPTS: usize = 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum CleanupPhase {
    BeforeNamespaceRemoval,
}

impl DeliveryCommandSandbox {
    pub(super) fn cleanup_inner(&mut self) -> Result<(), DeliverySourceError> {
        self.cleanup_inner_with_hook(&mut |_| {})
    }

    pub(super) fn cleanup_inner_with_hook(
        &mut self,
        hook: &mut dyn FnMut(CleanupPhase),
    ) -> Result<(), DeliverySourceError> {
        if self.cleaned {
            return Ok(());
        }
        self.validate_cleanup_target()?;

        let parent = self.cleanup_parent()?;
        let workspace = self.cleanup_workspace_handle()?;

        // DirectoryPathGuard deliberately blocks Windows namespace mutation.
        // Every exact identity needed below has already been retained, so the
        // guards can be released immediately before the adversarial boundary.
        drop(self.workspace_guard.take());
        hook(CleanupPhase::BeforeNamespaceRemoval);

        if !child_directory_matches(&parent, OsStr::new(&self.name), &workspace)
            .map_err(|_| cleanup_unproven())?
        {
            return Err(cleanup_unproven());
        }

        #[cfg(unix)]
        if self.stage != SandboxStage::Workspace {
            self.remove_config()?;
        }
        require_directory_empty(&workspace)?;

        let quarantined = quarantine_directory(&parent, OsStr::new(&self.name), &workspace)?;
        drop(self.workspace_directory.take());
        remove_quarantined_directory(&parent, quarantined, workspace)?;
        self.cleaned = true;

        self.parent.revalidate().map_err(|_| cleanup_unproven())?;
        require_child_presence(&parent, &self.name, false)
    }

    fn cleanup_parent(&self) -> Result<File, DeliverySourceError> {
        let root = self
            .parent
            .cloned_root_capability()
            .map_err(|_| cleanup_unproven())?;
        let handle = root.try_clone_root().map_err(|_| cleanup_unproven())?;
        reopen_directory_for_child_directory(&handle).map_err(|_| cleanup_unproven())
    }

    fn cleanup_workspace_handle(&self) -> Result<File, DeliverySourceError> {
        let root = self
            .workspace_directory
            .as_ref()
            .ok_or(cleanup_unproven())?
            .cloned_root_capability()
            .map_err(|_| cleanup_unproven())?;
        root.try_clone_root().map_err(|_| cleanup_unproven())
    }

    #[cfg(unix)]
    fn remove_config(&mut self) -> Result<(), DeliverySourceError> {
        drop(self.config_file.take());
        Ok(())
    }

    fn validate_cleanup_target(&self) -> Result<(), DeliverySourceError> {
        if self.stage == SandboxStage::Ready {
            self.validate_retained_state()
                .map_err(|_| cleanup_unproven())
        } else {
            self.parent.revalidate().map_err(|_| cleanup_unproven())?;
            self.validate_workspace_identity()
                .map_err(|_| cleanup_unproven())?;
            #[cfg(unix)]
            if self.stage == SandboxStage::Config {
                self.validate_config().map_err(|_| cleanup_unproven())?;
            }
            self.validate_workspace_entries()
                .map_err(|_| cleanup_unproven())
        }
    }
}

struct QuarantinedEntry {
    name: OsString,
    entry: File,
}

fn quarantine_directory(
    parent: &File,
    source: &OsStr,
    retained: &File,
) -> Result<QuarantinedEntry, DeliverySourceError> {
    if !child_directory_matches(parent, source, retained).map_err(|_| cleanup_unproven())? {
        return Err(cleanup_unproven());
    }
    let entry = reopen_directory_for_delete(retained).map_err(|_| cleanup_unproven())?;
    if !child_directory_matches(parent, source, retained).map_err(|_| cleanup_unproven())? {
        return Err(cleanup_unproven());
    }
    let name = quarantine_entry(parent, source, &entry)?;
    if !child_directory_matches(parent, &name, retained).map_err(|_| cleanup_unproven())? {
        return Err(cleanup_unproven());
    }
    Ok(QuarantinedEntry { name, entry })
}

fn quarantine_entry(
    parent: &File,
    source: &OsStr,
    entry: &File,
) -> Result<OsString, DeliverySourceError> {
    for _ in 0..MAX_QUARANTINE_ATTEMPTS {
        let name = random_quarantine_name()?;
        match quarantine_child_entry_no_replace(parent, source, &name, entry) {
            Ok(()) => return Ok(name),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(_) => return Err(cleanup_unproven()),
        }
    }
    Err(cleanup_unproven())
}

fn remove_quarantined_directory(
    parent: &File,
    quarantined: QuarantinedEntry,
    retained: File,
) -> Result<(), DeliverySourceError> {
    if !child_directory_matches(parent, &quarantined.name, &retained)
        .map_err(|_| cleanup_unproven())?
    {
        return Err(cleanup_unproven());
    }
    remove_child_directory(parent, &quarantined.name, &quarantined.entry)
        .map_err(|_| cleanup_unproven())?;
    drop(quarantined.entry);
    drop(retained);
    require_child_presence_os(parent, &quarantined.name, false)
}

fn require_directory_empty(directory: &File) -> Result<(), DeliverySourceError> {
    let mut reopened = reopen_directory(directory).map_err(|_| cleanup_unproven())?;
    if read_directory_names(&mut reopened, 1)
        .map_err(|_| cleanup_unproven())?
        .is_empty()
    {
        Ok(())
    } else {
        Err(cleanup_unproven())
    }
}

fn require_child_presence(
    parent: &File,
    name: &str,
    expected: bool,
) -> Result<(), DeliverySourceError> {
    require_child_presence_os(parent, OsStr::new(name), expected)
}

fn require_child_presence_os(
    parent: &File,
    name: &OsStr,
    expected: bool,
) -> Result<(), DeliverySourceError> {
    let present = child_entry_exists(parent, name).map_err(|_| cleanup_unproven())?;
    if present == expected {
        Ok(())
    } else {
        Err(cleanup_unproven())
    }
}

fn random_quarantine_name() -> Result<OsString, DeliverySourceError> {
    let mut random = [0u8; 16];
    getrandom::fill(&mut random).map_err(|_| cleanup_unproven())?;
    let mut name = String::from(".coding-agent-cleanup-v1-");
    for byte in random {
        use std::fmt::Write as _;
        write!(&mut name, "{byte:02x}").expect("writing hexadecimal bytes to String cannot fail");
    }
    Ok(OsString::from(name))
}

const fn cleanup_unproven() -> DeliverySourceError {
    DeliverySourceError::SandboxCleanupUnproven
}
