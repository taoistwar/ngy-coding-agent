#![cfg(windows)]

use std::ffi::OsString;
use std::os::windows::ffi::{OsStrExt, OsStringExt};
use std::path::{Path, PathBuf};

use coding_agent_runtime::{
    AtomicFileReplacer, AtomicReplaceError, AtomicReplaceLimits, FileToolError, FileToolLimits,
    FileTools, RelativePath, RootCapability,
};
use tokio_util::sync::CancellationToken;
use windows_sys::Win32::Storage::FileSystem::GetShortPathNameW;

#[test]
fn protected_metadata_identity_probe_accepts_both_file_and_directory_objects() {
    for metadata_is_directory in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let metadata = root.path().join(".git");
        if metadata_is_directory {
            std::fs::create_dir(&metadata).unwrap();
        } else {
            std::fs::write(&metadata, b"gitdir: elsewhere\n").unwrap();
        }
        std::fs::write(root.path().join("visible.txt"), b"visible\n").unwrap();
        let tools = FileTools::new(RootCapability::open(root.path()).unwrap(), file_limits());

        let result = tools
            .read_file(&RelativePath::parse("visible.txt").unwrap(), 1, 10)
            .unwrap();
        assert_eq!(result.lines[0].text, "visible");
    }
}

#[test]
fn dos_alias_of_git_directory_is_rejected_by_model_visible_file_tools_when_available() {
    let root = tempfile::tempdir().unwrap();
    let metadata = root.path().join(".git");
    std::fs::create_dir(&metadata).unwrap();
    std::fs::write(metadata.join("config"), b"protected\n").unwrap();
    let Some(alias) = distinct_short_component(&metadata) else {
        return;
    };
    let directory = RelativePath::parse(alias.clone()).unwrap();
    let file = RelativePath::parse(format!("{alias}/config")).unwrap();
    let tools = FileTools::new(RootCapability::open(root.path()).unwrap(), file_limits());

    assert_protected_file_error(tools.read_file(&file, 1, 10).unwrap_err());
    assert_protected_file_error(tools.list_files(&directory, 2, 100).unwrap_err());
    assert_protected_file_error(
        tools
            .search_text("protected", &directory, None, 100)
            .unwrap_err(),
    );

    let replacer = AtomicFileReplacer::new(
        RootCapability::open(root.path()).unwrap(),
        AtomicReplaceLimits::try_new(1_024).unwrap(),
    );
    let error = replacer
        .replace_file(&file, None, b"overwritten\n", &CancellationToken::new())
        .unwrap_err();
    assert_protected_replace_error(error);
    assert_eq!(
        std::fs::read(metadata.join("config")).unwrap(),
        b"protected\n"
    );
}

#[test]
fn dos_alias_of_git_file_is_rejected_after_final_target_open_when_available() {
    let root = tempfile::tempdir().unwrap();
    let metadata = root.path().join(".git");
    std::fs::write(&metadata, b"protected\n").unwrap();
    let Some(alias) = distinct_short_component(&metadata) else {
        return;
    };
    let path = RelativePath::parse(alias).unwrap();
    let replacer = AtomicFileReplacer::new(
        RootCapability::open(root.path()).unwrap(),
        AtomicReplaceLimits::try_new(1_024).unwrap(),
    );

    let error = replacer
        .replace_file(&path, None, b"overwritten\n", &CancellationToken::new())
        .unwrap_err();
    assert_protected_replace_error(error);
    assert_eq!(std::fs::read(metadata).unwrap(), b"protected\n");
}

fn file_limits() -> FileToolLimits {
    FileToolLimits::try_new(
        64 * 1_024,
        256 * 1_024,
        64 * 1_024,
        64 * 1_024,
        8,
        1_024,
        1_024,
        1_024,
    )
    .unwrap()
}

fn assert_protected_file_error(error: FileToolError) {
    assert!(
        matches!(error, FileToolError::Io(ref source) if source.kind() == std::io::ErrorKind::PermissionDenied),
        "unexpected protected-metadata error: {error:?}"
    );
}

fn assert_protected_replace_error(error: AtomicReplaceError) {
    assert!(
        matches!(error, AtomicReplaceError::AtomicReplaceFailed(ref source) if source.kind() == std::io::ErrorKind::PermissionDenied),
        "unexpected protected-metadata error: {error:?}"
    );
}

fn distinct_short_component(path: &Path) -> Option<String> {
    let input = path
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect::<Vec<_>>();
    let mut output = vec![0_u16; 32_768];
    let written = unsafe {
        GetShortPathNameW(
            input.as_ptr(),
            output.as_mut_ptr(),
            u32::try_from(output.len()).unwrap(),
        )
    };
    if written == 0 || written as usize >= output.len() {
        eprintln!(
            "SKIP: this Windows volume did not expose a DOS short name for {}",
            path.display()
        );
        return None;
    }
    let short_path = PathBuf::from(OsString::from_wide(&output[..written as usize]));
    let Some(component) = short_path.file_name().and_then(|name| name.to_str()) else {
        eprintln!(
            "SKIP: the DOS short name for {} was not Unicode",
            path.display()
        );
        return None;
    };
    if component.eq_ignore_ascii_case(".git") {
        eprintln!(
            "SKIP: this Windows volume returned no distinct DOS alias for {}",
            path.display()
        );
        return None;
    }
    let alias = component.to_owned();
    if RelativePath::parse(alias.clone()).is_err() || !path.with_file_name(&alias).exists() {
        eprintln!(
            "SKIP: the reported DOS alias for {} was not a usable relative component",
            path.display()
        );
        return None;
    }
    Some(alias)
}
