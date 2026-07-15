mod support;

use std::io::Write;

use coding_agent_app::{BrowserLauncher, PlatformPaths, PrivateFile};

#[test]
fn project_directories_use_the_approved_identity_and_runtime_fallback() {
    let project = directories::ProjectDirs::from("com", "ngy", "coding-agent")
        .expect("the current user has project directories");
    let paths = PlatformPaths::discover().expect("discover platform paths");

    assert_eq!(paths.data_dir, project.data_local_dir());
    assert_eq!(
        paths.runtime_dir,
        project
            .runtime_dir()
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| project.data_local_dir().join("run"))
    );
    assert_eq!(
        paths.database_path,
        paths.data_dir.join("coding-agent.sqlite3")
    );
    assert_eq!(paths.instance_lock, paths.runtime_dir.join("instance.lock"));
    assert_eq!(
        paths.instance_descriptor,
        paths.runtime_dir.join("instance.json")
    );
    assert_eq!(
        paths.unclean_shutdown,
        paths.data_dir.join("unclean-shutdown.json")
    );
}

#[test]
fn provisioning_is_idempotent_and_makes_both_directories_private() {
    let temp = tempfile::tempdir().expect("create private-directory fixture");
    let paths = PlatformPaths::new(temp.path().join("data"), temp.path().join("runtime"));

    paths.prepare().expect("prepare paths once");
    paths.prepare().expect("prepare paths twice");

    assert!(paths.data_dir.is_dir());
    assert!(paths.runtime_dir.is_dir());
    assert_private_path(&paths.data_dir);
    assert_private_path(&paths.runtime_dir);
}

#[test]
fn private_file_is_create_new_private_and_refuses_a_final_symlink() {
    let temp = tempfile::tempdir().expect("create private-file fixture");
    let path = temp.path().join("instance.json");
    let mut file = PrivateFile::create_new(&path).expect("create private file");
    file.write_all(b"private contents")
        .expect("write private contents");
    file.flush().expect("flush private contents");
    assert_private_path(&path);

    let duplicate =
        PrivateFile::create_new(&path).expect_err("create_new rejects an existing path");
    assert_eq!(duplicate.kind(), std::io::ErrorKind::AlreadyExists);

    let target = temp.path().join("target.json");
    std::fs::write(&target, b"must remain untouched").expect("write symlink target");
    let link = temp.path().join("linked.json");
    create_file_symlink(&target, &link).expect("create final-path symlink");
    let error = PrivateFile::create_new(&link).expect_err("final symlink is never followed");
    assert_eq!(error.kind(), std::io::ErrorKind::AlreadyExists);
    assert_eq!(
        std::fs::read(&target).expect("read symlink target"),
        b"must remain untouched"
    );
}

#[test]
fn browser_url_is_complete_and_launch_failure_keeps_the_copyable_url() {
    let url = BrowserLauncher::url(42_123, "launch-token");
    assert_eq!(
        url, "http://127.0.0.1:42123/#token=launch-token",
        "the browser receives the complete fragment URL"
    );

    let error = BrowserLauncher::open(0, "launch-token")
        .expect_err("port zero is not a launchable application URL");
    assert_eq!(
        error.url(),
        "http://127.0.0.1:0/#token=launch-token",
        "the caller can display and copy the complete failed URL"
    );
}

#[cfg(unix)]
fn assert_private_path(path: &std::path::Path) {
    use std::os::unix::fs::PermissionsExt;

    let mode = std::fs::metadata(path)
        .expect("read private path metadata")
        .permissions()
        .mode()
        & 0o777;
    let expected = if path.is_dir() { 0o700 } else { 0o600 };
    assert_eq!(mode, expected, "unexpected mode for {}", path.display());
}

#[cfg(windows)]
fn assert_private_path(path: &std::path::Path) {
    use std::os::windows::ffi::OsStrExt;
    use std::ptr::{null_mut, slice_from_raw_parts};

    use windows_sys::Win32::Foundation::{ERROR_SUCCESS, LocalFree};
    use windows_sys::Win32::Security::Authorization::{
        EXPLICIT_ACCESS_W, GetExplicitEntriesFromAclW, GetNamedSecurityInfoW, SE_FILE_OBJECT,
        TRUSTEE_IS_SID,
    };
    use windows_sys::Win32::Security::{
        ACL, DACL_SECURITY_INFORMATION, EqualSid, GetSecurityDescriptorControl, NO_INHERITANCE,
        OWNER_SECURITY_INFORMATION, PSECURITY_DESCRIPTOR, PSID, SE_DACL_PROTECTED,
    };

    let wide = path
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect::<Vec<_>>();
    let mut owner: PSID = null_mut();
    let mut dacl: *mut ACL = null_mut();
    let mut descriptor: PSECURITY_DESCRIPTOR = null_mut();
    let status = unsafe {
        GetNamedSecurityInfoW(
            wide.as_ptr(),
            SE_FILE_OBJECT,
            OWNER_SECURITY_INFORMATION | DACL_SECURITY_INFORMATION,
            &mut owner,
            null_mut(),
            &mut dacl,
            null_mut(),
            &mut descriptor,
        )
    };
    assert_eq!(status, ERROR_SUCCESS, "read DACL for {}", path.display());
    assert!(!owner.is_null());
    assert!(!dacl.is_null());

    let mut control = 0u16;
    let mut revision = 0u32;
    assert_ne!(
        unsafe { GetSecurityDescriptorControl(descriptor, &mut control, &mut revision) },
        0
    );
    assert_ne!(
        control & SE_DACL_PROTECTED,
        0,
        "DACL must reject inheritance"
    );

    let mut count = 0u32;
    let mut entries: *mut EXPLICIT_ACCESS_W = null_mut();
    let entries_status = unsafe { GetExplicitEntriesFromAclW(dacl, &mut count, &mut entries) };
    assert_eq!(entries_status, ERROR_SUCCESS);
    assert_eq!(count, 1, "only the owner may appear in the DACL");
    let entries = unsafe { &*slice_from_raw_parts(entries, count as usize) };
    let entry = entries[0];
    assert_eq!(entry.grfInheritance, NO_INHERITANCE);
    assert_eq!(entry.Trustee.TrusteeForm, TRUSTEE_IS_SID);
    assert_ne!(
        unsafe { EqualSid(owner, entry.Trustee.ptstrName.cast()) },
        0,
        "the sole access entry must be the current file owner"
    );

    unsafe {
        LocalFree(entries.as_ptr().cast_mut().cast());
        LocalFree(descriptor.cast());
    }
}

#[cfg(unix)]
fn create_file_symlink(target: &std::path::Path, link: &std::path::Path) -> std::io::Result<()> {
    std::os::unix::fs::symlink(target, link)
}

#[cfg(windows)]
fn create_file_symlink(target: &std::path::Path, link: &std::path::Path) -> std::io::Result<()> {
    std::os::windows::fs::symlink_file(target, link)
}
