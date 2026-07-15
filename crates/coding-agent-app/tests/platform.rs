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

    #[cfg(windows)]
    {
        let inherited_child = paths.data_dir.join("inherited-child.tmp");
        std::fs::File::create(&inherited_child)
            .expect("create a child file through the private directory DACL");
        assert_inherited_private_file(&inherited_child);
    }
}

#[test]
fn private_file_is_create_new_private_and_refuses_a_final_symlink() {
    let temp = tempfile::tempdir().expect("create private-file fixture");
    let path = temp.path().join("instance.json");
    let mut file = PrivateFile::create_new(&path).expect("create private file");
    assert_private_path(&path);
    file.write_all(b"private contents")
        .expect("write private contents");
    file.flush().expect("flush private contents");

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
    let expected_inheritance = if path.is_dir() {
        windows_sys::Win32::Security::SUB_CONTAINERS_AND_OBJECTS_INHERIT
    } else {
        windows_sys::Win32::Security::NO_INHERITANCE
    };
    assert_windows_owner_only_acl(path, true, Some(expected_inheritance));
}

#[cfg(windows)]
fn assert_inherited_private_file(path: &std::path::Path) {
    assert_windows_owner_only_acl(path, false, None);
}

#[cfg(windows)]
fn assert_windows_owner_only_acl(
    path: &std::path::Path,
    protected: bool,
    expected_inheritance: Option<u32>,
) {
    use std::mem::size_of;
    use std::os::windows::ffi::OsStrExt;
    use std::ptr::{null_mut, slice_from_raw_parts};

    use windows_sys::Win32::Foundation::{CloseHandle, ERROR_SUCCESS, LocalFree};
    use windows_sys::Win32::Security::Authorization::{
        BuildTrusteeWithSidW, EXPLICIT_ACCESS_W, GRANT_ACCESS, GetEffectiveRightsFromAclW,
        GetExplicitEntriesFromAclW, GetNamedSecurityInfoW, SE_FILE_OBJECT, TRUSTEE_IS_SID,
        TRUSTEE_W,
    };
    use windows_sys::Win32::Security::{
        ACCESS_ALLOWED_ACE, ACL, ACL_SIZE_INFORMATION, AclSizeInformation,
        DACL_SECURITY_INFORMATION, EqualSid, GetAce, GetAclInformation,
        GetSecurityDescriptorControl, GetTokenInformation, INHERITED_ACE,
        OWNER_SECURITY_INFORMATION, PSECURITY_DESCRIPTOR, PSID, SE_DACL_PROTECTED, TOKEN_QUERY,
        TOKEN_USER, TokenUser,
    };
    use windows_sys::Win32::Storage::FileSystem::FILE_ALL_ACCESS;
    use windows_sys::Win32::System::SystemServices::ACCESS_ALLOWED_ACE_TYPE;
    use windows_sys::Win32::System::Threading::{GetCurrentProcess, OpenProcessToken};

    let mut token = null_mut();
    assert_ne!(
        unsafe { OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut token) },
        0,
        "open current-process token"
    );
    let mut required = 0u32;
    unsafe {
        GetTokenInformation(token, TokenUser, null_mut(), 0, &mut required);
    }
    assert_ne!(required, 0, "measure TokenUser buffer");
    let mut token_buffer = vec![0usize; (required as usize).div_ceil(size_of::<usize>())];
    assert_ne!(
        unsafe {
            GetTokenInformation(
                token,
                TokenUser,
                token_buffer.as_mut_ptr().cast(),
                required,
                &mut required,
            )
        },
        0,
        "read TokenUser"
    );
    unsafe { CloseHandle(token) };
    let current_user = unsafe { (&*token_buffer.as_ptr().cast::<TOKEN_USER>()).User.Sid };

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
    assert_ne!(
        unsafe { EqualSid(owner, current_user) },
        0,
        "the path owner must be the current TokenUser"
    );

    let mut control = 0u16;
    let mut revision = 0u32;
    assert_ne!(
        unsafe { GetSecurityDescriptorControl(descriptor, &mut control, &mut revision) },
        0
    );
    assert_eq!(
        control & SE_DACL_PROTECTED != 0,
        protected,
        "unexpected DACL protection on {}",
        path.display()
    );

    let mut acl_size = ACL_SIZE_INFORMATION::default();
    assert_ne!(
        unsafe {
            GetAclInformation(
                dacl,
                (&mut acl_size as *mut ACL_SIZE_INFORMATION).cast(),
                size_of::<ACL_SIZE_INFORMATION>() as u32,
                AclSizeInformation,
            )
        },
        0,
        "read ACL size"
    );
    assert_eq!(
        acl_size.AceCount, 1,
        "only the owner may appear in the DACL"
    );
    let mut ace_ptr = null_mut();
    assert_ne!(
        unsafe { GetAce(dacl, 0, &mut ace_ptr) },
        0,
        "read owner ACE"
    );
    let ace = unsafe { &*ace_ptr.cast::<ACCESS_ALLOWED_ACE>() };
    assert_eq!(ace.Header.AceType as u32, ACCESS_ALLOWED_ACE_TYPE);
    assert_eq!(
        ace.Header.AceFlags as u32 & INHERITED_ACE != 0,
        !protected,
        "unexpected inherited-ACE state"
    );
    let ace_sid = std::ptr::addr_of!(ace.SidStart).cast_mut().cast();
    assert_ne!(
        unsafe { EqualSid(current_user, ace_sid) },
        0,
        "the sole allow ACE must name the current TokenUser"
    );

    let mut trustee = TRUSTEE_W::default();
    unsafe { BuildTrusteeWithSidW(&mut trustee, current_user) };
    let mut effective_rights = 0u32;
    assert_eq!(
        unsafe { GetEffectiveRightsFromAclW(dacl, &trustee, &mut effective_rights) },
        ERROR_SUCCESS,
        "read current-user effective rights"
    );
    assert_eq!(
        effective_rights & FILE_ALL_ACCESS,
        FILE_ALL_ACCESS,
        "the current user must retain full file access"
    );

    if let Some(expected_inheritance) = expected_inheritance {
        let mut count = 0u32;
        let mut entries_ptr: *mut EXPLICIT_ACCESS_W = null_mut();
        let entries_status =
            unsafe { GetExplicitEntriesFromAclW(dacl, &mut count, &mut entries_ptr) };
        assert_eq!(entries_status, ERROR_SUCCESS);
        assert_eq!(count, 1, "the protected object must have one explicit ACE");
        let entries = unsafe { &*slice_from_raw_parts(entries_ptr, count as usize) };
        let entry = entries[0];
        assert_eq!(
            entry.grfAccessMode, GRANT_ACCESS,
            "the owner ACE must allow"
        );
        assert_eq!(
            entry.grfInheritance,
            expected_inheritance,
            "unexpected ACE inheritance on {}",
            path.display()
        );
        assert_eq!(entry.Trustee.TrusteeForm, TRUSTEE_IS_SID);
        assert_ne!(
            unsafe { EqualSid(current_user, entry.Trustee.ptstrName.cast()) },
            0,
            "the explicit access entry must be the current TokenUser"
        );
        unsafe { LocalFree(entries_ptr.cast()) };
    }

    unsafe {
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
