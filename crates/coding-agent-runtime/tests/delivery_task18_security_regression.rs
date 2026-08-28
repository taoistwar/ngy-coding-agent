mod support;

#[allow(dead_code)]
#[path = "delivery_security/fixture.rs"]
mod security_fixture;

use std::path::Path;
#[cfg(windows)]
use std::path::PathBuf;

use security_fixture::SecurityFixture;
use tokio_util::sync::CancellationToken;

/// Completes the Task 18 helper matrix without adding a test-only production
/// shortcut. Every case enters through the ordinary authenticated source open,
/// and its executable value must remain absent from both effects and errors.
#[tokio::test]
async fn askpass_credential_signature_and_tool_helpers_are_rejected_and_redacted() {
    let fixture = SecurityFixture::new().await;
    let approved = fixture.fingerprint().await;
    let source = fixture.source_provisioner().unwrap();
    let sentinel = fixture.root.join("task18-external-helper-ran");
    let helper = shell_probe_command(&sentinel);

    for key in [
        "core.askPass",
        "credential.helper",
        "gpg.program",
        "gpg.ssh.program",
        "user.signingKey",
        "difftool.delivery-task18.cmd",
        "mergetool.delivery-task18.cmd",
    ] {
        fixture.set_common_config(key, &helper);
        let error = source
            .open_delivery_source(&fixture.reservation, approved, CancellationToken::new())
            .await
            .unwrap_err();

        assert_eq!(error.code(), "UNSAFE_GIT_CONFIGURATION", "key={key}");
        let rendered = format!("{error:?} {error}");
        assert!(!rendered.contains(&fixture.root.to_string_lossy().to_string()));
        assert!(!rendered.contains(&sentinel.to_string_lossy().to_string()));
        assert!(!rendered.contains("delivery-task18"));
        assert!(!sentinel.exists(), "external helper executed for {key}");
        fixture.unset_common_config(key);
    }
}

fn shell_probe_command(sentinel: &Path) -> String {
    if cfg!(windows) {
        format!("cmd.exe /C echo executed>{}", path_for_git(sentinel))
    } else {
        format!("touch {}", shell_quote(&path_for_git(sentinel)))
    }
}

fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\"'\"'"))
}

fn path_for_git(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

#[cfg(windows)]
#[test]
fn subst_alias_retains_the_authenticated_directory_identity() {
    use std::process::Command;

    use coding_agent_runtime::RootCapability;

    let temporary = tempfile::tempdir().unwrap();
    let directory = temporary.path().join("Task18SubstIdentity");
    std::fs::create_dir(&directory).unwrap();
    let physical = directory.canonicalize().unwrap();
    let expected = RootCapability::open(&physical)
        .unwrap()
        .identity_marker()
        .unwrap();
    let Some(drive) = (b'D'..=b'Z')
        .rev()
        .map(|letter| format!("{}:", char::from(letter)))
        .find(|drive| !Path::new(&format!("{drive}\\")).exists())
    else {
        eprintln!("SKIP[windows-subst-unavailable]: no unused drive letter is available");
        return;
    };
    let status = Command::new("subst.exe")
        .arg(&drive)
        .arg(temporary.path().canonicalize().unwrap())
        .status();
    let Ok(status) = status else {
        eprintln!("SKIP[windows-subst-unavailable]: subst.exe could not be started");
        return;
    };
    if !status.success() {
        eprintln!("SKIP[windows-subst-unavailable]: subst.exe rejected the temporary mapping");
        return;
    }
    let _mapping = SubstMapping(drive.clone());
    let alias = PathBuf::from(format!("{drive}\\Task18SubstIdentity"));

    assert_eq!(
        RootCapability::open(alias)
            .unwrap()
            .identity_marker()
            .unwrap(),
        expected,
    );
    eprintln!("EVIDENCE[windows-subst-directory-identity]: verified");
}

#[cfg(windows)]
struct SubstMapping(String);

#[cfg(windows)]
impl Drop for SubstMapping {
    fn drop(&mut self) {
        let _ = std::process::Command::new("subst.exe")
            .arg(&self.0)
            .arg("/D")
            .status();
    }
}

#[cfg(target_os = "linux")]
#[test]
fn unix_bind_mount_alias_retains_the_authenticated_directory_identity() {
    run_linux_bind_mount_alias_test();
}

// The body uses only portable Rust APIs and is type-checked on every host;
// the wrapper above is what prevents non-Linux systems from executing mount.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn run_linux_bind_mount_alias_test() {
    use std::io;
    use std::process::Command;

    use coding_agent_runtime::RootCapability;
    use linux_bind_mount_support::{
        LinuxBindMount, bind_mount_is_explicitly_unavailable, existing_program,
    };

    let Some(mount_program) = existing_program(["/usr/bin/mount", "/bin/mount"]) else {
        eprintln!("SKIP[unix-bind-mount-unavailable]: mount command is unavailable");
        return;
    };
    let Some(umount_program) = existing_program(["/usr/bin/umount", "/bin/umount"]) else {
        eprintln!("SKIP[unix-bind-mount-unavailable]: umount command is unavailable");
        return;
    };
    let temporary = tempfile::tempdir().unwrap();
    let source = temporary.path().join("physical");
    let alias = temporary.path().join("bind-alias");
    std::fs::create_dir(&source).unwrap();
    std::fs::create_dir(&alias).unwrap();
    std::fs::write(source.join("identity-sentinel"), b"physical directory\n").unwrap();
    let expected = RootCapability::open(source.canonicalize().unwrap())
        .unwrap()
        .identity_marker()
        .unwrap();

    let output = match Command::new(&mount_program)
        .arg("--bind")
        .arg(&source)
        .arg(&alias)
        .env("LC_ALL", "C")
        .env("LANG", "C")
        .output()
    {
        Ok(output) => output,
        Err(error)
            if matches!(
                error.kind(),
                io::ErrorKind::NotFound | io::ErrorKind::PermissionDenied
            ) =>
        {
            eprintln!(
                "SKIP[unix-bind-mount-unavailable]: mount command cannot be executed ({:?})",
                error.kind()
            );
            return;
        }
        Err(error) => panic!("mount command failed unexpectedly: {error}"),
    };
    if !output.status.success() {
        let diagnostic = String::from_utf8_lossy(&output.stderr).to_ascii_lowercase();
        if bind_mount_is_explicitly_unavailable(&diagnostic) {
            eprintln!(
                "SKIP[unix-bind-mount-unavailable]: mount --bind is unsupported or not permitted"
            );
            return;
        }
        panic!(
            "mount --bind failed without an explicit unsupported/permission diagnostic (status {:?})",
            output.status.code()
        );
    }

    let mut mapping = LinuxBindMount::new(umount_program, alias.clone());
    let observed = RootCapability::open(alias.canonicalize().unwrap())
        .unwrap()
        .identity_marker()
        .unwrap();
    assert_eq!(observed, expected);
    assert_eq!(
        std::fs::read(alias.join("identity-sentinel")).unwrap(),
        b"physical directory\n"
    );

    if let Err(error) = mapping.unmount() {
        mapping.abandon_cleanup();
        std::mem::forget(temporary);
        panic!(
            "bind mount cleanup failed; its private temporary tree was intentionally retained: {error}"
        );
    }
    assert!(std::fs::read_dir(&alias).unwrap().next().is_none());
    assert_eq!(
        std::fs::read(source.join("identity-sentinel")).unwrap(),
        b"physical directory\n"
    );
    eprintln!("EVIDENCE[unix-bind-mount-directory-identity]: verified");
}

#[cfg(all(unix, not(target_os = "linux")))]
#[test]
fn unix_bind_mount_alias_retains_the_authenticated_directory_identity() {
    eprintln!(
        "SKIP[unix-bind-mount-unavailable]: this host does not support the Linux mount --bind probe"
    );
}

// The mount support itself is deliberately compiled on non-Linux hosts too,
// so ordinary Windows development still type-checks the cleanup guard. Only
// the actual mount test is platform-gated.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
mod linux_bind_mount_support {
    use std::path::PathBuf;
    use std::process::Command;

    pub(super) fn existing_program<const N: usize>(candidates: [&str; N]) -> Option<PathBuf> {
        candidates
            .into_iter()
            .map(PathBuf::from)
            .find(|candidate| candidate.is_file())
    }

    pub(super) fn bind_mount_is_explicitly_unavailable(diagnostic: &str) -> bool {
        [
            "permission denied",
            "operation not permitted",
            "must be superuser",
            "only root",
            "not permitted",
            "unrecognized option",
            "unknown option",
            "illegal option",
            "invalid option",
            "not supported",
            "unsupported",
        ]
        .iter()
        .any(|reason| diagnostic.contains(reason))
    }

    pub(super) struct LinuxBindMount {
        umount_program: PathBuf,
        mount_point: PathBuf,
        active: bool,
    }

    impl LinuxBindMount {
        pub(super) fn new(umount_program: PathBuf, mount_point: PathBuf) -> Self {
            Self {
                umount_program,
                mount_point,
                active: true,
            }
        }

        pub(super) fn unmount(&mut self) -> Result<(), String> {
            let ordinary = Command::new(&self.umount_program)
                .arg("--")
                .arg(&self.mount_point)
                .env("LC_ALL", "C")
                .env("LANG", "C")
                .output()
                .map_err(|error| format!("umount could not start: {error}"))?;
            if ordinary.status.success() {
                self.active = false;
                return Ok(());
            }
            let lazy = Command::new(&self.umount_program)
                .arg("-l")
                .arg("--")
                .arg(&self.mount_point)
                .env("LC_ALL", "C")
                .env("LANG", "C")
                .output()
                .map_err(|error| format!("lazy umount could not start: {error}"))?;
            if lazy.status.success() {
                self.active = false;
                Ok(())
            } else {
                Err(format!(
                    "ordinary status {:?}, lazy status {:?}",
                    ordinary.status.code(),
                    lazy.status.code()
                ))
            }
        }

        pub(super) fn abandon_cleanup(&mut self) {
            self.active = false;
        }
    }

    impl Drop for LinuxBindMount {
        fn drop(&mut self) {
            if self.active {
                let _ = self.unmount();
            }
        }
    }
}

#[test]
fn bind_mount_skip_classifier_accepts_only_explicit_capability_failures() {
    use linux_bind_mount_support::bind_mount_is_explicitly_unavailable;

    for diagnostic in [
        "mount: permission denied",
        "mount: operation not permitted",
        "mount: only root can do that",
        "mount: unrecognized option '--bind'",
        "mount: bind mounts are not supported",
    ] {
        assert!(bind_mount_is_explicitly_unavailable(diagnostic));
    }
    for unexpected in [
        "",
        "mount: no such file or directory",
        "mount: input/output error",
        "mount: invalid source directory",
    ] {
        assert!(!bind_mount_is_explicitly_unavailable(unexpected));
    }
}
