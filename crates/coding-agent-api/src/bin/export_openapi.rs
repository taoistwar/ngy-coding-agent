use std::ffi::OsString;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::atomic::{AtomicU64, Ordering};

use coding_agent_api::ApiDoc;
use utoipa::OpenApi;

static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, thiserror::Error)]
enum ExportError {
    #[error("usage: export_openapi <output-path>")]
    Usage,
    #[error("failed to serialize OpenAPI: {0}")]
    Serialize(#[from] serde_json::Error),
    #[error("failed to export OpenAPI: {0}")]
    Io(#[from] io::Error),
}

fn main() -> ExitCode {
    match run(std::env::args_os()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}

fn run(arguments: impl IntoIterator<Item = OsString>) -> Result<(), ExportError> {
    let mut arguments = arguments.into_iter();
    let _program = arguments.next();
    let output = arguments.next().ok_or(ExportError::Usage)?;
    if arguments.next().is_some() {
        return Err(ExportError::Usage);
    }

    let mut bytes = serde_json::to_vec_pretty(&ApiDoc::openapi())?;
    bytes.push(b'\n');
    atomic_write(Path::new(&output), &bytes)?;
    Ok(())
}

fn atomic_write(destination: &Path, bytes: &[u8]) -> io::Result<()> {
    let file_name = destination.file_name().ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidInput, "output path must name a file")
    })?;
    let parent = destination
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent)?;

    let (mut temporary, temporary_path) = create_temporary(parent, file_name)?;
    let publish = (|| {
        temporary.write_all(bytes)?;
        temporary.flush()?;
        temporary.sync_all()?;
        drop(temporary);
        atomic_replace(&temporary_path, destination)?;
        sync_parent_best_effort(parent);
        Ok(())
    })();

    if publish.is_err() {
        let _ = fs::remove_file(&temporary_path);
    }
    publish
}

fn create_temporary(parent: &Path, file_name: &std::ffi::OsStr) -> io::Result<(File, PathBuf)> {
    for _ in 0..1024 {
        let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let temporary_path = parent.join(format!(
            ".{}.{}.{}.tmp",
            file_name.to_string_lossy(),
            std::process::id(),
            sequence
        ));
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary_path)
        {
            Ok(file) => return Ok((file, temporary_path)),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error),
        }
    }

    Err(io::Error::new(
        io::ErrorKind::AlreadyExists,
        "could not create a unique temporary file",
    ))
}

#[cfg(unix)]
fn atomic_replace(source: &Path, destination: &Path) -> io::Result<()> {
    fs::rename(source, destination)
}

#[cfg(windows)]
fn atomic_replace(source: &Path, destination: &Path) -> io::Result<()> {
    use std::iter;
    use std::os::windows::ffi::OsStrExt;

    use windows_sys::Win32::Storage::FileSystem::{
        MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH, MoveFileExW,
    };

    let source = source
        .as_os_str()
        .encode_wide()
        .chain(iter::once(0))
        .collect::<Vec<_>>();
    let destination = destination
        .as_os_str()
        .encode_wide()
        .chain(iter::once(0))
        .collect::<Vec<_>>();

    // SAFETY: both path buffers are live, NUL-terminated UTF-16 strings for the duration
    // of the call, and the flags request an in-place replacement with write-through.
    let result = unsafe {
        MoveFileExW(
            source.as_ptr(),
            destination.as_ptr(),
            MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
        )
    };
    if result == 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

#[cfg(not(any(unix, windows)))]
fn atomic_replace(source: &Path, destination: &Path) -> io::Result<()> {
    fs::rename(source, destination)
}

#[cfg(unix)]
fn sync_parent_best_effort(parent: &Path) {
    if let Ok(directory) = File::open(parent) {
        let _ = directory.sync_all();
    }
}

#[cfg(not(unix))]
fn sync_parent_best_effort(_parent: &Path) {}
