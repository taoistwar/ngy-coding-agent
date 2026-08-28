use std::ffi::c_void;
use std::mem::size_of;
use std::os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle};
use std::ptr::null;

use windows_sys::Win32::Foundation::{HANDLE, INVALID_HANDLE_VALUE};
use windows_sys::Win32::System::Diagnostics::ToolHelp::{
    CreateToolhelp32Snapshot, TH32CS_SNAPTHREAD, THREADENTRY32, Thread32First, Thread32Next,
};
use windows_sys::Win32::System::JobObjects::{
    AssignProcessToJobObject, CreateJobObjectW, JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
    JOBOBJECT_BASIC_ACCOUNTING_INFORMATION, JOBOBJECT_EXTENDED_LIMIT_INFORMATION,
    JobObjectBasicAccountingInformation, JobObjectExtendedLimitInformation,
    QueryInformationJobObject, SetInformationJobObject, TerminateJobObject,
};
use windows_sys::Win32::System::Threading::{
    CREATE_NO_WINDOW, CREATE_SUSPENDED, OpenThread, ResumeThread, THREAD_SUSPEND_RESUME,
};

use super::*;

pub(super) struct Executable {
    file: File,
    program: PathBuf,
}

impl Executable {
    pub(super) fn new(path: &std::path::Path, file: File) -> io::Result<Self> {
        Ok(Self {
            file,
            program: path.to_owned(),
        })
    }

    pub(super) fn program(&self) -> &std::path::Path {
        &self.program
    }
}

pub(super) struct Prepared {
    job: Arc<OwnedHandle>,
    executable_lease: File,
    working_directory_lease: File,
    working_directory_path_leases: Vec<File>,
    dependent_directory_leases: Vec<File>,
    dependent_directory_path_leases: Vec<File>,
}

pub(super) struct LeaderExit {
    job: Arc<OwnedHandle>,
    _dependent_directory_leases: Vec<File>,
    _dependent_directory_path_leases: Vec<File>,
}

pub(super) struct ProcessTree {
    job: Arc<OwnedHandle>,
}

pub(super) fn prepare(
    command: &mut Command,
    executable: Executable,
    working_directory: File,
    working_directory_path_leases: Vec<File>,
    dependent_directory_leases: Vec<File>,
    dependent_directory_path_leases: Vec<File>,
) -> io::Result<Prepared> {
    let job = unsafe { CreateJobObjectW(null(), null()) };
    if job.is_null() {
        return Err(io::Error::last_os_error());
    }
    let job = Arc::new(unsafe { OwnedHandle::from_raw_handle(job) });
    let mut information = JOBOBJECT_EXTENDED_LIMIT_INFORMATION::default();
    information.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
    let succeeded = unsafe {
        SetInformationJobObject(
            job.as_raw_handle() as HANDLE,
            JobObjectExtendedLimitInformation,
            (&information as *const JOBOBJECT_EXTENDED_LIMIT_INFORMATION).cast::<c_void>(),
            size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>() as u32,
        )
    };
    if succeeded == 0 {
        return Err(io::Error::last_os_error());
    }
    command.creation_flags(CREATE_SUSPENDED | CREATE_NO_WINDOW);
    Ok(Prepared {
        job,
        executable_lease: executable.file,
        working_directory_lease: working_directory,
        working_directory_path_leases,
        dependent_directory_leases,
        dependent_directory_path_leases,
    })
}

pub(super) fn leader_anchor_lost(_: &io::Error) -> bool {
    false
}

impl Prepared {
    pub(super) fn attach_and_resume(
        self,
        child: &Child,
    ) -> io::Result<(TreeKillHandle, LeaderExit)> {
        let Self {
            job,
            executable_lease,
            working_directory_lease,
            working_directory_path_leases,
            dependent_directory_leases,
            dependent_directory_path_leases,
        } = self;
        let process = child
            .raw_handle()
            .ok_or_else(|| io::Error::other("spawned process has no process handle"))?
            as HANDLE;
        if unsafe { AssignProcessToJobObject(job.as_raw_handle() as HANDLE, process) } == 0 {
            return Err(io::Error::last_os_error());
        }
        let process_id = child
            .id()
            .ok_or_else(|| io::Error::other("spawned process has no process id"))?;
        resume_only_thread(process_id)?;
        drop(executable_lease);
        drop(working_directory_lease);
        drop(working_directory_path_leases);
        Ok((
            TreeKillHandle::new(ProcessTree { job: job.clone() }),
            LeaderExit {
                job,
                _dependent_directory_leases: dependent_directory_leases,
                _dependent_directory_path_leases: dependent_directory_path_leases,
            },
        ))
    }
}

impl LeaderExit {
    pub(super) async fn wait(&mut self, child: &mut Child) -> io::Result<Option<ExitStatus>> {
        child.wait().await.map(Some)
    }

    pub(super) async fn wait_tree_before_reap(&mut self) -> io::Result<()> {
        Ok(())
    }

    pub(super) async fn wait_tree_after_reap(&mut self) -> io::Result<()> {
        loop {
            if active_processes(&self.job)? == 0 {
                return Ok(());
            }
            time::sleep(Duration::from_millis(2)).await;
        }
    }
}

fn active_processes(job: &OwnedHandle) -> io::Result<u32> {
    let mut information = JOBOBJECT_BASIC_ACCOUNTING_INFORMATION::default();
    let succeeded = unsafe {
        QueryInformationJobObject(
            job.as_raw_handle() as HANDLE,
            JobObjectBasicAccountingInformation,
            (&mut information as *mut JOBOBJECT_BASIC_ACCOUNTING_INFORMATION).cast::<c_void>(),
            size_of::<JOBOBJECT_BASIC_ACCOUNTING_INFORMATION>() as u32,
            std::ptr::null_mut(),
        )
    };
    if succeeded == 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(information.ActiveProcesses)
    }
}

fn resume_only_thread(process_id: u32) -> io::Result<()> {
    let snapshot = unsafe { CreateToolhelp32Snapshot(TH32CS_SNAPTHREAD, 0) };
    if snapshot == INVALID_HANDLE_VALUE {
        return Err(io::Error::last_os_error());
    }
    let snapshot = unsafe { OwnedHandle::from_raw_handle(snapshot) };
    let mut entry = THREADENTRY32 {
        dwSize: size_of::<THREADENTRY32>() as u32,
        ..THREADENTRY32::default()
    };
    let mut succeeded = unsafe { Thread32First(snapshot.as_raw_handle() as HANDLE, &mut entry) };
    while succeeded != 0 {
        if entry.th32OwnerProcessID == process_id {
            let thread = unsafe { OpenThread(THREAD_SUSPEND_RESUME, 0, entry.th32ThreadID) };
            if thread.is_null() {
                return Err(io::Error::last_os_error());
            }
            let thread = unsafe { OwnedHandle::from_raw_handle(thread) };
            let previous = unsafe { ResumeThread(thread.as_raw_handle() as HANDLE) };
            if previous == u32::MAX {
                return Err(io::Error::last_os_error());
            }
            if previous != 1 {
                return Err(io::Error::other(
                    "suspended primary thread had an unexpected suspend count",
                ));
            }
            return Ok(());
        }
        succeeded = unsafe { Thread32Next(snapshot.as_raw_handle() as HANDLE, &mut entry) };
    }
    Err(io::Error::new(
        io::ErrorKind::NotFound,
        "suspended primary thread was not found",
    ))
}

impl ProcessTree {
    #[cfg(test)]
    pub(super) fn active_processes(&self) -> io::Result<u32> {
        active_processes(&self.job)
    }

    pub(super) fn kill(&self) -> io::Result<()> {
        if unsafe { TerminateJobObject(self.job.as_raw_handle() as HANDLE, 1) } == 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }
}
