use std::ffi::{c_int, c_void};
use std::fs::File;
use std::io;
use std::os::fd::AsRawFd;
use std::ptr::null_mut;

type Acl = *mut c_void;
type AclEntry = *mut c_void;

const ACL_FIRST_ENTRY: c_int = 0;
const ACL_TYPE_EXTENDED: c_int = 0x0000_0100;

unsafe extern "C" {
    fn __error() -> *mut c_int;
    fn acl_free(object: *mut c_void) -> c_int;
    fn acl_get_entry(acl: Acl, entry_id: c_int, entry: *mut AclEntry) -> c_int;
    fn acl_get_fd_np(fd: c_int, acl_type: c_int) -> Acl;
    fn acl_init(count: c_int) -> Acl;
    fn acl_set_fd(fd: c_int, acl: Acl) -> c_int;
    fn acl_valid(acl: Acl) -> c_int;
}

pub(crate) fn clear_extended_acl(file: &File) -> io::Result<()> {
    let acl = unsafe { acl_init(0) };
    if acl.is_null() {
        return Err(io::Error::last_os_error());
    }

    let set_result = unsafe { acl_set_fd(file.as_raw_fd(), acl) };
    let set_error = (set_result != 0).then(io::Error::last_os_error);
    let free_result = unsafe { acl_free(acl) };
    if let Some(error) = set_error {
        return Err(error);
    }
    if free_result != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

pub(crate) fn validate_no_extended_acl(file: &File) -> io::Result<()> {
    clear_errno();
    let acl = unsafe { acl_get_fd_np(file.as_raw_fd(), ACL_TYPE_EXTENDED) };
    if acl.is_null() {
        let error = io::Error::last_os_error();
        return if error.kind() == io::ErrorKind::NotFound {
            Ok(())
        } else {
            Err(error)
        };
    }

    if unsafe { acl_valid(acl) } != 0 {
        let error = io::Error::last_os_error();
        unsafe { acl_free(acl) };
        return Err(error);
    }

    let mut first_entry = null_mut();
    clear_errno();
    let get_result = unsafe { acl_get_entry(acl, ACL_FIRST_ENTRY, &mut first_entry) };
    let get_error = (get_result != 0).then(io::Error::last_os_error);
    let free_result = unsafe { acl_free(acl) };
    if get_result == 0 {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "private path has an extended access-control entry",
        ));
    }
    if let Some(error) = get_error
        && error.kind() != io::ErrorKind::InvalidInput
    {
        return Err(error);
    }
    if free_result != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

fn clear_errno() {
    unsafe { *__error() = 0 };
}
