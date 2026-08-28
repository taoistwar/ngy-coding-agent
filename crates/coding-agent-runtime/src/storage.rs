use std::collections::hash_map::RandomState;
use std::fmt;
use std::fs::File;
use std::hash::{BuildHasher, Hash, Hasher};
use std::io;
use std::sync::OnceLock;

use crate::RootCapability;

/// Opaque identity for the physical volume behind an authenticated root.
///
/// The identity supports only equality and hashing. Its platform key is
/// deliberately private, its debug form is redacted, and it does not implement
/// serialization.
///
/// ```compile_fail
/// use coding_agent_runtime::VolumeIdentity;
///
/// fn require_serialize<T: serde::Serialize>() {}
/// require_serialize::<VolumeIdentity>();
/// ```
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct VolumeIdentity {
    key: VolumeIdentityKey,
    opaque_hash: u64,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum VolumeIdentityKey {
    #[cfg(unix)]
    UnixDevice(u64),
    #[cfg(windows)]
    WindowsVolumeSerial(u64),
    #[cfg(any(test, feature = "test-support"))]
    Test(u64),
    #[cfg(not(any(unix, windows, test, feature = "test-support")))]
    Unsupported,
}

impl VolumeIdentity {
    fn from_key(key: VolumeIdentityKey) -> Self {
        let opaque_hash = opaque_volume_identity_hash(|hasher| match key {
            #[cfg(unix)]
            VolumeIdentityKey::UnixDevice(device) => {
                hasher.write_u8(1);
                hasher.write_u64(device);
            }
            #[cfg(windows)]
            VolumeIdentityKey::WindowsVolumeSerial(volume_serial_number) => {
                hasher.write_u8(2);
                hasher.write_u64(volume_serial_number);
            }
            #[cfg(any(test, feature = "test-support"))]
            VolumeIdentityKey::Test(token) => {
                hasher.write_u8(3);
                hasher.write_u64(token);
            }
            #[cfg(not(any(unix, windows, test, feature = "test-support")))]
            VolumeIdentityKey::Unsupported => {
                hasher.write_u8(0);
            }
        });
        Self { key, opaque_hash }
    }

    /// Creates an opaque identity for deterministic cross-volume tests.
    ///
    /// This constructor is absent from production builds.
    #[cfg(any(test, feature = "test-support"))]
    pub fn for_test(token: u64) -> Self {
        Self::from_key(VolumeIdentityKey::Test(token))
    }
}

impl fmt::Debug for VolumeIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("VolumeIdentity(<opaque>)")
    }
}

impl Hash for VolumeIdentity {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.opaque_hash.hash(state);
    }
}

/// Current-user available space observed for one opaque physical volume.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct VolumeSample {
    identity: VolumeIdentity,
    available_bytes: u64,
}

impl VolumeSample {
    fn new(identity: VolumeIdentity, available_bytes: u64) -> Self {
        Self {
            identity,
            available_bytes,
        }
    }

    pub const fn identity(&self) -> VolumeIdentity {
        self.identity
    }

    pub const fn available_bytes(&self) -> u64 {
        self.available_bytes
    }

    /// Creates a deterministic sample for storage-policy and monitor tests.
    ///
    /// This constructor is absent from production builds.
    #[cfg(any(test, feature = "test-support"))]
    pub const fn for_test(identity: VolumeIdentity, available_bytes: u64) -> Self {
        Self {
            identity,
            available_bytes,
        }
    }
}

impl fmt::Debug for VolumeSample {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("VolumeSample(<opaque>)")
    }
}

/// A typed, path-free failure to sample a volume.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum VolumeSampleError {
    #[error("volume sample is unavailable")]
    Unavailable,
}

/// Capability-bound port for observing volume identity and available space.
///
/// Implementations are synchronous because the storage monitor owns timeout
/// and one-in-flight coordination. This keeps a timed-out blocking operation
/// registered as in-flight until the underlying system call actually exits.
pub trait VolumeSampler: Send + Sync {
    fn sample(&self, root: &RootCapability) -> Result<VolumeSample, VolumeSampleError>;
}

/// Native sampler for the current operating system.
#[derive(Debug, Default, Clone, Copy)]
pub struct NativeVolumeSampler;

impl NativeVolumeSampler {
    pub const fn new() -> Self {
        Self
    }
}

impl VolumeSampler for NativeVolumeSampler {
    fn sample(&self, root: &RootCapability) -> Result<VolumeSample, VolumeSampleError> {
        let directory = root.try_clone_root().map_err(unavailable_from_io_error)?;
        sample_authenticated_directory(&directory)
    }
}

fn unavailable_from_io_error(_: io::Error) -> VolumeSampleError {
    VolumeSampleError::Unavailable
}

fn checked_available_bytes(
    available_units: u64,
    bytes_per_unit: u64,
) -> Result<u64, VolumeSampleError> {
    if bytes_per_unit == 0 {
        return Err(VolumeSampleError::Unavailable);
    }
    available_units
        .checked_mul(bytes_per_unit)
        .ok_or(VolumeSampleError::Unavailable)
}

fn opaque_volume_identity_hash(write_identity: impl FnOnce(&mut dyn Hasher)) -> u64 {
    static RANDOM_STATE: OnceLock<RandomState> = OnceLock::new();
    let mut hasher = RANDOM_STATE.get_or_init(RandomState::new).build_hasher();
    write_identity(&mut hasher);
    hasher.finish()
}

#[cfg(unix)]
fn sample_authenticated_directory(directory: &File) -> Result<VolumeSample, VolumeSampleError> {
    use std::mem::MaybeUninit;
    use std::os::fd::AsRawFd;
    use std::os::unix::fs::MetadataExt;

    let metadata = directory.metadata().map_err(unavailable_from_io_error)?;
    let identity = VolumeIdentity::from_key(VolumeIdentityKey::UnixDevice(metadata.dev()));

    let mut statistics = MaybeUninit::<libc::statvfs>::zeroed();
    let result = unsafe { libc::fstatvfs(directory.as_raw_fd(), statistics.as_mut_ptr()) };
    if result != 0 {
        return Err(unavailable_from_io_error(io::Error::last_os_error()));
    }
    let statistics = unsafe { statistics.assume_init() };
    let available_bytes = unix_available_bytes(&statistics)?;
    Ok(VolumeSample::new(identity, available_bytes))
}

#[cfg(unix)]
fn unix_available_bytes(statistics: &libc::statvfs) -> Result<u64, VolumeSampleError> {
    let available_units = statvfs_value_to_u64(statistics.f_bavail)?;
    let fragment_size = statvfs_value_to_u64(statistics.f_frsize)?;
    checked_available_bytes(available_units, fragment_size)
}

#[cfg(unix)]
fn statvfs_value_to_u64(value: impl TryInto<u64>) -> Result<u64, VolumeSampleError> {
    value.try_into().map_err(|_| VolumeSampleError::Unavailable)
}

#[cfg(windows)]
fn sample_authenticated_directory(directory: &File) -> Result<VolumeSample, VolumeSampleError> {
    use std::ffi::c_void;
    use std::mem::{MaybeUninit, size_of};
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Wdk::Storage::FileSystem::{
        FileFsFullSizeInformation, NtQueryVolumeInformationFile,
    };
    use windows_sys::Wdk::System::SystemServices::FILE_FS_FULL_SIZE_INFORMATION;
    use windows_sys::Win32::Foundation::HANDLE;
    use windows_sys::Win32::Storage::FileSystem::{
        FILE_ID_INFO, FileIdInfo, GetFileInformationByHandleEx,
    };
    use windows_sys::Win32::System::IO::IO_STATUS_BLOCK;

    let handle = directory.as_raw_handle() as HANDLE;
    let mut identity_information = MaybeUninit::<FILE_ID_INFO>::zeroed();
    let identity_size =
        u32::try_from(size_of::<FILE_ID_INFO>()).map_err(|_| VolumeSampleError::Unavailable)?;
    let succeeded = unsafe {
        GetFileInformationByHandleEx(
            handle,
            FileIdInfo,
            identity_information.as_mut_ptr().cast::<c_void>(),
            identity_size,
        )
    };
    if succeeded == 0 {
        return Err(unavailable_from_io_error(io::Error::last_os_error()));
    }
    let identity_information = unsafe { identity_information.assume_init() };
    let identity = VolumeIdentity::from_key(VolumeIdentityKey::WindowsVolumeSerial(
        identity_information.VolumeSerialNumber,
    ));

    let mut io_status = IO_STATUS_BLOCK::default();
    let mut size_information = MaybeUninit::<FILE_FS_FULL_SIZE_INFORMATION>::zeroed();
    let information_size = u32::try_from(size_of::<FILE_FS_FULL_SIZE_INFORMATION>())
        .map_err(|_| VolumeSampleError::Unavailable)?;
    let status = unsafe {
        NtQueryVolumeInformationFile(
            handle,
            &mut io_status,
            size_information.as_mut_ptr().cast::<c_void>(),
            information_size,
            FileFsFullSizeInformation,
        )
    };
    if status < 0 {
        return Err(VolumeSampleError::Unavailable);
    }
    let size_information = unsafe { size_information.assume_init() };
    let available_bytes = windows_available_bytes(&size_information, io_status.Information)?;
    Ok(VolumeSample::new(identity, available_bytes))
}

#[cfg(windows)]
fn windows_available_bytes(
    information: &windows_sys::Wdk::System::SystemServices::FILE_FS_FULL_SIZE_INFORMATION,
    returned_bytes: usize,
) -> Result<u64, VolumeSampleError> {
    if returned_bytes
        < std::mem::size_of::<windows_sys::Wdk::System::SystemServices::FILE_FS_FULL_SIZE_INFORMATION>(
        )
    {
        return Err(VolumeSampleError::Unavailable);
    }
    let available_units = u64::try_from(information.CallerAvailableAllocationUnits)
        .map_err(|_| VolumeSampleError::Unavailable)?;
    let bytes_per_unit = u64::from(information.SectorsPerAllocationUnit)
        .checked_mul(u64::from(information.BytesPerSector))
        .ok_or(VolumeSampleError::Unavailable)?;
    checked_available_bytes(available_units, bytes_per_unit)
}

#[cfg(not(any(unix, windows)))]
fn sample_authenticated_directory(_: &File) -> Result<VolumeSample, VolumeSampleError> {
    Err(VolumeSampleError::Unavailable)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn fake_identities_deduplicate_same_volume_and_keep_other_volumes_independent() {
        let first = VolumeIdentity::for_test(41);
        let alias = VolumeIdentity::for_test(41);
        let second = VolumeIdentity::for_test(42);

        assert_eq!(first, alias);
        assert_ne!(first, second);

        let identities = HashSet::from([first, alias, second]);
        assert_eq!(identities.len(), 2);

        let sample = VolumeSample::for_test(first, 8192);
        assert_eq!(sample.identity(), first);
        assert_eq!(sample.available_bytes(), 8192);
    }

    #[test]
    fn checked_byte_calculation_rejects_zero_units_and_overflow() {
        assert_eq!(
            checked_available_bytes(7, 0),
            Err(VolumeSampleError::Unavailable)
        );
        assert_eq!(
            checked_available_bytes(u64::MAX, 2),
            Err(VolumeSampleError::Unavailable)
        );
        assert_eq!(checked_available_bytes(0, 4096), Ok(0));
        assert_eq!(checked_available_bytes(3, 4096), Ok(12_288));
    }

    #[test]
    fn io_failures_have_one_typed_unavailable_outcome() {
        for kind in [
            io::ErrorKind::PermissionDenied,
            io::ErrorKind::NotFound,
            io::ErrorKind::Unsupported,
            io::ErrorKind::Other,
        ] {
            assert_eq!(
                unavailable_from_io_error(io::Error::from(kind)),
                VolumeSampleError::Unavailable
            );
        }
    }

    #[cfg(unix)]
    #[test]
    fn unix_uses_current_user_available_blocks_not_total_free_blocks() {
        let mut statistics = unsafe { std::mem::zeroed::<libc::statvfs>() };
        statistics.f_bavail = 3;
        statistics.f_bfree = 99;
        statistics.f_frsize = 4096;

        assert_eq!(unix_available_bytes(&statistics), Ok(12_288));
    }

    #[cfg(windows)]
    #[test]
    fn windows_uses_caller_available_units_not_actual_available_units() {
        use windows_sys::Wdk::System::SystemServices::FILE_FS_FULL_SIZE_INFORMATION;

        let information = FILE_FS_FULL_SIZE_INFORMATION {
            CallerAvailableAllocationUnits: 3,
            ActualAvailableAllocationUnits: 99,
            SectorsPerAllocationUnit: 8,
            BytesPerSector: 512,
            ..Default::default()
        };

        assert_eq!(
            windows_available_bytes(
                &information,
                std::mem::size_of::<FILE_FS_FULL_SIZE_INFORMATION>(),
            ),
            Ok(12_288)
        );
    }

    #[cfg(windows)]
    #[test]
    fn windows_rejects_negative_short_zero_and_overflowing_results() {
        use windows_sys::Wdk::System::SystemServices::FILE_FS_FULL_SIZE_INFORMATION;

        let mut information = FILE_FS_FULL_SIZE_INFORMATION {
            CallerAvailableAllocationUnits: -1,
            SectorsPerAllocationUnit: 8,
            BytesPerSector: 512,
            ..Default::default()
        };
        let full_size = std::mem::size_of::<FILE_FS_FULL_SIZE_INFORMATION>();

        assert_eq!(
            windows_available_bytes(&information, full_size),
            Err(VolumeSampleError::Unavailable)
        );

        information.CallerAvailableAllocationUnits = 3;
        assert_eq!(
            windows_available_bytes(&information, full_size - 1),
            Err(VolumeSampleError::Unavailable)
        );

        information.SectorsPerAllocationUnit = 0;
        assert_eq!(
            windows_available_bytes(&information, full_size),
            Err(VolumeSampleError::Unavailable)
        );

        information.CallerAvailableAllocationUnits = i64::MAX;
        information.SectorsPerAllocationUnit = u32::MAX;
        information.BytesPerSector = u32::MAX;
        assert_eq!(
            windows_available_bytes(&information, full_size),
            Err(VolumeSampleError::Unavailable)
        );
    }
}
