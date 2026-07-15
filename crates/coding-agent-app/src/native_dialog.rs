use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

static PICKER_OPEN: AtomicBool = AtomicBool::new(false);

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum PickerError {
    #[error("a native repository picker is already open")]
    AlreadyOpen,
}

impl PickerError {
    pub const fn code(self) -> &'static str {
        match self {
            Self::AlreadyOpen => "PICKER_ALREADY_OPEN",
        }
    }
}

#[async_trait::async_trait]
trait PickerBackend: Send + Sync + 'static {
    async fn pick_repository(&self) -> Option<PathBuf>;
}

struct RfdPicker;

#[async_trait::async_trait]
impl PickerBackend for RfdPicker {
    async fn pick_repository(&self) -> Option<PathBuf> {
        rfd::AsyncFileDialog::new()
            .set_title("Select a Cargo repository")
            .pick_folder()
            .await
            .map(|handle| handle.path().to_path_buf())
    }
}

#[derive(Clone)]
pub struct NativeDialogService {
    backend: Arc<dyn PickerBackend>,
}

impl NativeDialogService {
    pub fn new() -> Self {
        Self {
            backend: Arc::new(RfdPicker),
        }
    }

    pub async fn pick_repository(&self) -> Result<Option<PathBuf>, PickerError> {
        let gate = PickerGate::acquire()?;
        let selected = self.backend.pick_repository().await;
        drop(gate);
        Ok(selected)
    }

    #[cfg(test)]
    fn with_backend(backend: Arc<dyn PickerBackend>) -> Self {
        Self { backend }
    }
}

impl Default for NativeDialogService {
    fn default() -> Self {
        Self::new()
    }
}

struct PickerGate;

impl PickerGate {
    fn acquire() -> Result<Self, PickerError> {
        PICKER_OPEN
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map(|_| Self)
            .map_err(|_| PickerError::AlreadyOpen)
    }
}

impl Drop for PickerGate {
    fn drop(&mut self) {
        PICKER_OPEN.store(false, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::Arc;

    use super::*;

    static TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    #[derive(Default)]
    struct BlockingPicker {
        opened: tokio::sync::Notify,
        release: tokio::sync::Notify,
    }

    #[async_trait::async_trait]
    impl PickerBackend for BlockingPicker {
        async fn pick_repository(&self) -> Option<PathBuf> {
            self.opened.notify_one();
            self.release.notified().await;
            None
        }
    }

    struct ImmediatePicker(PathBuf);

    #[async_trait::async_trait]
    impl PickerBackend for ImmediatePicker {
        async fn pick_repository(&self) -> Option<PathBuf> {
            Some(self.0.clone())
        }
    }

    #[tokio::test]
    async fn picker_is_globally_serialized_and_cancel_is_not_an_error() {
        let _test_guard = TEST_LOCK.lock().await;
        let picker = Arc::new(BlockingPicker::default());
        let service = NativeDialogService::with_backend(picker.clone());
        let first_service = service.clone();
        let first = tokio::spawn(async move { first_service.pick_repository().await });
        picker.opened.notified().await;

        let error = service
            .pick_repository()
            .await
            .expect_err("a concurrent picker is rejected");
        assert_eq!(error, PickerError::AlreadyOpen);
        assert_eq!(error.code(), "PICKER_ALREADY_OPEN");

        picker.release.notify_one();
        assert_eq!(first.await.expect("join first picker").unwrap(), None);

        let selected = PathBuf::from("selected-after-cancel");
        let next = NativeDialogService::with_backend(Arc::new(ImmediatePicker(selected.clone())));
        assert_eq!(next.pick_repository().await.unwrap(), Some(selected));
    }

    #[tokio::test]
    async fn aborting_a_picker_future_releases_the_global_gate() {
        let _test_guard = TEST_LOCK.lock().await;
        let picker = Arc::new(BlockingPicker::default());
        let service = NativeDialogService::with_backend(picker.clone());
        let first = tokio::spawn(async move { service.pick_repository().await });
        picker.opened.notified().await;
        first.abort();
        assert!(
            first
                .await
                .expect_err("picker task was aborted")
                .is_cancelled()
        );

        let selected = PathBuf::from("selected-after-abort");
        let next = NativeDialogService::with_backend(Arc::new(ImmediatePicker(selected.clone())));
        assert_eq!(next.pick_repository().await.unwrap(), Some(selected));
    }
}
