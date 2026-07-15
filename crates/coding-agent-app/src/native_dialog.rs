use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

static PICKER_OPEN: AtomicBool = AtomicBool::new(false);

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum PickerError {
    #[error("a native repository picker is already open")]
    AlreadyOpen,
    #[error("the native repository picker host is unavailable")]
    Unavailable,
    #[error("the native repository picker host must be created on the macOS main thread")]
    MainThreadRequired,
}

impl PickerError {
    pub const fn code(self) -> &'static str {
        match self {
            Self::AlreadyOpen => "PICKER_ALREADY_OPEN",
            Self::Unavailable => "PICKER_UNAVAILABLE",
            Self::MainThreadRequired => "PICKER_MAIN_THREAD_REQUIRED",
        }
    }
}

#[async_trait::async_trait]
trait PickerBackend: Send + Sync + 'static {
    async fn pick_repository(&self) -> Result<Option<PathBuf>, PickerError>;
}

#[cfg(any(target_os = "macos", test))]
struct DialogRequest {
    response: tokio::sync::oneshot::Sender<Option<PathBuf>>,
    _gate: PickerGate,
}

#[cfg(any(target_os = "macos", test))]
impl DialogRequest {
    fn respond(self, selected: Option<PathBuf>) {
        let _ = self.response.send(selected);
    }
}

#[cfg(any(target_os = "macos", test))]
#[derive(Clone)]
struct DialogDispatcher {
    requests: tokio::sync::mpsc::UnboundedSender<DialogRequest>,
}

#[cfg(any(target_os = "macos", test))]
impl DialogDispatcher {
    fn channel() -> (Self, DialogHost) {
        let (requests, receiver) = tokio::sync::mpsc::unbounded_channel();
        (Self { requests }, DialogHost { receiver })
    }
}

#[cfg(any(target_os = "macos", test))]
impl DialogDispatcher {
    async fn pick_repository(&self, gate: PickerGate) -> Result<Option<PathBuf>, PickerError> {
        let (response, selected) = tokio::sync::oneshot::channel();
        self.requests
            .send(DialogRequest {
                response,
                _gate: gate,
            })
            .map_err(|_| PickerError::Unavailable)?;
        selected.await.map_err(|_| PickerError::Unavailable)
    }
}

#[derive(Clone)]
enum DialogBackend {
    Direct(Arc<dyn PickerBackend>),
    #[cfg(any(target_os = "macos", test))]
    Dispatched(DialogDispatcher),
}

#[cfg(any(target_os = "macos", test))]
struct DialogHost {
    receiver: tokio::sync::mpsc::UnboundedReceiver<DialogRequest>,
}

#[cfg(any(target_os = "macos", test))]
impl DialogHost {
    async fn next_request(&mut self) -> Option<DialogRequest> {
        self.receiver.recv().await
    }
}

#[cfg(not(target_os = "macos"))]
struct RfdPicker;

#[async_trait::async_trait]
#[cfg(not(target_os = "macos"))]
impl PickerBackend for RfdPicker {
    async fn pick_repository(&self) -> Result<Option<PathBuf>, PickerError> {
        Ok(rfd::AsyncFileDialog::new()
            .set_title("Select a Cargo repository")
            .pick_folder()
            .await
            .map(|handle| handle.path().to_path_buf()))
    }
}

#[derive(Clone)]
pub struct NativeDialogService {
    backend: DialogBackend,
}

impl NativeDialogService {
    #[cfg(not(target_os = "macos"))]
    pub fn new() -> Self {
        Self {
            backend: DialogBackend::Direct(Arc::new(RfdPicker)),
        }
    }

    #[cfg(target_os = "macos")]
    pub fn new_on_main_thread() -> Result<(Self, NativeDialogMainThreadHost), PickerError> {
        if !macos_is_main_thread() {
            return Err(PickerError::MainThreadRequired);
        }
        let (dispatcher, host) = DialogDispatcher::channel();
        Ok((
            Self {
                backend: DialogBackend::Dispatched(dispatcher),
            },
            NativeDialogMainThreadHost {
                host,
                _not_send: std::marker::PhantomData,
            },
        ))
    }

    pub async fn pick_repository(&self) -> Result<Option<PathBuf>, PickerError> {
        let gate = PickerGate::acquire()?;
        let backend = self.backend.clone();
        let (response, selected) = tokio::sync::oneshot::channel();
        drop(tokio::spawn(async move {
            let result = match backend {
                DialogBackend::Direct(backend) => {
                    let result = backend.pick_repository().await;
                    drop(gate);
                    result
                }
                #[cfg(any(target_os = "macos", test))]
                DialogBackend::Dispatched(dispatcher) => dispatcher.pick_repository(gate).await,
            };
            let _ = response.send(result);
        }));
        selected.await.unwrap_or(Err(PickerError::Unavailable))
    }

    #[cfg(test)]
    fn with_backend(backend: Arc<dyn PickerBackend>) -> Self {
        Self {
            backend: DialogBackend::Direct(backend),
        }
    }

    #[cfg(test)]
    fn with_dispatcher(dispatcher: DialogDispatcher) -> Self {
        Self {
            backend: DialogBackend::Dispatched(dispatcher),
        }
    }
}

#[cfg(not(target_os = "macos"))]
impl Default for NativeDialogService {
    fn default() -> Self {
        Self::new()
    }
}

/// Pumps native picker requests on the macOS process main thread.
///
/// The host is deliberately not `Send`, so safe code cannot move its future
/// from the main thread that constructed it onto a Tokio worker.
///
/// ```compile_fail
/// use coding_agent_app::NativeDialogService;
/// let (_, host) = NativeDialogService::new_on_main_thread().unwrap();
/// std::thread::spawn(move || drop(host));
/// ```
#[cfg(target_os = "macos")]
pub struct NativeDialogMainThreadHost {
    host: DialogHost,
    _not_send: std::marker::PhantomData<std::rc::Rc<()>>,
}

#[cfg(target_os = "macos")]
impl NativeDialogMainThreadHost {
    pub async fn run(mut self) {
        while let Some(request) = self.host.next_request().await {
            let selected = rfd::FileDialog::new()
                .set_title("Select a Cargo repository")
                .pick_folder();
            request.respond(selected);
        }
    }
}

#[cfg(target_os = "macos")]
fn macos_is_main_thread() -> bool {
    unsafe extern "C" {
        fn pthread_main_np() -> std::os::raw::c_int;
    }

    unsafe { pthread_main_np() != 0 }
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
        finished: tokio::sync::Notify,
    }

    #[async_trait::async_trait]
    impl PickerBackend for BlockingPicker {
        async fn pick_repository(&self) -> Result<Option<PathBuf>, PickerError> {
            self.opened.notify_one();
            self.release.notified().await;
            self.finished.notify_one();
            Ok(None)
        }
    }

    struct ImmediatePicker(PathBuf);

    #[async_trait::async_trait]
    impl PickerBackend for ImmediatePicker {
        async fn pick_repository(&self) -> Result<Option<PathBuf>, PickerError> {
            Ok(Some(self.0.clone()))
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
    async fn aborting_a_picker_caller_keeps_the_gate_until_the_backend_finishes() {
        let _test_guard = TEST_LOCK.lock().await;
        let picker = Arc::new(BlockingPicker::default());
        let service = NativeDialogService::with_backend(picker.clone());
        let first_service = service.clone();
        let first = tokio::spawn(async move { first_service.pick_repository().await });
        picker.opened.notified().await;
        first.abort();
        assert!(
            first
                .await
                .expect_err("picker task was aborted")
                .is_cancelled()
        );

        let second = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            service.pick_repository(),
        )
        .await
        .expect("a second caller must fail immediately")
        .expect_err("the detached backend owner still holds the picker gate");
        assert_eq!(second, PickerError::AlreadyOpen);

        picker.release.notify_one();
        picker.finished.notified().await;

        let selected = PathBuf::from("selected-after-abort");
        let next = NativeDialogService::with_backend(Arc::new(ImmediatePicker(selected.clone())));
        assert_eq!(next.pick_repository().await.unwrap(), Some(selected));
    }

    #[tokio::test]
    async fn dispatched_picker_waits_for_the_main_thread_host_reply() {
        let _test_guard = TEST_LOCK.lock().await;
        let (dispatcher, mut host) = DialogDispatcher::channel();
        let service = NativeDialogService::with_dispatcher(dispatcher);
        let picker = tokio::spawn(async move { service.pick_repository().await });

        let request = host
            .next_request()
            .await
            .expect("main-thread host receives the picker request");
        assert!(
            !picker.is_finished(),
            "the HTTP-side future waits asynchronously for the host"
        );

        let selected = PathBuf::from("selected-on-main-thread");
        request.respond(Some(selected.clone()));
        assert_eq!(picker.await.unwrap().unwrap(), Some(selected));
    }

    #[tokio::test]
    async fn a_closed_main_thread_host_is_a_safe_stable_error() {
        let _test_guard = TEST_LOCK.lock().await;
        let (dispatcher, host) = DialogDispatcher::channel();
        let service = NativeDialogService::with_dispatcher(dispatcher);
        drop(host);

        let error = service
            .pick_repository()
            .await
            .expect_err("a closed host cannot service a picker request");
        assert_eq!(error, PickerError::Unavailable);
        assert_eq!(error.code(), "PICKER_UNAVAILABLE");
    }

    #[tokio::test]
    async fn aborting_the_http_waiter_keeps_the_gate_until_the_host_finishes() {
        let _test_guard = TEST_LOCK.lock().await;
        let (dispatcher, mut host) = DialogDispatcher::channel();
        let service = NativeDialogService::with_dispatcher(dispatcher);
        let first_service = service.clone();
        let first = tokio::spawn(async move { first_service.pick_repository().await });
        let first_request = host
            .next_request()
            .await
            .expect("host receives the first request");

        first.abort();
        assert!(first.await.unwrap_err().is_cancelled());
        let second = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            service.pick_repository(),
        )
        .await
        .expect("a concurrent request must fail immediately")
        .expect_err("the host still owns the first picker gate");
        assert_eq!(second, PickerError::AlreadyOpen);

        first_request.respond(None);
        let third_service = service.clone();
        let third = tokio::spawn(async move { third_service.pick_repository().await });
        let third_request = host
            .next_request()
            .await
            .expect("the host accepts a request after finishing the first");
        third_request.respond(None);
        assert_eq!(third.await.unwrap().unwrap(), None);
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn macos_host_construction_rejects_a_worker_thread() {
        let error = std::thread::spawn(|| match NativeDialogService::new_on_main_thread() {
            Ok(_) => panic!("a worker thread cannot own the AppKit host"),
            Err(error) => error,
        })
        .join()
        .expect("join worker-thread construction attempt");

        assert_eq!(error, PickerError::MainThreadRequired);
        assert_eq!(error.code(), "PICKER_MAIN_THREAD_REQUIRED");
    }
}
