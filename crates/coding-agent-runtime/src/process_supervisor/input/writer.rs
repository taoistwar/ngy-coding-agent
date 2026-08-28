use std::io;
use std::process::Stdio;
use std::sync::Arc;

use tokio::io::{AsyncWrite, AsyncWriteExt as _};
use tokio::process::{Child, ChildStdin};
use tokio::task::JoinHandle;
use tokio::time::{self, Instant};
use tokio_util::task::TaskTracker;

use super::super::ProcessError;
use super::model::ExactChildInput;

pub(in super::super) fn child_stdin(input: &Option<ExactChildInput>) -> Stdio {
    if input.is_some() {
        Stdio::piped()
    } else {
        Stdio::null()
    }
}

pub(in super::super) fn spawn_writer(
    child: &mut Child,
    input: Option<ExactChildInput>,
    tasks: &TaskTracker,
) -> Result<Option<SupervisedExactInputWriter>, ProcessError> {
    let Some(input) = input else {
        return Ok(None);
    };
    let stdin = child.stdin.take().ok_or(ProcessError::MissingInputPipe)?;
    let handle = tasks.spawn(write_exact_input(stdin, input.into_bytes()));
    Ok(Some(SupervisedExactInputWriter {
        handle: Some(handle),
    }))
}

pub(in super::super) async fn abort_and_join(writer: Option<SupervisedExactInputWriter>) {
    if let Some(writer) = writer {
        writer.abort_and_join().await;
    }
}

pub(in super::super) async fn complete(
    writer: Option<SupervisedExactInputWriter>,
    require_success: bool,
    deadline: Instant,
) -> Result<(), ProcessError> {
    let Some(writer) = writer else {
        return Ok(());
    };
    if !require_success {
        writer.abort_and_join().await;
        return Ok(());
    }
    writer.complete(deadline).await
}

pub(in super::super) struct SupervisedExactInputWriter {
    handle: Option<JoinHandle<Result<(), ExactInputWriteError>>>,
}

impl SupervisedExactInputWriter {
    fn is_running(&self) -> bool {
        self.handle.is_some()
    }

    async fn wait(&mut self) -> Result<(), ProcessError> {
        let result = self
            .handle
            .as_mut()
            .expect("exact input writer handle is present")
            .await;
        self.handle.take();
        match result {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => Err(map_write_error(error)),
            Err(_) => Err(ProcessError::InputCompletionUnknown),
        }
    }

    async fn abort_and_join(mut self) {
        if let Some(handle) = self.handle.take() {
            handle.abort();
            let _ = handle.await;
        }
    }

    async fn complete(mut self, deadline: Instant) -> Result<(), ProcessError> {
        let Some(mut handle) = self.handle.take() else {
            return Ok(());
        };
        match time::timeout_at(deadline, &mut handle).await {
            Ok(Ok(Ok(()))) => Ok(()),
            Ok(Ok(Err(error))) => Err(map_write_error(error)),
            Ok(Err(_)) => Err(ProcessError::InputCompletionUnknown),
            Err(_) => {
                handle.abort();
                let _ = handle.await;
                Err(ProcessError::InputCompletionUnknown)
            }
        }
    }
}

pub(in super::super) async fn wait_for_completion(
    writer: &mut Option<SupervisedExactInputWriter>,
) -> Result<(), ProcessError> {
    match writer {
        Some(writer) if writer.is_running() => writer.wait().await,
        _ => std::future::pending().await,
    }
}

impl Drop for SupervisedExactInputWriter {
    fn drop(&mut self) {
        if let Some(handle) = &self.handle {
            handle.abort();
        }
    }
}

enum ExactInputWriteError {
    Write(io::Error),
    Close(io::Error),
}

async fn write_exact_input(
    mut stdin: ChildStdin,
    bytes: Arc<[u8]>,
) -> Result<(), ExactInputWriteError> {
    write_exact(&mut stdin, &bytes).await
}

async fn write_exact(
    writer: &mut (impl AsyncWrite + Unpin),
    bytes: &[u8],
) -> Result<(), ExactInputWriteError> {
    writer
        .write_all(bytes)
        .await
        .map_err(ExactInputWriteError::Write)?;
    // Windows ChildStdin accepts bytes into Tokio's blocking buffer before the
    // underlying WriteFile completes, and its shutdown is intentionally a
    // no-op. Flush is therefore the completion proof for the exact write.
    writer.flush().await.map_err(ExactInputWriteError::Write)?;
    writer.shutdown().await.map_err(ExactInputWriteError::Close)
}

fn map_write_error(error: ExactInputWriteError) -> ProcessError {
    match error {
        ExactInputWriteError::Write(error) | ExactInputWriteError::Close(error)
            if input_was_closed_early(&error) =>
        {
            ProcessError::InputClosedEarly
        }
        ExactInputWriteError::Write(error) => ProcessError::InputWriteFailed(error),
        ExactInputWriteError::Close(error) => ProcessError::InputCloseFailed(error),
    }
}

fn input_was_closed_early(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::BrokenPipe
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::NotConnected
            | io::ErrorKind::UnexpectedEof
    )
}

#[cfg(test)]
mod tests {
    use std::pin::Pin;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Poll};

    use tokio::io::AsyncWrite;

    use super::*;

    #[tokio::test]
    async fn writer_distinguishes_write_close_and_early_close_failures() {
        let mut write_failure = FailingWriter::write();
        assert!(matches!(
            write_exact(&mut write_failure, b"bytes").await,
            Err(ExactInputWriteError::Write(_))
        ));

        let mut flush_failure = FailingWriter::flush();
        assert!(matches!(
            write_exact(&mut flush_failure, b"bytes").await,
            Err(ExactInputWriteError::Write(_))
        ));

        let mut close_failure = FailingWriter::close();
        assert!(matches!(
            write_exact(&mut close_failure, b"bytes").await,
            Err(ExactInputWriteError::Close(_))
        ));

        assert!(matches!(
            map_write_error(ExactInputWriteError::Write(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "fixed test error",
            ))),
            ProcessError::InputClosedEarly
        ));
        assert!(matches!(
            map_write_error(ExactInputWriteError::Write(io::Error::other(
                "fixed write failure",
            ))),
            ProcessError::InputWriteFailed(_)
        ));
        assert!(matches!(
            map_write_error(ExactInputWriteError::Close(io::Error::other(
                "fixed close failure",
            ))),
            ProcessError::InputCloseFailed(_)
        ));
    }

    #[tokio::test]
    async fn panicked_writer_is_an_input_unknown_not_a_cleanup_unproven_error() {
        let handle = tokio::spawn(async {
            panic!("fixed writer panic");
            #[allow(unreachable_code)]
            Ok::<(), ExactInputWriteError>(())
        });
        let mut writer = SupervisedExactInputWriter {
            handle: Some(handle),
        };
        let error = writer.wait().await.unwrap_err();
        assert!(matches!(error, ProcessError::InputCompletionUnknown));
        assert!(!error.process_cleanup_is_unproven());
    }

    #[tokio::test]
    async fn abort_path_joins_the_writer_instead_of_detaching_it() {
        let active = Arc::new(AtomicUsize::new(0));
        let writer_active = active.clone();
        let handle = tokio::spawn(async move {
            let _guard = ActiveWriterGuard::new(writer_active);
            std::future::pending::<()>().await;
            #[allow(unreachable_code)]
            Ok::<(), ExactInputWriteError>(())
        });
        tokio::task::yield_now().await;
        assert_eq!(active.load(Ordering::SeqCst), 1);
        SupervisedExactInputWriter {
            handle: Some(handle),
        }
        .abort_and_join()
        .await;
        assert_eq!(active.load(Ordering::SeqCst), 0);
    }

    struct ActiveWriterGuard(Arc<AtomicUsize>);

    impl ActiveWriterGuard {
        fn new(active: Arc<AtomicUsize>) -> Self {
            active.fetch_add(1, Ordering::SeqCst);
            Self(active)
        }
    }

    impl Drop for ActiveWriterGuard {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::SeqCst);
        }
    }

    struct FailingWriter {
        stage: FailureStage,
    }

    enum FailureStage {
        Write,
        Flush,
        Close,
    }

    impl FailingWriter {
        fn write() -> Self {
            Self {
                stage: FailureStage::Write,
            }
        }

        fn flush() -> Self {
            Self {
                stage: FailureStage::Flush,
            }
        }

        fn close() -> Self {
            Self {
                stage: FailureStage::Close,
            }
        }
    }

    impl AsyncWrite for FailingWriter {
        fn poll_write(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
            bytes: &[u8],
        ) -> Poll<io::Result<usize>> {
            if matches!(self.stage, FailureStage::Write) {
                Poll::Ready(Err(io::Error::other("fixed write failure")))
            } else {
                Poll::Ready(Ok(bytes.len()))
            }
        }

        fn poll_flush(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<io::Result<()>> {
            if matches!(self.stage, FailureStage::Flush) {
                Poll::Ready(Err(io::Error::other("fixed flush failure")))
            } else {
                Poll::Ready(Ok(()))
            }
        }

        fn poll_shutdown(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<io::Result<()>> {
            if matches!(self.stage, FailureStage::Close) {
                Poll::Ready(Err(io::Error::other("fixed close failure")))
            } else {
                Poll::Ready(Ok(()))
            }
        }
    }
}
