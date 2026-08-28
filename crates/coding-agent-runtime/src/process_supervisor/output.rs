use super::*;

pub(super) fn join_captured(
    result: Result<io::Result<CapturedStream>, JoinError>,
) -> Result<CapturedStream, ProcessError> {
    match result {
        Ok(Ok(output)) => Ok(output),
        Ok(Err(error)) => Err(ProcessError::OutputDrainFailed(error)),
        Err(_) => Err(ProcessError::WorkerFailed),
    }
}

pub(super) async fn drain_stream(
    mut stream: impl AsyncRead + Unpin,
    limit: usize,
) -> io::Result<CapturedStream> {
    let mut capture = HeadTailCapture::new(limit);
    let mut buffer = [0u8; 8 * 1_024];
    loop {
        let read = stream.read(&mut buffer).await?;
        if read == 0 {
            return Ok(capture.finish());
        }
        capture.push(&buffer[..read]);
    }
}

pub(super) struct HeadTailCapture {
    head: Vec<u8>,
    tail: VecDeque<u8>,
    head_capacity: usize,
    tail_capacity: usize,
    observed_bytes: u64,
}

impl HeadTailCapture {
    pub(super) fn new(limit: usize) -> Self {
        let head_capacity = if limit == 1 { 1 } else { limit / 2 };
        Self {
            head: Vec::with_capacity(head_capacity),
            tail: VecDeque::with_capacity(limit - head_capacity),
            head_capacity,
            tail_capacity: limit - head_capacity,
            observed_bytes: 0,
        }
    }

    pub(super) fn push(&mut self, bytes: &[u8]) {
        self.observed_bytes = self.observed_bytes.saturating_add(bytes.len() as u64);
        for byte in bytes {
            if self.head.len() < self.head_capacity {
                self.head.push(*byte);
            } else if self.tail_capacity != 0 {
                if self.tail.len() == self.tail_capacity {
                    self.tail.pop_front();
                }
                self.tail.push_back(*byte);
            }
        }
    }

    pub(super) fn finish(mut self) -> CapturedStream {
        let retained = self.head.len().saturating_add(self.tail.len());
        let truncated = self.observed_bytes > retained as u64;
        if !truncated {
            self.head.extend(self.tail.drain(..));
        }
        CapturedStream {
            head: self.head,
            tail: self.tail.into_iter().collect(),
            observed_bytes: self.observed_bytes,
            omitted_observed_bytes: self.observed_bytes.saturating_sub(retained as u64),
            truncated,
            complete: true,
        }
    }
}
