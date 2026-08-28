use std::fmt;
use std::sync::Arc;

/// The largest exact, redacted stdin payload admitted by the process
/// supervisor.  This matches the production delivery fingerprint's
/// single-file bound, so a reviewed source file never becomes ineligible only
/// because Task 11 must feed its already captured bytes to Git.
pub(crate) const MAX_EXACT_CHILD_INPUT_BYTES: usize = 8 * 1024 * 1024;

#[derive(Clone)]
pub(crate) struct ExactChildInput {
    bytes: Arc<[u8]>,
}

impl ExactChildInput {
    pub(crate) const fn maximum_bytes() -> usize {
        MAX_EXACT_CHILD_INPUT_BYTES
    }

    pub(crate) fn try_new(bytes: Vec<u8>) -> Result<Self, ExactChildInputError> {
        if bytes.len() > MAX_EXACT_CHILD_INPUT_BYTES {
            return Err(ExactChildInputError::TooLarge);
        }
        Ok(Self {
            bytes: Arc::from(bytes),
        })
    }

    pub(super) fn into_bytes(self) -> Arc<[u8]> {
        self.bytes
    }
}

impl fmt::Debug for ExactChildInput {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ExactChildInput(<redacted>)")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(crate) enum ExactChildInputError {
    #[error("exact child input exceeds its fixed byte limit")]
    TooLarge,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_input_is_bounded_and_its_debug_is_fully_redacted() {
        let secret = b"input-module-secret".to_vec();
        let input = ExactChildInput::try_new(secret.clone()).unwrap();
        assert_eq!(format!("{input:?}"), "ExactChildInput(<redacted>)");
        assert!(!format!("{input:?}").contains(std::str::from_utf8(&secret).unwrap()));
        assert!(ExactChildInput::try_new(vec![0; MAX_EXACT_CHILD_INPUT_BYTES]).is_ok());
        assert_eq!(
            ExactChildInput::try_new(vec![0; MAX_EXACT_CHILD_INPUT_BYTES + 1]).unwrap_err(),
            ExactChildInputError::TooLarge
        );
    }
}
