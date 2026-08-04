use std::collections::HashSet;
use std::fmt;

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;

use crate::delivery::{DeliveryError, MergeConflictPathEncoding, MergeConflictRecord};

pub const MAX_MERGE_CONFLICT_PATHS: usize = 128;
pub const MAX_MERGE_CONFLICT_PATH_BYTES: usize = 4096;
pub const MAX_MERGE_CONFLICT_PAYLOAD_BYTES: usize = 65_536;

#[derive(Clone, PartialEq, Eq)]
pub struct MergeConflictPaths {
    pub(in crate::delivery::merges) encoded: Vec<MergeConflictRecord>,
}

impl fmt::Debug for MergeConflictPaths {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MergeConflictPaths")
            .field("path_count", &self.encoded.len())
            .field("path_values", &"<redacted>")
            .finish()
    }
}

impl MergeConflictPaths {
    pub fn try_from_raw(raw_paths: Vec<Vec<u8>>) -> Result<Self, DeliveryError> {
        if raw_paths.len() > MAX_MERGE_CONFLICT_PATHS {
            return Err(DeliveryError::InvalidCommandRequest);
        }
        let mut seen = HashSet::with_capacity(raw_paths.len());
        let mut payload_bytes = 0usize;
        let mut encoded = Vec::with_capacity(raw_paths.len());
        for (ordinal, raw) in raw_paths.into_iter().enumerate() {
            if raw.is_empty()
                || raw.len() > MAX_MERGE_CONFLICT_PATH_BYTES
                || !raw_relative_path_is_canonical(&raw)
                || !seen.insert(raw.clone())
            {
                return Err(DeliveryError::InvalidCommandRequest);
            }
            let (path_encoding, path_value) = match String::from_utf8(raw.clone()) {
                Ok(value) => (MergeConflictPathEncoding::Utf8, value.into_bytes()),
                Err(_) => (
                    MergeConflictPathEncoding::Base64Url,
                    URL_SAFE_NO_PAD.encode(&raw).into_bytes(),
                ),
            };
            if path_value.is_empty() || path_value.len() > MAX_MERGE_CONFLICT_PATH_BYTES {
                return Err(DeliveryError::InvalidCommandRequest);
            }
            payload_bytes = payload_bytes
                .checked_add(path_value.len())
                .ok_or(DeliveryError::InvalidCommandRequest)?;
            if payload_bytes > MAX_MERGE_CONFLICT_PAYLOAD_BYTES {
                return Err(DeliveryError::InvalidCommandRequest);
            }
            encoded.push(MergeConflictRecord {
                ordinal: u8::try_from(ordinal).map_err(|_| DeliveryError::InvalidCommandRequest)?,
                path_encoding,
                path_value,
            });
        }
        Ok(Self { encoded })
    }

    pub fn len(&self) -> usize {
        self.encoded.len()
    }

    pub fn is_empty(&self) -> bool {
        self.encoded.is_empty()
    }
}

pub(in crate::delivery) fn raw_relative_path_is_canonical(raw: &[u8]) -> bool {
    if raw.is_empty() || raw.contains(&0) || raw.first() == Some(&b'/') {
        return false;
    }
    raw.split(|byte| *byte == b'/')
        .all(|component| !component.is_empty() && component != b"." && component != b"..")
}
