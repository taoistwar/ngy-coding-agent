use sha2::{Digest, Sha256};

use super::{DELIVERY_COMMAND_REQUEST_HASH_DOMAIN, DELIVERY_COMMAND_REQUEST_HASH_VERSION};
use crate::delivery::Sha256Digest;

pub(super) struct CanonicalRequestHasher(Sha256);

impl CanonicalRequestHasher {
    pub(super) fn new() -> Self {
        let mut hasher = Self(Sha256::new());
        hasher.field("domain", DELIVERY_COMMAND_REQUEST_HASH_DOMAIN.as_bytes());
        hasher.field(
            "version",
            &DELIVERY_COMMAND_REQUEST_HASH_VERSION.to_be_bytes(),
        );
        hasher
    }

    pub(super) fn field(&mut self, tag: &str, value: &[u8]) {
        let tag_length = u16::try_from(tag.len()).expect("canonical request tag length fits u16");
        let value_length =
            u64::try_from(value.len()).expect("canonical request value length fits u64");
        self.0.update(tag_length.to_be_bytes());
        self.0.update(tag.as_bytes());
        self.0.update(value_length.to_be_bytes());
        self.0.update(value);
    }

    pub(super) fn finish(self) -> Sha256Digest {
        let digest = format!("{:x}", self.0.finalize());
        digest
            .parse()
            .expect("SHA-256 output is a canonical SHA-256 digest")
    }
}
