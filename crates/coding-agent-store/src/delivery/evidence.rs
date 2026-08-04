use coding_agent_domain::{CheckEvidenceStatus, ReviewEvidence, ReviewVerdict};
use serde::Serialize;
use sha2::{Digest as _, Sha256};

use crate::StoreError;
use crate::reviews::StoredReview;

use super::{DeliveryIdentity, EvidenceIdentityV1, Sha256Digest};

const FRAMING_MAGIC: &[u8] = b"coding-agent-delivery-evidence";
const FRAMING_VERSION: u16 = 1;
const CHECKS_DOMAIN: &[u8] = b"checks";
const COVERAGE_DOMAIN: &[u8] = b"coverage";

pub(crate) fn derive_evidence_identity(
    identity: DeliveryIdentity,
    stored: &StoredReview,
) -> Result<EvidenceIdentityV1, StoreError> {
    let review = &stored.review;
    validate_approved_evidence(review)?;
    let required_checks = canonical_json(review.required_checks())?;
    let check_evidence = canonical_json(review.check_evidence())?;
    let coverage = canonical_json(review.coverage().ok_or_else(evidence_invariant)?)?;
    let checks_digest = framed_digest(
        CHECKS_DOMAIN,
        &[
            (b"required_checks".as_slice(), required_checks.as_slice()),
            (b"check_evidence".as_slice(), check_evidence.as_slice()),
        ],
    )?;
    let coverage_digest = framed_digest(
        COVERAGE_DOMAIN,
        &[(b"coverage".as_slice(), coverage.as_slice())],
    )?;
    EvidenceIdentityV1::try_new(
        identity,
        review.round(),
        stored.event_id,
        review.workspace_generation(),
        review.workspace_digest().value().parse()?,
        checks_digest,
        coverage_digest,
    )
    .map_err(StoreError::from)
}

fn validate_approved_evidence(review: &ReviewEvidence) -> Result<(), StoreError> {
    let valid = review.verdict() == ReviewVerdict::Approved
        && review
            .coverage()
            .is_some_and(|coverage| coverage.is_complete())
        && review.check_evidence().len() == review.required_checks().len()
        && review
            .check_evidence()
            .iter()
            .all(|evidence| evidence.status() == CheckEvidenceStatus::Passed);
    if valid {
        Ok(())
    } else {
        Err(evidence_invariant())
    }
}

fn canonical_json<T: Serialize + ?Sized>(value: &T) -> Result<Vec<u8>, StoreError> {
    serde_json::to_vec(value).map_err(|_| evidence_invariant())
}

fn framed_digest(domain: &[u8], fields: &[(&[u8], &[u8])]) -> Result<Sha256Digest, StoreError> {
    let mut hash = Sha256::new();
    write_part(&mut hash, b"magic", FRAMING_MAGIC)?;
    write_part(&mut hash, b"domain", domain)?;
    write_part(&mut hash, b"version", &FRAMING_VERSION.to_be_bytes())?;
    for (tag, value) in fields {
        write_part(&mut hash, tag, value)?;
    }
    let bytes = hash.finalize();
    let mut encoded = String::with_capacity(64);
    for byte in bytes {
        use std::fmt::Write as _;
        write!(&mut encoded, "{byte:02x}").expect("writing to String cannot fail");
    }
    encoded.parse().map_err(StoreError::from)
}

fn write_part(hash: &mut Sha256, tag: &[u8], value: &[u8]) -> Result<(), StoreError> {
    let tag_len = u16::try_from(tag.len()).map_err(|_| evidence_invariant())?;
    let value_len = u64::try_from(value.len()).map_err(|_| evidence_invariant())?;
    hash.update(tag_len.to_be_bytes());
    hash.update(tag);
    hash.update(value_len.to_be_bytes());
    hash.update(value);
    Ok(())
}

fn evidence_invariant() -> StoreError {
    StoreError::InvariantViolation("delivery evidence snapshot is inconsistent")
}

#[cfg(test)]
mod tests {
    use super::{CHECKS_DOMAIN, COVERAGE_DOMAIN, framed_digest};

    #[test]
    fn identical_payloads_are_separated_by_their_evidence_domain() {
        let fields = [(b"payload".as_slice(), b"identical".as_slice())];
        let checks = framed_digest(CHECKS_DOMAIN, &fields).unwrap();
        let coverage = framed_digest(COVERAGE_DOMAIN, &fields).unwrap();

        assert_ne!(checks, coverage);
    }
}
