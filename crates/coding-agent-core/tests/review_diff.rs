use std::fmt::Write as _;
use std::sync::atomic::{AtomicUsize, Ordering};

use coding_agent_core::{
    ContextRedactor, DiffFileStatus, MAX_REVIEW_DIFF_BATCHES, MAX_REVIEW_DIFF_CHUNKS,
    MAX_REVIEW_DIFF_TYPED_CHUNK_BYTES, REVIEW_DIFF_MANIFEST_DOMAIN, ReviewDiffBundle,
    ReviewDiffCheckpoint, ReviewDiffChunkRequest, ReviewDiffError, ReviewDiffInputFile,
    WorkspaceCheckpoint, WorkspaceFingerprint, validate_review_coverage,
    validate_terminal_review_coverage,
};
use regex::Regex;
use serde_json::Value;
use sha2::{Digest, Sha256};

struct IdentityRedactor;

impl ContextRedactor for IdentityRedactor {
    fn redact(&self, content: &str) -> String {
        content.to_owned()
    }
}

static IDENTITY_REDACTOR: IdentityRedactor = IdentityRedactor;

fn review_bundle(
    checkpoint: &ReviewDiffCheckpoint,
    files: Vec<ReviewDiffInputFile>,
) -> Result<ReviewDiffBundle, ReviewDiffError> {
    ReviewDiffBundle::try_new(checkpoint, files, &IDENTITY_REDACTOR)
}

fn checkpoint(generation: u64, fingerprint_byte: u8) -> ReviewDiffCheckpoint {
    let checkpoint = WorkspaceCheckpoint::try_at_generation(
        generation,
        WorkspaceFingerprint::from_bytes([fingerprint_byte; 32]),
    )
    .unwrap();
    ReviewDiffCheckpoint::from_workspace_checkpoint(&checkpoint)
}

fn file(path: &str, patch: impl Into<String>) -> ReviewDiffInputFile {
    ReviewDiffInputFile::try_new(path, DiffFileStatus::Modified, 1, 1, patch).unwrap()
}

fn visible_stream(bundle: &ReviewDiffBundle) -> String {
    let mut visible = String::new();
    let manifest = bundle.manifest();
    for start in (0..manifest.chunk_count()).step_by(2) {
        let count = (manifest.chunk_count() - start).min(2);
        let request = ReviewDiffChunkRequest::for_manifest(manifest, start, count).unwrap();
        for chunk in bundle.chunk_batch(&request).unwrap().chunks() {
            visible.push_str(chunk.content());
        }
    }
    visible
}

struct ReplaceRedactor {
    needle: &'static str,
    replacement: &'static str,
}

impl ContextRedactor for ReplaceRedactor {
    fn redact(&self, content: &str) -> String {
        content.replace(self.needle, self.replacement)
    }
}

struct AppendingPatchRedactor;

impl ContextRedactor for AppendingPatchRedactor {
    fn redact(&self, content: &str) -> String {
        if content.contains("PATCH") {
            format!("{content}x")
        } else {
            content.to_owned()
        }
    }
}

struct NulRedactor;

impl ContextRedactor for NulRedactor {
    fn redact(&self, content: &str) -> String {
        content.replace("SECRET", "\0")
    }
}

struct AlternatingPatchRedactor {
    calls: AtomicUsize,
}

struct RegexRedactor {
    regex: Regex,
}

impl ContextRedactor for RegexRedactor {
    fn redact(&self, content: &str) -> String {
        self.regex
            .replace_all(content, "${1}<redacted>")
            .into_owned()
    }
}

impl ContextRedactor for AlternatingPatchRedactor {
    fn redact(&self, content: &str) -> String {
        if content.contains("PATCH") {
            let suffix = self.calls.fetch_add(1, Ordering::Relaxed) % 2;
            format!("{content}{suffix}")
        } else {
            content.to_owned()
        }
    }
}

#[test]
fn manifest_is_stably_sorted_domain_separated_and_chunked() {
    let checkpoint = checkpoint(7, 0x2a);
    let bundle = review_bundle(
        &checkpoint,
        vec![
            file("zeta.rs", "-old\n+new\n"),
            file("alpha.rs", "-before\n+after\n"),
        ],
    )
    .unwrap();
    let manifest = bundle.manifest();

    assert_eq!(
        manifest
            .files()
            .iter()
            .map(|entry| entry.path())
            .collect::<Vec<_>>(),
        vec!["alpha.rs", "zeta.rs"]
    );
    assert!(manifest.chunk_count() <= MAX_REVIEW_DIFF_CHUNKS);
    assert!(manifest.required_batch_count() <= MAX_REVIEW_DIFF_BATCHES);

    let mut hasher = Sha256::new();
    hasher.update(REVIEW_DIFF_MANIFEST_DOMAIN);
    hasher.update(manifest.canonical_bytes());
    let digest = hasher.finalize();
    let mut expected = String::new();
    for byte in digest {
        write!(&mut expected, "{byte:02x}").unwrap();
    }
    assert_eq!(manifest.manifest_sha256(), expected);
    let visible: Value = serde_json::from_slice(manifest.canonical_bytes()).unwrap();
    assert_eq!(visible["format_version"], 1);
    assert_eq!(visible["generation"], 7);
    assert_eq!(
        visible["digest"]["value"],
        manifest.workspace_digest().value()
    );
    assert_eq!(visible["files"][0]["path"], "alpha.rs");
    assert_eq!(visible["files"][0]["status"], "modified");
    assert_eq!(
        visible["files"][0]["patch_bytes"],
        manifest.files()[0].patch_bytes()
    );
    assert_eq!(
        visible["files"][0]["patch_sha256"],
        manifest.files()[0].patch_sha256()
    );
    assert_eq!(visible["chunk_count"], manifest.chunk_count());
    let canonical_text = std::str::from_utf8(manifest.canonical_bytes()).unwrap();
    assert!(
        canonical_text.starts_with("{\"format_version\":1,\"generation\":7,\"digest\":"),
        "fixed field order is part of canonical v1"
    );
    assert!(
        canonical_text
            .contains("\"digest\":{\"algorithm\":\"workspace_fingerprint_v1\",\"value\":")
    );

    let mut wrong_domain = Sha256::new();
    wrong_domain.update(b"coding-agent-review-diff-manifest-v1");
    wrong_domain.update(manifest.canonical_bytes());
    assert_ne!(
        manifest.manifest_sha256(),
        format!("{:x}", wrong_domain.finalize())
    );

    for start in (0..manifest.chunk_count()).step_by(2) {
        let count = (manifest.chunk_count() - start).min(2);
        let request = ReviewDiffChunkRequest::for_manifest(manifest, start, count).unwrap();
        let batch = bundle.chunk_batch(&request).unwrap();
        assert_eq!(batch.start_chunk(), start);
        assert_eq!(batch.chunks().len(), usize::from(count));
        for (offset, chunk) in batch.chunks().iter().enumerate() {
            assert_eq!(chunk.index(), start + u8::try_from(offset).unwrap());
            assert!(chunk.content().len() <= MAX_REVIEW_DIFF_TYPED_CHUNK_BYTES);
            assert!(std::str::from_utf8(chunk.content().as_bytes()).is_ok());
        }
    }
}

#[test]
fn empty_diff_has_zero_chunks_and_complete_empty_coverage() {
    let bundle = review_bundle(&checkpoint(0, 1), Vec::new()).unwrap();
    let manifest = bundle.manifest();
    assert!(manifest.files().is_empty());
    assert_eq!(manifest.chunk_count(), 0);
    assert_eq!(manifest.required_batch_count(), 0);
    let coverage = manifest.coverage_evidence(Vec::new()).unwrap();
    assert!(coverage.is_complete());
    assert_eq!(
        validate_review_coverage(manifest, Some(&coverage), true),
        Ok(())
    );
}

#[test]
fn utf8_chunks_are_contiguous_and_requests_are_exact() {
    let patch = format!(
        "-old\n+{}\n",
        "界".repeat(MAX_REVIEW_DIFF_TYPED_CHUNK_BYTES)
    );
    let bundle = review_bundle(&checkpoint(2, 2), vec![file("unicode.rs", patch)]).unwrap();
    let manifest = bundle.manifest();
    assert!(manifest.chunk_count() >= 2);

    let request = ReviewDiffChunkRequest::for_manifest(manifest, 0, 2).unwrap();
    let batch = bundle.chunk_batch(&request).unwrap();
    assert_eq!(
        batch.chunks()[0].stream_end(),
        batch.chunks()[1].stream_start()
    );
    assert!(ReviewDiffChunkRequest::for_manifest(manifest, 0, 3).is_err());
    assert!(ReviewDiffChunkRequest::for_manifest(manifest, manifest.chunk_count(), 1).is_err());

    let other = review_bundle(
        &checkpoint(2, 2),
        vec![file("unicode.rs", "-different\n+content\n")],
    )
    .unwrap();
    let other_request = ReviewDiffChunkRequest::for_manifest(other.manifest(), 0, 1).unwrap();
    assert_eq!(
        bundle.chunk_batch(&other_request),
        Err(ReviewDiffError::ManifestMismatch)
    );
}

#[test]
fn eight_chunks_form_exactly_four_contiguous_batches() {
    let patch = "x".repeat(MAX_REVIEW_DIFF_TYPED_CHUNK_BYTES * 7);
    let bundle = review_bundle(&checkpoint(2, 7), vec![file("eight.txt", patch)]).unwrap();
    let manifest = bundle.manifest();
    assert_eq!(manifest.chunk_count(), MAX_REVIEW_DIFF_CHUNKS);
    assert_eq!(manifest.required_batch_count(), MAX_REVIEW_DIFF_BATCHES);
    for batch_index in 0..MAX_REVIEW_DIFF_BATCHES {
        let start = batch_index * 2;
        let request = ReviewDiffChunkRequest::for_manifest(manifest, start, 2).unwrap();
        let batch = bundle.chunk_batch(&request).unwrap();
        assert_eq!(batch.chunks().len(), 2);
        assert_eq!(batch.chunks()[0].index(), start);
        assert_eq!(batch.chunks()[1].index(), start + 1);
    }
}

#[test]
fn binary_equivalent_nul_unsafe_paths_and_typed_limits_fail_closed() {
    assert_eq!(
        ReviewDiffInputFile::try_new("binary.bin", DiffFileStatus::Modified, 0, 0, "a\0b"),
        Err(ReviewDiffError::InvalidPatch)
    );
    assert_eq!(
        ReviewDiffInputFile::try_new("bad\npath.rs", DiffFileStatus::Modified, 0, 0, "patch"),
        Err(ReviewDiffError::InvalidPath)
    );
    assert_eq!(
        ReviewDiffInputFile::try_new(
            "count.rs",
            DiffFileStatus::Modified,
            9_007_199_254_740_992,
            0,
            "patch"
        ),
        Err(ReviewDiffError::InvalidFileMetadata)
    );
    assert_eq!(
        review_bundle(
            &checkpoint(1, 3),
            vec![file(
                "huge.rs",
                "x".repeat(MAX_REVIEW_DIFF_TYPED_CHUNK_BYTES * usize::from(MAX_REVIEW_DIFF_CHUNKS))
            )]
        ),
        Err(ReviewDiffError::TooManyChunks)
    );
    assert_eq!(
        review_bundle(
            &checkpoint(1, 3),
            vec![file("same.rs", "one"), file("same.rs", "two")]
        ),
        Err(ReviewDiffError::DuplicatePath)
    );

    let long_files = (0..110)
        .map(|index| {
            let path = format!("dir-{index:03}/{}.rs", "a".repeat(180));
            file(&path, "")
        })
        .collect();
    assert_eq!(
        review_bundle(&checkpoint(1, 4), long_files),
        Err(ReviewDiffError::ManifestTooLarge)
    );
}

#[test]
fn coverage_identity_and_fresh_terminal_manifest_are_exact() {
    let reviewer_bundle =
        review_bundle(&checkpoint(4, 5), vec![file("src/lib.rs", "-a\n+b\n")]).unwrap();
    let reviewer = reviewer_bundle.manifest();
    let partial = reviewer.coverage_evidence(Vec::new()).unwrap();
    assert_eq!(
        validate_review_coverage(reviewer, Some(&partial), true),
        Err(ReviewDiffError::IncompleteCoverage)
    );
    assert_eq!(
        validate_review_coverage(reviewer, Some(&partial), false),
        Ok(())
    );
    assert_eq!(validate_review_coverage(reviewer, None, false), Ok(()));

    let complete = reviewer
        .coverage_evidence((0..reviewer.chunk_count()).collect())
        .unwrap();
    assert_eq!(
        validate_terminal_review_coverage(&complete, reviewer),
        Ok(())
    );

    let changed_terminal =
        review_bundle(&checkpoint(5, 6), vec![file("src/lib.rs", "-a\n+b\n")]).unwrap();
    assert_eq!(
        validate_terminal_review_coverage(&complete, changed_terminal.manifest()),
        Err(ReviewDiffError::CoverageMismatch)
    );
    let same_checkpoint_changed_content =
        review_bundle(&checkpoint(4, 5), vec![file("src/lib.rs", "-a\n+c\n")]).unwrap();
    assert_eq!(
        validate_terminal_review_coverage(&complete, same_checkpoint_changed_content.manifest()),
        Err(ReviewDiffError::CoverageMismatch)
    );
}

#[test]
fn generation_prevents_a_b_a_manifest_replay() {
    let first = review_bundle(&checkpoint(1, 9), vec![file("same.rs", "-a\n+b\n")]).unwrap();
    let later_same_fingerprint =
        review_bundle(&checkpoint(3, 9), vec![file("same.rs", "-a\n+b\n")]).unwrap();
    assert_ne!(
        first.manifest().manifest_sha256(),
        later_same_fingerprint.manifest().manifest_sha256()
    );
}

#[test]
fn complete_patch_is_redacted_before_hashing_and_chunking_across_raw_boundaries() {
    let raw = format!(
        "{}SECRET-tail",
        "a".repeat(MAX_REVIEW_DIFF_TYPED_CHUNK_BYTES - 3)
    );
    let replacement = "[REDACTED-LONGER-VALUE]";
    let redactor = ReplaceRedactor {
        needle: "SECRET",
        replacement,
    };
    let bundle =
        ReviewDiffBundle::try_new(&checkpoint(1, 10), vec![file("secret.rs", &raw)], &redactor)
            .unwrap();
    let expected = raw.replace("SECRET", replacement);
    let manifest_file = &bundle.manifest().files()[0];
    assert_eq!(manifest_file.patch_bytes(), expected.len() as u64);
    assert_eq!(
        manifest_file.patch_sha256(),
        format!("{:x}", Sha256::digest(expected.as_bytes()))
    );
    let visible = visible_stream(&bundle);
    assert!(!visible.contains("SECRET"));
    assert!(visible.contains(replacement));
    assert!(!String::from_utf8_lossy(bundle.manifest().canonical_bytes()).contains("SECRET"));
    assert_eq!(
        manifest_file.first_chunk(),
        0,
        "the sole file starts in the first redacted chunk"
    );
    assert_eq!(
        manifest_file.chunk_count(),
        bundle.manifest().chunk_count(),
        "chunk ranges describe the model-visible redacted stream"
    );
}

#[test]
fn redaction_growth_and_shrinkage_drive_typed_limits_and_manifest_identity() {
    let raw = "before SECRET after";
    let growing = ReplaceRedactor {
        needle: "SECRET",
        replacement: "[A-MUCH-LONGER-REDACTED-VALUE]",
    };
    let shrinking = ReplaceRedactor {
        needle: "SECRET",
        replacement: "X",
    };
    let grown =
        ReviewDiffBundle::try_new(&checkpoint(1, 11), vec![file("value.rs", raw)], &growing)
            .unwrap();
    let shrunk =
        ReviewDiffBundle::try_new(&checkpoint(1, 11), vec![file("value.rs", raw)], &shrinking)
            .unwrap();
    assert_eq!(
        grown.manifest().files()[0].patch_bytes(),
        raw.replace("SECRET", growing.replacement).len() as u64
    );
    assert_eq!(
        shrunk.manifest().files()[0].patch_bytes(),
        raw.replace("SECRET", shrinking.replacement).len() as u64
    );
    assert_ne!(
        grown.manifest().files()[0].patch_sha256(),
        shrunk.manifest().files()[0].patch_sha256()
    );
    assert_ne!(
        grown.manifest().manifest_sha256(),
        shrunk.manifest().manifest_sha256()
    );

    // Bounds are applied after whole-patch redaction: a large raw secret can
    // become a small, safe visible patch.
    let large_raw = "SECRET".repeat(30_000);
    let removing = ReplaceRedactor {
        needle: "SECRET",
        replacement: "",
    };
    let reduced = ReviewDiffBundle::try_new(
        &checkpoint(1, 12),
        vec![file("large.rs", large_raw)],
        &removing,
    )
    .unwrap();
    assert_eq!(reduced.manifest().files()[0].patch_bytes(), 0);
    assert!(!visible_stream(&reduced).contains("SECRET"));
}

#[test]
fn structured_path_mutation_and_unstable_or_nul_redaction_fail_closed() {
    let path_mutating = ReplaceRedactor {
        needle: "secret",
        replacement: "[redacted]",
    };
    assert_eq!(
        ReviewDiffBundle::try_new(
            &checkpoint(1, 13),
            vec![file("secret.rs", "safe patch")],
            &path_mutating,
        ),
        Err(ReviewDiffError::UnsafeRedaction)
    );
    assert_eq!(
        ReviewDiffBundle::try_new(
            &checkpoint(1, 13),
            vec![file("safe.rs", "PATCH")],
            &AlternatingPatchRedactor {
                calls: AtomicUsize::new(0),
            },
        ),
        Err(ReviewDiffError::UnsafeRedaction)
    );
    assert_eq!(
        ReviewDiffBundle::try_new(
            &checkpoint(1, 13),
            vec![file("safe.rs", "PATCH")],
            &AppendingPatchRedactor,
        ),
        Err(ReviewDiffError::UnsafeRedaction)
    );
    assert_eq!(
        ReviewDiffBundle::try_new(
            &checkpoint(1, 13),
            vec![file("safe.rs", "SECRET")],
            &NulRedactor,
        ),
        Err(ReviewDiffError::UnsafeRedaction)
    );
}

#[test]
fn production_header_regex_cannot_emerge_across_patch_and_framing_boundary() {
    let redactor = RegexRedactor {
        regex: Regex::new(
            r"(?i)((?:authorization|x-api-key|api-key|x-auth-token)\s*[:=]\s*)(?:bearer\s+)?[^\s,;]+",
        )
        .unwrap(),
    };
    let patch = "+authorization\n";
    assert_eq!(redactor.redact(patch), patch);
    assert_eq!(
        ReviewDiffBundle::try_new(&checkpoint(1, 14), vec![file("safe.rs", patch)], &redactor,),
        Err(ReviewDiffError::UnsafeRedaction),
        "the suffix begins with '=', creating an authorization header match only after framing"
    );
}

#[test]
fn canonical_json_composition_must_be_redactor_stable_before_hashing() {
    let redactor = RegexRedactor {
        regex: Regex::new(r#"("path":"manifest-boundary\.rs","status":"modified")"#).unwrap(),
    };
    assert_eq!(
        redactor.redact("manifest-boundary.rs"),
        "manifest-boundary.rs"
    );
    assert_eq!(
        ReviewDiffBundle::try_new(
            &checkpoint(1, 15),
            vec![file("manifest-boundary.rs", "safe patch")],
            &redactor,
        ),
        Err(ReviewDiffError::UnsafeRedaction),
        "adjacent canonical JSON fields form a context match absent from each typed field"
    );
}
