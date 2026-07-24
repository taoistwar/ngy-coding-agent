use coding_agent_core::{
    ContextRedactor, DiffFileStatus, EXECUTOR_REVIEWER_RETAINED_RESERVATION,
    MAX_RETAINED_REVIEW_CHUNK_BATCH_BYTES, MAX_RETAINED_REVIEW_CHUNK_BYTES,
    MAX_RETAINED_REVIEW_COVERAGE_BYTES, MAX_RETAINED_REVIEW_MANIFEST_BYTES,
    MAX_REVIEW_DIFF_TYPED_CHUNK_BYTES, REVIEW_DIFF_BATCH_RETAINED_RESULT_LIMIT,
    REVIEW_DIFF_CHUNK_RETAINED_RESULT_LIMIT, REVIEW_MANIFEST_RETAINED_RESULT_LIMIT,
    RetainedResultError, RetainedToolResult, ReviewDiffBundle, ReviewDiffCheckpoint,
    ReviewDiffChunkRequest, ReviewDiffInputFile, ToolResult, ToolStatus, WorkspaceCheckpoint,
    WorkspaceFingerprint, canonical_tool_result_wire_value,
};

struct IdentityRedactor;

impl ContextRedactor for IdentityRedactor {
    fn redact(&self, content: &str) -> String {
        content.to_owned()
    }
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

struct WrapperCompositionRedactor;

impl ContextRedactor for WrapperCompositionRedactor {
    fn redact(&self, content: &str) -> String {
        if content.contains("\"role\":\"tool\"") {
            content.replace("\"role\":\"tool\"", "\"role\":\"<redacted>\"")
        } else {
            content.to_owned()
        }
    }
}

fn review_checkpoint() -> ReviewDiffCheckpoint {
    let checkpoint = WorkspaceCheckpoint::new(WorkspaceFingerprint::from_bytes([0x37; 32]));
    ReviewDiffCheckpoint::from_workspace_checkpoint(&checkpoint)
}

fn review_bundle(patch: String) -> ReviewDiffBundle {
    ReviewDiffBundle::try_new(
        &review_checkpoint(),
        vec![
            ReviewDiffInputFile::try_new("src/lib.rs", DiffFileStatus::Modified, 1, 1, patch)
                .unwrap(),
        ],
        &IdentityRedactor,
    )
    .unwrap()
}

#[test]
fn canonical_wrapper_measures_the_exact_json_value_and_truncates_on_utf8_boundaries() {
    let tool_result = ToolResult::text("引用 \"quoted\" and \\\\ paths\n".repeat(2_000));
    let retained = RetainedToolResult::try_from_tool_result_with_limit(
        "call-\"quoted\"-\\\\",
        &tool_result,
        &IdentityRedactor,
        1_024,
    )
    .unwrap();

    assert!(retained.truncated());
    assert!(retained.wrapper_len() <= 1_024);
    assert!(std::str::from_utf8(retained.wrapper_bytes()).is_ok());
    let wire: serde_json::Value = serde_json::from_slice(retained.wrapper_bytes()).unwrap();
    assert_eq!(wire["role"], "tool");
    assert_eq!(wire["tool_call_id"], retained.tool_call_id());
    assert_eq!(wire["content"], retained.content());
    assert!(retained.content().starts_with("[tool_status=succeeded"));
    assert!(retained.content().contains("[tool result truncated]"));
    assert_eq!(
        retained.wrapper_bytes(),
        serde_json::to_vec(
            &canonical_tool_result_wire_value(retained.tool_call_id(), retained.content()).unwrap()
        )
        .unwrap()
    );

    assert_eq!(
        RetainedToolResult::try_from_tool_result_with_limit(
            "call",
            &tool_result,
            &IdentityRedactor,
            32,
        ),
        Err(RetainedResultError::WrapperLimitTooSmall)
    );
}

#[test]
fn complete_wrapper_redaction_rejects_id_content_and_composition_secrets() {
    let id_redactor = ReplaceRedactor {
        needle: "SECRET-ID",
        replacement: "<redacted>",
    };
    assert_eq!(
        RetainedToolResult::try_from_parts(
            "SECRET-ID",
            "safe",
            ToolStatus::Succeeded,
            false,
            &id_redactor,
        ),
        Err(RetainedResultError::RedactionUnstable)
    );

    let content_redactor = ReplaceRedactor {
        needle: "SECRET-CONTENT",
        replacement: "<redacted>",
    };
    let redacted = RetainedToolResult::try_from_tool_result(
        "call",
        &ToolResult::text("before SECRET-CONTENT after"),
        &content_redactor,
    )
    .unwrap();
    assert!(!redacted.content().contains("SECRET-CONTENT"));
    assert!(redacted.truncated());

    assert_eq!(
        RetainedToolResult::try_from_parts(
            "call",
            "individually safe",
            ToolStatus::Succeeded,
            false,
            &WrapperCompositionRedactor,
        ),
        Err(RetainedResultError::RedactionUnstable)
    );
}

#[test]
fn authoritative_review_wrappers_prove_24_20_40_184_kib_complete_bounds() {
    assert_eq!(
        REVIEW_MANIFEST_RETAINED_RESULT_LIMIT,
        MAX_RETAINED_REVIEW_MANIFEST_BYTES
    );
    assert_eq!(
        REVIEW_DIFF_CHUNK_RETAINED_RESULT_LIMIT,
        MAX_RETAINED_REVIEW_CHUNK_BYTES
    );
    assert_eq!(
        REVIEW_DIFF_BATCH_RETAINED_RESULT_LIMIT,
        MAX_RETAINED_REVIEW_CHUNK_BATCH_BYTES
    );
    assert_eq!(
        EXECUTOR_REVIEWER_RETAINED_RESERVATION,
        MAX_RETAINED_REVIEW_COVERAGE_BYTES
    );

    let bundle = review_bundle("x".repeat(MAX_REVIEW_DIFF_TYPED_CHUNK_BYTES * 7));
    let manifest = bundle.manifest();
    assert_eq!(manifest.chunk_count(), 8);

    let opaque_id = format!("review-{}", "\"\\q".repeat(80));
    assert!(opaque_id.len() <= 256);
    let retained_manifest =
        RetainedToolResult::try_review_manifest(opaque_id.clone(), manifest, &IdentityRedactor)
            .unwrap();
    assert!(retained_manifest.wrapper_len() <= MAX_RETAINED_REVIEW_MANIFEST_BYTES);

    let mut coverage_bytes = retained_manifest.wrapper_len();
    for start in (0..manifest.chunk_count()).step_by(2) {
        let request = ReviewDiffChunkRequest::for_manifest(manifest, start, 2).unwrap();
        let batch = bundle.chunk_batch(&request).unwrap();
        let retained_batch = RetainedToolResult::try_review_chunk_batch(
            opaque_id.clone(),
            &batch,
            &IdentityRedactor,
        )
        .unwrap();
        assert!(retained_batch.wrapper_len() <= MAX_RETAINED_REVIEW_CHUNK_BATCH_BYTES);
        coverage_bytes += retained_batch.wrapper_len();

        for chunk_start in start..start + 2 {
            let request = ReviewDiffChunkRequest::for_manifest(manifest, chunk_start, 1).unwrap();
            let single = bundle.chunk_batch(&request).unwrap();
            let retained_single = RetainedToolResult::try_review_chunk_batch(
                opaque_id.clone(),
                &single,
                &IdentityRedactor,
            )
            .unwrap();
            assert!(retained_single.wrapper_len() <= MAX_RETAINED_REVIEW_CHUNK_BYTES);
        }
    }
    assert!(coverage_bytes <= MAX_RETAINED_REVIEW_COVERAGE_BYTES);
}

#[test]
fn review_wrapper_overflow_fails_closed_without_truncating_authority() {
    // U+0001 is valid textual diff data but expands under JSON escaping. The
    // typed bundle may represent it; the wrapper-complete authority must
    // reject it rather than truncate a coverage chunk and still mark it read.
    let bundle = review_bundle("\u{1}".repeat(MAX_REVIEW_DIFF_TYPED_CHUNK_BYTES));
    let manifest = bundle.manifest();
    let request =
        ReviewDiffChunkRequest::for_manifest(manifest, 0, manifest.chunk_count().min(2)).unwrap();
    let batch = bundle.chunk_batch(&request).unwrap();
    assert!(matches!(
        RetainedToolResult::try_review_chunk_batch("chunks", &batch, &IdentityRedactor),
        Err(RetainedResultError::ReviewChunkTooLarge
            | RetainedResultError::ReviewChunkBatchTooLarge)
    ));
}
