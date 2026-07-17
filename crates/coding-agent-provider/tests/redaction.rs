use std::sync::Arc;

use coding_agent_core::ContextRedactor;
use coding_agent_provider::SecretRedactor;

#[test]
fn log_and_user_boundaries_remove_known_and_structural_secrets() {
    let redactor = SecretRedactor::new().with_secret("known-provider-secret");
    let raw = concat!(
        "Authorization: Bearer known-provider-secret\n",
        "{\"api_key\":\"unknown-api-key\",\"token\":\"unknown-token\"}\n",
        "https://provider.example/path?token=query-secret&ok=yes\n",
        "body repeats known-provider-secret"
    );

    for redacted in [redactor.for_log(raw), redactor.for_user(raw)] {
        let rendered = redacted.as_str();
        for secret in [
            "known-provider-secret",
            "unknown-api-key",
            "unknown-token",
            "query-secret",
        ] {
            assert!(!rendered.contains(secret), "leaked {secret:?}");
        }
        assert!(rendered.contains("<redacted>"));
        assert!(!format!("{redacted:?}").contains("known-provider-secret"));
        assert!(!format!("{redacted}").contains("known-provider-secret"));
    }
}

#[test]
fn boundary_output_is_utf8_safe_and_bounded_after_redaction() {
    let redactor = SecretRedactor::new().with_secret("sekret");
    let raw = format!("sekret {}", "界".repeat(100));
    let redacted = redactor.for_user_bounded(&raw, 32);

    assert!(redacted.as_str().len() <= 32);
    assert!(!redacted.as_str().contains("sekret"));
    assert!(redacted.as_str().ends_with("<truncated>"));
}

#[test]
fn redactor_debug_and_display_do_not_expose_registered_secrets() {
    let redactor = SecretRedactor::new().with_secret("known-provider-secret");
    for rendered in [format!("{redactor:?}"), format!("{redactor}")] {
        assert!(!rendered.contains("known-provider-secret"));
        assert!(rendered.contains("redacted"));
    }
}

#[test]
fn secret_redactor_is_a_context_redactor_without_an_extra_size_cap() {
    let redactor: Arc<dyn ContextRedactor> =
        Arc::new(SecretRedactor::new().with_secret("known-provider-secret"));
    let content = format!("{} known-provider-secret", "x".repeat(8 * 1024));

    let redacted = redactor.redact(&content);

    assert_eq!(
        redacted.len(),
        content.len() - "known-provider-secret".len() + "<redacted>".len()
    );
    assert!(redacted.starts_with(&"x".repeat(8 * 1024)));
    assert!(!redacted.contains("known-provider-secret"));
}
