use coding_agent_provider::{
    MAX_PROVIDER_CONFIG_BYTES, MIN_PROVIDER_API_KEY_BYTES, PROVIDER_CONFIG_INVALID, ProviderConfig,
    ProviderConfigErrorReason,
};

fn config_json(base_url: &str, model: &str, api_key: &str) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "base_url": base_url,
        "model": model,
        "api_key": api_key,
    }))
    .expect("encode config fixture")
}

#[test]
fn strict_https_config_exposes_only_non_secret_fields() {
    let config = ProviderConfig::from_json(&config_json(
        "https://provider.example/openai",
        "coding-model",
        "known-provider-secret",
    ))
    .expect("valid production config");

    assert_eq!(
        config.base_url().as_str(),
        "https://provider.example/openai/"
    );
    assert_eq!(config.model(), "coding-model");
    assert_eq!(
        config.chat_completions_url().as_str(),
        "https://provider.example/openai/v1/chat/completions"
    );

    for rendered in [
        format!("{config:?}"),
        format!("{config}"),
        format!("{:?}", config.api_key()),
        format!("{}", config.api_key()),
    ] {
        assert!(!rendered.contains("known-provider-secret"));
        assert!(rendered.contains("redacted"));
    }
}

#[test]
fn config_rendering_stays_safe_when_non_secret_fields_repeat_the_api_key() {
    let config = ProviderConfig::from_json(&config_json(
        "https://provider.example/known-provider-secret",
        "known-provider-secret",
        "known-provider-secret",
    ))
    .expect("syntactically valid duplicate secret fixture");

    for rendered in [format!("{config:?}"), format!("{config}")] {
        assert!(!rendered.contains("known-provider-secret"));
        assert!(rendered.contains("redacted"));
    }
    let redacted = config.redactor().for_log(
        "endpoint=https://provider.example/known-provider-secret model=known-provider-secret",
    );
    assert!(!redacted.as_str().contains("known-provider-secret"));
}

#[test]
fn persisted_schema_rejects_unknown_or_missing_fields_and_oversized_documents() {
    for json in [
        br#"{"base_url":"https://provider.example","model":"m","api_key":"k","timeout_ms":1}"#
            .as_slice(),
        br#"{"base_url":"https://provider.example","model":"m"}"#.as_slice(),
        br#"[]"#.as_slice(),
    ] {
        let error = ProviderConfig::from_json(json).expect_err("schema must be exact");
        assert_eq!(error.code(), PROVIDER_CONFIG_INVALID);
        assert_eq!(error.reason(), ProviderConfigErrorReason::InvalidDocument);
        assert!(!error.retryable());
    }

    let oversized = vec![b' '; MAX_PROVIDER_CONFIG_BYTES + 1];
    let error = ProviderConfig::from_json(&oversized).expect_err("config is bounded");
    assert_eq!(error.reason(), ProviderConfigErrorReason::DocumentTooLarge);
}

#[test]
fn production_rejects_http_while_the_explicit_test_policy_allows_ip_loopback_only() {
    let remote = config_json("http://provider.example", "m", "12345678");
    let error = ProviderConfig::from_json(&remote).expect_err("remote HTTP is forbidden");
    assert_eq!(error.reason(), ProviderConfigErrorReason::InsecureBaseUrl);
    assert!(
        ProviderConfig::from_json_allow_loopback_http_for_test(&remote).is_err(),
        "the test exception must not permit remote cleartext"
    );

    for loopback in ["http://127.0.0.1:4317", "http://[::1]:4317/api/"] {
        assert!(ProviderConfig::from_json(&config_json(loopback, "m", "12345678")).is_err());
        ProviderConfig::from_json_allow_loopback_http_for_test(&config_json(
            loopback, "m", "12345678",
        ))
        .expect("explicit test-only IP loopback HTTP");
    }

    assert!(
        ProviderConfig::from_json_allow_loopback_http_for_test(&config_json(
            "http://localhost:4317",
            "m",
            "12345678"
        ))
        .is_err(),
        "a DNS name is not an IP-literal loopback exception"
    );
}

#[test]
fn base_url_forbids_userinfo_query_and_fragment() {
    let cases = [
        (
            "https://user:password@provider.example",
            ProviderConfigErrorReason::BaseUrlUserInfo,
        ),
        (
            "https://provider.example?api_key=secret",
            ProviderConfigErrorReason::BaseUrlQuery,
        ),
        (
            "https://provider.example#secret",
            ProviderConfigErrorReason::BaseUrlFragment,
        ),
    ];

    for (base_url, reason) in cases {
        let error = ProviderConfig::from_json(&config_json(base_url, "m", "12345678"))
            .expect_err("unsafe base URL component");
        assert_eq!(error.reason(), reason);
        assert!(!format!("{error:?}").contains("password"));
        assert!(!format!("{error}").contains("secret"));
    }
}

#[test]
fn config_rejects_empty_or_header_unsafe_values_without_echoing_them() {
    for (model, key, reason) in [
        ("", "12345678", ProviderConfigErrorReason::InvalidModel),
        ("m", "", ProviderConfigErrorReason::InvalidApiKey),
        (
            "m",
            "secret\r\nx-injected: yes",
            ProviderConfigErrorReason::InvalidApiKey,
        ),
    ] {
        let error = ProviderConfig::from_json(&config_json("https://provider.example", model, key))
            .expect_err("invalid scalar value");
        assert_eq!(error.reason(), reason);
        assert!(!format!("{error:?}").contains("x-injected"));
    }
}

#[test]
fn api_key_minimum_prevents_pathological_exact_secret_matching() {
    assert_eq!(MIN_PROVIDER_API_KEY_BYTES, 8);
    let too_short = "k".repeat(MIN_PROVIDER_API_KEY_BYTES - 1);
    let minimum = "k".repeat(MIN_PROVIDER_API_KEY_BYTES);

    let error =
        ProviderConfig::from_json(&config_json("https://provider.example", "m", &too_short))
            .expect_err("short API keys are unsafe for exact-match redaction");
    assert_eq!(error.reason(), ProviderConfigErrorReason::InvalidApiKey);
    ProviderConfig::from_json(&config_json("https://provider.example", "m", &minimum))
        .expect("the documented minimum is accepted");
}
