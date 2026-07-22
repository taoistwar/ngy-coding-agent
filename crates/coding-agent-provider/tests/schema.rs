use coding_agent_provider::{
    MAX_PROVIDER_CONFIG_BYTES, MIN_PROVIDER_API_KEY_BYTES, PROVIDER_CONFIG_INVALID, ProviderConfig,
    ProviderConfigErrorReason, ProviderThinkingMode, ProviderToolChoiceCompatibility,
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
        config.tool_choice_compatibility(),
        ProviderToolChoiceCompatibility::Strict
    );
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
fn deepseek_thinking_can_be_explicitly_enabled_or_disabled() {
    let disabled = serde_json::to_vec(&serde_json::json!({
        "base_url": "https://provider.example",
        "model": "deepseek-v4-flash",
        "api_key": "known-provider-secret",
        "thinking": "disabled",
    }))
    .expect("encode disabled-thinking config");
    let config = ProviderConfig::from_json(&disabled).expect("disabled thinking is supported");
    assert_eq!(config.thinking_mode(), Some(ProviderThinkingMode::Disabled));

    let enabled = serde_json::to_vec(&serde_json::json!({
        "base_url": "https://provider.example",
        "model": "deepseek-v4-flash",
        "api_key": "known-provider-secret",
        "thinking": "enabled",
    }))
    .expect("encode enabled-thinking config");
    let config = ProviderConfig::from_json(&enabled).expect("enabled thinking is supported");
    assert_eq!(config.thinking_mode(), Some(ProviderThinkingMode::Enabled));

    for thinking in ["off", "false"] {
        let unsupported = serde_json::to_vec(&serde_json::json!({
            "base_url": "https://provider.example",
            "model": "deepseek-v4-flash",
            "api_key": "known-provider-secret",
            "thinking": thinking,
        }))
        .expect("encode unsupported thinking config");
        let error = ProviderConfig::from_json(&unsupported)
            .expect_err("reasoning output is outside the supported provider subset");
        assert_eq!(error.reason(), ProviderConfigErrorReason::InvalidDocument);
    }
}

#[test]
fn tool_choice_compatibility_is_strict_by_default_and_accepts_only_named_modes() {
    for (encoded_mode, expected) in [
        ("strict", ProviderToolChoiceCompatibility::Strict),
        (
            "required_as_required",
            ProviderToolChoiceCompatibility::RequiredAsRequired,
        ),
        (
            "required_as_auto",
            ProviderToolChoiceCompatibility::RequiredAsAuto,
        ),
    ] {
        let encoded = serde_json::to_vec(&serde_json::json!({
            "base_url": "https://provider.example",
            "model": "deepseek-v4-flash",
            "api_key": "known-provider-secret",
            "tool_choice_compatibility": encoded_mode,
        }))
        .expect("encode tool-choice compatibility config");
        let config = ProviderConfig::from_json(&encoded).expect("supported compatibility mode");
        assert_eq!(config.tool_choice_compatibility(), expected);
    }

    for invalid in [
        serde_json::Value::Null,
        serde_json::json!(true),
        serde_json::json!(1),
        serde_json::json!([]),
        serde_json::json!({}),
        serde_json::json!("auto"),
        serde_json::json!("required_auto"),
        serde_json::json!("required-as-required"),
        serde_json::json!("REQUIRED_AS_REQUIRED"),
    ] {
        let encoded = serde_json::to_vec(&serde_json::json!({
            "base_url": "https://provider.example",
            "model": "deepseek-v4-flash",
            "api_key": "known-provider-secret",
            "tool_choice_compatibility": invalid,
        }))
        .expect("encode invalid compatibility fixture");
        let error = ProviderConfig::from_json(&encoded)
            .expect_err("the compatibility schema must remain closed");
        assert_eq!(error.reason(), ProviderConfigErrorReason::InvalidDocument);
        assert!(!format!("{error:?}").contains("required-as-required"));
    }
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
fn explicit_insecure_http_allows_only_ip_literal_private_networks_and_loopback() {
    for base_url in [
        "http://10.0.0.1:19001",
        "http://172.16.1.20:19001",
        "http://172.31.255.254:19001/api/",
        "http://192.168.1.10:19001",
        "http://127.0.0.1:19001",
        "http://[fc00::1]:19001",
        "http://[fd12:3456::1]:19001/api/",
        "http://[::1]:19001",
    ] {
        let encoded = serde_json::to_vec(&serde_json::json!({
            "base_url": base_url,
            "model": "m",
            "api_key": "12345678",
            "allow_insecure_http": true,
        }))
        .expect("encode explicit private HTTP config");
        let config = ProviderConfig::from_json(&encoded)
            .expect("the explicit development exception accepts private IP HTTP");
        assert_eq!(config.base_url().scheme(), "http");
    }

    for allow_insecure_http in [None, Some(false)] {
        let mut value = serde_json::json!({
            "base_url": "http://172.16.1.20:19001",
            "model": "m",
            "api_key": "12345678",
        });
        if let Some(allow) = allow_insecure_http {
            value["allow_insecure_http"] = serde_json::json!(allow);
        }
        let encoded = serde_json::to_vec(&value).expect("encode fail-closed config");
        let error = ProviderConfig::from_json(&encoded)
            .expect_err("private HTTP requires an explicit true opt-in");
        assert_eq!(error.reason(), ProviderConfigErrorReason::InsecureBaseUrl);
    }
}

#[test]
fn insecure_http_opt_in_rejects_dns_public_special_and_link_local_addresses() {
    for base_url in [
        "http://localhost:19001",
        "http://provider.lan:19001",
        "http://8.8.8.8:19001",
        "http://0.0.0.0:19001",
        "http://100.64.0.1:19001",
        "http://169.254.169.254:19001",
        "http://224.0.0.1:19001",
        "http://[::]:19001",
        "http://[::ffff:172.16.1.20]:19001",
        "http://[fe80::1]:19001",
        "http://[2001:db8::1]:19001",
        "http://[ff02::1]:19001",
    ] {
        let encoded = serde_json::to_vec(&serde_json::json!({
            "base_url": base_url,
            "model": "m",
            "api_key": "12345678",
            "allow_insecure_http": true,
        }))
        .expect("encode rejected insecure HTTP config");
        let error = ProviderConfig::from_json(&encoded)
            .expect_err("the development exception must remain private-IP-only");
        assert_eq!(error.reason(), ProviderConfigErrorReason::InsecureBaseUrl);
    }

    for invalid in [
        serde_json::Value::Null,
        serde_json::json!("true"),
        serde_json::json!(1),
        serde_json::json!({}),
    ] {
        let encoded = serde_json::to_vec(&serde_json::json!({
            "base_url": "http://172.16.1.20:19001",
            "model": "m",
            "api_key": "12345678",
            "allow_insecure_http": invalid,
        }))
        .expect("encode malformed opt-in");
        let error = ProviderConfig::from_json(&encoded)
            .expect_err("the insecure HTTP opt-in must be a literal boolean");
        assert_eq!(error.reason(), ProviderConfigErrorReason::InvalidDocument);
    }
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
