use std::io::Write;

use coding_agent_app::{
    PROVIDER_CONFIG_INVALID, PlatformPaths, PrivateFile, ProviderConfigLoadErrorKind,
    load_provider_config,
};
use coding_agent_provider::ProviderToolChoiceCompatibility;

fn valid_json(api_key: &str) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "base_url": "https://provider.example",
        "model": "coding-model",
        "api_key": api_key,
        "tool_choice_compatibility": "required_as_required",
    }))
    .unwrap()
}

fn fixture() -> (tempfile::TempDir, PlatformPaths) {
    let temp = tempfile::tempdir().expect("create config fixture");
    let paths = PlatformPaths::new(temp.path().join("data"), temp.path().join("runtime"));
    paths.prepare().expect("prepare private app paths");
    (temp, paths)
}

fn write_private_config(paths: &PlatformPaths, bytes: &[u8]) {
    let path = paths.data_dir.join("provider.json");
    let mut file = PrivateFile::create_new(path).expect("create private provider config");
    file.write_all(bytes).expect("write provider config");
    file.flush().expect("flush provider config");
}

#[test]
fn app_loads_the_exact_private_provider_json_from_its_data_directory() {
    let (_temp, paths) = fixture();
    write_private_config(&paths, &valid_json("known-provider-secret"));

    let config = load_provider_config(&paths).expect("load private config");
    assert_eq!(config.model(), "coding-model");
    assert_eq!(config.base_url().as_str(), "https://provider.example/");
    assert_eq!(
        config.tool_choice_compatibility(),
        ProviderToolChoiceCompatibility::RequiredAsRequired
    );
    assert!(format!("{}", config.api_key()).contains("redacted"));
    assert!(!format!("{config:?}").contains("known-provider-secret"));
}

#[test]
fn app_loads_an_explicit_private_ip_http_development_provider() {
    let (_temp, paths) = fixture();
    let encoded = serde_json::to_vec(&serde_json::json!({
        "base_url": "http://172.16.1.20:19001",
        "model": "deepseek-v4-flash",
        "api_key": "known-provider-secret",
        "allow_insecure_http": true,
        "thinking": "enabled",
        "tool_choice_compatibility": "required_as_auto",
    }))
    .expect("encode private-network development provider");
    write_private_config(&paths, &encoded);

    let config = load_provider_config(&paths).expect("load explicit private HTTP provider");
    assert_eq!(config.base_url().as_str(), "http://172.16.1.20:19001/");
    assert_eq!(config.model(), "deepseek-v4-flash");
    assert!(!format!("{config:?}").contains("known-provider-secret"));
}

#[test]
fn invalid_tool_choice_compatibility_uses_the_secret_safe_app_diagnostic() {
    let (_temp, paths) = fixture();
    write_private_config(
        &paths,
        br#"{"base_url":"https://provider.example","model":"m","api_key":"known-provider-secret","tool_choice_compatibility":"invalid-known-provider-secret"}"#,
    );

    let error = load_provider_config(&paths).expect_err("reject invalid compatibility mode");
    assert_eq!(error.kind(), ProviderConfigLoadErrorKind::Invalid);
    assert_eq!(error.code(), PROVIDER_CONFIG_INVALID);
    assert!(!error.retryable());
    assert!(!format!("{error:?}").contains("known-provider-secret"));
    assert!(!format!("{error}").contains("known-provider-secret"));
}

#[test]
fn missing_invalid_and_non_private_configs_have_one_stable_secret_safe_boundary_code() {
    let (_temp, paths) = fixture();
    let missing = load_provider_config(&paths).expect_err("missing config");
    assert_eq!(missing.kind(), ProviderConfigLoadErrorKind::Missing);
    assert_eq!(missing.code(), PROVIDER_CONFIG_INVALID);
    assert!(!missing.retryable());

    write_private_config(
        &paths,
        br#"{"base_url":"https://provider.example","model":"m","api_key":"known-provider-secret","unknown":true}"#,
    );
    let invalid = load_provider_config(&paths).expect_err("unknown config field");
    assert_eq!(invalid.kind(), ProviderConfigLoadErrorKind::Invalid);
    assert!(!format!("{invalid:?}").contains("known-provider-secret"));
    assert!(!format!("{invalid}").contains("known-provider-secret"));

    std::fs::remove_file(paths.data_dir.join("provider.json")).unwrap();
    std::fs::write(
        paths.data_dir.join("provider.json"),
        valid_json("known-provider-secret"),
    )
    .expect("write ordinary inherited-permission config");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(
            paths.data_dir.join("provider.json"),
            std::fs::Permissions::from_mode(0o644),
        )
        .unwrap();
    }
    let non_private = load_provider_config(&paths).expect_err("non-private config");
    assert_eq!(non_private.kind(), ProviderConfigLoadErrorKind::NotPrivate);
    assert!(!format!("{non_private:?}").contains("known-provider-secret"));
}

#[test]
fn app_rejects_oversized_config_before_parsing_or_echoing_it() {
    let (_temp, paths) = fixture();
    let mut oversized = vec![b' '; coding_agent_app::MAX_PROVIDER_CONFIG_BYTES + 1];
    oversized[..21].copy_from_slice(b"known-provider-secret");
    write_private_config(&paths, &oversized);

    let error = load_provider_config(&paths).expect_err("bounded private read");
    assert_eq!(error.kind(), ProviderConfigLoadErrorKind::TooLarge);
    assert!(!format!("{error:?}").contains("known-provider-secret"));
    assert!(!format!("{error}").contains("known-provider-secret"));
}
