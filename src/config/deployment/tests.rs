use super::*;
use crate::config::env_var::EnvVarConfig;
use crate::config::secret_registry::SecretRegistry;
use concepts::cas::Cas;
use concepts::{ContentDigest, StrVariant, component_id::Digest};
use sha2::{Digest as _, Sha256};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::str::FromStr;
use wasm_workers::workflow::workflow_worker::{
    DEFAULT_NON_BLOCKING_EVENT_BATCHING, JoinNextBlockingStrategy,
};

fn digest_of(bytes: &[u8]) -> ContentDigest {
    ContentDigest(Digest(Sha256::digest(bytes).into()))
}

mod outbound_http {
    use super::*;

    #[test]
    fn server_allowlist_uses_deployment_allowed_host_shape() {
        let config: ServerConfigToml = toml::from_str(
            r#"
                [secrets]
                API_KEY = { env = "API_KEY_SOURCE" }

                [[outbound_http.allowed_host]]
                pattern = "api.example.com"
                methods = ["POST"]
                request_url_regex = "^POST https://api\\.example\\.com/v1/"
                secrets = ["API_KEY"]
                replace_in = ["headers"]
                "#,
        )
        .unwrap();

        let entry = &config.outbound_http.allowed_hosts[0];
        assert_eq!(entry.pattern, "api.example.com");
        assert_eq!(entry.secrets, ["API_KEY"]);
        assert!(matches!(
            entry.methods,
            Some(MethodsInput::List(ref methods)) if methods.as_slice() == ["POST"]
        ));
        assert!(matches!(entry.replace_in.as_slice(), [ReplaceIn::Headers]));
    }

    #[test]
    fn omitted_server_allowlist_is_empty() {
        let config: ServerConfigToml = toml::from_str("").unwrap();
        assert!(config.outbound_http.allowed_hosts.is_empty());
    }
}

mod blocking_strategy {
    use super::*;
    use crate::config::deployment::common::{
        BlockingStrategyAwaitConfig, BlockingStrategyConfigCustomized,
        BlockingStrategyConfigSimple, default_non_blocking_event_batching,
    };
    use serde::Deserialize;

    // Helper struct to deserialize into
    #[derive(Deserialize, Debug, PartialEq)]
    struct TestConfig {
        strategy: BlockingStrategyConfigToml,
    }

    #[test]
    fn deserialize_simple_interrupt() {
        let toml_str = r#"
strategy = "interrupt"
"#;
        let expected = TestConfig {
            strategy: BlockingStrategyConfigToml::Simple(BlockingStrategyConfigSimple::Interrupt),
        };
        let actual: TestConfig = toml::from_str(toml_str).expect("Should parse interrupt string");
        assert_eq!(actual, expected);

        // Verify From impl result
        assert_eq!(
            actual.strategy.into_blocking_strategy(None),
            JoinNextBlockingStrategy::Interrupt
        );
    }

    #[test]
    fn deserialize_simple_await() {
        let toml_str = r#"
strategy = "await"
"#;
        let expected = TestConfig {
            strategy: BlockingStrategyConfigToml::Simple(
                BlockingStrategyConfigSimple::Await, // The default variant of Simple
            ),
        };
        let actual: TestConfig = toml::from_str(toml_str).expect("Should parse await string");
        assert_eq!(actual, expected);

        // Verify From impl result (uses default batching)
        assert_eq!(
            actual.strategy.into_blocking_strategy(None),
            JoinNextBlockingStrategy::Await {
                non_blocking_event_batching: DEFAULT_NON_BLOCKING_EVENT_BATCHING,
                subscription_interruption: None,
            }
        );
    }

    #[test]
    fn deserialize_tagged_await_default_batching() {
        let toml_str = r#"
strategy = { kind = "await" }
"#;
        let expected = TestConfig {
            strategy: BlockingStrategyConfigToml::Tagged(BlockingStrategyConfigCustomized::Await(
                BlockingStrategyAwaitConfig {
                    non_blocking_event_batching: default_non_blocking_event_batching(),
                },
            )),
        };
        let actual: TestConfig =
            toml::from_str(toml_str).expect("Should parse tagged await with default batching");
        assert_eq!(actual, expected);

        // Verify From impl result (uses default batching)
        assert_eq!(
            actual.strategy.into_blocking_strategy(None),
            JoinNextBlockingStrategy::Await {
                non_blocking_event_batching: DEFAULT_NON_BLOCKING_EVENT_BATCHING,
                subscription_interruption: None,
            }
        );
    }

    #[test]
    fn deserialize_tagged_await_custom_batching() {
        let toml_str = r#"
strategy = { kind = "await", non_blocking_event_batching = 99 }
"#;
        let expected = TestConfig {
            strategy: BlockingStrategyConfigToml::Tagged(BlockingStrategyConfigCustomized::Await(
                BlockingStrategyAwaitConfig {
                    non_blocking_event_batching: 99,
                },
            )),
        };
        let actual: TestConfig =
            toml::from_str(toml_str).expect("Should parse tagged await with custom batching");
        assert_eq!(actual, expected);

        // Verify From impl result (uses custom batching)
        assert_eq!(
            actual.strategy.into_blocking_strategy(None),
            JoinNextBlockingStrategy::Await {
                non_blocking_event_batching: 99,
                subscription_interruption: None,
            }
        );
    }

    #[test]
    fn deserialize_invalid_string_should_fail() {
        let toml_str = r#"
strategy = "unknown"
"#;
        let result = toml::from_str::<TestConfig>(toml_str);
        assert!(result.is_err(), "Should fail on unknown string");
        // Check for a more specific error if needed, e.g., contains "unknown variant"
    }

    #[test]
    fn deserialize_invalid_kind_in_tagged_should_fail() {
        let toml_str = r#"
strategy = { kind = "interrupt", non_blocking_event_batching = 10 }
"#;
        let result = toml::from_str::<TestConfig>(toml_str);
        assert!(result.is_err(), "Should fail on invalid kind in map");
    }

    #[test]
    fn deserialize_invalid_structure_missing_kind_should_fail() {
        let toml_str = r#"
strategy = { name = "await", non_blocking_event_batching = 10 } # Missing 'kind'
"#;
        let result = toml::from_str::<TestConfig>(toml_str);
        // Fails `Tagged` because 'kind' is missing. Fails `Simple` because it's not a string.
        assert!(result.is_err(), "Should fail on map missing 'kind'");
    }

    #[test]
    fn deserialize_invalid_type_should_fail() {
        let toml_str = r"
strategy = 123
";
        let result = toml::from_str::<TestConfig>(toml_str);
        // Fails `Tagged` because not a map. Fails `Simple` because not a string.
        assert!(result.is_err(), "Should fail on incorrect type (integer)");
    }

    #[test]
    fn deserialize_tagged_await_with_extra_field_should_fail() {
        // TOML allows extra fields by default, Serde ignores them if not in the struct
        let toml_str = r#"
strategy = { kind = "await", non_blocking_event_batching = 25, extra_stuff = "hello" }
"#;
        let result = toml::from_str::<TestConfig>(toml_str);
        assert!(result.is_err(), "Should fail on `extra_stuff`");
    }
}

mod allow_exec_activities {
    use super::*;

    #[derive(serde::Deserialize, Debug)]
    struct TestConfig {
        #[serde(default)]
        allow: AllowExecActivities,
    }

    const DIGEST: &str = "sha256:abababababababababababababababababababababababababababababababab";

    #[test]
    fn deserialize_bool_map_and_legacy_digest_list() {
        let actual: TestConfig = toml::from_str("allow = true").unwrap();
        assert_eq!(AllowExecActivities::AllowAny, actual.allow);
        let actual: TestConfig = toml::from_str("allow = false").unwrap();
        assert_eq!(AllowExecActivities::Deny, actual.allow);
        let actual: TestConfig = toml::from_str("").unwrap();
        assert_eq!(AllowExecActivities::Deny, actual.allow);
        let actual: TestConfig = toml::from_str(&format!("[allow]\ngreet = \"{DIGEST}\"")).unwrap();
        assert_eq!(
            AllowExecActivities::Allowlist(BTreeMap::from([(
                "greet".to_string(),
                DIGEST.parse().unwrap()
            )])),
            actual.allow
        );
        let actual: TestConfig = toml::from_str(&format!("allow = [\"{DIGEST}\"]")).unwrap();
        assert_eq!(
            AllowExecActivities::LegacyAllowlist(vec![DIGEST.parse().unwrap()]),
            actual.allow
        );
    }

    #[test]
    fn deserialize_bool_string_as_sent_by_env_override() {
        // `OBELISK__ALLOW_EXEC_ACTIVITIES=true` reaches serde as a string.
        let actual: TestConfig = toml::from_str(r#"allow = "true""#).unwrap();
        assert_eq!(AllowExecActivities::AllowAny, actual.allow);
        toml::from_str::<TestConfig>(r#"allow = "yes""#).unwrap_err();
    }
}

mod allowed_hosts {
    use super::*;

    fn allowed_host_with_regex(request_url_regex: &str) -> AllowedHostToml {
        AllowedHostToml {
            pattern: "api.example.com".to_string(),
            methods: Some(MethodsInput::List(vec!["GET".to_string()])),
            request_url_regex: Some(request_url_regex.to_string()),
            secrets: Vec::new(),
            replace_in: Vec::new(),
        }
    }

    #[test]
    fn request_url_regex_interpolates_env_vars() {
        let (hosts, _advisories) = resolve_allowed_hosts(
            vec![allowed_host_with_regex(
                r"^GET https://${OBELISK_TEST_REQUEST_URL_REGEX_DOMAIN:-api\.example\.com}/v1/",
            )],
            false,
            &std::sync::Arc::new(SecretRegistry::empty()),
        )
        .unwrap();

        let regex = hosts[0].request_url_regex.as_ref().unwrap();
        assert!(regex.is_match("GET https://api.example.com/v1/items"));
        assert!(!regex.is_match("GET https://apiXexampleYcom/v1/items"));
    }

    #[test]
    fn request_url_regex_missing_env_var_fails_when_not_ignored() {
        const VAR: &str = "OBELISK_TEST_MISSING_REQUEST_URL_REGEX_DOMAIN_9E5F58E0";
        let error = resolve_allowed_hosts(
            vec![allowed_host_with_regex(&format!(
                "^GET https://${{{VAR}}}/"
            ))],
            false,
            &std::sync::Arc::new(SecretRegistry::empty()),
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains(VAR), "unexpected error: {error}");
    }

    #[test]
    fn request_url_regex_missing_env_var_skips_when_ignored() {
        const VAR: &str = "OBELISK_TEST_MISSING_REQUEST_URL_REGEX_DOMAIN_IGNORED_9E5F58E0";
        let (hosts, _advisories) = resolve_allowed_hosts(
            vec![allowed_host_with_regex(&format!(
                "^GET https://${{{VAR}}}/"
            ))],
            true,
            &std::sync::Arc::new(SecretRegistry::empty()),
        )
        .unwrap();
        assert!(hosts.is_empty());
    }
}

mod env_vars {
    use super::*;

    #[test]
    fn missing_key_value_interpolation_honors_ignore_missing() {
        const VAR: &str = "OBELISK_TEST_MISSING_KEY_VALUE_ENV_VAR_1C5D78B2";
        let env_vars = vec![EnvVarConfig::KeyValue {
            key: "RENAMED_ENV_VAR".to_string(),
            value: format!("${{{VAR}}}"),
        }];

        let error = resolve_env_vars_plaintext(env_vars.clone(), false, &SecretRegistry::empty())
            .unwrap_err()
            .to_string();
        assert!(error.contains(VAR), "unexpected error: {error}");

        let resolved =
            resolve_env_vars_plaintext(env_vars, true, &SecretRegistry::empty()).unwrap();
        assert_eq!(resolved[0].key, "RENAMED_ENV_VAR");
        assert_eq!(resolved[0].val, "");
    }
}

mod component_location {
    use super::*;

    #[test]
    fn parse_local_path() {
        let location: ComponentLocationToml = "./my-component.wasm".parse().unwrap();
        assert!(matches!(location, ComponentLocationToml::Path(p) if p == "./my-component.wasm"));
    }

    #[test]
    fn parse_oci_reference() {
        let location: ComponentLocationToml =
            "oci://ghcr.io/obeli-sk/obelisk:v0.34.1".parse().unwrap();
        assert!(matches!(location, ComponentLocationToml::Oci(_)));
    }
}

mod activity_stub {
    use crate::config::deployment::tests::digest_of;

    use super::*;

    #[test]
    fn deserialize_file_mode() {
        let toml_str = r#"
name = "my_stub"
location = "./stub.wasm"
"#;
        let stub: ActivityStubComponentConfigToml = toml::from_str(toml_str).unwrap();
        assert!(matches!(stub, ActivityStubComponentConfigToml::File(_)));
    }

    #[test]
    fn deserialize_inline_mode() {
        let toml_str = r#"
name = "my_stub"
ffqn = "ns:pkg/ifc.fn"
params = [{ name = "id", type = "u64" }]
return_type = "result<string, string>"
"#;
        let stub: ActivityStubComponentConfigToml = toml::from_str(toml_str).unwrap();
        assert!(matches!(stub, ActivityStubComponentConfigToml::Inline(_)));
    }

    #[test]
    fn reject_both_location_and_ffqn() {
        let toml_str = r#"
name = "my_stub"
location = "./stub.wasm"
ffqn = "ns:pkg/ifc.fn"
"#;
        toml::from_str::<ActivityStubComponentConfigToml>(toml_str).unwrap_err();
    }

    #[test]
    fn reject_neither_location_nor_ffqn() {
        let toml_str = r#"
name = "my_stub"
"#;
        toml::from_str::<ActivityStubComponentConfigToml>(toml_str).unwrap_err();
    }

    #[tokio::test]
    async fn file_mode_rejects_mismatched_content_digest() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("stub.wasm");
        tokio::fs::write(&path, b"actual").await.unwrap();
        let stub = ActivityStubComponentConfigResolved::File(ActivityStubFileConfigToml {
            common: ComponentCommon {
                name: ConfigName::new(StrVariant::from("my_stub")).unwrap(),
                location: ComponentLocationToml::Path(path.to_string_lossy().into_owned()),
            },
            content_digest: Some(digest_of(b"different")),
        });

        let err = stub
            .fetch_and_verify(dir.path().into(), dir.path().into())
            .await
            .unwrap_err()
            .to_string();

        assert!(
            err.contains("content digest mismatch"),
            "unexpected error: {err}"
        );
    }
}

mod activity_exec {
    use secrecy::{ExposeSecret as _, SecretString};

    use crate::config::deployment::tests::digest_of;

    use super::*;

    /// A config that references the registered secret name `MY_SECRET`.
    fn exec_config_with_secret() -> ActivityExecComponentConfigResolved {
        ActivityExecComponentConfigResolved {
            name: ConfigName::new(StrVariant::from("exec-test")).unwrap(),
            location: ScriptLocationResolved::Content {
                content: "#!/usr/bin/env bash\necho null\n".into(),
                file_name: "exec-test".into(),
            },
            content_digest: None,
            ffqn: "testing:integration/exec-secret.expose".parse().unwrap(),
            interface: FunctionInterfaceResolved::Inline(InlineFunctionInterfaceResolved {
                params: Some(vec![]),
                return_type: Some("result<string, string>".into()),
            }),
            component_digest: None,
            exec: ExecConfigToml::default(),
            max_retries: default_max_retries(),
            retry_exp_backoff: default_retry_exp_backoff(),
            forward_stdout: ComponentStdOutputToml::default(),
            forward_stderr: ComponentStdOutputToml::default(),
            logs_store_min_level: LogLevelToml::default(),
            env_vars: vec![],
            max_output_bytes: default_max_output_bytes(),
            secrets: vec!["MY_SECRET".to_string()],
            params_via_stdin: false,
        }
    }

    fn exec_config_with_source(
        location: ScriptLocationResolved,
        content_digest: Option<ContentDigest>,
    ) -> ActivityExecComponentConfigResolved {
        ActivityExecComponentConfigResolved {
            name: ConfigName::new(StrVariant::from("exec-test")).unwrap(),
            location,
            content_digest,
            ffqn: "testing:integration/exec-secret.expose".parse().unwrap(),
            interface: FunctionInterfaceResolved::Inline(InlineFunctionInterfaceResolved {
                params: Some(vec![]),
                return_type: Some("result<string, string>".into()),
            }),
            component_digest: None,
            exec: ExecConfigToml::default(),
            max_retries: default_max_retries(),
            retry_exp_backoff: default_retry_exp_backoff(),
            forward_stdout: ComponentStdOutputToml::default(),
            forward_stderr: ComponentStdOutputToml::default(),
            logs_store_min_level: LogLevelToml::default(),
            env_vars: vec![],
            max_output_bytes: default_max_output_bytes(),
            secrets: Vec::new(),
            params_via_stdin: false,
        }
    }

    fn inline_program() -> ResolvedExecProgram {
        ResolvedExecProgram {
            program: PathBuf::from("/tmp/fake-exec-script.sh"),
            content_digest: digest_of(b"#!/usr/bin/env bash\necho null\n"),
        }
    }

    /// A declared secret name is always carried; an unregistered one simply
    /// resolves to nothing at use (the child never receives it).
    /// `config_prepass::preflight` owns the fatal/continue/fix decision.
    #[test]
    fn fetch_and_verify_activity_exec_secret_dropped_when_unregistered() {
        let config = exec_config_with_secret();
        let verified = config
            .fetch_and_verify(
                inline_program(),
                false,
                &std::sync::Arc::new(SecretRegistry::empty()),
                None,
            )
            .unwrap();
        let secrets = verified.secrets.expect("declared secret name is carried");
        assert!(secrets.names.contains(&"MY_SECRET".to_string()));
        // Unregistered: the resolver supplies no value, so it is dropped at use.
        assert!(secrets.resolver.secret_lookup("MY_SECRET").is_none());
    }

    #[test]
    fn fetch_and_verify_activity_exec_secret_resolves_from_registry() {
        let config = exec_config_with_secret();
        let registry = std::sync::Arc::new(SecretRegistry::from_test_values([(
            "MY_SECRET".to_string(),
            SecretString::from("s3cret_value"),
        )]));
        let verified = config
            .fetch_and_verify(inline_program(), false, &registry, None)
            .unwrap();
        let secrets = verified.secrets.expect("secret must be declared");
        // Only the name is carried; the value is fetched on demand via the resolver.
        assert!(secrets.names.contains(&"MY_SECRET".to_string()));
        assert_eq!(
            secrets
                .resolver
                .secret_lookup("MY_SECRET")
                .expect("resolver supplies the declared secret")
                .expose_secret(),
            "s3cret_value"
        );
    }

    #[test]
    fn fetch_and_verify_activity_exec_hashes_resolved_source_not_oci_reference() {
        let source = b"#!/usr/bin/env bash\necho null\n".to_vec();
        let inline = exec_config_with_source(
            ScriptLocationResolved::Content {
                content: String::from_utf8(source.clone()).unwrap(),
                file_name: "exec-test".into(),
            },
            None,
        );
        let oci = exec_config_with_source(
            ScriptLocationResolved::Oci {
                image: "registry.example.com/ns/exec:latest".parse().unwrap(),
            },
            None,
        );

        let inline_verified = inline
            .fetch_and_verify(
                ResolvedExecProgram {
                    program: PathBuf::from("/tmp/fake-exec-script.sh"),
                    content_digest: digest_of(&source),
                },
                true,
                &std::sync::Arc::new(SecretRegistry::empty()),
                None,
            )
            .unwrap();
        let oci_verified = oci
            .fetch_and_verify(
                ResolvedExecProgram {
                    program: PathBuf::from("/tmp/fake-exec-script.sh"),
                    content_digest: digest_of(&source),
                },
                true,
                &std::sync::Arc::new(SecretRegistry::empty()),
                None,
            )
            .unwrap();

        assert_eq!(inline_verified.component_id, oci_verified.component_id);
    }

    #[tokio::test]
    async fn resolve_activity_exec_validates_inline_content_digest() {
        let config = exec_config_with_source(
            ScriptLocationResolved::Content {
                content: "#!/usr/bin/env bash\necho null\n".into(),
                file_name: "exec-test".into(),
            },
            Some(
                "sha256:1111111111111111111111111111111111111111111111111111111111111111"
                    .parse()
                    .unwrap(),
            ),
        );
        let error = config
            .resolve(std::path::Path::new("/tmp"))
            .await
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("content digest mismatch"),
            "unexpected error: {error}"
        );
    }
}

mod script_location {
    use crate::config::deployment::tests::digest_of;

    use super::*;
    use concepts::cas::InMemoryCas;

    fn javascript(
        location: Option<ScriptLocationPathOrOci>,
        content: Option<String>,
        component_files: BTreeMap<String, ContentDigest>,
    ) -> ScriptToml {
        ScriptToml::JavaScript {
            location,
            content,
            component_files,
        }
    }

    #[tokio::test]
    async fn inline_content_becomes_owned() {
        let cas = InMemoryCas::default();
        let location = resolve_script_toml(
            javascript(
                None,
                Some("export const x = 1;".to_string()),
                BTreeMap::new(),
            ),
            "foo.js".to_string(),
            &cas,
            None,
        )
        .await
        .unwrap();
        assert_matches::assert_matches!(
            location,
            ScriptLocationResolved::Content { content, file_name }
                if content == "export const x = 1;" && file_name == "foo.js"
        );
    }

    #[tokio::test]
    async fn relative_file_is_owned_and_mirrors_subpath() {
        let cas = InMemoryCas::default();
        let source = "export default 'owned content';";
        let digest = cas.write_blob(source.as_bytes()).await.unwrap();

        // Bare relative path (implicit `${DEPLOYMENT_DIR}` prefix).
        let location = resolve_script_toml(
            javascript(
                Some(ScriptLocationPathOrOci::Path("scripts/a.js".to_string())),
                None,
                BTreeMap::new(),
            ),
            "ignored.js".to_string(),
            &cas,
            Some(&digest),
        )
        .await
        .unwrap();
        assert_matches::assert_matches!(
            location,
            ScriptLocationResolved::Content { content, file_name }
                if content == source && file_name == "scripts/a.js"
        );
    }

    #[tokio::test]
    async fn explicit_deployment_dir_prefix_is_owned() {
        let cas = InMemoryCas::default();
        let digest = cas
            .write_blob(b"export default 'owned content';")
            .await
            .unwrap();

        let location = resolve_script_toml(
            javascript(
                Some(ScriptLocationPathOrOci::Path(
                    "${DEPLOYMENT_DIR}/scripts/a.js".to_string(),
                )),
                None,
                BTreeMap::new(),
            ),
            "ignored.js".to_string(),
            &cas,
            Some(&digest),
        )
        .await
        .unwrap();
        assert_matches::assert_matches!(
            location,
            ScriptLocationResolved::Content { file_name, .. } if file_name == "scripts/a.js"
        );
    }

    #[tokio::test]
    async fn absolute_path_is_rejected() {
        let cas = InMemoryCas::default();
        let abs = "/tmp/outside.js".to_string();
        let err = resolve_script_toml(
            javascript(
                Some(ScriptLocationPathOrOci::Path(abs.clone())),
                None,
                BTreeMap::new(),
            ),
            "ignored.js".to_string(),
            &cas,
            None,
        )
        .await
        .unwrap_err()
        .to_string();
        assert!(
            err.contains("absolute local paths are not allowed"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn parent_dir_escape_is_rejected() {
        let cas = InMemoryCas::default();
        for raw in ["../escape.js", "${DEPLOYMENT_DIR}/../escape.js"] {
            let err = resolve_script_toml(
                javascript(
                    Some(ScriptLocationPathOrOci::Path(raw.to_string())),
                    None,
                    BTreeMap::new(),
                ),
                "ignored.js".to_string(),
                &cas,
                None,
            )
            .await
            .unwrap_err()
            .to_string();
            assert!(err.contains("`..`"), "unexpected error for `{raw}`: {err}");
        }
    }

    #[tokio::test]
    async fn oci_becomes_oci() {
        let cas = InMemoryCas::default();
        let reference =
            oci_client::Reference::from_str("docker.io/library/example:latest").unwrap();
        let location = resolve_script_toml(
            javascript(
                Some(ScriptLocationPathOrOci::Oci(reference)),
                None,
                BTreeMap::new(),
            ),
            "ignored.js".to_string(),
            &cas,
            None,
        )
        .await
        .unwrap();
        assert_matches::assert_matches!(
            location,
            ScriptLocationResolved::Oci { image }
                if image.to_string() == "docker.io/library/example:latest"
        );
    }

    #[tokio::test]
    async fn content_digest_verified_at_submit() {
        let cas = InMemoryCas::default();
        let content = "export const x = 1;";

        // Matching digest succeeds.
        resolve_script_toml(
            javascript(None, Some(content.to_string()), BTreeMap::new()),
            "foo.js".to_string(),
            &cas,
            Some(&digest_of(content.as_bytes())),
        )
        .await
        .expect("matching digest should pass");

        // Mismatching digest on inline content fails.
        let wrong = digest_of(b"different");
        let err = resolve_script_toml(
            javascript(None, Some(content.to_string()), BTreeMap::new()),
            "foo.js".to_string(),
            &cas,
            Some(&wrong),
        )
        .await
        .unwrap_err()
        .to_string();
        assert!(
            err.contains("content digest mismatch"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn relative_file_missing_blob_is_rejected() {
        // A relative script whose pinned digest is not in the CAS cannot be resolved: in the
        // content-addressed model a wrong digest is a missing blob, not a hash mismatch.
        let cas = InMemoryCas::default();
        let missing = digest_of(b"nope");
        let err = resolve_script_toml(
            javascript(
                Some(ScriptLocationPathOrOci::Path("script.js".to_string())),
                None,
                BTreeMap::new(),
            ),
            "ignored.js".to_string(),
            &cas,
            Some(&missing),
        )
        .await
        .unwrap_err();
        let err = format!("{err:#}");
        assert!(
            err.contains("not present in the CAS"),
            "unexpected error: {err}"
        );
    }
}

mod export {
    use super::*;

    fn js_activity(
        name: &str,
        location: ScriptLocationResolved,
    ) -> ActivityJsComponentConfigResolved {
        ActivityJsComponentConfigResolved {
            name: ConfigName::new(StrVariant::from(name.to_string())).unwrap(),
            location,
            content_digest: None,
            component_digest: None,
            ffqn: "ns:pkg/ifc.fn".parse().unwrap(),
            interface: FunctionInterfaceResolved::Inline(InlineFunctionInterfaceResolved {
                params: Some(vec![]),
                return_type: None,
            }),
            exec: ExecConfigToml::default(),
            max_retries: default_max_retries(),
            retry_exp_backoff: default_retry_exp_backoff(),
            forward_stdout: ComponentStdOutputToml::default(),
            forward_stderr: ComponentStdOutputToml::default(),
            logs_store_min_level: LogLevelToml::default(),
            env_vars: vec![],
            allowed_hosts: vec![],
        }
    }

    #[test]
    fn submit_rejects_owned_file_name_collision() {
        // Two distinct owned scripts resolving to the same `file_name` must be rejected
        // at submit time, since `deployment get` could never write both to disk.
        let mut deployment = DeploymentResolved::default();
        deployment.activities_js.push(js_activity(
            "a",
            ScriptLocationResolved::Content {
                content: "export const a = 1;".to_string(),
                file_name: "foo".to_string(),
            },
        ));
        deployment.activities_js.push(js_activity(
            "b",
            ScriptLocationResolved::Content {
                content: "export const b = 2;".to_string(),
                file_name: "foo".to_string(),
            },
        ));
        let err = validate_owned_source_file_names(&deployment)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("two deployment-owned source files would be written to `foo`"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn submit_allows_identical_owned_content_under_same_name() {
        // Same file_name with identical content dedupes on export, so it must pass submit.
        let mut deployment = DeploymentResolved::default();
        for name in ["a", "b"] {
            deployment.activities_js.push(js_activity(
                name,
                ScriptLocationResolved::Content {
                    content: "export const shared = 1;".to_string(),
                    file_name: "shared.js".to_string(),
                },
            ));
        }
        validate_owned_source_file_names(&deployment).unwrap();
    }
}

mod backtrace {
    use crate::config::deployment::tests::digest_of;

    use super::*;

    #[test]
    fn wasm_deployment_dir_escape_rejected_but_subpath_ok() {
        let dir = std::path::Path::new("/dep");

        let mut escape = "${DEPLOYMENT_DIR}/../evil.wasm".to_string();
        let err = format!(
            "{:#}",
            DeploymentToml::expand_deployment_dir(&mut escape, dir).unwrap_err()
        );
        assert!(err.contains("`..`"), "unexpected error: {err}");

        let mut ok = "${DEPLOYMENT_DIR}/components/a.wasm".to_string();
        DeploymentToml::expand_deployment_dir(&mut ok, dir).unwrap();
        assert_eq!(ok, "/dep/components/a.wasm");

        // Bare relative paths are anchored to the deployment dir too.
        let mut bare = "components/a.wasm".to_string();
        DeploymentToml::expand_deployment_dir(&mut bare, dir).unwrap();
        assert_eq!(bare, "/dep/components/a.wasm");

        let mut bare_escape = "../evil.wasm".to_string();
        let err = format!(
            "{:#}",
            DeploymentToml::expand_deployment_dir(&mut bare_escape, dir).unwrap_err()
        );
        assert!(err.contains("`..`"), "unexpected error: {err}");

        // Author-provided absolute paths are rejected.
        let mut abs = "/other/a.wasm".to_string();
        let err = format!(
            "{:#}",
            DeploymentToml::expand_deployment_dir(&mut abs, dir).unwrap_err()
        );
        assert!(
            err.contains("absolute local paths are not allowed"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn resolved_retains_relative_subpath() {
        let digest = digest_of(b"SRC");
        let component_files =
            BTreeMap::from([("crates/foo/src/lib.rs".to_string(), digest.clone())]);

        let mut bt = ComponentBacktraceConfig::default();
        bt.frame_files_to_sources.insert(
            ".../src/lib.rs".to_string(),
            "${DEPLOYMENT_DIR}/crates/foo/src/lib.rs".to_string(),
        );
        let resolved = resolve_backtrace(&bt, &component_files).unwrap();
        let src = resolved
            .frame_files_to_sources
            .get(".../src/lib.rs")
            .unwrap();
        assert_eq!(src.content_digest, digest);
        assert_eq!(src.file_name, "crates/foo/src/lib.rs");
    }

    #[test]
    fn bare_relative_source_is_deployment_dir_relative() {
        // A bare relative backtrace source (no `${DEPLOYMENT_DIR}` prefix) resolves to the
        // same deployment-relative file name as the explicit-prefix form.
        let digest = digest_of(b"SRC");
        let component_files =
            BTreeMap::from([("crates/foo/src/lib.rs".to_string(), digest.clone())]);

        let mut bt = ComponentBacktraceConfig::default();
        bt.frame_files_to_sources.insert(
            ".../src/lib.rs".to_string(),
            "crates/foo/src/lib.rs".to_string(),
        );
        let resolved = resolve_backtrace(&bt, &component_files).unwrap();
        let src = resolved
            .frame_files_to_sources
            .get(".../src/lib.rs")
            .unwrap();
        assert_eq!(src.content_digest, digest);
        assert_eq!(src.file_name, "crates/foo/src/lib.rs");
    }

    #[test]
    fn source_parent_dir_escape_is_rejected() {
        let mut bt = ComponentBacktraceConfig::default();
        bt.frame_files_to_sources.insert(
            "frame".to_string(),
            "${DEPLOYMENT_DIR}/../escape.rs".to_string(),
        );
        let err = format!(
            "{:#}",
            resolve_backtrace(&bt, &BTreeMap::new()).unwrap_err()
        );
        assert!(err.contains("`..`"), "unexpected error: {err}");
    }

    #[test]
    #[should_panic(expected = "must be rejected before resolution")]
    fn absolute_source_panics_after_validation() {
        // Absolute backtrace sources are rejected by the pre-resolve validation pass,
        // so reaching `resolve_backtrace` with one is an internal invariant violation.
        let mut bt = ComponentBacktraceConfig::default();
        bt.frame_files_to_sources
            .insert(".../src/lib.rs".to_string(), "/nested/lib.rs".to_string());
        let _ = resolve_backtrace(&bt, &BTreeMap::new());
    }
}
