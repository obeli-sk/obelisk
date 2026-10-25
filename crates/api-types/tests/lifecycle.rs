use obelisk_api_types::{AppPolicyV1, DeploymentSwitchOutcome, DeploymentSwitchResponse};
use serde_json::{Value, json};

fn policy() -> Value {
    json!({
        "format": "obelisk-app-config-v1",
        "secrets": {
            "KEY": {"optional": false, "exposed_to": {"chat": ["KEY"]}},
            "SELF": {"type": "internal", "exposed_to": {}}
        },
        "public_env": ["REGION", {"name": "LABEL", "optional": true},
            {"name": "SELF_API", "type": "internal", "source": "api-url"}],
        "allowed_exec_activities": {"exec": ["sha256:example"]},
        "outbound_http": [{"pattern": "https://example.com", "methods": "*",
            "secrets": ["KEY"], "replace_in": ["headers"], "request_url_regex": null}]
    })
}

#[test]
fn policy_roundtrip_preserves_wildcard_methods_and_declarations() {
    let value = policy();
    let policy: AppPolicyV1 = serde_json::from_value(value.clone()).unwrap();
    let serialized = serde_json::to_value(policy).unwrap();
    assert_eq!(serialized["outbound_http"][0]["methods"], "*");
    assert_eq!(serialized["secrets"], value["secrets"]);
    assert_eq!(serialized["public_env"], value["public_env"]);
}

#[test]
fn policy_rejects_missing_fields_and_malformed_declarations() {
    for field in policy().as_object().unwrap().keys() {
        let mut value = policy();
        value.as_object_mut().unwrap().remove(field);
        assert!(
            serde_json::from_value::<AppPolicyV1>(value).is_err(),
            "{field}"
        );
    }
    for (pointer, invalid) in [
        ("/format", json!("unknown-format")),
        ("/secrets/KEY", json!({"optional": false})),
        ("/public_env", json!([{"name": "ENV", "type": "unknown"}])),
        ("/allowed_exec_activities", json!([])),
        ("/outbound_http/0/methods", json!("GET")),
    ] {
        let mut value = policy();
        *value.pointer_mut(pointer).unwrap() = invalid;
        assert!(
            serde_json::from_value::<AppPolicyV1>(value).is_err(),
            "{pointer}"
        );
    }
}

#[test]
fn switch_requires_a_recognized_success_outcome() {
    let switched: DeploymentSwitchResponse =
        serde_json::from_value(json!({"ok": "switched"})).unwrap();
    assert_eq!(switched.ok, DeploymentSwitchOutcome::Switched);
    let restart: DeploymentSwitchResponse =
        serde_json::from_value(json!({"ok": "restart_required"})).unwrap();
    assert_eq!(restart.ok, DeploymentSwitchOutcome::RestartRequired);
    for invalid in [
        json!({}),
        json!("switched"),
        json!({"ok": 42}),
        json!({"ok": "unknown"}),
        json!({"err": "failed"}),
    ] {
        assert!(serde_json::from_value::<DeploymentSwitchResponse>(invalid).is_err());
    }
}
