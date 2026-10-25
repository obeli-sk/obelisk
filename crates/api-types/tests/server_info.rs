use obelisk_api_types::ServerInfoResponse;
use serde_json::{Value, json};

fn response() -> Value {
    json!({
        "app_name": "demo",
        "app_env_prefix": "OBELISK_APP_V1_",
        "obelisk_version": "0.43.0",
        "deployment_id": "Dep_01ARZ3NDEKTSV4RRFFQ69G5FAV",
        "node_run_id": "Run_01ARZ3NDEKTSV4RRFFQ69G5FAV",
        "app_config_digest": "sha256:example",
        "started_at": "2026-10-25T00:00:00Z"
    })
}

#[test]
fn server_info_rejects_missing_and_malformed_required_fields() {
    for field in response().as_object().unwrap().keys() {
        let mut missing = response();
        missing.as_object_mut().unwrap().remove(field);
        let error = serde_json::from_value::<ServerInfoResponse>(missing).unwrap_err();
        assert!(error.to_string().contains(field), "{error}");

        for value in [Value::Null, json!(42), json!({})] {
            let mut malformed = response();
            malformed[field] = value;
            assert!(
                serde_json::from_value::<ServerInfoResponse>(malformed).is_err(),
                "{field}"
            );
        }
    }
    let mut malformed = response();
    malformed["started_at"] = json!("invalid timestamp");
    assert!(serde_json::from_value::<ServerInfoResponse>(malformed).is_err());
}

#[test]
fn server_info_accepts_additive_fields() {
    let mut value = response();
    value["future_field"] = json!({"nested": true});
    let info: ServerInfoResponse = serde_json::from_value(value.clone()).unwrap();
    assert_eq!(info.deployment_id, value["deployment_id"]);
    assert_eq!(info.node_run_id, value["node_run_id"]);
    assert_eq!(info.app_env_prefix, value["app_env_prefix"]);
}
