use obeli_sk_db_http::openapi;
use serde_json::Value;

fn check_references(document: &Value, value: &Value) {
    match value {
        Value::Object(fields) => {
            if let Some(Value::String(reference)) = fields.get("$ref") {
                let pointer = reference.strip_prefix('#').unwrap();
                assert!(
                    document.pointer(pointer).is_some(),
                    "unresolved {reference}"
                );
            }
            for child in fields.values() {
                check_references(document, child);
            }
        }
        Value::Array(values) => {
            for child in values {
                check_references(document, child);
            }
        }
        _ => {}
    }
}

#[test]
fn production_schema_has_resolvable_references_and_excludes_test_operations() {
    let document = openapi::schema();
    check_references(&document, &document);
    let paths = document["paths"].as_object().unwrap();
    assert!(paths.contains_key("/v1/DbExecutor/lock_pending_by_ffqns"));
    assert!(paths.contains_key("/v1/Notifications/responses"));
    assert!(paths.contains_key("/v1/blobs/{digest}"));
    assert!(!paths.contains_key("/v1/DbExecutor/lock_one"));
    assert!(!paths.contains_key("/v1/DbExternalApi/get_active_deployment"));
    assert!(!paths.keys().any(|path| path.contains("DbConnectionTest")));
}
