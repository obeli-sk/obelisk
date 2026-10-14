use super::*;
use rest_types::admin::{SystemEventResponse, SystemEventsResponse};

async fn events(server: &TestServer, token: &str) -> Vec<SystemEventResponse> {
    server
        .client
        .get(format!("{}/v1/admin/system-events", server.base_url))
        .bearer_auth(token)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json::<SystemEventsResponse>()
        .await
        .unwrap()
        .events
        .into_iter()
        .filter(|event| event.code.starts_with("api.token_hashes_"))
        .collect()
}

pub(super) async fn wait_for_set(
    server: &TestServer,
    token: &str,
    hashes: &[concepts::component_id::Digest],
) -> Vec<SystemEventResponse> {
    let expected = serde_json::to_value(crate::server::auth::reload::fingerprint(hashes)).unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let events = events(server, token).await;
            if events
                .first()
                .is_some_and(|event| event.details["fingerprint"] == expected)
            {
                return events;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("token hash acknowledgment timed out")
}

pub(super) fn replace(path: &std::path::Path, contents: &str) {
    let replacement = path.with_extension("new");
    std::fs::write(&replacement, contents).unwrap();
    if let Ok(modified) = path.metadata().and_then(|metadata| metadata.modified()) {
        std::fs::File::options()
            .write(true)
            .open(&replacement)
            .unwrap()
            .set_modified(modified)
            .unwrap();
    }
    std::fs::rename(replacement, path).unwrap();
}

#[tokio::test]
async fn api_auth_should_reload_token_hashes() {
    let first = crate::api::generate_token();
    let second = crate::api::generate_token();
    let explicit = crate::api::generate_token();
    let first_hash = crate::api::token_hash(&first);
    let second_hash = crate::api::token_hash(&second);
    let server = TestServer::start_empty_with_auth(
        test_addr!(192),
        &format!("api.token_hashes = [\"{first_hash}\"]"),
        Some(&explicit),
    )
    .await;
    let path = server.tmp_dir.path().join("server.toml");
    let original = std::fs::read_to_string(&path).unwrap();
    let initial = wait_for_set(&server, &explicit, std::slice::from_ref(&first_hash)).await;
    assert_eq!(initial.len(), 1);
    assert_eq!(initial[0].code, "api.token_hashes_loaded");
    assert_eq!(initial[0].details["count"], 1);
    let node_run: Value = server
        .client
        .get(format!("{}/v1/admin/node-run-id", server.base_url))
        .bearer_auth(&explicit)
        .header("Accept", "application/json")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        serde_json::to_value(initial[0].node_run_id).unwrap(),
        node_run
    );

    let status = async |token: &str| {
        server
            .client
            .get(format!("{}/v1/functions", server.base_url))
            .bearer_auth(token)
            .send()
            .await
            .unwrap()
            .status()
    };
    assert!(status(&first).await.is_success());
    let updated = original.replace(&first_hash.to_string(), &second_hash.to_string());
    assert_eq!(updated.len(), original.len());
    replace(&path, &updated);
    let changed = wait_for_set(&server, &explicit, std::slice::from_ref(&second_hash)).await;
    assert_eq!(changed.len(), 2);
    assert_eq!(changed[0].code, "api.token_hashes_changed");
    assert!(changed[0].event_id > initial[0].event_id);
    assert_eq!(status(&first).await, reqwest::StatusCode::UNAUTHORIZED);
    assert!(status(&second).await.is_success());

    let mut grpc = FunctionRepositoryClient::connect(format!("http://{}", server.api_addr()))
        .await
        .unwrap();
    for (token, accepted) in [(&first, false), (&second, true)] {
        let mut request = tonic::Request::new(ListComponentsRequest {
            function_name: None,
            component_digest: None,
            extensions: false,
            deployment_id: None,
        });
        request
            .metadata_mut()
            .insert("authorization", format!("Bearer {token}").parse().unwrap());
        let result = grpc.list_components(request).await;
        if accepted {
            result.unwrap();
        } else {
            assert_eq!(result.unwrap_err().code(), tonic::Code::Unauthenticated);
        }
    }

    for (token, accepted) in [(&first, false), (&second, true)] {
        let response = server
            .client
            .post(format!(
                "{}/obelisk.FunctionRepository/ListComponents",
                server.base_url
            ))
            .header("Content-Type", "application/grpc-web+proto")
            .bearer_auth(token)
            .body(vec![0_u8; 5])
            .send()
            .await
            .unwrap();
        assert!(response.status().is_success());
        assert_eq!(response.headers().get("grpc-status").is_none(), accepted);
        if !accepted {
            assert_eq!(response.headers()["grpc-status"], "16");
        }
    }

    replace(
        &path,
        &format!("api.token_hashes = ['{first_hash}', '{second_hash}']"),
    );
    let added = wait_for_set(
        &server,
        &explicit,
        &[first_hash.clone(), second_hash.clone()],
    )
    .await;
    assert_eq!(added.len(), 3);
    assert_eq!(added[0].details["count"], 2);
    assert!(status(&first).await.is_success());
    replace(
        &path,
        &format!("api.token_hashes = ['{second_hash}', '{first_hash}', '{second_hash}']"),
    );
    tokio::time::sleep(Duration::from_millis(1200)).await;
    assert_eq!(events(&server, &explicit).await.len(), 3);
    replace(&path, "api.token_hashes = ['invalid-secret-value']");
    tokio::time::sleep(Duration::from_millis(1200)).await;
    assert!(status(&second).await.is_success());
    assert_eq!(events(&server, &explicit).await.len(), 3);
    std::fs::remove_file(&path).unwrap();
    tokio::time::sleep(Duration::from_millis(1200)).await;
    assert!(status(&second).await.is_success());
    replace(&path, "api.token_hashes = []");
    let empty = wait_for_set(&server, &explicit, &[]).await;
    assert_eq!(empty.len(), 4);
    assert_eq!(empty[0].details["count"], 0);
    assert_eq!(status(&second).await, reqwest::StatusCode::UNAUTHORIZED);
    assert!(status(&explicit).await.is_success());
    server.shutdown().await;
}
