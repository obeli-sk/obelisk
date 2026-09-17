use super::*;

const DEPLOYMENT: &str = r#"[[webhook_endpoint_js]]
name = "test_get_status_webhook"
content = '''
export default function handle(request) {
    const execId = request.headers.get("x-execution-id");
    const status = obelisk.getStatus(execId);
    return Response.json({ executionStatus: status });
}
import * as obelisk from "obelisk:webhook@1.0.0";
'''
routes = [{ methods = ["GET"], route = "/get-status" }]
"#;

/// A JS webhook's `getStatus` reports the first-class `cancelling` state.
#[tokio::test]
async fn get_status_cancelling() {
    use concepts::prefixed_ulid::DEPLOYMENT_ID_DUMMY;
    use concepts::storage::{
        AppendRequest, CreateRequest, ExecutionRequest, HistoryEvent, JoinSetRequest,
    };
    use concepts::{
        ComponentId, ExecutionId, ExecutionMetadata, JoinSetId, JoinSetKind, Params, StrVariant,
    };

    // The uncancellable child keeps the parent cancelling until the webhook reads it.
    const PARENT_FFQN: FunctionFqn =
        FunctionFqn::new_static("testing:cancel/ifc", "parent-cancellable");
    const CHILD_FFQN: FunctionFqn = FunctionFqn::new_static("testing:cancel/ifc", "child");

    let server = TestServer::start_inline_deployment(test_addr!(86), "", DEPLOYMENT, &[]).await;

    let parent_id = {
        let pool = SqlitePool::new(&server.sqlite_file, SqliteConfig::default())
            .await
            .unwrap();
        let conn = pool.connection().await.unwrap();
        let now = chrono::Utc::now();
        let create = |execution_id: ExecutionId, ffqn: FunctionFqn| CreateRequest {
            created_at: now,
            execution_id,
            ffqn,
            params: Params::empty(),
            parent: None,
            scheduled_at: now,
            component_id: ComponentId::dummy_workflow(),
            deployment_id: DEPLOYMENT_ID_DUMMY,
            metadata: ExecutionMetadata::empty(),
            scheduled_by: None,
            paused: false,
            max_persisted_value_size_bytes: u64::MAX,
        };

        let parent_id = ExecutionId::generate();
        let version = conn
            .create(create(parent_id.clone(), PARENT_FFQN))
            .await
            .unwrap();
        let join_set_id = JoinSetId::new(JoinSetKind::OneOff, StrVariant::empty()).unwrap();
        let version = conn
            .append(
                parent_id.clone(),
                version,
                AppendRequest {
                    created_at: now,
                    event: ExecutionRequest::HistoryEvent {
                        event: HistoryEvent::JoinSetCreate {
                            join_set_id: join_set_id.clone(),
                        },
                    },
                },
            )
            .await
            .unwrap();
        let child_id = parent_id.next_level(&join_set_id);
        conn.append(
            parent_id.clone(),
            version,
            AppendRequest {
                created_at: now,
                event: ExecutionRequest::HistoryEvent {
                    event: HistoryEvent::JoinSetRequest {
                        join_set_id,
                        request: JoinSetRequest::ChildExecutionRequest {
                            child_execution_id: child_id.clone(),
                            target_ffqn: CHILD_FFQN,
                            params: concepts::storage::PersistedParams::Inline(Params::empty()),
                            params_hash: None,
                            result: Ok(()),
                        },
                    },
                },
            },
        )
        .await
        .unwrap();
        conn.create(create(ExecutionId::Derived(child_id), CHILD_FFQN))
            .await
            .unwrap();
        conn.append_cancel_workflow_requested_with_retries(&parent_id, now)
            .await
            .unwrap();
        pool.close().await;
        parent_id
    };

    let resp = server
        .client
        .get(format!("{}/get-status", server.webhook_base_url))
        .header("x-execution-id", parent_id.to_string())
        .send()
        .await
        .expect("webhook request failed");
    assert_eq!(resp.status().as_u16(), 200);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(
        body["executionStatus"]["status"],
        serde_json::json!("cancelling")
    );
    server.shutdown().await;
}
