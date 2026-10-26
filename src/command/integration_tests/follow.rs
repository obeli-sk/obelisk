//! `follow=true` SSE streams of `/responses`, `/logs` and `/events-and-responses`.
//! The workflow blocks on a stub the test resolves while the streams are open.

use super::*;
use concepts::{ExecutionId, JoinSetId, JoinSetKind, StrVariant};

const DEPLOYMENT: &str = r#"[[workflow_js]]
name = "test_await_stub_workflow"
ffqn = "testing:integration/workflow-await-stub.await-stub"
content = '''
import * as dynamic from "obelisk:workflow-dynamic@1.0.0";
// Block until the test stubs the child through the API.
export default function await_stub(id) {
    const js = obelisk.createJoinSet({ name: 'stub-set' });
    dynamic.submit(js, 'testing:integration/stubs.my-stub', [id]);
    console.log('submitted');
    const result = js.joinNext();
    console.log('stubbed');
    return result;
}
import * as obelisk from "obelisk:workflow@1.0.0";
'''
params = [
  { name = "id", type = "u64" },
]
return_type = "result<string, string>"

[[activity_stub]]
ffqn = "testing:integration/stubs.my-stub"
params = [
  { name = "id", type = "u64" },
]
return_type = "result<string, string>"
"#;

const STREAM_TIMEOUT: Duration = Duration::from_secs(30);

struct SseFrame {
    event: String,
    id: String,
    data: Value,
}

struct SseReader {
    response: reqwest::Response,
    buffer: Vec<u8>,
}

impl SseReader {
    async fn open(server: &TestServer, path_and_query: &str, last_event_id: Option<&str>) -> Self {
        let mut request = server
            .client
            .get(format!("{}/v1/executions/{path_and_query}", server.base_url))
            .header("Accept", "text/event-stream");
        if let Some(id) = last_event_id {
            request = request.header("Last-Event-ID", id);
        }
        let response = request.send().await.expect("follow request failed");
        assert_eq!(response.status(), reqwest::StatusCode::OK);
        assert_eq!(
            response.headers()[reqwest::header::CONTENT_TYPE],
            "text/event-stream"
        );
        Self {
            response,
            buffer: Vec::new(),
        }
    }

    /// Next data frame, `None` once the server ends the stream.
    async fn next(&mut self) -> Option<SseFrame> {
        tokio::time::timeout(STREAM_TIMEOUT, async {
            loop {
                if let Some(end) = self.buffer.windows(2).position(|pair| pair == b"\n\n") {
                    let frame = String::from_utf8(self.buffer.drain(..end + 2).collect())
                        .expect("SSE frame must be UTF-8");
                    let field = |name: &str| {
                        frame
                            .lines()
                            .find_map(|line| line.strip_prefix(name))
                            .map(str::to_string)
                    };
                    // Keep-alive frames carry only a comment.
                    if let Some(data) = field("data: ") {
                        return Some(SseFrame {
                            event: field("event: ").expect("event name"),
                            id: field("id: ").expect("event id"),
                            data: serde_json::from_str(&data).expect("data must be JSON"),
                        });
                    }
                } else {
                    let chunk = self.response.chunk().await.expect("stream read failed")?;
                    self.buffer.extend_from_slice(&chunk);
                }
            }
        })
        .await
        .expect("follow stream timed out")
    }

    async fn collect(mut self) -> Vec<SseFrame> {
        let mut frames = Vec::new();
        while let Some(frame) = self.next().await {
            frames.push(frame);
        }
        frames
    }
}

fn versions(frames: &[SseFrame]) -> Vec<u64> {
    frames
        .iter()
        .flat_map(|frame| frame.data["events"].as_array().unwrap())
        .map(|event| event["version"].as_u64().unwrap())
        .collect()
}

#[tokio::test]
async fn follow_streams_end_when_execution_finishes() {
    let server =
        TestServer::start_inline_deployment(test_addr!(40_159), "", DEPLOYMENT, &[]).await;
    let execution_id = server.generate_execution_id().await;
    let join_set_id = JoinSetId::new(JoinSetKind::Named, StrVariant::from("stub-set")).unwrap();
    let child_id = ExecutionId::Derived(
        execution_id
            .parse::<ExecutionId>()
            .unwrap()
            .next_level(&join_set_id),
    )
    .to_string();
    let submitted = server
        .client
        .put(format!("{}/v1/executions/{execution_id}", server.base_url))
        .header("Accept", "application/json")
        .json(&json!({
            "ffqn": "testing:integration/workflow-await-stub.await-stub",
            "params": [1],
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(submitted.status(), reqwest::StatusCode::CREATED);

    let mut events = SseReader::open(
        &server,
        &format!("{execution_id}/events-and-responses?follow=true"),
        None,
    )
    .await;
    let responses =
        SseReader::open(&server, &format!("{execution_id}/responses?follow=true"), None).await;
    let mut logs =
        SseReader::open(&server, &format!("{execution_id}/logs?follow=true"), None).await;

    // The first page is sent right away even without new rows.
    let first = events.next().await.unwrap();
    assert_eq!(first.event, "events_and_responses");
    assert_ne!(first.data["current_status"]["pending_state"]["status"], "finished");
    let first_log = logs.next().await.unwrap();
    assert_eq!(first_log.event, "logs");
    assert_eq!(first_log.data[0]["message"], "submitted");

    // The child is written once the workflow blocks on `joinNext`.
    let deadline = tokio::time::Instant::now() + STREAM_TIMEOUT;
    loop {
        let stubbed = server
            .client
            .put(format!("{}/v1/executions/{child_id}/stub", server.base_url))
            .json(&json!({ "ok": "stub-ok" }))
            .send()
            .await
            .unwrap();
        if stubbed.status().is_success() {
            break;
        }
        assert!(tokio::time::Instant::now() < deadline, "child was never created");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let responses = responses.collect().await;
    assert_eq!(1, responses.len());
    assert_eq!(responses[0].event, "responses");
    assert_eq!(1, responses[0].data["responses"].as_array().unwrap().len());

    let messages: Vec<_> = logs
        .collect()
        .await
        .iter()
        .flat_map(|frame| frame.data.as_array().unwrap().clone())
        .map(|log| log["message"].clone())
        .collect();
    assert_eq!(messages, vec![json!("stubbed")]);

    let mut event_frames = vec![first];
    event_frames.extend(events.collect().await);
    let last = event_frames.last().unwrap();
    assert_eq!(last.data["current_status"]["pending_state"]["status"], "finished");
    let all_versions = versions(&event_frames);
    let max_version = last.data["max_version"].as_u64().unwrap();
    assert_eq!(all_versions, (0..=max_version).collect::<Vec<_>>());
    let response_count: usize = event_frames
        .iter()
        .map(|frame| frame.data["responses"].as_array().unwrap().len())
        .sum();
    assert_eq!(1, response_count);

    // Resuming after the last event only repeats the current status.
    let resumed = SseReader::open(
        &server,
        &format!("{execution_id}/events-and-responses?follow=true"),
        Some(&last.id),
    )
    .await
    .collect()
    .await;
    assert_eq!(1, resumed.len());
    assert!(resumed[0].data["events"].as_array().unwrap().is_empty());
    assert!(resumed[0].data["responses"].as_array().unwrap().is_empty());

    // `length` caps the whole stream.
    let capped = SseReader::open(
        &server,
        &format!("{execution_id}/events-and-responses?follow=true&events_length=2"),
        None,
    )
    .await
    .collect()
    .await;
    assert_eq!(versions(&capped), vec![0, 1]);

    let older = server
        .client
        .get(format!(
            "{}/v1/executions/{execution_id}/responses?follow=true&direction=older",
            server.base_url
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(older.status(), reqwest::StatusCode::BAD_REQUEST);

    server.shutdown().await;
}
