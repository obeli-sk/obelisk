//! `follow=true` SSE streams of `/responses`, `/logs`, `/events-and-responses` and the
//! status batch. The workflow blocks on a stub the test resolves while the streams are open.

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

async fn wait_finished(server: &TestServer, execution_id: &str) -> Value {
    tokio::time::timeout(STREAM_TIMEOUT, async {
        loop {
            let status = server.get_status(execution_id).await;
            if status["pending_state"]["status"] == "finished" {
                break status;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn api_stream_limit_closes_silent_streams_and_preserves_cursors() {
    let server = TestServer::start_inline_deployment(
        test_addr!(40_161),
        "[api.max_stream_duration]\nmilliseconds = 200\n",
        DEPLOYMENT,
        &[],
    )
    .await;
    let execution_id = submit(&server).await;
    tokio::time::timeout(STREAM_TIMEOUT, async {
        loop {
            let status = server.get_status(&execution_id).await;
            let logs = server.get_logs(&execution_id, 200).await;
            if status["pending_state"]["status"] == "blocked_by_join_set"
                && logs
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|row| row["message"] == "submitted")
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let mut statuses =
        SseReader::open(&server, &format!("{execution_id}/status?follow=true"), None).await;
    let initial = statuses.next().await.unwrap();
    assert!(statuses.next().await.is_none());
    let resumed = SseReader::open(
        &server,
        &format!("{execution_id}/status?follow=true"),
        initial.id.as_deref(),
    )
    .await
    .collect()
    .await;
    assert!(
        resumed.is_empty(),
        "unchanged status must not be repeated after expiry"
    );
    for path in [
        format!("{execution_id}?follow=true"),
        format!("{execution_id}/responses?follow=true"),
    ] {
        assert!(
            SseReader::open(&server, &path, None)
                .await
                .collect()
                .await
                .is_empty()
        );
    }
    let events = SseReader::open(
        &server,
        &format!("{execution_id}/events-and-responses?follow=true"),
        None,
    )
    .await
    .collect()
    .await;
    assert!(!events.is_empty());
    let logs = SseReader::open(&server, &format!("{execution_id}/logs?follow=true"), None)
        .await
        .collect()
        .await;
    assert_eq!(
        logs.iter()
            .flat_map(|frame| frame.data.as_array().unwrap())
            .filter(|row| row["message"] == "submitted")
            .count(),
        1
    );
    let batch = SseReader::send(
        server
            .client
            .post(format!(
                "{}/v1/executions/status/batch?follow=true",
                server.base_url
            ))
            .json(&json!({"execution_ids": [execution_id]})),
    )
    .await
    .collect()
    .await;
    assert_eq!(batch.len(), 1);
    let last_id = events.last().unwrap().id.as_deref();
    stub_child(&server, &execution_id).await;
    wait_finished(&server, &execution_id).await;
    let resumed = SseReader::open(
        &server,
        &format!("{execution_id}/events-and-responses?follow=true"),
        last_id,
    )
    .await
    .collect()
    .await;
    let mut all = events;
    all.extend(resumed);
    let actual = versions(&all);
    assert_eq!(
        actual,
        (0..=all.last().unwrap().data["max_version"].as_u64().unwrap()).collect::<Vec<_>>()
    );
    server.shutdown().await;
}

#[tokio::test]
async fn cli_follow_reconnects_after_clean_eof_with_last_event_id() {
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{header, method},
    };
    let server = TestServer::start_inline_deployment(test_addr!(40_162), "", DEPLOYMENT, &[]).await;
    let execution_id = submit(&server).await;
    let pending = server.get_status(&execution_id).await;
    stub_child(&server, &execution_id).await;
    let finished = wait_finished(&server, &execution_id).await;
    let sink = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "text/event-stream")
                .set_body_string(format!("event: status\nid: pending\ndata: {pending}\n\n")),
        )
        .up_to_n_times(1)
        .with_priority(1)
        .expect(1)
        .mount(&sink)
        .await;
    Mock::given(header("last-event-id", "pending"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "text/event-stream")
                .set_body_string(format!("event: status\nid: finished\ndata: {finished}\n\n")),
        )
        .expect(1)
        .mount(&sink)
        .await;
    tokio::time::timeout(
        STREAM_TIMEOUT,
        crate::args::Execution::Status {
            execution_id: execution_id.parse().unwrap(),
            follow: true,
            no_reconnect: false,
            json: true,
        }
        .run(crate::client::ClientStartup::new(sink.uri(), None, false)),
    )
    .await
    .unwrap()
    .unwrap();
    sink.verify().await;
    server.shutdown().await;
}

struct SseFrame {
    event: String,
    id: Option<String>,
    data: Value,
}

struct SseReader {
    response: reqwest::Response,
    buffer: Vec<u8>,
}

impl SseReader {
    async fn open(server: &TestServer, path_and_query: &str, last_event_id: Option<&str>) -> Self {
        let mut request = server.client.get(format!(
            "{}/v1/executions/{path_and_query}",
            server.base_url
        ));
        if let Some(id) = last_event_id {
            request = request.header("Last-Event-ID", id);
        }
        Self::send(request).await
    }

    async fn send(request: reqwest::RequestBuilder) -> Self {
        let response = request
            .header("Accept", "text/event-stream")
            .send()
            .await
            .expect("follow request failed");
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
                            id: field("id: "),
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

async fn submit(server: &TestServer) -> String {
    let execution_id = server.generate_execution_id().await;
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
    execution_id
}

/// Resolve the workflow's child, which is written once the workflow blocks on `joinNext`.
async fn stub_child(server: &TestServer, execution_id: &str) {
    let join_set_id = JoinSetId::new(JoinSetKind::Named, StrVariant::from("stub-set")).unwrap();
    let child_id = ExecutionId::Derived(
        execution_id
            .parse::<ExecutionId>()
            .unwrap()
            .next_level(&join_set_id),
    );
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
        assert!(
            tokio::time::Instant::now() < deadline,
            "child was never created"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
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
    let server = TestServer::start_inline_deployment(test_addr!(40_159), "", DEPLOYMENT, &[]).await;
    let execution_id = submit(&server).await;

    let mut events = SseReader::open(
        &server,
        &format!("{execution_id}/events-and-responses?follow=true"),
        None,
    )
    .await;
    let responses = SseReader::open(
        &server,
        &format!("{execution_id}/responses?follow=true"),
        None,
    )
    .await;
    let mut logs =
        SseReader::open(&server, &format!("{execution_id}/logs?follow=true"), None).await;

    // The first page is sent right away even without new rows.
    let first = events.next().await.unwrap();
    assert_eq!(first.event, "events_and_responses");
    assert_ne!(
        first.data["current_status"]["pending_state"]["status"],
        "finished"
    );
    let first_log = logs.next().await.unwrap();
    assert_eq!(first_log.event, "logs");
    assert_eq!(first_log.data[0]["message"], "submitted");

    stub_child(&server, &execution_id).await;

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
    assert_eq!(
        last.data["current_status"]["pending_state"]["status"],
        "finished"
    );
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
        Some(last.id.as_deref().expect("event id")),
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

#[tokio::test]
async fn status_batch_follows_until_all_finish() {
    let server = TestServer::start_inline_deployment(test_addr!(40_160), "", DEPLOYMENT, &[]).await;
    let first_id = submit(&server).await;
    let second_id = submit(&server).await;
    let missing_id = ExecutionId::generate().to_string();
    let batch_url = format!("{}/v1/executions/status/batch", server.base_url);
    let mut stream = SseReader::send(
        server
            .client
            .post(format!("{batch_url}?follow=true"))
            .json(&json!({ "execution_ids": [first_id, missing_id, second_id, first_id] })),
    )
    .await;

    // The first event lists every existing execution once.
    let initial = stream.next().await.unwrap();
    assert_eq!(initial.event, "statuses");
    assert!(initial.id.is_none());
    let ids: Vec<_> = initial
        .data
        .as_array()
        .unwrap()
        .iter()
        .map(|item| item["execution_id"].clone())
        .collect();
    assert_eq!(ids, vec![json!(first_id), json!(second_id)]);

    stub_child(&server, &first_id).await;
    stub_child(&server, &second_id).await;

    // Each later event only carries changes, and a finished execution is not sent again.
    let mut finished = Vec::new();
    let mut previous: std::collections::HashMap<String, Value> = std::collections::HashMap::new();
    for item in std::iter::once(initial)
        .chain(stream.collect().await)
        .flat_map(|frame| frame.data.as_array().unwrap().clone())
    {
        let execution_id = item["execution_id"].as_str().unwrap().to_string();
        assert!(
            !finished.contains(&execution_id),
            "{execution_id} sent after finishing"
        );
        assert!(item["max_version"].is_u64());
        assert_ne!(
            previous.get(&execution_id),
            Some(&item),
            "unchanged status was sent"
        );
        if item["pending_state"]["status"] == "finished" {
            finished.push(execution_id.clone());
        }
        previous.insert(execution_id, item);
    }
    finished.sort();
    let mut expected = vec![first_id.clone(), second_id.clone()];
    expected.sort();
    assert_eq!(finished, expected);

    let statuses: Value = server
        .client
        .post(&batch_url)
        .json(&json!({ "execution_ids": [missing_id, first_id] }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(statuses.as_array().unwrap().len(), 1);
    assert_eq!(statuses[0]["pending_state"]["status"], "finished");

    // A batch of only missing executions gets one empty event and ends.
    let frames = SseReader::send(
        server
            .client
            .post(format!("{batch_url}?follow=true"))
            .json(&json!({ "execution_ids": [missing_id] })),
    )
    .await
    .collect()
    .await;
    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].data, json!([]));

    let empty = server
        .client
        .post(&batch_url)
        .json(&json!({ "execution_ids": [] }))
        .send()
        .await
        .unwrap();
    assert_eq!(empty.status(), reqwest::StatusCode::BAD_REQUEST);

    server.shutdown().await;
}
