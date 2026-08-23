use crate::args;
use crate::args::CancelCommand;
use crate::args::FunctionFqnOrShort;
use crate::args::params::parse_params;
use crate::client::{ClientStartup, send_empty, send_json};
use crate::server::web_api_server::components::ComponentConfig;
use crate::server::web_api_server::logs::{LogEntryRowSer, LogEntrySer};
use crate::server::web_api_server::{
    AdvanceRequestSer, ExecutionEventsResponse, ExecutionResponsesResponse, ExecutionSubmitPayload,
    ExecutionUpgradePayload, ExecutionWithStateSer, PersistBacktracesResponseSer,
    ReplayResponseSer, format_execution_status_text,
};
use anyhow::Context as _;
use anyhow::bail;
use base64::Engine as _;
use concepts::ExecutionId;
use concepts::prefixed_ulid::ExecutionIdDerived;
use http::header::ACCEPT;
use serde::Deserialize;
use std::fmt::Write as _;
use std::time::Duration;
use tracing::instrument;

impl args::Execution {
    pub(crate) async fn run(self, client_startup: ClientStartup) -> Result<(), anyhow::Error> {
        match self {
            args::Execution::List {
                api_url,
                ffqn_prefix: ffqn,
                execution_id_prefix,
                show_derived,
                hide_finished,
                limit,
                json,
            } => {
                execution_list(
                    &client_startup,
                    &api_url,
                    ffqn,
                    execution_id_prefix,
                    show_derived,
                    hide_finished,
                    limit,
                    json,
                )
                .await
            }
            args::Execution::Logs {
                api_url,
                execution_id,
                show_derived,
                level,
                stream_type,
                show_run_id,
                after,
                follow,
                limit,
                json,
            } => {
                let opts =
                    LogsOpts::from_args(level, stream_type, show_derived, show_run_id, limit)?;
                if follow {
                    follow_logs(&client_startup, &api_url, &execution_id, &opts, after, json).await
                } else {
                    execution_logs_cmd(&client_startup, &api_url, execution_id, &opts, after, json)
                        .await
                }
            }
            args::Execution::Events {
                api_url,
                execution_id,
                from,
                limit,
                json,
            } => {
                execution_events_cmd(&client_startup, &api_url, execution_id, from, limit, json)
                    .await
            }
            args::Execution::Responses {
                api_url,
                execution_id,
                from,
                limit,
                json,
            } => {
                execution_responses_cmd(&client_startup, &api_url, execution_id, from, limit, json)
                    .await
            }
            args::Execution::Submit {
                api_url,
                execution_id,
                ffqn,
                params,
                follow,
                no_reconnect,
                paused,
                json,
            } => {
                let opts = if json {
                    SubmitOutputOpts::Json {
                        follow,
                        no_reconnect,
                    }
                } else {
                    SubmitOutputOpts::Plain {
                        follow,
                        no_reconnect,
                    }
                };
                submit(
                    &client_startup,
                    &api_url,
                    execution_id,
                    ffqn,
                    parse_params(params)?,
                    paused,
                    opts,
                )
                .await
            }
            args::Execution::Stub(args::Stub {
                api_url,
                execution_id,
                return_value,
            }) => stub(&client_startup, &api_url, execution_id, return_value).await,
            args::Execution::Status {
                api_url,
                execution_id,
                follow,
                no_reconnect,
                json,
            } => {
                get_execution_status_rest(
                    &client_startup,
                    &api_url,
                    execution_id,
                    follow,
                    no_reconnect,
                    json,
                )
                .await
            }
            args::Execution::Result {
                api_url,
                execution_id,
                follow,
                no_reconnect,
                json,
            } => {
                get_execution_result_rest(
                    &client_startup,
                    &api_url,
                    execution_id,
                    follow,
                    no_reconnect,
                    json,
                )
                .await
            }
            args::Execution::Cancel(cancel_request) => {
                cancel_request.execute(&client_startup).await
            }
            args::Execution::Pause { api_url, id } => {
                execution_pause_change(&client_startup, &api_url, id, "pause").await?;
                println!("Paused");
                Ok(())
            }
            args::Execution::Unpause { api_url, id } => {
                execution_pause_change(&client_startup, &api_url, id, "unpause").await?;
                println!("Unpaused");
                Ok(())
            }
            args::Execution::Replay {
                api_url,
                execution_id,
                json,
            } => replay(&client_startup, &api_url, execution_id, json).await,
            args::Execution::PersistBacktraces {
                api_url,
                execution_id,
            } => persist_backtraces(&client_startup, &api_url, execution_id).await,
            args::Execution::Advance {
                api_url,
                execution_id,
                json,
                trim,
                pause_all,
                pause_submitted_executions,
                pause_delays,
                force,
            } => {
                let pause_submitted_executions = pause_all || pause_submitted_executions;
                let pause_delays = pause_all || pause_delays;
                advance(
                    &client_startup,
                    &api_url,
                    execution_id,
                    AdvanceOpts {
                        json,
                        trim,
                        pause_submitted_executions,
                        pause_delays,
                        force,
                    },
                )
                .await
            }
            args::Execution::Upgrade {
                api_url,
                execution_id,
                skip_determinism_check,
            } => {
                upgrade(
                    &client_startup,
                    &api_url,
                    execution_id,
                    skip_determinism_check,
                )
                .await
            }
        }
    }
}

#[derive(PartialEq)]
pub(crate) enum SubmitOutputOpts {
    Plain { follow: bool, no_reconnect: bool },
    Json { follow: bool, no_reconnect: bool },
}

#[instrument(skip_all)]
pub(crate) async fn submit(
    client_startup: &ClientStartup,
    api_url: &str,
    execution_id: Option<ExecutionId>,
    ffqn: FunctionFqnOrShort,
    params: Vec<serde_json::Value>,
    paused: bool,
    opts: SubmitOutputOpts,
) -> anyhow::Result<()> {
    let client = client_startup.web_api_client()?;
    let ffqn = match ffqn {
        FunctionFqnOrShort::Short {
            ifc_name,
            function_name,
        } => {
            // Guess function
            let components: Vec<ComponentConfig> = send_json(
                client
                    .get(format!("{api_url}/v1/components"))
                    .header(ACCEPT, "application/json")
                    .query(&[("exports", "true"), ("extensions", "false")]),
            )
            .await?;
            let mut matched = Vec::new();
            for export in components
                .into_iter()
                .flat_map(|component| component.exports.unwrap_or_default())
            {
                if export.ffqn.function_name.as_ref() == function_name
                    && export.ffqn.ifc_fqn.ifc_name() == ifc_name
                {
                    matched.push(export.ffqn);
                }
            }
            let ffqn = match matched.as_slice() {
                [] => bail!("no matching function found"),
                [_] => matched.remove(0),
                _ => bail!("more than one matching function found: {matched:?}"),
            };
            if matches!(opts, SubmitOutputOpts::Plain { .. }) {
                println!("Matched {ffqn}");
            }
            ffqn
        }
        FunctionFqnOrShort::Ffqn(ffqn) => ffqn,
    };
    let execution_id = execution_id.unwrap_or_else(ExecutionId::generate);
    let (follow, no_reconnect, json) = match opts {
        SubmitOutputOpts::Plain {
            follow,
            no_reconnect,
        } => (follow, no_reconnect, false),
        SubmitOutputOpts::Json {
            follow,
            no_reconnect,
        } => (follow, no_reconnect, true),
    };
    let request_follow = follow && json;
    let url = format!("{api_url}/v1/executions/{execution_id}?follow={request_follow}");
    loop {
        let response = client
            .put(&url)
            .header(ACCEPT, "application/json")
            .json(&ExecutionSubmitPayload {
                ffqn: ffqn.clone(),
                params: params.clone(),
                paused,
            })
            .send()
            .await;
        match response {
            Ok(response) if response.status().is_success() => {
                if request_follow {
                    match response.json::<RetValWire>().await {
                        Ok(result) => return print_retval(&result, json),
                        Err(err) if !no_reconnect => {
                            eprintln!("failed to read response body: {err:#}. retrying...");
                        }
                        Err(err) => return Err(err).context("failed to parse execution result"),
                    }
                } else {
                    let response: ApiOk = response.json().await?;
                    if json {
                        print_json(&response)?;
                    } else {
                        println!("{}", response.ok);
                    }
                    return if follow {
                        get_execution_result_rest(
                            client_startup,
                            api_url,
                            execution_id,
                            true,
                            no_reconnect,
                            json,
                        )
                        .await
                    } else {
                        Ok(())
                    };
                }
            }
            Ok(response) => {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                bail!("server returned {status}: {body}");
            }
            Err(err) if !no_reconnect => {
                eprintln!("connection failed: {err:#}. retrying...");
            }
            Err(err) => return Err(err).context("failed to send execution request"),
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

#[derive(Debug, serde::Serialize, Deserialize)]
struct ApiOk {
    ok: String,
}

#[derive(Deserialize)]
struct ApiError {
    err: String,
}

#[derive(Debug, serde::Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum RetValWire {
    Ok(Option<serde_json::Value>),
    Err(Option<serde_json::Value>),
    ExecutionFailed(concepts::FinishedExecutionFailure),
}

pub(crate) async fn stub(
    client_startup: &ClientStartup,
    api_url: &str,
    execution_id: ExecutionIdDerived,
    return_value: String,
) -> anyhow::Result<()> {
    let return_value: serde_json::Value =
        serde_json::from_str(&return_value).context("return value must be valid JSON")?;
    let client = client_startup.web_api_client()?;
    send_empty(
        client
            .put(format!("{api_url}/v1/executions/{execution_id}/stub"))
            .header(ACCEPT, "application/json")
            .json(&return_value),
    )
    .await
}

#[derive(Debug, thiserror::Error)]
#[error("")]
struct AlreadyPrintedError;

async fn fetch_execution_status_json(
    client: &reqwest::Client,
    api_url: &str,
    execution_id: &ExecutionId,
) -> anyhow::Result<ExecutionWithStateSer> {
    send_json(
        client
            .get(format!("{api_url}/v1/executions/{execution_id}/status"))
            .header(ACCEPT, "application/json"),
    )
    .await
}

fn execution_status_is_finished(status: &ExecutionWithStateSer) -> bool {
    matches!(
        status.pending_state,
        concepts::storage::PendingState::Finished(_)
    )
}

fn print_json(value: &impl serde::Serialize) -> anyhow::Result<()> {
    println!(
        "{}",
        serde_json::to_string_pretty(value).context("failed to format JSON output")?
    );
    Ok(())
}

async fn fetch_execution_result_json(
    client: &reqwest::Client,
    api_url: &str,
    execution_id: &ExecutionId,
    follow: bool,
) -> anyhow::Result<RetValWire> {
    send_json(
        client
            .get(format!("{api_url}/v1/executions/{execution_id}"))
            .query(&[("follow", follow.to_string())])
            .header(ACCEPT, "application/json"),
    )
    .await
}

async fn get_execution_status_rest(
    client_startup: &ClientStartup,
    api_url: &str,
    execution_id: ExecutionId,
    follow: bool,
    no_reconnect: bool,
    json: bool,
) -> anyhow::Result<()> {
    let client = client_startup.web_api_client()?;
    let reconnect = !no_reconnect;
    let mut last_status = None;

    loop {
        let status = match fetch_execution_status_json(&client, api_url, &execution_id).await {
            Ok(status) => status,
            Err(err) => {
                if reconnect {
                    eprintln!("Got error while polling the status, reconnecting - {err}");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                }
                return Err(err);
            }
        };
        let rendered = serde_json::to_string(&status)?;
        if last_status.as_ref() != Some(&rendered) {
            if json {
                print_json(&status)?;
            } else {
                println!("{}", format_execution_status_text(&status.pending_state));
            }
        }

        if execution_status_is_finished(&status) {
            return Ok(());
        }

        if !follow {
            return Ok(());
        }

        last_status = Some(rendered);
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

async fn get_execution_result_rest(
    client_startup: &ClientStartup,
    api_url: &str,
    execution_id: ExecutionId,
    follow: bool,
    no_reconnect: bool,
    json: bool,
) -> anyhow::Result<()> {
    if follow && !json {
        get_execution_status_rest(
            client_startup,
            api_url,
            execution_id.clone(),
            true,
            no_reconnect,
            false,
        )
        .await?;
    }
    let client = client_startup.web_api_client()?;
    let reconnect = follow && !no_reconnect;
    let stream_result = follow && json;

    loop {
        match fetch_execution_result_json(&client, api_url, &execution_id, stream_result).await {
            Ok(result) => {
                return print_retval(&result, json);
            }
            Err(err) => {
                if reconnect {
                    eprintln!("Got error while polling the result, reconnecting - {err}");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                } else {
                    return Err(err);
                }
            }
        }
    }
}

fn print_retval(result: &RetValWire, json: bool) -> anyhow::Result<()> {
    if json {
        print_json(result)?;
    } else {
        match result {
            RetValWire::Ok(value) => println!(
                "Execution finished: OK: {}",
                value.as_ref().map_or_else(
                    || "(no return value)".to_string(),
                    |value| serde_json::to_string(value).expect("return value is serializable"),
                )
            ),
            RetValWire::Err(value) => {
                println!(
                    "Execution finished: Error: {}",
                    value.as_ref().map_or_else(
                        || "(no return value)".to_string(),
                        |value| serde_json::to_string(value).expect("return value is serializable"),
                    )
                );
                return Err(AlreadyPrintedError.into());
            }
            RetValWire::ExecutionFailed(failure) => {
                let mut message = format!("Execution failure ({})", failure.kind);
                if let Some(reason) = &failure.reason {
                    write!(&mut message, ": `{reason}`").expect("writing to string");
                }
                if let Some(detail) = &failure.detail {
                    write!(&mut message, "\n{detail}").expect("writing to string");
                }
                println!("Execution finished: {message}");
                return Err(AlreadyPrintedError.into());
            }
        }
    }
    Ok(())
}

async fn replay(
    client_startup: &ClientStartup,
    api_url: &str,
    execution_id: ExecutionId,
    json: bool,
) -> anyhow::Result<()> {
    let response = replay_json(client_startup, api_url, &execution_id).await?;
    if json {
        return print_json(&response);
    }
    match response {
        ReplayResponseSer::Advanceable { captured_writes } => {
            println!("outcome: advanceable, {} writes", captured_writes.len());
        }
        ReplayResponseSer::Finished { retval } => {
            println!("outcome: finished\nresult: {retval}");
        }
        ReplayResponseSer::Blocked => println!("outcome: blocked"),
        ReplayResponseSer::ReplayFailed {
            error,
            captured_writes,
        } => {
            println!(
                "outcome: replay_failed, error: {error}, {} writes",
                captured_writes.len()
            );
        }
    }
    Ok(())
}

async fn persist_backtraces(
    client_startup: &ClientStartup,
    api_url: &str,
    execution_id: ExecutionId,
) -> anyhow::Result<()> {
    let client = client_startup.web_api_client()?;
    let response: PersistBacktracesResponseSer = send_json(
        client
            .put(format!(
                "{api_url}/v1/executions/{execution_id}/backtrace/persist"
            ))
            .header(ACCEPT, "application/json"),
    )
    .await?;
    println!(
        "Persisted {} backtraces",
        response.persisted_backtrace_count
    );
    Ok(())
}

/// Fetch replay response as JSON. Accepts both 200 (success) and 422 (replay failed with body).
async fn replay_json(
    client_startup: &ClientStartup,
    api_url: &str,
    execution_id: &ExecutionId,
) -> anyhow::Result<ReplayResponseSer> {
    let client = client_startup.web_api_client()?;
    let resp = client
        .put(format!("{api_url}/v1/executions/{execution_id}/replay"))
        .header(ACCEPT, "application/json")
        .send()
        .await
        .context("failed to send replay request")?;
    let status = resp.status();
    if !status.is_success() && status != reqwest::StatusCode::CONFLICT {
        let body = resp.text().await.unwrap_or_default();
        bail!("server returned {status}: {body}");
    }
    resp.json()
        .await
        .context("failed to decode replay response as JSON")
}

fn pause_submitted_in_replay(advance_request: &mut AdvanceRequestSer) {
    for captured_write in &mut advance_request.captured_writes {
        if let crate::server::web_api_server::CapturedWriteSer::AppendBatchCreateNewExecution {
            child_requests,
            ..
        } = captured_write
        {
            for child_request in child_requests {
                child_request.paused = true;
            }
        }
    }
}

fn pause_delays_in_replay(advance_request: &mut AdvanceRequestSer) {
    fn pause_delays_in_events(events: &mut [concepts::storage::AppendRequest]) {
        for append in events {
            if let concepts::storage::ExecutionRequest::HistoryEvent {
                event:
                    concepts::storage::HistoryEvent::JoinSetRequest {
                        request: concepts::storage::JoinSetRequest::DelayRequest { paused, .. },
                        ..
                    },
            } = &mut append.event
            {
                *paused = true;
            }
        }
    }
    for captured_write in &mut advance_request.captured_writes {
        match captured_write {
            crate::server::web_api_server::CapturedWriteSer::Append { event, .. } => {
                pause_delays_in_events(std::slice::from_mut(event));
            }
            crate::server::web_api_server::CapturedWriteSer::AppendBatch { events, .. }
            | crate::server::web_api_server::CapturedWriteSer::AppendBatchCreateNewExecution {
                events,
                ..
            }
            | crate::server::web_api_server::CapturedWriteSer::AppendStubResponse {
                events, ..
            } => pause_delays_in_events(events),
            // An already-due `sleep(now)` is resolved in the same transaction; there is
            // no pending delay to pause.
            crate::server::web_api_server::CapturedWriteSer::AppendBatchWithDelayResponse {
                ..
            }
            | crate::server::web_api_server::CapturedWriteSer::AppendFinished { .. } => {}
        }
    }
}

fn trim_replay(advance_request: &mut AdvanceRequestSer, trim: usize) {
    advance_request.captured_writes.truncate(trim);
}

fn replay_to_advanceable_request(
    replay: ReplayResponseSer,
    force: bool,
) -> anyhow::Result<AdvanceRequestSer> {
    match replay {
        ReplayResponseSer::Advanceable { captured_writes } => Ok(AdvanceRequestSer {
            captured_writes,
            persist_backtrace: true,
        }),
        ReplayResponseSer::Finished { .. } => {
            bail!("execution is already finished")
        }
        ReplayResponseSer::Blocked => {
            bail!("execution is blocked")
        }
        ReplayResponseSer::ReplayFailed {
            error,
            captured_writes,
        } => {
            if force {
                eprintln!(
                    "Replay failed: {error}. Advancing with --force to persist execution failure."
                );
                Ok(AdvanceRequestSer {
                    captured_writes,
                    persist_backtrace: true,
                })
            } else {
                bail!(
                    "replay failed: {error}, {} captured writes available (use --force to advance with execution failure)",
                    captured_writes.len()
                )
            }
        }
    }
}

#[expect(clippy::struct_excessive_bools)]
struct AdvanceOpts {
    json: bool,
    trim: Option<usize>,
    pause_submitted_executions: bool,
    pause_delays: bool,
    /// Advance to an execution failure
    force: bool,
}

async fn advance(
    client_startup: &ClientStartup,
    api_url: &str,
    execution_id: ExecutionId,
    opts: AdvanceOpts,
) -> anyhow::Result<()> {
    let replay = replay_json(client_startup, api_url, &execution_id).await?;
    let mut advance_request = replay_to_advanceable_request(replay, opts.force)?;
    if let Some(trim) = opts.trim {
        trim_replay(&mut advance_request, trim);
    }
    if opts.pause_submitted_executions {
        pause_submitted_in_replay(&mut advance_request);
    }
    if opts.pause_delays {
        pause_delays_in_replay(&mut advance_request);
    }
    let client = client_startup.web_api_client()?;
    #[derive(Debug, serde::Serialize, Deserialize)]
    #[serde(tag = "type", rename_all = "snake_case")]
    enum AdvanceResponseWire {
        Finished {
            value: RetValWire,
        },
        InProgress {
            pending_state: concepts::storage::PendingState,
        },
    }
    let response: AdvanceResponseWire = send_json(
        client
            .put(format!("{api_url}/v1/executions/{execution_id}/advance"))
            .header(ACCEPT, "application/json")
            .json(&advance_request),
    )
    .await?;
    if opts.json {
        print_json(&response)
    } else {
        match response {
            AdvanceResponseWire::Finished { value } => {
                println!("success:\n{}", serde_json::to_string_pretty(&value)?);
                Ok(())
            }
            AdvanceResponseWire::InProgress { pending_state } => {
                println!("success, current state: {pending_state}");
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{pause_delays_in_replay, pause_submitted_in_replay};
    use crate::server::web_api_server::{AdvanceRequestSer, CapturedWriteSer};
    use chrono::{DateTime, Utc};
    use serde_json::json;

    #[test]
    fn pause_submitted_marks_only_new_child_requests_as_paused() {
        let current_time: DateTime<Utc> = "2026-01-01T00:00:00Z".parse().unwrap();
        let mut advance_request = serde_json::from_value::<AdvanceRequestSer>(json!({
            "captured_writes": [
                {
                    "type": "append_batch",
                    "execution_id": "Exec_01",
                    "version": 1,
                    "events": []
                },
                {
                    "type": "append_batch_create_new_execution",
                    "events": [],
                    "execution_id": "Exec_01",
                    "version": 2,
                    "child_requests": [
                        {
                            "execution_id": "Exec_02",
                            "ffqn": "pkg:ifc/fn",
                            "params": [],
                            "parent_execution_id": null,
                            "parent_join_set_id": null,
                            "scheduled_at": current_time,
                            "component_id": {
                                "component_type": "workflow",
                                "name": "wf",
                                "component_digest": "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
                            },
                            "deployment_id": "Dep_01",
                            "created_at": current_time,
                            "metadata": {},
                            "scheduled_by": null,
                            "paused": false
                        }
                    ],
                    "backtraces": []
                }
            ]
        }))
        .unwrap();
        pause_submitted_in_replay(&mut advance_request);

        match &advance_request.captured_writes[0] {
            CapturedWriteSer::AppendBatch { .. } => {}
            other => panic!("expected first write to remain append_batch, got {other:?}"),
        }

        match &advance_request.captured_writes[1] {
            CapturedWriteSer::AppendBatchCreateNewExecution { child_requests, .. } => {
                assert_eq!(child_requests.len(), 1);
                assert!(child_requests[0].paused);
            }
            other => panic!(
                "expected second write to remain append_batch_create_new_execution, got {other:?}"
            ),
        }
    }

    #[test]
    fn pause_delays_marks_only_delay_requests_as_paused() {
        let current_time: DateTime<Utc> = "2026-01-01T00:00:00Z".parse().unwrap();
        let execution_id = concepts::ExecutionId::generate();
        let delay_join_set_id = concepts::JoinSetId::new(
            concepts::JoinSetKind::Generated,
            concepts::StrVariant::Static("delay"),
        )
        .unwrap();
        let child_join_set_id = concepts::JoinSetId::new(
            concepts::JoinSetKind::Generated,
            concepts::StrVariant::Static("child"),
        )
        .unwrap();
        let delay_id = concepts::prefixed_ulid::DelayId::new(&execution_id, &delay_join_set_id);
        let child_execution_id = execution_id.next_level(&child_join_set_id);
        let mut advance_request = AdvanceRequestSer {
            persist_backtrace: true,
            captured_writes: vec![
                CapturedWriteSer::Append {
                    execution_id: execution_id.to_string(),
                    version: 1,
                    event: concepts::storage::AppendRequest {
                        created_at: current_time,
                        event: concepts::storage::ExecutionRequest::HistoryEvent {
                            event: concepts::storage::HistoryEvent::JoinSetRequest {
                                join_set_id: delay_join_set_id,
                                request: concepts::storage::JoinSetRequest::DelayRequest {
                                    delay_id,
                                    expires_at: current_time,
                                    schedule_at: concepts::storage::HistoryEventScheduleAt::Now,
                                    paused: false,
                                },
                            },
                        },
                    },
                    backtraces: vec![],
                },
                CapturedWriteSer::AppendBatch {
                    execution_id: execution_id.to_string(),
                    version: 2,
                    events: vec![concepts::storage::AppendRequest {
                        created_at: current_time,
                        event: concepts::storage::ExecutionRequest::HistoryEvent {
                            event: concepts::storage::HistoryEvent::JoinSetRequest {
                                join_set_id: child_join_set_id,
                                request: concepts::storage::JoinSetRequest::ChildExecutionRequest {
                                    child_execution_id,
                                    target_ffqn: "testing:fibo/fibo.fibo".parse().unwrap(),
                                    params: concepts::Params::empty(),
                                    result: Ok(()),
                                },
                            },
                        },
                    }],
                    backtraces: vec![],
                },
            ],
        };

        pause_delays_in_replay(&mut advance_request);

        match &advance_request.captured_writes[0] {
            CapturedWriteSer::Append { event, .. } => {
                let concepts::storage::ExecutionRequest::HistoryEvent {
                    event:
                        concepts::storage::HistoryEvent::JoinSetRequest {
                            request: concepts::storage::JoinSetRequest::DelayRequest { paused, .. },
                            ..
                        },
                } = &event.event
                else {
                    panic!("expected append history event with delay request");
                };
                assert!(*paused);
            }
            other => panic!("expected first write to remain append, got {other:?}"),
        }

        match &advance_request.captured_writes[1] {
            CapturedWriteSer::AppendBatch { events, .. } => {
                let concepts::storage::ExecutionRequest::HistoryEvent {
                    event:
                        concepts::storage::HistoryEvent::JoinSetRequest {
                            request: concepts::storage::JoinSetRequest::ChildExecutionRequest { .. },
                            ..
                        },
                } = &events[0].event
                else {
                    panic!("expected batch history event with child request");
                };
            }
            other => panic!("expected second write to remain append_batch, got {other:?}"),
        }
    }
}

#[expect(clippy::too_many_arguments)]
async fn execution_list(
    client_startup: &ClientStartup,
    api_url: &str,
    ffqn: Option<String>,
    execution_id_prefix: Option<String>,
    show_derived: bool,
    hide_finished: bool,
    limit: u16,
    json: bool,
) -> anyhow::Result<()> {
    let client = client_startup.web_api_client()?;
    let mut req = client
        .get(format!("{api_url}/v1/executions"))
        .header(ACCEPT, "application/json")
        .query(&[("length", limit.to_string())]);
    if let Some(ffqn) = ffqn {
        req = req.query(&[("ffqn_prefix", ffqn)]);
    }
    if let Some(execution_id_prefix) = execution_id_prefix {
        req = req.query(&[("execution_id_prefix", execution_id_prefix)]);
    }
    if show_derived {
        req = req.query(&[("show_derived", "true")]);
    }
    if hide_finished {
        req = req.query(&[("hide_finished", "true")]);
    }
    let executions: Vec<ExecutionWithStateSer> = send_json(req).await?;
    if json {
        print_json(&executions)
    } else {
        for execution in executions {
            println!(
                "{} `{}` {} `{}`",
                execution.execution_id,
                execution.pending_state,
                execution.ffqn,
                execution.first_scheduled_at,
            );
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
enum StreamType {
    Stdout,
    Stderr,
}

impl StreamType {
    fn as_str(self) -> &'static str {
        match self {
            Self::Stdout => "stdout",
            Self::Stderr => "stderr",
        }
    }
}

/// Resolved log filter parameters shared between the one-shot and follow paths.
/// `json` is intentionally excluded — it controls output format, not the query filter.
struct LogsOpts {
    /// Empty slice means `show_logs=false` (level was `off`).
    levels: &'static [&'static str],
    /// Empty means `show_streams=false` (`--stream-type none`).
    /// Defaults to `[Stdout, Stderr]` when no `--stream-type` is given.
    stream_types: Vec<StreamType>,
    show_derived: bool,
    show_run_id: bool,
    limit: u16,
}

impl LogsOpts {
    fn from_args(
        level: args::LogLevelArg,
        stream_type_arg: Option<args::LogStreamTypeArg>,
        show_derived: bool,
        show_run_id: bool,
        limit: u16,
    ) -> anyhow::Result<Self> {
        use args::LogLevelArg;
        let levels: &'static [&'static str] = match level {
            LogLevelArg::Off => &[],
            LogLevelArg::Trace => &["trace", "debug", "info", "warn", "error"],
            LogLevelArg::Debug => &["debug", "info", "warn", "error"],
            LogLevelArg::Info => &["info", "warn", "error"],
            LogLevelArg::Warn => &["warn", "error"],
            LogLevelArg::Error => &["error"],
        };
        let stream_types = match stream_type_arg {
            None => vec![StreamType::Stdout, StreamType::Stderr],
            Some(args::LogStreamTypeArg::Stdout) => vec![StreamType::Stdout],
            Some(args::LogStreamTypeArg::Stderr) => vec![StreamType::Stderr],
            Some(args::LogStreamTypeArg::None) => vec![],
        };
        if levels.is_empty() && stream_types.is_empty() {
            anyhow::bail!(
                "either `--level` must not be `off`, or `--stream-type` must not be `none`"
            );
        }
        Ok(Self {
            levels,
            stream_types,
            show_derived,
            show_run_id,
            limit,
        })
    }

    fn apply_to_request(&self, mut req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        let show_logs = !self.levels.is_empty();
        req = req.query(&[("show_logs", if show_logs { "true" } else { "false" })]);
        if show_logs {
            for level_str in self.levels {
                req = req.query(&[("level", *level_str)]);
            }
        }
        let show_streams = !self.stream_types.is_empty();
        req = req.query(&[("show_streams", if show_streams { "true" } else { "false" })]);
        for st in &self.stream_types {
            req = req.query(&[("stream_type", st.as_str())]);
        }
        if self.show_derived {
            req = req.query(&[("show_derived", "true")]);
        }
        if self.show_run_id {
            req = req.query(&[("show_run_id", "true")]);
        }
        req
    }
}

/// Parse a JSON log response into items, print them (as JSONL when `json` is true,
/// as human-readable text otherwise), and return the cursor of the last item.
fn print_log_items(
    items: &[LogEntryRowSer],
    json: bool,
    show_run_id: bool,
    show_derived: bool,
) -> anyhow::Result<Option<String>> {
    if items.is_empty() {
        return Ok(None);
    }
    if json {
        for item in items {
            println!(
                "{}",
                serde_json::to_string(item).context("failed to serialize log item")?
            );
        }
    } else {
        let mut output = String::new();
        for item in items {
            let mut prefix = String::new();
            if show_run_id {
                write!(&mut prefix, "{} ", item.run_id).expect("writing to string");
            }
            if show_derived {
                write!(&mut prefix, "{} ", item.execution_id).expect("writing to string");
            }
            match &item.info {
                LogEntrySer::Log {
                    created_at,
                    level,
                    message,
                } => {
                    writeln!(&mut output, "{created_at} [{level:<6}] {prefix}{message}")
                        .expect("writing to string");
                }
                LogEntrySer::Stream {
                    created_at,
                    payload,
                    stream_type,
                } => {
                    let payload_bytes = base64::engine::general_purpose::STANDARD
                        .decode(payload)
                        .unwrap_or_default();
                    let payload_utf8 = String::from_utf8_lossy(&payload_bytes);
                    writeln!(
                        &mut output,
                        "{created_at} [{stream_type:<6}] {prefix}{payload_utf8}",
                    )
                    .expect("writing to string");
                }
            }
        }
        print!("{output}");
    }
    let cursor = items.last().map(|item| item.cursor.clone());
    Ok(cursor)
}

async fn fetch_logs(
    client: &reqwest::Client,
    logs_url: &str,
    opts: &LogsOpts,
    cursor: Option<&str>,
    after: Option<&str>,
    direction: &str,
) -> anyhow::Result<Vec<LogEntryRowSer>> {
    let mut req = client
        .get(logs_url)
        .header(ACCEPT, "application/json")
        .query(&[
            ("length", opts.limit.to_string()),
            ("direction", direction.into()),
        ]);
    req = opts.apply_to_request(req);
    if let Some(c) = cursor {
        req = req
            .query(&[("cursor", c)])
            .query(&[("including_cursor", "false")]);
    }
    if let Some(after) = after {
        req = req.query(&[("after", after)]);
    }
    send_json(req).await
}

async fn execution_logs_cmd(
    client_startup: &ClientStartup,
    api_url: &str,
    execution_id: ExecutionId,
    opts: &LogsOpts,
    after: Option<String>,
    json: bool,
) -> anyhow::Result<()> {
    let client = client_startup.web_api_client()?;
    let logs_url = format!("{api_url}/v1/executions/{execution_id}/logs");
    let items = fetch_logs(&client, &logs_url, opts, None, after.as_deref(), "newer").await?;
    print_log_items(&items, json, opts.show_run_id, opts.show_derived)?;
    Ok(())
}

async fn follow_logs(
    client_startup: &ClientStartup,
    api_url: &str,
    execution_id: &ExecutionId,
    opts: &LogsOpts,
    initial_after: Option<String>,
    json: bool,
) -> anyhow::Result<()> {
    let client = client_startup.web_api_client()?;
    let logs_url = format!("{api_url}/v1/executions/{execution_id}/logs");
    let status_url = format!("{api_url}/v1/executions/{execution_id}/status");
    let mut cursor: Option<String> = None;

    loop {
        let items = fetch_logs(
            &client,
            &logs_url,
            opts,
            cursor.as_deref(),
            initial_after.as_deref(),
            "newer",
        )
        .await?;
        let new_cursor = print_log_items(&items, json, opts.show_run_id, opts.show_derived)?;
        let has_items = new_cursor.is_some();
        if let Some(c) = new_cursor {
            cursor = Some(c);
        }

        // Check if the execution has finished.
        let finished = {
            let status: ExecutionWithStateSer =
                send_json(client.get(&status_url).header(ACCEPT, "application/json")).await?;
            execution_status_is_finished(&status)
        };

        if finished && !has_items {
            break;
        }

        if !has_items {
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    Ok(())
}

async fn execution_events_cmd(
    client_startup: &ClientStartup,
    api_url: &str,
    execution_id: ExecutionId,
    from: Option<u32>,
    limit: u16,
    json: bool,
) -> anyhow::Result<()> {
    let client = client_startup.web_api_client()?;
    let mut req = client
        .get(format!("{api_url}/v1/executions/{execution_id}/events"))
        .header(ACCEPT, "application/json")
        .query(&[("length", limit.to_string())]);
    if let Some(from) = from {
        req = req
            .query(&[("version", from.to_string())])
            .query(&[("including_cursor", "true")]);
    } else {
        // Without an explicit cursor, fetch the newest events from the latest version.
        req = req.query(&[("direction", "older")]);
    }
    let response: ExecutionEventsResponse = send_json(req).await?;
    if json {
        print_json(&response)
    } else {
        for event in response.events {
            println!("{} `{}` {}", event.version, event.created_at, event.event);
        }
        Ok(())
    }
}

async fn execution_responses_cmd(
    client_startup: &ClientStartup,
    api_url: &str,
    execution_id: ExecutionId,
    from: Option<u32>,
    limit: u16,
    json: bool,
) -> anyhow::Result<()> {
    let client = client_startup.web_api_client()?;
    let mut req = client
        .get(format!("{api_url}/v1/executions/{execution_id}/responses"))
        .header(ACCEPT, "application/json")
        .query(&[("length", limit.to_string())]);
    if let Some(from) = from {
        req = req
            .query(&[("cursor", from.to_string())])
            .query(&[("including_cursor", "true")]);
    } else {
        // Without an explicit cursor, fetch the newest responses from the latest cursor.
        req = req.query(&[("direction", "older")]);
    }
    let response: ExecutionResponsesResponse = send_json(req).await?;
    if json {
        print_json(&response)
    } else {
        for response in response.responses {
            println!(
                "{} `{}` {} {}",
                response.cursor,
                response.event.created_at,
                response.event.event.join_set_id,
                response.event.event.event,
            );
        }
        Ok(())
    }
}

impl CancelCommand {
    #[instrument(skip_all)]
    pub(crate) async fn execute(self, client_startup: &ClientStartup) -> anyhow::Result<()> {
        let client = client_startup.web_api_client()?;
        let url = match self.id {
            args::ExecutionIdOrDelayId::Execution(id) => {
                format!("{}/v1/executions/{id}/cancel", self.api_url)
            }
            args::ExecutionIdOrDelayId::Delay(id) => {
                format!("{}/v1/delays/{id}/cancel", self.api_url)
            }
        };
        let response = client
            .put(url)
            .header(ACCEPT, "application/json")
            .send()
            .await?;
        let status = response.status();
        if status.is_success() {
            let body: ApiOk = response.json().await?;
            println!("{}", capitalize(&body.ok));
            return Ok(());
        }
        let body: ApiError = response.json().await?;
        if status == reqwest::StatusCode::CONFLICT {
            println!("{}", capitalize(&body.err));
            return Ok(());
        }
        bail!("server returned {status}: {}", body.err)
    }
}

async fn execution_pause_change(
    client_startup: &ClientStartup,
    api_url: &str,
    id: args::ExecutionIdOrDelayId,
    action: &str,
) -> anyhow::Result<()> {
    let client = client_startup.web_api_client()?;
    let url = match id {
        args::ExecutionIdOrDelayId::Execution(id) => {
            format!("{api_url}/v1/executions/{id}/{action}")
        }
        args::ExecutionIdOrDelayId::Delay(id) => {
            format!("{api_url}/v1/delays/{id}/{action}")
        }
    };
    send_empty(client.put(url).header(ACCEPT, "application/json")).await
}

fn capitalize(value: &str) -> String {
    let mut chars = value.chars();
    chars.next().map_or_else(String::new, |first| {
        first.to_uppercase().collect::<String>() + chars.as_str()
    })
}

async fn upgrade(
    client_startup: &ClientStartup,
    api_url: &str,
    execution_id: ExecutionId,
    skip_determinism_check: bool,
) -> anyhow::Result<()> {
    let client = client_startup.web_api_client()?;
    let summary = fetch_execution_status_json(&client, api_url, &execution_id).await?;
    let ffqn = summary.ffqn;
    let old_digest = summary.component_digest;

    // Step 2: find the component that currently exports this ffqn.
    let components: Vec<ComponentConfig> = send_json(
        client
            .get(format!("{api_url}/v1/components"))
            .header(ACCEPT, "application/json")
            .query(&[("ffqn", ffqn.to_string())]),
    )
    .await
    .context("failed to list components")?;

    let new_digest = match components.as_slice() {
        [] => bail!("no component in the active deployment exports `{ffqn}`"),
        [component] => component.component_id.component_digest.clone(),
        _ => bail!(
            "multiple components export `{ffqn}`: {:?}",
            components
                .iter()
                .map(|c| &c.component_id.name)
                .collect::<Vec<_>>()
        ),
    };

    if old_digest == new_digest {
        println!("Already up to date ({old_digest})");
        return Ok(());
    }

    println!("Upgrading from {old_digest} to {new_digest}");

    send_empty(
        client
            .put(format!("{api_url}/v1/executions/{execution_id}/upgrade"))
            .header(ACCEPT, "application/json")
            .json(&ExecutionUpgradePayload {
                old: old_digest,
                new: new_digest,
                skip_determinism_check,
            }),
    )
    .await
    .context("upgrade failed")?;

    println!("Upgraded");
    Ok(())
}
