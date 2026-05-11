use crate::ExecutionRepositoryClient;
use crate::args;
use crate::args::CancelCommand;
use crate::args::FunctionFqnOrShort;
use crate::args::params::parse_params;
use crate::get_execution_repository_client;
use crate::get_fn_repository_client;
use crate::server::web_api_server::ExecutionSubmitPayload;
use anyhow::Context as _;
use anyhow::bail;
use base64::Engine as _;
use chrono::DateTime;
use concepts::ExecutionFailureKind;
use concepts::JoinSetId;
use concepts::JoinSetKind;
use concepts::prefixed_ulid::DelayId;
use concepts::prefixed_ulid::ExecutionIdDerived;
use concepts::{ExecutionId, FunctionFqn};
use grpc::grpc_gen;
use grpc::grpc_gen::CancelRequest;
use grpc::grpc_gen::cancel_request;
use grpc::grpc_gen::cancel_request::CancelRequestActivity;
use grpc::grpc_gen::cancel_request::CancelRequestDelay;
use grpc::grpc_gen::cancel_response::CancelOutcome;
use grpc::grpc_gen::execution_status::BlockedByJoinSet;
use grpc::grpc_gen::execution_status::Finished;
use grpc::to_channel;
use grpc_gen::execution_status::Status;
use http::header::ACCEPT;
use itertools::Either;
use std::fmt::Write as _;
use std::str::FromStr;
use std::time::Duration;
use tracing::instrument;

impl args::Execution {
    pub(crate) async fn run(self) -> Result<(), anyhow::Error> {
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
                    follow_logs(&api_url, &execution_id, &opts, after, json).await
                } else {
                    execution_logs_cmd(&api_url, execution_id, &opts, after, json).await
                }
            }
            args::Execution::Events {
                api_url,
                execution_id,
                from,
                limit,
                json,
            } => execution_events_cmd(&api_url, execution_id, from, limit, json).await,
            args::Execution::Responses {
                api_url,
                execution_id,
                from,
                limit,
                json,
            } => execution_responses_cmd(&api_url, execution_id, from, limit, json).await,
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
            }) => {
                let channel = to_channel(&api_url).await?;
                let client = get_execution_repository_client(channel).await?;
                stub(client, execution_id, return_value).await
            }
            args::Execution::Get {
                api_url,
                execution_id,
                follow,
                no_reconnect,
            } => {
                let channel = to_channel(&api_url).await?;
                let client = get_execution_repository_client(channel).await?;
                let opts = GetStatusOptions {
                    follow,
                    no_reconnect,
                };
                get_status(client, execution_id, opts).await
            }
            args::Execution::Cancel(cancel_request) => cancel_request.execute().await,
            args::Execution::Pause {
                api_url,
                execution_id,
            } => {
                let channel = to_channel(&api_url).await?;
                let mut client = get_execution_repository_client(channel).await?;
                client
                    .pause_execution(tonic::Request::new(grpc_gen::PauseExecutionRequest {
                        execution_id: Some(grpc_gen::ExecutionId::from(execution_id)),
                    }))
                    .await?;
                println!("Paused");
                Ok(())
            }
            args::Execution::Unpause {
                api_url,
                execution_id,
            } => {
                let channel = to_channel(&api_url).await?;
                let mut client = get_execution_repository_client(channel).await?;
                client
                    .unpause_execution(tonic::Request::new(grpc_gen::UnpauseExecutionRequest {
                        execution_id: Some(grpc_gen::ExecutionId::from(execution_id)),
                    }))
                    .await?;
                println!("Unpaused");
                Ok(())
            }
            args::Execution::Replay {
                api_url,
                execution_id,
                json,
            } => replay(&api_url, execution_id, json).await,
            args::Execution::Advance {
                api_url,
                execution_id,
                json,
                trim,
                pause_submitted,
            } => advance(&api_url, execution_id, json, trim, pause_submitted).await,
            args::Execution::Upgrade {
                api_url,
                execution_id,
                skip_determinism_check,
            } => upgrade(&api_url, execution_id, skip_determinism_check).await,
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
    api_url: &str,
    execution_id: Option<ExecutionId>,
    ffqn: FunctionFqnOrShort,
    params: Vec<serde_json::Value>,
    paused: bool,
    opts: SubmitOutputOpts,
) -> anyhow::Result<()> {
    let channel = to_channel(api_url).await?;
    let mut client = get_execution_repository_client(channel.clone()).await?;
    let mut component_client = get_fn_repository_client(channel).await?;
    let ffqn = match ffqn {
        FunctionFqnOrShort::Short {
            ifc_name,
            function_name,
        } => {
            // Guess function
            let components = component_client
                .list_components(tonic::Request::new(grpc_gen::ListComponentsRequest {
                    function_name: None,
                    component_digest: None,
                    extensions: false,
                }))
                .await?
                .into_inner()
                .components;
            let mut matched = Vec::new();
            for export in components
                .into_iter()
                .flat_map(|component| component.exports)
                .map(|detail| detail.function_name.expect("function_name is sent"))
            {
                if export.function_name == function_name.as_ref() {
                    let ffqn =
                        FunctionFqn::try_from(export).expect("sent FunctionName must be parseable");
                    if ffqn.ifc_fqn.ifc_name() == ifc_name {
                        matched.push(ffqn);
                    }
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
    match opts {
        SubmitOutputOpts::Plain {
            follow,
            no_reconnect,
        } => {
            client
                .submit(tonic::Request::new(grpc_gen::SubmitRequest {
                    execution_id: Some(execution_id.clone().into()),
                    params: Some(prost_wkt_types::Any {
                        type_url: format!("urn:obelisk:json:params:{ffqn}"),
                        value: serde_json::Value::Array(params).to_string().into_bytes(),
                    }),
                    function_name: Some(grpc_gen::FunctionName::from(ffqn)),
                    paused,
                }))
                .await?;
            println!("{execution_id}");
            if follow {
                let opts = GetStatusOptions {
                    follow: true,
                    no_reconnect,
                };
                get_status(client, execution_id, opts).await?;
            }
        }
        SubmitOutputOpts::Json {
            follow,
            no_reconnect,
        } => {
            let client = reqwest::Client::builder()
                .build()
                .context("failed to build HTTP client")?;

            let url = format!("{api_url}/v1/executions/{execution_id}?follow={follow}");

            loop {
                let request = client.put(&url).header(ACCEPT, "application/json").json(
                    &ExecutionSubmitPayload {
                        ffqn: ffqn.clone(),
                        params: params.clone(),
                        paused,
                    },
                );
                match request.send().await {
                    Ok(resp) => {
                        let status = resp.status();
                        if status.is_success() {
                            // Execution was submitted. If following, response will be streamed later.
                            // Connection drop here means we can retry.
                            match resp.json::<serde_json::Value>().await {
                                Ok(resp_json) => {
                                    let output = serde_json::to_string_pretty(&resp_json)
                                        .context("failed to format JSON output")?;
                                    println!("{output}");
                                    break; // Success!
                                }
                                Err(e) => {
                                    // If we can't read the body, the server probably died mid-response
                                    if no_reconnect {
                                        return Err(e).context("failed to parse JSON response");
                                    }
                                    eprintln!("failed to read response body: {e:#}. retrying...");
                                    // Fall through to sleep & retry
                                }
                            }
                        } else {
                            // Handle 4xx/5xx errors. No retry here as 5xx might indicate a serious problem,
                            // for example in database connection.
                            // We try to read the error text, but that might also fail if connection dropped.
                            match resp.text().await {
                                Ok(error_body) => {
                                    return Err(anyhow::anyhow!(
                                        "server returned status {status}: {error_body}"
                                    ));
                                }
                                Err(e) => {
                                    return Err(e).context("failed to read error body");
                                }
                            }
                        }
                    }
                    Err(e) => {
                        // Handle connection refused / timeout
                        if no_reconnect {
                            return Err(e).context("failed to send execution request");
                        }
                        eprintln!("connection failed: {e:#}. retrying...");
                    }
                }
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        }
    }
    Ok(())
}

pub(crate) async fn stub(
    mut client: ExecutionRepositoryClient,
    execution_id: ExecutionIdDerived,
    return_value: String,
) -> anyhow::Result<()> {
    let execution_id = ExecutionId::Derived(execution_id);

    client
        .stub(tonic::Request::new(grpc_gen::StubRequest {
            execution_id: Some(execution_id.clone().into()),
            return_value: Some(prost_wkt_types::Any {
                type_url: "urn:obelisk:json:retval:TBD".to_string(),
                value: return_value.into_bytes(),
            }),
        }))
        .await?
        .into_inner();
    Ok(())
}

/// Return true if the status is Finished.
fn print_status(response: grpc_gen::GetStatusResponse) -> Result<bool, AlreadyPrintedError> {
    use grpc_gen::get_status_response::Message;
    let message = response.message.expect("message expected");

    let status_or_finished = match message {
        Message::Summary(summary) => Either::Left(summary.current_status.expect("sent by server")),
        Message::CurrentStatus(status) => Either::Left(status),
        Message::FinishedStatus(finished) => Either::Right(finished),
    };
    match status_or_finished {
        Either::Left(status) => {
            println!("{}", format_pending_status(status));
            Ok(false)
        }
        Either::Right(finished) => {
            print_finished_status(finished)?;
            Ok(true)
        }
    }
}

fn format_pending_status(pending_status: grpc_gen::ExecutionStatus) -> String {
    let status = pending_status.status.expect("status is sent by the server");
    match status {
        Status::Locked(_) => "Locked".to_string(),
        Status::PendingAt(grpc_gen::execution_status::PendingAt { scheduled_at }) => {
            let scheduled_at = scheduled_at.expect("sent by the server");
            format!("Pending at {scheduled_at}")
        }
        Status::BlockedByJoinSet(BlockedByJoinSet {
            closing,
            join_set_id: Some(grpc_gen::JoinSetId { name, kind }),
            lock_expires_at: _,
        }) => {
            let kind = grpc_gen::join_set_id::JoinSetKind::try_from(kind)
                .map_err(|_| ())
                .and_then(JoinSetKind::try_from)
                .expect("JoinSetKind must be valid");
            let join_set_id =
                JoinSetId::new(kind, name.into()).expect("server sends valid join sets");
            format!(
                "BlockedByJoinSet {join_set_id}{closing}",
                closing = if closing { " (closing)" } else { "" }
            )
        }
        Status::Finished(Finished { .. }) => {
            // the final result will be sent in the next message, since we set `send_finished_status` to true.
            "Finished".to_string()
        }
        Status::Paused(grpc_gen::execution_status::Paused {}) => "Paused".to_string(),
        illegal @ Status::BlockedByJoinSet(_) => panic!("illegal state {illegal:?}"),
    }
}

fn print_finished_status(
    finished_status: grpc_gen::FinishedStatus,
) -> Result<(), AlreadyPrintedError> {
    let created_at = DateTime::from(
        finished_status
            .created_at
            .expect("`created_at` is sent by the server"),
    );
    let finished_at = DateTime::from(
        finished_status
            .finished_at
            .expect("`finished_at` is sent by the server"),
    );

    let (new_pending_status, res) = match finished_status
        .value
        .expect("`result_detail` is sent by the server")
        .value
    {
        Some(grpc_gen::supported_function_result::Value::Ok(
            grpc_gen::supported_function_result::OkPayload {
                return_value: Some(return_value),
            },
        )) => {
            let return_value = String::from_utf8_lossy(&return_value.value);
            (format!("OK: {return_value}"), Ok(()))
        }
        Some(grpc_gen::supported_function_result::Value::Ok(
            grpc_gen::supported_function_result::OkPayload { return_value: None },
        )) => ("OK: (no return value)".to_string(), Ok(())),
        Some(grpc_gen::supported_function_result::Value::Error(
            grpc_gen::supported_function_result::ErrorPayload {
                return_value: Some(return_value),
            },
        )) => {
            let return_value = String::from_utf8_lossy(&return_value.value);
            (format!("Error: {return_value}"), Err(AlreadyPrintedError))
        }
        Some(grpc_gen::supported_function_result::Value::Error(
            grpc_gen::supported_function_result::ErrorPayload { return_value: None },
        )) => (
            "Error: (no return value)".to_string(),
            Err(AlreadyPrintedError),
        ),
        Some(grpc_gen::supported_function_result::Value::ExecutionFailure(
            grpc_gen::supported_function_result::ExecutionFailure {
                kind,
                reason,
                detail,
            },
        )) => {
            let kind = grpc_gen::ExecutionFailureKind::try_from(kind)
                .map_err(|_| ())
                .and_then(ExecutionFailureKind::try_from)
                .expect("ExecutionFailureKind must be in sync with the server");

            let mut string = format!("Execution failure ({kind})");
            if let Some(reason) = reason {
                string.push_str(": `");
                string.push_str(&reason);
                string.push('`');
            }
            if let Some(detail) = detail {
                string.push('\n');
                string.push_str(&detail);
            }
            (string, Err(AlreadyPrintedError))
        }
        other => unreachable!("unexpected variant {other:?}"),
    };

    println!("Execution finished: {new_pending_status}");
    println!(
        "\nExecution took {since_created:?}.",
        since_created = (finished_at - created_at)
            .to_std()
            .expect("must be non-negative")
    );
    res
}

#[derive(Debug, thiserror::Error)]
#[error("")]
struct AlreadyPrintedError;

#[derive(Clone, Copy, Default)]
pub(crate) struct GetStatusOptions {
    pub(crate) follow: bool,
    pub(crate) no_reconnect: bool,
}

pub(crate) async fn get_status(
    mut client: ExecutionRepositoryClient,
    execution_id: ExecutionId,
    opts: GetStatusOptions,
) -> anyhow::Result<()> {
    let reconnect = !opts.no_reconnect;
    loop {
        match poll_get_status_stream(&mut client, &execution_id, opts).await {
            Ok(()) => return Ok(()),
            Err(err) => {
                if reconnect {
                    if err.downcast_ref::<tonic::Status>().is_some() {
                        eprintln!("Got error while polling the status, reconnecting - {err}");
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    } else if err.downcast_ref::<AlreadyPrintedError>().is_some() {
                        // Already printed.
                        return Err(err);
                    } else {
                        eprintln!("Encountered unrecoverable error, not reconnecting - {err:?}");
                        return Err(err);
                    }
                } else {
                    return Err(err);
                }
            }
        }
    }
}

async fn poll_get_status_stream(
    client: &mut ExecutionRepositoryClient,
    execution_id: &ExecutionId,
    opts: GetStatusOptions,
) -> anyhow::Result<()> {
    let mut stream = client
        .get_status(tonic::Request::new(grpc_gen::GetStatusRequest {
            execution_id: Some(grpc_gen::ExecutionId::from(execution_id.clone())),
            follow: opts.follow,
            send_finished_status: true,
        }))
        .await?
        .into_inner();
    while let Some(status) = stream.message().await? {
        let finished = print_status(status)?;
        if finished {
            // Do not print last backtrace on finished.
            return Ok(());
        }
    }
    Ok(())
}

/// Send a GET request and forward the response body to stdout.
async fn send_and_print(req: reqwest::RequestBuilder) -> anyhow::Result<()> {
    let resp = req.send().await.context("failed to send request")?;
    let status = resp.status();
    if status.is_success() {
        let body = resp.text().await.context("failed to read response body")?;
        // Ensure output ends with a newline even when the server omits it (e.g. JSON).
        if body.ends_with('\n') {
            print!("{body}");
        } else {
            println!("{body}");
        }
        Ok(())
    } else {
        let body = resp.text().await.unwrap_or_default();
        Err(anyhow::anyhow!("server returned {status}: {body}"))
    }
}

async fn replay(api_url: &str, execution_id: ExecutionId, json: bool) -> anyhow::Result<()> {
    let client = reqwest::Client::new();
    let accept = if json {
        "application/json"
    } else {
        "text/plain"
    };
    let req = client
        .put(format!("{api_url}/v1/executions/{execution_id}/replay"))
        .header(ACCEPT, accept);
    send_and_print(req).await
}

async fn replay_json(
    api_url: &str,
    execution_id: &ExecutionId,
) -> anyhow::Result<serde_json::Value> {
    let client = reqwest::Client::new();
    let resp = client
        .put(format!("{api_url}/v1/executions/{execution_id}/replay"))
        .header(ACCEPT, "application/json")
        .send()
        .await
        .context("failed to send replay request")?;
    let status = resp.status();
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        bail!("server returned {status}: {body}");
    }
    resp.json()
        .await
        .context("failed to decode replay response as JSON")
}

fn pause_submitted_in_replay(replay: &mut serde_json::Value) -> anyhow::Result<()> {
    let captured_writes = replay
        .get_mut("captured_writes")
        .and_then(serde_json::Value::as_array_mut)
        .context("replay response missing captured_writes array")?;

    for captured_write in captured_writes {
        let Some(write_type) = captured_write
            .get("type")
            .and_then(serde_json::Value::as_str)
        else {
            continue;
        };
        if write_type != "append_batch_create_new_execution" {
            continue;
        }

        let child_requests = captured_write
            .get_mut("child_requests")
            .and_then(serde_json::Value::as_array_mut)
            .context("append_batch_create_new_execution missing child_requests array")?;

        for child_request in child_requests {
            let child_request = child_request
                .as_object_mut()
                .context("child request must be a JSON object")?;
            child_request.insert("paused".to_string(), serde_json::Value::Bool(true));
        }
    }

    Ok(())
}

fn trim_replay(replay: &mut serde_json::Value, trim: usize) -> anyhow::Result<()> {
    let captured_writes = replay
        .get_mut("captured_writes")
        .and_then(serde_json::Value::as_array_mut)
        .context("replay response missing captured_writes array")?;
    captured_writes.truncate(trim);
    Ok(())
}

async fn advance(
    api_url: &str,
    execution_id: ExecutionId,
    json: bool,
    trim: Option<usize>,
    pause_submitted: bool,
) -> anyhow::Result<()> {
    let mut replay = replay_json(api_url, &execution_id).await?;
    if let Some(trim) = trim {
        trim_replay(&mut replay, trim)?;
    }
    if pause_submitted {
        pause_submitted_in_replay(&mut replay)?;
    }
    let client = reqwest::Client::new();
    let accept = if json {
        "application/json"
    } else {
        "text/plain"
    };
    let req = client
        .put(format!("{api_url}/v1/executions/{execution_id}/advance"))
        .header(ACCEPT, accept)
        .json(&replay);
    send_and_print(req).await
}

#[cfg(test)]
mod tests {
    use super::pause_submitted_in_replay;
    use serde_json::json;

    #[test]
    fn pause_submitted_rewrites_child_requests_only() {
        let mut replay = json!({
            "captured_writes": [
                {
                    "type": "append",
                    "execution_id": "Exec_01",
                    "version": 1,
                    "event": {}
                },
                {
                    "type": "append_batch_create_new_execution",
                    "current_time": "2026-01-01T00:00:00Z",
                    "events": [],
                    "execution_id": "Exec_01",
                    "version": 2,
                    "child_requests": [
                        {
                            "execution_id": "Exec_02",
                            "ffqn": "pkg:ifc/fn",
                            "params": [],
                            "scheduled_at": "2026-01-01T00:00:00Z",
                            "component_id": {
                                "component_type": "workflow",
                                "name": "wf",
                                "component_digest": "sha256:deadbeef"
                            },
                            "deployment_id": "Dep_01",
                            "created_at": "2026-01-01T00:00:00Z",
                            "metadata": {},
                            "paused": false
                        }
                    ],
                    "backtraces": []
                }
            ]
        });

        pause_submitted_in_replay(&mut replay).unwrap();

        assert_eq!(
            replay["captured_writes"][1]["child_requests"][0]["paused"],
            json!(true)
        );
        assert!(replay["captured_writes"][0].get("child_requests").is_none());
    }
}

async fn execution_list(
    api_url: &str,
    ffqn: Option<String>,
    execution_id_prefix: Option<String>,
    show_derived: bool,
    hide_finished: bool,
    limit: u16,
    json: bool,
) -> anyhow::Result<()> {
    let client = reqwest::Client::new();
    let accept = if json {
        "application/json"
    } else {
        "text/plain"
    };
    let mut req = client
        .get(format!("{api_url}/v1/executions"))
        .header(ACCEPT, accept)
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
    send_and_print(req).await
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
    body: &str,
    json: bool,
    show_run_id: bool,
    show_derived: bool,
) -> anyhow::Result<Option<String>> {
    let items: Vec<serde_json::Value> =
        serde_json::from_str(body).context("failed to parse logs JSON")?;
    if items.is_empty() {
        return Ok(None);
    }
    if json {
        for item in &items {
            println!(
                "{}",
                serde_json::to_string(item).context("failed to serialize log item")?
            );
        }
    } else {
        let mut output = String::new();
        for item in &items {
            let created_at = item["created_at"]
                .as_str()
                .context("missing created_at in log item")?;
            let run_id = item["run_id"].as_str().unwrap_or_default();
            let execution_id = item["execution_id"].as_str().unwrap_or_default();
            let mut prefix = String::new();
            if show_run_id {
                write!(&mut prefix, "{run_id} ").expect("writing to string");
            }
            if show_derived {
                write!(&mut prefix, "{execution_id} ").expect("writing to string");
            }
            match item["type"].as_str() {
                Some("log") => {
                    let level = item["level"].as_str().unwrap_or("INFO");
                    let message = item["message"].as_str().unwrap_or_default();
                    writeln!(
                        &mut output,
                        "{created_at} [{level:<6}] {prefix}{message}",
                        level = level.to_uppercase(),
                    )
                    .expect("writing to string");
                }
                Some("stream") => {
                    let stream_type = item["stream_type"].as_str().unwrap_or("STDOUT");
                    let payload_b64 = item["payload"].as_str().unwrap_or_default();
                    let payload_bytes = base64::engine::general_purpose::STANDARD
                        .decode(payload_b64)
                        .unwrap_or_default();
                    let payload_utf8 = String::from_utf8_lossy(&payload_bytes);
                    writeln!(
                        &mut output,
                        "{created_at} [{stream_type:<6}] {prefix}{payload_utf8}",
                        stream_type = stream_type.to_uppercase(),
                    )
                    .expect("writing to string");
                }
                _ => {}
            }
        }
        print!("{output}");
    }
    let cursor = items
        .last()
        .and_then(|v| v["cursor"].as_str())
        .map(str::to_string);
    Ok(cursor)
}

async fn fetch_logs(
    client: &reqwest::Client,
    logs_url: &str,
    opts: &LogsOpts,
    cursor: Option<&str>,
    direction: &str,
) -> anyhow::Result<String> {
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
    let resp = req.send().await.context("failed to send logs request")?;
    let resp_status = resp.status();
    if !resp_status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        return Err(anyhow::anyhow!("server returned {resp_status}: {body}"));
    }
    resp.text().await.context("failed to read logs response")
}

async fn execution_logs_cmd(
    api_url: &str,
    execution_id: ExecutionId,
    opts: &LogsOpts,
    after: Option<String>,
    json: bool,
) -> anyhow::Result<()> {
    let client = reqwest::Client::new();
    let logs_url = format!("{api_url}/v1/executions/{execution_id}/logs");
    let body = fetch_logs(&client, &logs_url, opts, after.as_deref(), "newer").await?;
    print_log_items(&body, json, opts.show_run_id, opts.show_derived)?;
    Ok(())
}

async fn follow_logs(
    api_url: &str,
    execution_id: &ExecutionId,
    opts: &LogsOpts,
    initial_after: Option<String>,
    json: bool,
) -> anyhow::Result<()> {
    let client = reqwest::Client::new();
    let logs_url = format!("{api_url}/v1/executions/{execution_id}/logs");
    let status_url = format!("{api_url}/v1/executions/{execution_id}/status");
    let mut cursor: Option<String> = initial_after;

    loop {
        let body = fetch_logs(&client, &logs_url, opts, cursor.as_deref(), "newer").await?;
        let new_cursor = print_log_items(&body, json, opts.show_run_id, opts.show_derived)?;
        let has_items = new_cursor.is_some();
        if let Some(c) = new_cursor {
            cursor = Some(c);
        }

        // Check if the execution has finished.
        let finished = {
            let resp = client
                .get(&status_url)
                .header(ACCEPT, "application/json")
                .send()
                .await
                .context("failed to get execution status")?;
            let st = resp.status();
            if !st.is_success() {
                let body = resp.text().await.unwrap_or_default();
                return Err(anyhow::anyhow!("status check returned {st}: {body}"));
            }
            let status_json: serde_json::Value = resp
                .json()
                .await
                .context("failed to parse status response")?;
            status_json["pending_state"]["status"].as_str() == Some("finished")
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
    api_url: &str,
    execution_id: ExecutionId,
    from: Option<u32>,
    limit: u16,
    json: bool,
) -> anyhow::Result<()> {
    let client = reqwest::Client::new();
    let accept = if json {
        "application/json"
    } else {
        "text/plain"
    };
    let mut req = client
        .get(format!("{api_url}/v1/executions/{execution_id}/events"))
        .header(ACCEPT, accept)
        .query(&[("length", limit.to_string())]);
    if let Some(from) = from {
        req = req
            .query(&[("version", from.to_string())])
            .query(&[("including_cursor", "true")]);
    }
    send_and_print(req).await
}

async fn execution_responses_cmd(
    api_url: &str,
    execution_id: ExecutionId,
    from: Option<u32>,
    limit: u16,
    json: bool,
) -> anyhow::Result<()> {
    let client = reqwest::Client::new();
    let accept = if json {
        "application/json"
    } else {
        "text/plain"
    };
    let mut req = client
        .get(format!("{api_url}/v1/executions/{execution_id}/responses"))
        .header(ACCEPT, accept)
        .query(&[("length", limit.to_string())]);
    if let Some(from) = from {
        req = req
            .query(&[("cursor", from.to_string())])
            .query(&[("including_cursor", "true")]);
    }
    send_and_print(req).await
}

impl CancelCommand {
    #[instrument(skip_all)]
    pub(crate) async fn execute(self) -> anyhow::Result<()> {
        let channel = to_channel(&self.api_url).await?;
        let mut client = get_execution_repository_client(channel).await?;
        let request = if let Ok(execution_id) = ExecutionId::from_str(&self.id) {
            cancel_request::Request::Activity(CancelRequestActivity {
                execution_id: Some(grpc_gen::ExecutionId {
                    id: execution_id.to_string(),
                }),
            })
        } else if let Ok(delay_id) = DelayId::from_str(&self.id) {
            cancel_request::Request::Delay(CancelRequestDelay {
                delay_id: Some(grpc_gen::DelayId {
                    id: delay_id.to_string(),
                }),
            })
        } else {
            bail!("id is not a derived execution id nor a delay id")
        };
        let resp = client
            .cancel(tonic::Request::new(CancelRequest {
                request: Some(request),
            }))
            .await?
            .into_inner();

        match resp.outcome() {
            CancelOutcome::Unspecified => panic!("unspecified"),
            CancelOutcome::Cancelled => println!("Cancelled"),
            CancelOutcome::AlreadyFinished => println!("Already successfully finished"),
        }
        Ok(())
    }
}

async fn upgrade(
    api_url: &str,
    execution_id: ExecutionId,
    skip_determinism_check: bool,
) -> anyhow::Result<()> {
    let channel = to_channel(api_url).await?;

    // Step 1: fetch the execution summary to get current component digest and ffqn.
    let mut exec_client = get_execution_repository_client(channel.clone()).await?;
    let summary = exec_client
        .get_status(tonic::Request::new(grpc_gen::GetStatusRequest {
            execution_id: Some(grpc_gen::ExecutionId::from(execution_id.clone())),
            follow: false,
            send_finished_status: false,
        }))
        .await
        .context("failed to get execution status")?
        .into_inner()
        .message()
        .await
        .context("failed to read status stream")?
        .context("empty status stream")?;

    let summary = match summary
        .message
        .context("missing message in status response")?
    {
        grpc::grpc_gen::get_status_response::Message::Summary(s) => s,
        other => bail!("expected ExecutionSummary, got {other:?}"),
    };

    let ffqn = FunctionFqn::try_from(
        summary
            .function_name
            .context("missing function_name in summary")?,
    )
    .map_err(|e| anyhow::anyhow!("failed to parse ffqn: {e}"))?;

    let old_digest = summary
        .component_digest
        .context("missing component_digest in summary")?
        .digest;

    // Step 2: find the component that currently exports this ffqn.
    let mut fn_client = get_fn_repository_client(channel.clone()).await?;
    let components = fn_client
        .list_components(tonic::Request::new(grpc_gen::ListComponentsRequest {
            function_name: Some(grpc_gen::FunctionName::from(&ffqn)),
            component_digest: None,
            extensions: false,
        }))
        .await
        .context("failed to list components")?
        .into_inner()
        .components;

    let new_digest = match components.as_slice() {
        [] => bail!("no component in the active deployment exports `{ffqn}`"),
        [component] => component
            .component_id
            .as_ref()
            .and_then(|id| id.digest.as_ref())
            .map(|d| d.digest.clone())
            .context("component is missing digest")?,
        _ => bail!(
            "multiple components export `{ffqn}`: {:?}",
            components
                .iter()
                .filter_map(|c| c.component_id.as_ref())
                .map(|id| &id.name)
                .collect::<Vec<_>>()
        ),
    };

    if old_digest == new_digest {
        println!("Already up to date ({old_digest})");
        return Ok(());
    }

    println!("Upgrading from {old_digest} to {new_digest}");

    // Step 3: perform the upgrade.
    exec_client
        .upgrade_execution_component(tonic::Request::new(
            grpc_gen::UpgradeExecutionComponentRequest {
                execution_id: Some(grpc_gen::ExecutionId::from(execution_id)),
                expected_component_digest: Some(grpc_gen::ContentDigest { digest: old_digest }),
                new_component_digest: Some(grpc_gen::ContentDigest { digest: new_digest }),
                skip_determinism_check,
            },
        ))
        .await
        .context("upgrade failed")?;

    println!("Upgraded");
    Ok(())
}
