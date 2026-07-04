use crate::ExecutionRepositoryClient;
use crate::args;
use crate::args::CancelCommand;
use crate::args::FunctionFqnOrShort;
use crate::args::params::parse_params;
use crate::get_execution_repository_client;
use crate::get_fn_repository_client;
use crate::server::web_api_server::{AdvanceRequestSer, ExecutionSubmitPayload, ReplayResponseSer};
use anyhow::Context as _;
use anyhow::bail;
use base64::Engine as _;
use chrono::DateTime;
use concepts::ExecutionFailureKind;
use concepts::JoinSetId;
use concepts::JoinSetKind;
use concepts::prefixed_ulid::ExecutionIdDerived;
use concepts::{ExecutionId, FunctionFqn};
use grpc::grpc_gen;
use grpc::grpc_gen::CancelDelayRequest;
use grpc::grpc_gen::CancelExecutionRequest;
use grpc::grpc_gen::cancel_delay_response::CancelDelayOutcome;
use grpc::grpc_gen::cancel_execution_response::CancelExecutionOutcome;
use grpc::grpc_gen::execution_status::BlockedByJoinSet;
use grpc::grpc_gen::execution_status::Finished;
use grpc::to_channel;
use grpc_gen::execution_status::Status;
use http::header::ACCEPT;
use itertools::Either;
use std::fmt::Write as _;
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
            args::Execution::Status {
                api_url,
                execution_id,
                follow,
                no_reconnect,
                json,
            } => {
                if json {
                    get_execution_status_json(&api_url, execution_id, follow, no_reconnect).await
                } else {
                    let channel = to_channel(&api_url).await?;
                    let client = get_execution_repository_client(channel).await?;
                    let opts = GetStatusOptions {
                        follow,
                        no_reconnect,
                    };
                    get_execution_status(client, execution_id, opts).await
                }
            }
            args::Execution::Result {
                api_url,
                execution_id,
                follow,
                no_reconnect,
                json,
            } => {
                if json {
                    get_execution_result_json(&api_url, execution_id, follow, no_reconnect).await
                } else {
                    let channel = to_channel(&api_url).await?;
                    let client = get_execution_repository_client(channel).await?;
                    let opts = GetStatusOptions {
                        follow,
                        no_reconnect,
                    };
                    get_execution_result(client, execution_id, opts).await
                }
            }
            args::Execution::Cancel(cancel_request) => cancel_request.execute().await,
            args::Execution::Pause { api_url, id } => {
                let channel = to_channel(&api_url).await?;
                let mut client = get_execution_repository_client(channel).await?;
                match id {
                    args::ExecutionIdOrDelayId::Execution(execution_id) => {
                        client
                            .pause_execution(tonic::Request::new(grpc_gen::PauseExecutionRequest {
                                execution_id: Some(grpc_gen::ExecutionId::from(execution_id)),
                            }))
                            .await?;
                    }
                    args::ExecutionIdOrDelayId::Delay(delay_id) => {
                        client
                            .pause_delay(tonic::Request::new(grpc_gen::PauseDelayRequest {
                                delay_id: Some(grpc_gen::DelayId::from(delay_id)),
                            }))
                            .await?;
                    }
                }
                println!("Paused");
                Ok(())
            }
            args::Execution::Unpause { api_url, id } => {
                let channel = to_channel(&api_url).await?;
                let mut client = get_execution_repository_client(channel).await?;
                match id {
                    args::ExecutionIdOrDelayId::Execution(execution_id) => {
                        client
                            .unpause_execution(tonic::Request::new(
                                grpc_gen::UnpauseExecutionRequest {
                                    execution_id: Some(grpc_gen::ExecutionId::from(execution_id)),
                                },
                            ))
                            .await?;
                    }
                    args::ExecutionIdOrDelayId::Delay(delay_id) => {
                        client
                            .unpause_delay(tonic::Request::new(grpc_gen::UnpauseDelayRequest {
                                delay_id: Some(grpc_gen::DelayId::from(delay_id)),
                            }))
                            .await?;
                    }
                }
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
                pause_all,
                pause_submitted_executions,
                pause_delays,
                force,
            } => {
                let pause_submitted_executions = pause_all || pause_submitted_executions;
                let pause_delays = pause_all || pause_delays;
                advance(
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
                    deployment_id: None,
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
                get_execution_result(client, execution_id, opts).await?;
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

#[derive(Clone, Copy)]
enum GetMode {
    Status,
    Result,
}

enum PollOutcome {
    Pending,
    Finished,
}

fn print_status(
    response: grpc_gen::GetStatusResponse,
    mode: GetMode,
) -> Result<PollOutcome, AlreadyPrintedError> {
    use grpc_gen::get_status_response::Message;
    let message = response.message.expect("message expected");

    let status_or_finished = match message {
        Message::Summary(summary) => Either::Left(summary.current_status.expect("sent by server")),
        Message::CurrentStatus(status) => Either::Left(status),
        Message::FinishedStatus(finished) => match mode {
            GetMode::Status => {
                println!("Finished");
                return Ok(PollOutcome::Finished);
            }
            GetMode::Result => Either::Right(finished),
        },
    };
    match status_or_finished {
        Either::Left(status) => {
            let is_finished = matches!(
                status
                    .status
                    .as_ref()
                    .expect("status is sent by the server"),
                Status::Finished(_)
            );
            println!("{}", format_pending_status(status));
            match mode {
                GetMode::Status if is_finished => Ok(PollOutcome::Finished),
                GetMode::Status | GetMode::Result => Ok(PollOutcome::Pending),
            }
        }
        Either::Right(finished) => {
            print_finished_status(finished)?;
            Ok(PollOutcome::Finished)
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
        Status::Finished(Finished { result_kind, .. }) => {
            // The final result will be sent in the next message for `GetMode::Result`,
            // but status mode still needs to distinguish success from failure.
            format_finished_status(result_kind)
        }
        Status::Paused(grpc_gen::execution_status::Paused {}) => "Paused".to_string(),
        Status::Cancelling(grpc_gen::execution_status::Cancelling {}) => "Cancelling".to_string(),
        illegal @ Status::BlockedByJoinSet(_) => panic!("illegal state {illegal:?}"),
    }
}

fn format_finished_status(result_kind: Option<grpc_gen::ResultKind>) -> String {
    match result_kind.and_then(format_finished_result_kind) {
        Some(result_kind) => format!("Finished: {result_kind}"),
        None => "Finished".to_string(),
    }
}

fn format_finished_result_kind(result_kind: grpc_gen::ResultKind) -> Option<String> {
    use grpc_gen::result_kind::Value;

    match result_kind.value {
        Some(Value::Ok(_)) => Some("OK".to_string()),
        Some(Value::Error(_)) => Some("Error".to_string()),
        Some(Value::ExecutionFailureKind(kind)) => {
            let kind = grpc_gen::ExecutionFailureKind::try_from(kind).ok()?;
            let kind = ExecutionFailureKind::try_from(kind).ok()?;
            Some(format!("Execution failure ({kind})"))
        }
        None => None,
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

pub(crate) async fn get_execution_result(
    mut client: ExecutionRepositoryClient,
    execution_id: ExecutionId,
    opts: GetStatusOptions,
) -> anyhow::Result<()> {
    let reconnect = !opts.no_reconnect;
    loop {
        match poll_get_status_stream(&mut client, &execution_id, opts, GetMode::Result).await {
            Ok(PollOutcome::Finished) => return Ok(()),
            Ok(PollOutcome::Pending) => bail!("execution not finished yet"),
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

pub(crate) async fn get_execution_status(
    mut client: ExecutionRepositoryClient,
    execution_id: ExecutionId,
    opts: GetStatusOptions,
) -> anyhow::Result<()> {
    let reconnect = !opts.no_reconnect;
    loop {
        match poll_get_status_stream(&mut client, &execution_id, opts, GetMode::Status).await {
            Ok(_) => return Ok(()),
            Err(err) => {
                if reconnect {
                    if err.downcast_ref::<tonic::Status>().is_some() {
                        eprintln!("Got error while polling the status, reconnecting - {err}");
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    } else if err.downcast_ref::<AlreadyPrintedError>().is_some() {
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
    mode: GetMode,
) -> anyhow::Result<PollOutcome> {
    let mut stream = client
        .get_status(tonic::Request::new(grpc_gen::GetStatusRequest {
            execution_id: Some(grpc_gen::ExecutionId::from(execution_id.clone())),
            follow: opts.follow,
            send_finished_status: matches!(mode, GetMode::Result),
        }))
        .await?
        .into_inner();
    while let Some(status) = stream.message().await? {
        let outcome = print_status(status, mode)?;
        if matches!(outcome, PollOutcome::Finished) {
            return Ok(outcome);
        }
    }
    Ok(PollOutcome::Pending)
}

async fn fetch_execution_status_json(
    client: &reqwest::Client,
    api_url: &str,
    execution_id: &ExecutionId,
) -> anyhow::Result<serde_json::Value> {
    let response = client
        .get(format!("{api_url}/v1/executions/{execution_id}/status"))
        .header(ACCEPT, "application/json")
        .send()
        .await
        .context("failed to get execution status")?;
    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        bail!("server returned {status}: {body}");
    }
    let value: serde_json::Value = response
        .json()
        .await
        .context("failed to parse execution status response")?;
    Ok(value)
}

fn execution_status_json_is_finished(status: &serde_json::Value) -> bool {
    status["pending_state"]["status"].as_str() == Some("finished")
}

fn print_json(value: &serde_json::Value) -> anyhow::Result<()> {
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
) -> anyhow::Result<serde_json::Value> {
    let response = client
        .get(format!("{api_url}/v1/executions/{execution_id}"))
        .query(&[("follow", follow.to_string())])
        .header(ACCEPT, "application/json")
        .send()
        .await
        .context("failed to get execution result")?;
    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        bail!("server returned {status}: {body}");
    }
    response
        .json()
        .await
        .context("failed to parse execution result response")
}

async fn get_execution_status_json(
    api_url: &str,
    execution_id: ExecutionId,
    follow: bool,
    no_reconnect: bool,
) -> anyhow::Result<()> {
    let client = reqwest::Client::new();
    let reconnect = !no_reconnect;
    let mut last_status: Option<serde_json::Value> = None;

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
        if last_status.as_ref() != Some(&status) {
            print_json(&status)?;
        }

        if execution_status_json_is_finished(&status) {
            return Ok(());
        }

        if !follow {
            return Ok(());
        }

        last_status = Some(status);
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

async fn get_execution_result_json(
    api_url: &str,
    execution_id: ExecutionId,
    follow: bool,
    no_reconnect: bool,
) -> anyhow::Result<()> {
    let client = reqwest::Client::new();
    let reconnect = follow && !no_reconnect;

    loop {
        match fetch_execution_result_json(&client, api_url, &execution_id, follow).await {
            Ok(result) => {
                print_json(&result)?;
                return Ok(());
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

/// Fetch replay response as JSON. Accepts both 200 (success) and 422 (replay failed with body).
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
            crate::server::web_api_server::CapturedWriteSer::AppendFinished { .. } => {}
        }
    }
}

fn trim_replay(advance_request: &mut AdvanceRequestSer, trim: usize) {
    advance_request.captured_writes.truncate(trim);
}

fn replay_to_advanceable_request(
    replay: &serde_json::Value,
    force: bool,
) -> anyhow::Result<AdvanceRequestSer> {
    let replay: ReplayResponseSer = serde_json::from_value(replay.clone())
        .context("failed to decode replay response as ReplayResponseSer")?;
    match replay {
        ReplayResponseSer::Advanceable { captured_writes } => {
            Ok(AdvanceRequestSer { captured_writes })
        }
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
                Ok(AdvanceRequestSer { captured_writes })
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
    api_url: &str,
    execution_id: ExecutionId,
    opts: AdvanceOpts,
) -> anyhow::Result<()> {
    let replay = replay_json(api_url, &execution_id).await?;
    let mut advance_request = replay_to_advanceable_request(&replay, opts.force)?;
    if let Some(trim) = opts.trim {
        trim_replay(&mut advance_request, trim);
    }
    if opts.pause_submitted_executions {
        pause_submitted_in_replay(&mut advance_request);
    }
    if opts.pause_delays {
        pause_delays_in_replay(&mut advance_request);
    }
    let client = reqwest::Client::new();
    let accept = if opts.json {
        "application/json"
    } else {
        "text/plain"
    };
    let req = client
        .put(format!("{api_url}/v1/executions/{execution_id}/advance"))
        .header(ACCEPT, accept)
        .json(&advance_request);
    send_and_print(req).await
}

#[cfg(test)]
mod tests {
    use super::{
        execution_status_json_is_finished, format_finished_result_kind, format_finished_status,
        pause_delays_in_replay, pause_submitted_in_replay,
    };
    use crate::server::web_api_server::{AdvanceRequestSer, CapturedWriteSer};
    use chrono::{DateTime, Utc};
    use grpc::grpc_gen;
    use serde_json::json;

    #[test]
    fn execution_status_json_finished_detection() {
        assert!(execution_status_json_is_finished(&json!({
            "pending_state": {
                "status": "finished"
            }
        })));
        assert!(!execution_status_json_is_finished(&json!({
            "pending_state": {
                "status": "locked"
            }
        })));
    }

    #[test]
    fn finished_status_includes_success_and_error_kind() {
        assert_eq!(
            format_finished_status(Some(grpc_gen::ResultKind {
                value: Some(grpc_gen::result_kind::Value::Ok(
                    grpc_gen::result_kind::Ok {},
                )),
            })),
            "Finished: OK"
        );
        assert_eq!(
            format_finished_status(Some(grpc_gen::ResultKind {
                value: Some(grpc_gen::result_kind::Value::Error(
                    grpc_gen::result_kind::Error {},
                )),
            })),
            "Finished: Error"
        );
    }

    #[test]
    fn finished_status_includes_execution_failure_kind() {
        assert_eq!(
            format_finished_result_kind(grpc_gen::ResultKind {
                value: Some(grpc_gen::result_kind::Value::ExecutionFailureKind(
                    grpc_gen::ExecutionFailureKind::TimedOut.into(),
                )),
            }),
            Some("Execution failure (TimedOut)".to_string())
        );
    }

    #[test]
    fn finished_status_falls_back_to_plain_finished_for_missing_or_unknown_result_kind() {
        assert_eq!(format_finished_status(None), "Finished");
        assert_eq!(
            format_finished_status(Some(grpc_gen::ResultKind { value: None })),
            "Finished"
        );
        assert_eq!(
            format_finished_status(Some(grpc_gen::ResultKind {
                value: Some(grpc_gen::result_kind::Value::ExecutionFailureKind(999)),
            })),
            "Finished"
        );
    }

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
    after: Option<&str>,
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
    if let Some(after) = after {
        req = req.query(&[("after", after)]);
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
    let body = fetch_logs(&client, &logs_url, opts, None, after.as_deref(), "newer").await?;
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
    let mut cursor: Option<String> = None;

    loop {
        let body = fetch_logs(
            &client,
            &logs_url,
            opts,
            cursor.as_deref(),
            initial_after.as_deref(),
            "newer",
        )
        .await?;
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
    } else {
        // Without an explicit cursor, fetch the newest events from the latest version.
        req = req.query(&[("direction", "older")]);
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
    } else {
        // Without an explicit cursor, fetch the newest responses from the latest cursor.
        req = req.query(&[("direction", "older")]);
    }
    send_and_print(req).await
}

impl CancelCommand {
    #[instrument(skip_all)]
    pub(crate) async fn execute(self) -> anyhow::Result<()> {
        let channel = to_channel(&self.api_url).await?;
        let mut client = get_execution_repository_client(channel).await?;
        match self.id {
            args::ExecutionIdOrDelayId::Execution(execution_id) => {
                let resp = client
                    .cancel_execution(tonic::Request::new(CancelExecutionRequest {
                        execution_id: Some(grpc_gen::ExecutionId::from(execution_id)),
                    }))
                    .await?
                    .into_inner();
                match resp.outcome() {
                    CancelExecutionOutcome::Unspecified => panic!("unspecified"),
                    CancelExecutionOutcome::CancellationRequested => {
                        println!("Cancellation requested");
                    }
                    CancelExecutionOutcome::AlreadyFinished => {
                        println!("Already successfully finished");
                    }
                    CancelExecutionOutcome::AlreadyCancelling => {
                        println!("Already cancelling");
                    }
                }
            }
            args::ExecutionIdOrDelayId::Delay(delay_id) => {
                let resp = client
                    .cancel_delay(tonic::Request::new(CancelDelayRequest {
                        delay_id: Some(grpc_gen::DelayId {
                            id: delay_id.to_string(),
                        }),
                    }))
                    .await?
                    .into_inner();
                match resp.outcome() {
                    CancelDelayOutcome::Unspecified => panic!("unspecified"),
                    CancelDelayOutcome::Cancelled => println!("Cancelled"),
                    CancelDelayOutcome::AlreadyFinished => {
                        println!("Already successfully finished");
                    }
                }
            }
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
            deployment_id: None,
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
