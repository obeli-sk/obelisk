use crate::ExecutionRepositoryClient;
use crate::args;
use crate::args::CancelCommand;
use crate::args::params::parse_params;
use crate::get_execution_repository_client;
use crate::get_fn_repository_client;
use crate::server::web_api_server::ExecutionSubmitPayload;
use anyhow::Context as _;
use anyhow::bail;
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
use std::str::FromStr;
use std::time::Duration;
use tracing::instrument;

impl args::Execution {
    pub(crate) async fn run(self) -> Result<(), anyhow::Error> {
        match self {
            args::Execution::Submit {
                api_url,
                execution_id,
                ffqn,
                params,
                follow,
                no_reconnect,
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
                submit(&api_url, execution_id, ffqn, parse_params(params)?, opts).await
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
    ffqn: FunctionFqn,
    params: Vec<serde_json::Value>,
    opts: SubmitOutputOpts,
) -> anyhow::Result<()> {
    let channel = to_channel(api_url).await?;
    let mut client = get_execution_repository_client(channel.clone()).await?;
    let mut component_client = get_fn_repository_client(channel).await?;
    let ffqn = if let Some(ifc_name) = ffqn.ifc_fqn.strip_prefix(".../") {
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
            if export.function_name == ffqn.function_name.as_ref() {
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
    } else {
        ffqn
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
