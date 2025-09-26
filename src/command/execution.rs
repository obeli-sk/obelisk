use crate::ExecutionRepositoryClient;
use chrono::DateTime;
use concepts::JOIN_SET_ID_INFIX;
use concepts::JoinSetKind;
use concepts::prefixed_ulid::ExecutionIdDerived;
use concepts::{ExecutionId, FunctionFqn};
use grpc::grpc_gen;
use grpc::grpc_gen::execution_status::BlockedByJoinSet;
use grpc::grpc_gen::execution_status::Finished;
use grpc_gen::execution_status::Status;
use itertools::Either;
use serde_json::json;
use std::time::Duration;
use tracing::instrument;

#[derive(PartialEq)]
pub(crate) enum SubmitOutputOpts {
    Json,
    PlainFollow { no_reconnect: bool },
}

#[instrument(skip_all)]
pub(crate) async fn submit(
    mut client: ExecutionRepositoryClient,
    ffqn: FunctionFqn,
    params: Vec<u8>,
    follow: bool,
    opts: SubmitOutputOpts,
) -> anyhow::Result<()> {
    let execution_id = ExecutionId::generate();
    client
        .submit(tonic::Request::new(grpc_gen::SubmitRequest {
            execution_id: Some(execution_id.clone().into()),
            params: Some(prost_wkt_types::Any {
                type_url: format!("urn:obelisk:json:params:{ffqn}"),
                value: params,
            }),
            function_name: Some(ffqn.into()),
        }))
        .await?;
    if opts == SubmitOutputOpts::Json {
        let json = json!(
            {"submit": {"execution_id": execution_id }}
        );
        println!("[{json}");
    } else {
        println!("{execution_id}");
    }
    if follow {
        match opts {
            SubmitOutputOpts::Json => get_status_json(client, execution_id, true, true).await?,
            SubmitOutputOpts::PlainFollow { no_reconnect } => {
                let opts = GetStatusOptions {
                    follow: true,
                    no_reconnect,
                };
                get_status(client, execution_id, opts).await?;
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
fn print_status(response: grpc_gen::GetStatusResponse) -> Result<bool, ExecutionError> {
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
            let kind = JoinSetKind::from(
                grpc_gen::join_set_id::JoinSetKind::try_from(kind)
                    .expect("JoinSetKind must be valid"),
            );
            let closing = if closing { " (closing)" } else { "" };
            format!("BlockedByJoinSet {kind}{JOIN_SET_ID_INFIX}{name}{closing}")
        }
        Status::Finished(Finished { .. }) => {
            // the final result will be sent in the next message, since we set `send_finished_status` to true.
            "Finished".to_string()
        }
        illegal @ Status::BlockedByJoinSet(_) => panic!("illegal state {illegal:?}"),
    }
}

#[derive(Debug, thiserror::Error)]
// This error is printed in the same function that constructs it.
enum ExecutionError {
    #[error("fallible error")]
    FallibleError,
    #[error("timeout")]
    Timeout,
    #[error("execution failure")]
    ExecutionFailure,
}

fn print_finished_status(finished_status: grpc_gen::FinishedStatus) -> Result<(), ExecutionError> {
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
        .result_detail
        .expect("`result_detail` is sent by the server")
        .value
    {
        Some(grpc_gen::result_detail::Value::Ok(grpc_gen::result_detail::Ok {
            return_value: Some(return_value),
        })) => {
            let return_value = String::from_utf8_lossy(&return_value.value);
            (format!("OK: {return_value}"), Ok(()))
        }
        Some(grpc_gen::result_detail::Value::Ok(grpc_gen::result_detail::Ok {
            return_value: None,
        })) => ("OK: (no return value)".to_string(), Ok(())),
        Some(grpc_gen::result_detail::Value::FallibleError(
            grpc_gen::result_detail::FallibleError {
                return_value: Some(return_value),
            },
        )) => {
            let return_value = String::from_utf8_lossy(&return_value.value);
            (
                format!("Err: {return_value}"),
                Err(ExecutionError::FallibleError),
            )
        }
        Some(grpc_gen::result_detail::Value::Timeout(_)) => {
            ("Timeout".to_string(), Err(ExecutionError::Timeout))
        }
        Some(grpc_gen::result_detail::Value::ExecutionFailure(
            grpc_gen::result_detail::ExecutionFailure {
                reason,
                detail: Some(detail),
            },
        )) => (
            format!("Execution failure: {reason}\ndetail: {detail}"),
            Err(ExecutionError::ExecutionFailure),
        ),
        Some(grpc_gen::result_detail::Value::ExecutionFailure(
            grpc_gen::result_detail::ExecutionFailure {
                reason,
                detail: None,
            },
        )) => (
            format!("Execution failure: {reason}"),
            Err(ExecutionError::ExecutionFailure),
        ),
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

fn print_finished_status_json(
    finished_status: grpc_gen::FinishedStatus,
) -> Result<(), ExecutionError> {
    let created_at = finished_status
        .created_at
        .expect("`created_at` is sent by the server");
    let scheduled_at = finished_status
        .scheduled_at
        .expect("`scheduled_at` is sent by the server");
    let finished_at = finished_status
        .finished_at
        .expect("`finished_at` is sent by the server");

    let (mut json, res) = match finished_status
        .result_detail
        .expect("`result_detail` is sent by the server")
        .value
    {
        Some(grpc_gen::result_detail::Value::Ok(grpc_gen::result_detail::Ok {
            return_value: Some(return_value),
        })) => {
            let return_value: serde_json::Value = serde_json::from_slice(&return_value.value)
                .expect("return_value must be JSON encoded");
            (json!({"ok": return_value}), Ok(()))
        }
        Some(grpc_gen::result_detail::Value::Ok(grpc_gen::result_detail::Ok {
            return_value: None,
        })) => (json!({"ok": null}), Ok(())),
        Some(grpc_gen::result_detail::Value::FallibleError(
            grpc_gen::result_detail::FallibleError {
                return_value: Some(return_value),
            },
        )) => {
            let return_value: serde_json::Value = serde_json::from_slice(&return_value.value)
                .expect("return_value must be JSON encoded");
            (
                json!({"fallible_error": return_value}),
                Err(ExecutionError::FallibleError),
            )
        }
        Some(grpc_gen::result_detail::Value::Timeout(_)) => {
            (json!({"timeout": null}), Err(ExecutionError::Timeout))
        }
        Some(grpc_gen::result_detail::Value::ExecutionFailure(
            grpc_gen::result_detail::ExecutionFailure { reason, detail },
        )) => (
            json!({"execution_failure": {"reason": reason, "detail": detail}}),
            Err(ExecutionError::ExecutionFailure),
        ),
        other => unreachable!("unexpected variant {other:?}"),
    };
    json["created_at"] = serde_json::Value::String(created_at.to_string());
    json["scheduled_at"] = serde_json::Value::String(scheduled_at.to_string());
    json["finished_at"] = serde_json::Value::String(finished_at.to_string());
    print!("{json}");
    res
}

pub(crate) async fn get_status_json(
    mut client: ExecutionRepositoryClient,
    execution_id: ExecutionId,
    follow: bool,
    mut json_output_started: bool, // true if `[{..}` was printed alraedy.
) -> anyhow::Result<()> {
    let mut stream = client
        .get_status(tonic::Request::new(grpc_gen::GetStatusRequest {
            execution_id: Some(grpc_gen::ExecutionId::from(execution_id.clone())),
            follow,
            send_finished_status: true,
        }))
        .await?
        .into_inner();

    if !json_output_started {
        println!("[");
    }
    let mut res = Ok(());
    while let Some(status) = stream.message().await? {
        if json_output_started {
            println!(",");
        }
        res = print_status_json(status);
        if res.is_err() {
            break;
        }
        json_output_started = true;
    }
    println!("\n]");
    res.map_err(anyhow::Error::from)
}

fn print_status_json(response: grpc_gen::GetStatusResponse) -> Result<(), ExecutionError> {
    use grpc_gen::get_status_response::Message;
    let message = response.message.expect("message expected");

    match message {
        Message::Summary(_) | Message::CurrentStatus(_) => {
            print!(
                "{}",
                serde_json::to_string(&message).expect("must be serializable")
            );
            Ok(())
        }
        Message::FinishedStatus(finished_sattus) => print_finished_status_json(finished_sattus),
    }
}

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
                    } else if err.downcast_ref::<ExecutionError>().is_some() {
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
