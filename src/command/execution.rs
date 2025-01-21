use super::grpc;
use super::grpc::execution_status::BlockedByJoinSet;
use crate::command::grpc::execution_status::Finished;
use crate::grpc_util::grpc_mapping::TonicClientResultExt;
use crate::ExecutionRepositoryClient;
use anyhow::anyhow;
use anyhow::Context;
use chrono::DateTime;
use concepts::{ExecutionId, FunctionFqn};
use grpc::execution_status::Status;
use std::str::FromStr;
use tracing::instrument;

#[instrument(skip_all)]
pub(crate) async fn submit(
    mut client: ExecutionRepositoryClient,
    ffqn: FunctionFqn,
    params: Vec<serde_json::Value>,
    follow: bool,
    verbosity: ExecutionVerbosity,
) -> anyhow::Result<()> {
    let resp = client
        .submit(tonic::Request::new(grpc::SubmitRequest {
            execution_id: Some(ExecutionId::generate().into()),
            params: Some(prost_wkt_types::Any {
                type_url: format!("urn:obelisk:json:params:{ffqn}"),
                value: serde_json::Value::Array(params).to_string().into_bytes(),
            }),
            function_name: Some(ffqn.into()),
        }))
        .await
        .to_anyhow()?
        .into_inner();
    let execution_id = resp
        .execution_id
        .context("response field `execution_id` must be present")
        .map(|execution_id| {
            ExecutionId::from_str(&execution_id.id).context("cannot parse `execution_id`")
        })??;
    println!("{execution_id}");
    if follow {
        get(client, execution_id, follow, verbosity).await?;
    }
    Ok(())
}

fn print_status(
    response: grpc::GetStatusResponse,
    old_pending_status: &mut String,
) -> anyhow::Result<()> {
    use grpc::get_status_response::Message;
    match response.message.expect("message expected") {
        Message::Summary(summary) => {
            let ffqn = FunctionFqn::try_from(summary.function_name.expect("sent by server"))
                .expect("ffqn sent by the server must be valid");
            println!("Function: {ffqn}");

            print_pending_status(
                summary.current_status.expect("sent by server"),
                old_pending_status,
            );
        }
        Message::CurrentStatus(pending_status) => {
            print_pending_status(pending_status, old_pending_status);
        }
        Message::FinishedStatus(finished_sattus) => {
            print_finished_status(finished_sattus)?;
        }
    }
    Ok(())
}

fn print_pending_status(pending_status: grpc::ExecutionStatus, old_pending_status: &mut String) {
    let status = pending_status.status.expect("status is sent by the server");
    let new_pending_status = match status {
        Status::Locked(_) => "Locked".to_string(),
        Status::PendingAt(_) => "Pending".to_string(),
        Status::BlockedByJoinSet(BlockedByJoinSet { closing: false, .. }) => {
            "BlockedByJoinSet".to_string()
        }
        Status::BlockedByJoinSet(BlockedByJoinSet { closing: true, .. }) => {
            "BlockedByJoinSetClosing".to_string()
        }
        Status::Finished(Finished { .. }) => {
            // Skip, the final result will be sent in the next message, since we set `send_finished_status` to true.
            return;
        }
    };
    if *old_pending_status != new_pending_status {
        println!("{new_pending_status}");
        let _ = std::mem::replace(old_pending_status, new_pending_status);
    }
}

fn print_finished_status(finished_status: grpc::FinishedStatus) -> anyhow::Result<()> {
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
        Some(grpc::result_detail::Value::Ok(grpc::result_detail::Ok {
            return_value: Some(return_value),
        })) => {
            let return_value = String::from_utf8_lossy(&return_value.value);
            (format!("OK: {return_value}"), Ok(()))
        }
        Some(grpc::result_detail::Value::Ok(grpc::result_detail::Ok { return_value: None })) => {
            ("OK: (no return value)".to_string(), Ok(()))
        }
        Some(grpc::result_detail::Value::FallibleError(grpc::result_detail::FallibleError {
            return_value: Some(return_value),
        })) => {
            let return_value = String::from_utf8_lossy(&return_value.value);
            (
                format!("Err: {return_value}"),
                Err(anyhow!("fallible error")),
            )
        }
        Some(grpc::result_detail::Value::Timeout(_)) => {
            ("Timeout".to_string(), Err(anyhow!("timeout")))
        }
        Some(grpc::result_detail::Value::ExecutionFailure(
            grpc::result_detail::ExecutionFailure {
                reason,
                detail: Some(detail),
            },
        )) => (
            format!("Execution failure: {reason}\ndetail: {detail}"),
            Err(anyhow!("failure")),
        ),
        Some(grpc::result_detail::Value::ExecutionFailure(
            grpc::result_detail::ExecutionFailure {
                reason,
                detail: None,
            },
        )) => (
            format!("Execution failure: {reason}"),
            Err(anyhow!("failure")),
        ),
        Some(grpc::result_detail::Value::NondeterminismDetected(
            grpc::result_detail::NondeterminismDetected { reason },
        )) => (
            format!("Nondeterminism detected: {reason}"),
            Err(anyhow!("nondeterminism")),
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

#[derive(Copy, Clone, PartialEq, Eq, Debug, Default)]
pub(crate) enum ExecutionVerbosity {
    #[default]
    PendingState,
    EventHistory,
    Full,
}

impl From<u8> for ExecutionVerbosity {
    fn from(value: u8) -> Self {
        match value {
            0 => ExecutionVerbosity::PendingState,
            1 => ExecutionVerbosity::EventHistory,
            _ => ExecutionVerbosity::Full,
        }
    }
}

pub(crate) async fn get(
    mut client: ExecutionRepositoryClient,
    execution_id: ExecutionId,
    follow: bool,
    _verbosity: ExecutionVerbosity, // TODO
) -> anyhow::Result<()> {
    let mut stream = client
        .get_status(tonic::Request::new(grpc::GetStatusRequest {
            execution_id: Some(grpc::ExecutionId::from(execution_id)),
            follow,
            send_finished_status: true,
        }))
        .await
        .to_anyhow()?
        .into_inner();
    let mut pending_status_cache = String::new();
    while let Some(status) = stream.message().await? {
        print_status(status, &mut pending_status_cache)?;
    }
    Ok(())
}
