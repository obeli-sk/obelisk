use super::grpc;
use super::grpc::execution_status::BlockedByJoinSet;
use crate::command::grpc::execution_status::Finished;
use crate::grpc_util::grpc_mapping::TonicClientResultExt;
use crate::SchedulerClient;
use anyhow::Context;
use chrono::DateTime;
use concepts::FinishedExecutionResult;
use concepts::{ExecutionId, FunctionFqn};
use grpc::execution_status::Status;
use std::str::FromStr;
use tracing::instrument;

#[instrument(skip_all)]
pub(crate) async fn submit(
    mut client: SchedulerClient,
    ffqn: FunctionFqn,
    params: Vec<serde_json::Value>,
    follow: bool,
    verbosity: ExecutionVerbosity,
) -> anyhow::Result<()> {
    let resp = client
        .submit(tonic::Request::new(grpc::SubmitRequest {
            params: Some(prost_wkt_types::Any {
                type_url: format!("urn:obelisk:json:params:{ffqn}"),
                value: serde_json::Value::Array(params).to_string().into_bytes(),
            }),
            function: Some(ffqn.into()),
            execution_id: None,
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
) -> Result<(), anyhow::Error> {
    use grpc::get_status_response::Message;
    match response.message {
        Some(Message::Summary(summary)) => {
            if let Some(ffqn) = summary.function_name.map(FunctionFqn::from) {
                println!("Function: {ffqn}");
            }
            if let Some(pending_status) = summary.current_status {
                print_pending_status(pending_status, old_pending_status)?;
            }
        }
        Some(Message::CurrentStatus(pending_status)) => {
            print_pending_status(pending_status, old_pending_status)?;
        }
        None => {}
    }
    Ok(())
}

fn print_pending_status(
    pending_status: grpc::ExecutionStatus,
    old_pending_status: &mut String,
) -> Result<(), anyhow::Error> {
    let Some(status) = pending_status.status else {
        return Ok(());
    };
    let new_pending_status = match status {
        Status::Locked(_) => "Locked".to_string(),
        Status::PendingAt(_) => "Pending".to_string(),
        Status::BlockedByJoinSet(BlockedByJoinSet { closing: false, .. }) => {
            "BlockedByJoinSet".to_string()
        }
        Status::BlockedByJoinSet(BlockedByJoinSet { closing: true, .. }) => {
            "BlockedByJoinSetClosing".to_string()
        }
        Status::Finished(Finished {
            result,
            created_at,
            finished_at,
            result_kind,
        }) => {
            let created_at = DateTime::from(created_at.context("`created_at` must exist")?);
            let finished_at = DateTime::from(finished_at.context("`finished_at` must exist")?);
            let result = String::from_utf8(result.context("`result` must exist")?.value)
                .context("`result` must be UTF-8 encoded")?;
            let result_kind = grpc::execution_status::ResultKind::try_from(result_kind)
                .expect("must be convertible back to ResultKind");
            let result: FinishedExecutionResult =
                serde_json::from_str(&result).context("cannot deserialize `result`")?;
            let mut new_pending_status = match &result {
                Ok(ret_val) => {
                    let mut new_pending_status =
                        format!("Execution finished: {}", result_kind.as_str_name());
                    let val = serde_json::to_string_pretty(&ret_val.value()).unwrap();
                    new_pending_status.push_str(&format!("\n{val}"));
                    new_pending_status
                }
                Err(err) => {
                    format!("Execution error - {err}")
                }
            };
            new_pending_status.push_str(&format!(
                "\nExecution took {since_created:?}.",
                since_created = (finished_at - created_at)
                    .to_std()
                    .expect("must be non-negative")
            ));
            new_pending_status
        }
    };
    if *old_pending_status != new_pending_status {
        println!("{new_pending_status}");
        let _ = std::mem::replace(old_pending_status, new_pending_status);
    }
    Ok(())
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
    mut client: SchedulerClient,
    execution_id: ExecutionId,
    follow: bool,
    _verbosity: ExecutionVerbosity, // TODO
) -> anyhow::Result<()> {
    let mut stream = client
        .get_status(tonic::Request::new(grpc::GetStatusRequest {
            execution_id: Some(execution_id.into()),
            follow,
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
