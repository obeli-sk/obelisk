use super::grpc;
use super::grpc::scheduler_client::SchedulerClient;
use crate::command::grpc::execution_status::Finished;
use crate::grpc_util::grpc_mapping::TonicClientResultExt;
use anyhow::Context;
use chrono::DateTime;
use concepts::FinishedExecutionResult;
use concepts::SupportedFunctionReturnValue;
use concepts::{ExecutionId, FunctionFqn};
use grpc::execution_status::Status;
use std::str::FromStr;
use tonic::transport::Channel;
use val_json::wast_val::WastVal;
use val_json::wast_val::WastValWithType;

pub(crate) async fn submit(
    mut client: SchedulerClient<Channel>,
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

fn print_status(response: grpc::GetStatusResponse) -> Result<(), anyhow::Error> {
    use grpc::get_status_response::Message;
    match response.message {
        Some(Message::Summary(summary)) => {
            if let Some(ffqn) = summary.function_name.map(FunctionFqn::from) {
                println!("Function: {ffqn}");
            }
            if let Some(pending_status) = summary.current_status {
                print_pending_status(pending_status)?;
            }
        }
        Some(Message::CurrentStatus(pending_status)) => print_pending_status(pending_status)?,
        None => {}
    }
    Ok(())
}

fn print_pending_status(pending_status: grpc::ExecutionStatus) -> Result<(), anyhow::Error> {
    let Some(status) = pending_status.status else {
        return Ok(());
    };
    match status {
        Status::Locked(_) => println!("Locked"),
        Status::PendingAt(_) => println!("Pending"),
        Status::BlockedByJoinSet(_) => println!("BlockedByJoinSet"),
        Status::Finished(Finished {
            result,
            created_at,
            finished_at,
        }) => {
            let created_at = DateTime::from(created_at.context("`created_at` must exist")?);
            let finished_at = DateTime::from(finished_at.context("`finished_at` must exist")?);
            let result = String::from_utf8(result.context("`result` must exist")?.value)
                .context("`result` must be UTF-8 encoded")?;
            let result: FinishedExecutionResult =
                serde_json::from_str(&result).context("cannot deserialize `result`")?;
            match &result {
                Ok(
                    result @ (SupportedFunctionReturnValue::None
                    | SupportedFunctionReturnValue::Infallible(_)
                    | SupportedFunctionReturnValue::Fallible(WastValWithType {
                        value: WastVal::Result(Ok(_)),
                        ..
                    })),
                ) => {
                    println!("Finished OK");
                    let value = match result {
                        SupportedFunctionReturnValue::Infallible(WastValWithType { value, .. }) => {
                            Some(value)
                        }
                        SupportedFunctionReturnValue::Fallible(WastValWithType {
                            value: WastVal::Result(Ok(Some(value))),
                            ..
                        }) => Some(value.as_ref()),
                        _ => None,
                    };
                    if let Some(value) = value {
                        println!("{value:?}");
                    }
                }
                _ => {
                    println!("Finished with an error\n{result:?}");
                }
            }

            println!(
                "Execution took {since_created:?}.",
                since_created = (finished_at - created_at)
                    .to_std()
                    .expect("must be non-negative")
            );
        }
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
    mut client: SchedulerClient<Channel>,
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
    while let Some(status) = stream.message().await? {
        print_status(status)?;
    }
    Ok(())
}
