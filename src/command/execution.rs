use super::grpc;
use super::grpc::scheduler_client::SchedulerClient;
use crate::command::grpc::execution_status::Finished;
use crate::grpc_util::grpc_mapping::unwrap_friendly_resp;
use anyhow::Context;
use chrono::DateTime;
use concepts::FinishedExecutionResult;
use concepts::SupportedFunctionResult;
use concepts::{ExecutionId, FunctionFqn};
use grpc::execution_status::Status;
use std::str::FromStr;
use tonic::transport::Channel;
use tracing::trace;
use val_json::wast_val::WastVal;
use val_json::wast_val::WastValWithType;

pub(crate) async fn submit(
    mut client: SchedulerClient<Channel>,
    ffqn: FunctionFqn,
    params: Vec<serde_json::Value>,
    follow: bool,
) -> anyhow::Result<()> {
    let resp = client
        .submit(tonic::Request::new(grpc::SubmitRequest {
            function: Some(grpc::FunctionName {
                interface_name: ffqn.ifc_fqn.to_string(),
                function_name: ffqn.function_name.to_string(),
            }),
            params: Some(prost_wkt_types::Any {
                type_url: format!("urn:obelisk:json:params:{ffqn}"),
                value: serde_json::Value::Array(params).to_string().into_bytes(),
            }),
            execution_id: None,
        }))
        .await;
    trace!("{resp:?}");
    let resp = unwrap_friendly_resp(resp)?;
    let execution_id = resp
        .into_inner()
        .execution_id
        .context("response field `execution_id` must be present")
        .map(|execution_id| {
            ExecutionId::from_str(&execution_id.id).context("cannot parse `execution_id`")
        })??;
    println!("{execution_id} submitted");
    if follow {
        get(client, execution_id, follow, None).await?;
    }
    Ok(())
}

fn print_status(response: grpc::ExecutionStatus) -> Result<(), anyhow::Error> {
    match response.status.context("status field must exist")? {
        Status::Locked(_) => println!("Locked"),
        Status::PendingAt(_) => println!("Pending"),
        Status::BlockedByJoinSet(_) => println!("BlockedByJoinSet"),
        Status::Finished(Finished {
            result,
            created_at,
            first_locked_at,
            finished_at,
        }) => {
            let created_at = DateTime::from(created_at.context("`created_at` must exist")?);
            let first_locked_at =
                DateTime::from(first_locked_at.context("`first_locked_at` must exist")?);
            let finished_at = DateTime::from(finished_at.context("`finished_at` must exist")?);
            let result = String::from_utf8(result.context("`result` must exist")?.value)
                .context("`result` must be UTF-8 encoded")?;
            let result: FinishedExecutionResult =
                serde_json::from_str(&result).context("cannot deserialize `result`")?;
            match &result {
                Ok(
                    result @ (SupportedFunctionResult::None
                    | SupportedFunctionResult::Infallible(_)
                    | SupportedFunctionResult::Fallible(WastValWithType {
                        value: WastVal::Result(Ok(_)),
                        ..
                    })),
                ) => {
                    println!("Finished OK");
                    let value = match result {
                        SupportedFunctionResult::Infallible(WastValWithType { value, .. }) => {
                            Some(value)
                        }
                        SupportedFunctionResult::Fallible(WastValWithType {
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

            println!("Execution took {since_locked:?} since first locked, {since_created:?} since created.",
                since_locked = (finished_at - first_locked_at).to_std().expect("must be non-negative"),
                since_created = (finished_at - created_at).to_std().expect("must be non-negative")
            );
        }
    }
    Ok(())
}

#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub(crate) enum ExecutionVerbosity {
    EventHistory,
    Full,
}

pub(crate) async fn get(
    mut client: SchedulerClient<Channel>,
    execution_id: ExecutionId,
    follow: bool,
    verbosity: Option<ExecutionVerbosity>,
) -> anyhow::Result<()> {
    if follow {
        let mut stream = unwrap_friendly_resp(
            client
                .stream_status(tonic::Request::new(grpc::StreamStatusRequest {
                    execution_id: Some(execution_id.into()),
                }))
                .await,
        )?
        .into_inner();
        while let Some(status) = stream.message().await? {
            print_status(status)?;
        }
    } else {
        let status = unwrap_friendly_resp(
            client
                .get_status(grpc::GetStatusRequest {
                    execution_id: Some(execution_id.into()),
                })
                .await,
        )?
        .into_inner()
        .status
        .context("`status` must exist")?;
        print_status(status)?;
    }
    // println!("Function: {}", execution_log.ffqn());
    // if print_result_if_finished(&execution_log).is_none() {
    //     println!("Current state: {}", execution_log.pending_state);
    // }
    // if let Some(verbosity) = verbosity {
    //     println!();
    //     println!("Event history:");
    //     for event in execution_log.event_history() {
    //         println!("{event}");
    //     }
    //     if verbosity == ExecutionVerbosity::Full {
    //         println!();
    //         println!("Execution log:");
    //         for ExecutionEvent { created_at, event } in execution_log.events {
    //             println!("{created_at}\t{event}");
    //         }
    //     }
    // }
    Ok(())
}
