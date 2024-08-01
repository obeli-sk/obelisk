use super::grpc;
use super::grpc::scheduler_client::SchedulerClient;
use anyhow::Context;
use concepts::storage::ExecutionEvent;
use concepts::storage::ExecutionEventInner;
use concepts::storage::ExecutionLog;
use concepts::SupportedFunctionResult;
use concepts::{ExecutionId, FunctionFqn};
use std::str::FromStr;
use tonic::transport::Channel;
use tracing::trace;
use val_json::wast_val::WastVal;
use val_json::wast_val::WastValWithType;

pub(crate) async fn submit(
    mut client: SchedulerClient<Channel>,
    ffqn: FunctionFqn,
    params: Vec<serde_json::Value>,
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
        .await?;
    trace!("{resp:?}");
    let execution_id = resp
        .into_inner()
        .execution_id
        .context("response field `execution_id` must be present")
        .map(|execution_id| {
            ExecutionId::from_str(&execution_id.id).context("cannot parse `execution_id`")
        })??;
    println!("{execution_id}\nWaiting for result...");
    let mut stream = client
        .stream_status(tonic::Request::new(grpc::StreamStatusRequest {
            execution_id: Some(execution_id.into()),
        }))
        .await
        .context("cannot stream response status")?
        .into_inner();
    while let Some(response) = stream.message().await? {
        println!("{:?}", response.status);
    }
    // let execution_log = db_connection
    //     .wait_for_pending_state(execution_id, PendingState::Finished, None)
    //     .await?;
    // print_result_if_finished(&execution_log);
    Ok(())
}

fn print_result_if_finished(execution_log: &ExecutionLog) -> Option<()> {
    let finished = execution_log.finished_result();
    if let Some(res) = finished {
        let first_locked_at = execution_log
            .events
            .iter()
            .find(|event| matches!(event.event, ExecutionEventInner::Locked { .. }))
            .expect("must have been locked")
            .created_at;
        let duration = (execution_log.last_event().created_at - first_locked_at)
            .to_std()
            .unwrap();

        match res {
            Ok(
                res @ (SupportedFunctionResult::None
                | SupportedFunctionResult::Infallible(_)
                | SupportedFunctionResult::Fallible(WastValWithType {
                    value: WastVal::Result(Ok(_)),
                    ..
                })),
            ) => {
                println!("Finished OK");
                let value = match res {
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
                println!("Finished with an error\n{res:?}");
            }
        }
        println!("Execution took {duration:?}");
    }
    finished.map(|_| ())
}

#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub(crate) enum ExecutionVerbosity {
    EventHistory,
    Full,
}

pub(crate) async fn get(
    mut client: SchedulerClient<Channel>,
    execution_id: ExecutionId,
    verbosity: Option<ExecutionVerbosity>,
) -> anyhow::Result<()> {
    let status = client
        .get_status(grpc::GetStatusRequest {
            execution_id: Some(execution_id.into()),
        })
        .await?;
    println!("{status:?}");
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
