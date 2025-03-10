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
use serde_json::json;
use std::str::FromStr;
use tracing::instrument;

// TODO: CamelCase + snake_case  mixed in JSON output

#[instrument(skip_all)]
pub(crate) async fn submit(
    mut client: ExecutionRepositoryClient,
    ffqn: FunctionFqn,
    params: Vec<serde_json::Value>,
    follow: bool,
    json_output: bool,
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
    if json_output {
        let json = json!(
            {"submit": {"execution_id": execution_id }}
        );
        println!("[{json}");
    } else {
        println!("{execution_id}");
    }
    if follow {
        let json_output_started = if json_output { Some(true) } else { None };
        get(client, execution_id, follow, json_output_started).await?;
    }
    if json_output {
        println!("\n]");
    }
    Ok(())
}

fn print_status(
    response: grpc::GetStatusResponse,
    old_pending_status: &mut String,
    json_output_started: Option<bool>, // None if json is disabled. Some(true) if `[{..}` was printed alraedy.
) -> anyhow::Result<()> {
    use grpc::get_status_response::Message;
    let message = response.message.expect("message expected");
    if json_output_started.is_some() {
        if !old_pending_status.is_empty() || json_output_started == Some(true) {
            println!(",");
        }
        *old_pending_status = match message {
            Message::Summary(_) => {
                serde_json::to_string(&message).expect("summary must be serializable")
            }
            Message::CurrentStatus(_) => {
                serde_json::to_string(&message).expect("pending_status must be serializable")
            }
            Message::FinishedStatus(finished_sattus) => print_finished_status_json(finished_sattus),
        };

        print!("{old_pending_status}");
    } else {
        match message {
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
    }
    Ok(())
}

fn print_pending_status(pending_status: grpc::ExecutionStatus, old_pending_status: &mut String) {
    let status = pending_status.status.expect("status is sent by the server");
    let new_pending_status = match status {
        Status::Locked(_) => "Locked".to_string(),
        Status::PendingAt(_) => "Pending".to_string(),
        Status::BlockedByJoinSet(BlockedByJoinSet {
            closing,
            join_set_id: Some(grpc::JoinSetId { name, kind }),
            lock_expires_at: _,
        }) => {
            let kind = grpc::join_set_id::JoinSetKind::try_from(kind)
                .expect("JoinSetKind must be valid")
                .as_str_name()
                .to_lowercase();

            let closing = if closing { "-closing" } else { "" };
            format!("BlockedByJoinSet {kind} `{name}`{closing}")
        }
        Status::Finished(Finished { .. }) => {
            // Skip, the final result will be sent in the next message, since we set `send_finished_status` to true.
            return;
        }
        illegal @ Status::BlockedByJoinSet(_) => panic!("illegal state {illegal:?}"),
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
        Some(grpc::result_detail::Value::UnhandledChildExecutionError(
            grpc::result_detail::UnhandledChildExecutionError {
                child_execution_id,
                root_cause_id,
            },
        )) => (
            format!(
                "Unhandled child execution, child_execution_id: {child_execution_id}, root_cause_id: {root_cause_id}",
                child_execution_id = child_execution_id.unwrap().id,
                root_cause_id = root_cause_id.unwrap().id
            ),
            Err(anyhow!("unhandled child execution error")),
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

fn print_finished_status_json(finished_status: grpc::FinishedStatus) -> String {
    let created_at = finished_status
        .created_at
        .expect("`created_at` is sent by the server");
    let scheduled_at = finished_status
        .scheduled_at
        .expect("`scheduled_at` is sent by the server");
    let finished_at = finished_status
        .finished_at
        .expect("`finished_at` is sent by the server");

    let mut json = match finished_status
        .result_detail
        .expect("`result_detail` is sent by the server")
        .value
    {
        Some(grpc::result_detail::Value::Ok(grpc::result_detail::Ok {
            return_value: Some(return_value),
        })) => {
            let return_value: serde_json::Value = serde_json::from_slice(&return_value.value)
                .expect("return_value must be JSON encoded");
            json!({"ok": return_value})
        }
        Some(grpc::result_detail::Value::Ok(grpc::result_detail::Ok { return_value: None })) => {
            json!({"ok": null})
        }
        Some(grpc::result_detail::Value::FallibleError(grpc::result_detail::FallibleError {
            return_value: Some(return_value),
        })) => {
            let return_value: serde_json::Value = serde_json::from_slice(&return_value.value)
                .expect("return_value must be JSON encoded");
            json!({"fallible_error": return_value})
        }
        Some(grpc::result_detail::Value::Timeout(_)) => json!({"timeout": null}),
        Some(grpc::result_detail::Value::ExecutionFailure(
            grpc::result_detail::ExecutionFailure { reason, detail },
        )) => json!({"execution_failure": {"reason": reason, "detail": detail}}),
        Some(grpc::result_detail::Value::UnhandledChildExecutionError(
            grpc::result_detail::UnhandledChildExecutionError {
                child_execution_id,
                root_cause_id,
            },
        )) => json!({"unhandled_child_execution_error": {"child_execution_id": child_execution_id,
                "root_cause_id": root_cause_id}}),
        other => unreachable!("unexpected variant {other:?}"),
    };
    json["created_at"] = serde_json::Value::String(created_at.to_string());
    json["scheduled_at"] = serde_json::Value::String(scheduled_at.to_string());
    json["finished_at"] = serde_json::Value::String(finished_at.to_string());
    json.to_string()
}

pub(crate) async fn get(
    mut client: ExecutionRepositoryClient,
    execution_id: ExecutionId,
    follow: bool,
    json_output_started: Option<bool>, // None if json is disabled. Some(true) if `[{..}` was printed alraedy.
) -> anyhow::Result<()> {
    let mut stream = client
        .get_status(tonic::Request::new(grpc::GetStatusRequest {
            execution_id: Some(grpc::ExecutionId::from(execution_id.clone())),
            follow,
            send_finished_status: true,
        }))
        .await
        .to_anyhow()?
        .into_inner();
    let mut old_pending_status = String::new();
    if json_output_started == Some(false) {
        println!("[");
    }
    while let Some(status) = stream.message().await? {
        print_status(status, &mut old_pending_status, json_output_started)?;
        if json_output_started.is_none() {
            fetch_backtrace(&mut client, &execution_id).await?;
        }
    }
    if json_output_started == Some(false) {
        println!("\n]");
    }
    Ok(())
}

async fn fetch_backtrace(
    client: &mut ExecutionRepositoryClient,
    execution_id: &ExecutionId,
) -> anyhow::Result<()> {
    let backtrace_response = client
        .get_last_backtrace(tonic::Request::new(grpc::GetLastBacktraceRequest {
            execution_id: Some(grpc::ExecutionId::from(execution_id)),
        }))
        .await;
    let backtrace_response = match backtrace_response {
        Err(status) if status.code() == tonic::Code::NotFound => {
            return Ok(());
        }
        err @ Err(_) => return Err(err.to_anyhow().unwrap_err()),
        Ok(ok) => ok.into_inner(),
    };

    // let backtrace_response = backtrace_response.to_anyhow()?.into_inner();
    if let Some(backtrace) = backtrace_response.wasm_backtrace {
        print_backtrace(&backtrace);
    }
    Ok(())
}

fn print_backtrace(backtrace: &grpc::WasmBacktrace) {
    println!("\nBacktrace:");
    for (i, frame) in backtrace.frames.iter().enumerate() {
        println!("{}. Module: {}", i, frame.module);
        if let Some(func_name) = &frame.func_name {
            println!("   Function: {}", func_name);
        }

        for (j, symbol) in frame.symbols.iter().enumerate() {
            println!("   Symbol {}:", j);
            if let Some(func_name) = &symbol.func_name {
                println!("     Function: {}", func_name);
            }

            let location = match (&symbol.file, symbol.line, symbol.col) {
                (Some(file), Some(line), Some(col)) => format!("{}:{}:{}", file, line, col),
                (Some(file), Some(line), None) => format!("{}:{}", file, line),
                (Some(file), None, None) => file.clone(),
                _ => "unknown location".to_string(),
            };
            println!("     Location: {}", location);
        }
    }
}
