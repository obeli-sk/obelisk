use super::grpc;
use super::grpc::execution_status::BlockedByJoinSet;
use super::grpc::GetLastBacktraceResponse;
use crate::command::grpc::execution_status::Finished;
use crate::grpc_util::grpc_mapping::TonicClientResultExt;
use crate::ExecutionRepositoryClient;
use anyhow::anyhow;
use anyhow::Context;
use chrono::DateTime;
use concepts::{ExecutionId, FunctionFqn};
use crossterm::{
    cursor,
    style::{Color, Print, ResetColor, SetBackgroundColor, SetForegroundColor},
    terminal::{self, ClearType},
    ExecutableCommand,
};
use grpc::execution_status::Status;
use serde_json::json;
use std::io::Stdout;
use std::io::{stdout, Write};
use std::path::PathBuf;
use std::str::FromStr;
use syntect::easy::HighlightLines;
use syntect::highlighting::{Style, ThemeSet};
use syntect::parsing::SyntaxSetBuilder;
use syntect::util::as_24_bit_terminal_escaped;
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
        if json_output {
            get_json(client, execution_id, true, true).await?;
        } else {
            get(client, execution_id, true).await?;
        }
    }
    if json_output {
        println!("\n]");
    }
    Ok(())
}

/// Return true if the status is Finished.
fn print_status(
    response: grpc::GetStatusResponse,
    old_pending_status: &mut String,
) -> anyhow::Result<bool> {
    use grpc::get_status_response::Message;
    let message = response.message.expect("message expected");
    match message {
        Message::Summary(summary) => {
            let ffqn = FunctionFqn::try_from(summary.function_name.expect("sent by server"))
                .expect("ffqn sent by the server must be valid");
            println!("Function: {ffqn}");

            print_pending_status(
                summary.current_status.expect("sent by server"),
                old_pending_status,
            );
            Ok(false)
        }
        Message::CurrentStatus(pending_status) => {
            print_pending_status(pending_status, old_pending_status);
            Ok(false)
        }
        Message::FinishedStatus(finished_sattus) => {
            print_finished_status(finished_sattus)?;
            Ok(true)
        }
    }
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

pub(crate) async fn get_json(
    mut client: ExecutionRepositoryClient,
    execution_id: ExecutionId,
    follow: bool,
    json_output_started: bool, // true if `[{..}` was printed alraedy.
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
    if !json_output_started {
        println!("[");
    }
    while let Some(status) = stream.message().await? {
        print_status_json(status, &mut old_pending_status, json_output_started)?;
    }
    if !json_output_started {
        println!("\n]");
    }
    Ok(())
}

fn print_status_json(
    response: grpc::GetStatusResponse,
    old_pending_status: &mut String,
    json_output_started: bool,
) -> anyhow::Result<()> {
    use grpc::get_status_response::Message;
    let message = response.message.expect("message expected");

    if !old_pending_status.is_empty() || json_output_started {
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
    Ok(())
}

pub(crate) async fn get(
    mut client: ExecutionRepositoryClient,
    execution_id: ExecutionId,
    follow: bool,
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

    let mut stdout = stdout();
    let (_terminal_cols, terminal_rows) = terminal::size()?;

    while let Some(status) = stream.message().await? {
        // Move to (0, 0).
        stdout.execute(cursor::MoveTo(0, 0))?;
        // Clear the ENTIRE screen, line by line.
        for _ in 0..terminal_rows {
            stdout.execute(terminal::Clear(ClearType::CurrentLine))?;
            stdout.execute(cursor::MoveDown(1))?;
        }
        stdout.execute(cursor::MoveTo(0, 0))?;
        println!("{execution_id}");

        let finished = print_status(status, &mut old_pending_status)?;
        if finished {
            // Do not print last backtrace on finished.
            return Ok(());
        }
        if let Some(backtrace_response) = fetch_backtrace(&mut client, &execution_id).await? {
            let mut seen_lines = hashbrown::HashSet::new();
            println!("\nBacktrace:");
            for (i, frame) in backtrace_response
                .wasm_backtrace
                .expect("`wasm_backtrace` is sent")
                .frames
                .into_iter()
                .enumerate()
            {
                println!("{}. Module: {}", i, frame.module);
                if let Some(func_name) = &frame.func_name {
                    println!("   Function: {func_name}");
                }

                for (j, symbol) in frame.symbols.into_iter().enumerate() {
                    println!("   Symbol {j}:");
                    if let Some(func_name) = &symbol.func_name {
                        println!("     Function: {func_name}");
                    }

                    let location = match (&symbol.file, symbol.line, symbol.col) {
                        (Some(file), Some(line), Some(col)) => format!("{file}:{line}:{col}"),
                        (Some(file), Some(line), None) => format!("{file}:{line}"),
                        (Some(file), None, None) => file.clone(),
                        _ => "unknown location".to_string(),
                    };
                    // Print header.
                    stdout
                        .execute(SetForegroundColor(Color::Green))?
                        .execute(Print(format!("=== {location} ===\n")))?
                        .execute(ResetColor)?;
                    // Print source file.
                    if let (Some(file), Some(line)) = (&symbol.file, symbol.line) {
                        let new = seen_lines.insert((file.clone(), line));
                        if new {
                            if let Ok(source) = client
                                .get_backtrace_source(tonic::Request::new(
                                    // FIXME: Cache
                                    grpc::GetBacktraceSourceRequest {
                                        component_id: Some(
                                            backtrace_response
                                                .component_id
                                                .clone()
                                                .expect("`component_id` is sent"),
                                        ),
                                        file: file.clone(),
                                    },
                                ))
                                .await
                            {
                                let source = source.into_inner().content;
                                print_backtrace_with_content(
                                    &mut stdout,
                                    source,
                                    PathBuf::from(file).extension().and_then(|e| e.to_str()),
                                    usize::try_from(line).unwrap(),
                                    symbol.col.map(|col| usize::try_from(col).unwrap()),
                                )?;
                            }
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

fn print_backtrace_with_content(
    stdout: &mut Stdout,
    file_content: String,
    extension: Option<&str>,
    line: usize,
    col: Option<usize>,
) -> Result<(), anyhow::Error> {
    const BEFORE: usize = 2;
    const AFTER: usize = 2;

    let col = col.unwrap_or_default(); // 0 == not available

    let ts = ThemeSet::load_defaults();
    let mut builder = SyntaxSetBuilder::new();
    builder.add_plain_text_syntax();
    let ss = builder.build();

    let syntax = extension
        .and_then(|extension| ss.find_syntax_by_extension(extension))
        .unwrap_or_else(|| ss.find_syntax_plain_text());

    let theme = &ts.themes["base16-ocean.dark"];
    let mut h = HighlightLines::new(syntax, theme);

    let row = line - 1;

    let start_line = row.saturating_sub(BEFORE);
    let end_line = (row + AFTER + 1).min(file_content.lines().count());

    // --- Print Output ---

    for (line_number, line) in file_content
        .lines()
        .enumerate()
        .skip(start_line)
        .take(end_line.saturating_sub(start_line))
    {
        // Highlight only the line number.
        if line_number == row {
            stdout.execute(SetBackgroundColor(Color::White))?;
            stdout.execute(SetForegroundColor(Color::Black))?;
        }
        stdout.execute(Print(format!("{:>4} ", line_number + 1)))?;
        stdout.execute(ResetColor)?;

        let ranges: Vec<(Style, &str)> = h.highlight_line(line, &ss).unwrap();

        if line_number == row && col > 0 {
            // Highlight the character.
            let col = col - 1; // switch from 1-based.
            let mut current_col = 0;
            for (style, text) in ranges {
                if current_col <= col && col < current_col + text.len() {
                    let (before, rest) = text.split_at(col - current_col);
                    let (highlighted, after) = rest.split_at(1);

                    print!("{}", as_24_bit_terminal_escaped(&[(style, before)], false));
                    stdout.execute(SetBackgroundColor(Color::Yellow))?;
                    print!(
                        "{}",
                        as_24_bit_terminal_escaped(&[(style, highlighted)], false)
                    );
                    stdout.execute(ResetColor)?;
                    print!("{}", as_24_bit_terminal_escaped(&[(style, after)], false));
                } else {
                    print!("{}", as_24_bit_terminal_escaped(&[(style, text)], false));
                }
                current_col += text.len();
            }
            println!();
        } else {
            // Normal line.
            print!("{}", as_24_bit_terminal_escaped(&ranges[..], false));
            println!();
        }
    }
    stdout.flush()?;

    Ok(())
}

async fn fetch_backtrace(
    client: &mut ExecutionRepositoryClient,
    execution_id: &ExecutionId,
) -> anyhow::Result<Option<GetLastBacktraceResponse>> {
    let backtrace_response = client
        .get_last_backtrace(tonic::Request::new(grpc::GetLastBacktraceRequest {
            execution_id: Some(grpc::ExecutionId::from(execution_id)),
        }))
        .await;
    let backtrace_response = match backtrace_response {
        Err(status) if status.code() == tonic::Code::NotFound => {
            return Ok(None);
        }
        err @ Err(_) => return Err(err.to_anyhow().unwrap_err()),
        Ok(ok) => ok.into_inner(),
    };
    Ok(Some(backtrace_response))
}
