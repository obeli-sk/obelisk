use super::grpc;
use super::grpc::GetBacktraceResponse;
use super::grpc::execution_status::BlockedByJoinSet;
use crate::ExecutionRepositoryClient;
use crate::command::grpc::execution_status::Finished;
use anyhow::Context;
use chrono::DateTime;
use concepts::JOIN_SET_ID_INFIX;
use concepts::JoinSetKind;
use concepts::{ExecutionId, FunctionFqn};
use crossterm::{
    ExecutableCommand, cursor,
    style::{Color, Print, ResetColor, SetBackgroundColor, SetForegroundColor},
    terminal::{self, ClearType},
};
use grpc::execution_status::Status;
use itertools::Either;
use serde_json::json;
use std::io::Stdout;
use std::io::{Write, stdout};
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;
use syntect::easy::HighlightLines;
use syntect::highlighting::{Style, ThemeSet};
use syntect::parsing::SyntaxSetBuilder;
use syntect::util::as_24_bit_terminal_escaped;
use tracing::instrument;

#[derive(PartialEq)]
pub(crate) enum SubmitOutputOpts {
    Json,
    PlainFollow {
        no_backtrace: bool,
        no_reconnect: bool,
    },
}

#[instrument(skip_all)]
pub(crate) async fn submit(
    mut client: ExecutionRepositoryClient,
    ffqn: FunctionFqn,
    params: Vec<serde_json::Value>,
    follow: bool,
    opts: SubmitOutputOpts,
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
        .await?
        .into_inner();
    let execution_id = resp
        .execution_id
        .context("response field `execution_id` must be present")
        .map(|execution_id| {
            ExecutionId::from_str(&execution_id.id).context("cannot parse `execution_id`")
        })??;
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
            SubmitOutputOpts::PlainFollow {
                no_backtrace,
                no_reconnect,
            } => {
                let opts = GetStatusOptions {
                    follow: true,
                    no_backtrace,
                    no_reconnect,
                };
                get_status(client, execution_id, opts).await?;
            }
        }
    }
    Ok(())
}

/// Return true if the status is Finished.
fn print_status(response: grpc::GetStatusResponse) -> Result<bool, ExecutionError> {
    use grpc::get_status_response::Message;
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

fn format_pending_status(pending_status: grpc::ExecutionStatus) -> String {
    let status = pending_status.status.expect("status is sent by the server");
    match status {
        Status::Locked(_) => "Locked".to_string(),
        Status::PendingAt(grpc::execution_status::PendingAt { scheduled_at }) => {
            let scheduled_at = scheduled_at.expect("sent by the server");
            format!("Pending at {scheduled_at}")
        }
        Status::BlockedByJoinSet(BlockedByJoinSet {
            closing,
            join_set_id: Some(grpc::JoinSetId { name, kind }),
            lock_expires_at: _,
        }) => {
            let kind = JoinSetKind::from(
                grpc::join_set_id::JoinSetKind::try_from(kind).expect("JoinSetKind must be valid"),
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
    #[error("unhandled child execution error")]
    #[expect(clippy::enum_variant_names)]
    UnhandledChildExecutionError,
}

fn print_finished_status(finished_status: grpc::FinishedStatus) -> Result<(), ExecutionError> {
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
                Err(ExecutionError::FallibleError),
            )
        }
        Some(grpc::result_detail::Value::Timeout(_)) => {
            ("Timeout".to_string(), Err(ExecutionError::Timeout))
        }
        Some(grpc::result_detail::Value::ExecutionFailure(
            grpc::result_detail::ExecutionFailure {
                reason,
                detail: Some(detail),
            },
        )) => (
            format!("Execution failure: {reason}\ndetail: {detail}"),
            Err(ExecutionError::ExecutionFailure),
        ),
        Some(grpc::result_detail::Value::ExecutionFailure(
            grpc::result_detail::ExecutionFailure {
                reason,
                detail: None,
            },
        )) => (
            format!("Execution failure: {reason}"),
            Err(ExecutionError::ExecutionFailure),
        ),
        Some(grpc::result_detail::Value::UnhandledChildExecutionError(
            grpc::result_detail::UnhandledChildExecutionError {
                child_execution_id,
                root_cause_id,
            },
        )) => (
            format!(
                "Unhandled child execution error, child_execution_id: {child_execution_id}, root_cause_id: {root_cause_id}",
                child_execution_id = child_execution_id.unwrap().id,
                root_cause_id = root_cause_id.unwrap().id
            ),
            Err(ExecutionError::UnhandledChildExecutionError),
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

fn print_finished_status_json(finished_status: grpc::FinishedStatus) -> Result<(), ExecutionError> {
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
        Some(grpc::result_detail::Value::Ok(grpc::result_detail::Ok {
            return_value: Some(return_value),
        })) => {
            let return_value: serde_json::Value = serde_json::from_slice(&return_value.value)
                .expect("return_value must be JSON encoded");
            (json!({"ok": return_value}), Ok(()))
        }
        Some(grpc::result_detail::Value::Ok(grpc::result_detail::Ok { return_value: None })) => {
            (json!({"ok": null}), Ok(()))
        }
        Some(grpc::result_detail::Value::FallibleError(grpc::result_detail::FallibleError {
            return_value: Some(return_value),
        })) => {
            let return_value: serde_json::Value = serde_json::from_slice(&return_value.value)
                .expect("return_value must be JSON encoded");
            (
                json!({"fallible_error": return_value}),
                Err(ExecutionError::FallibleError),
            )
        }
        Some(grpc::result_detail::Value::Timeout(_)) => {
            (json!({"timeout": null}), Err(ExecutionError::Timeout))
        }
        Some(grpc::result_detail::Value::ExecutionFailure(
            grpc::result_detail::ExecutionFailure { reason, detail },
        )) => (
            json!({"execution_failure": {"reason": reason, "detail": detail}}),
            Err(ExecutionError::ExecutionFailure),
        ),
        Some(grpc::result_detail::Value::UnhandledChildExecutionError(
            grpc::result_detail::UnhandledChildExecutionError {
                child_execution_id,
                root_cause_id,
            },
        )) => (
            json!({"unhandled_child_execution_error": {"child_execution_id": child_execution_id,
                "root_cause_id": root_cause_id}}),
            Err(ExecutionError::UnhandledChildExecutionError),
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
        .get_status(tonic::Request::new(grpc::GetStatusRequest {
            execution_id: Some(grpc::ExecutionId::from(execution_id.clone())),
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

fn print_status_json(response: grpc::GetStatusResponse) -> Result<(), ExecutionError> {
    use grpc::get_status_response::Message;
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
    pub(crate) no_backtrace: bool,
    pub(crate) no_reconnect: bool,
}

pub(crate) async fn get_status(
    mut client: ExecutionRepositoryClient,
    execution_id: ExecutionId,
    opts: GetStatusOptions,
) -> anyhow::Result<()> {
    let mut stdout = stdout();
    let mut source_cache = hashbrown::HashMap::new();
    let reconnect = !opts.no_reconnect;
    loop {
        match poll_get_status_stream(
            &mut client,
            &execution_id,
            opts,
            &mut stdout,
            &mut source_cache,
        )
        .await
        {
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

fn clear_screen(stdout: &mut Stdout) -> anyhow::Result<()> {
    // Clear the screen.
    stdout.execute(terminal::Clear(ClearType::All))?;
    stdout.execute(cursor::MoveTo(0, 0))?;
    Ok(())
}

async fn poll_get_status_stream(
    client: &mut ExecutionRepositoryClient,
    execution_id: &ExecutionId,
    opts: GetStatusOptions,
    stdout: &mut Stdout,
    source_cache: &mut hashbrown::HashMap<String, String>,
) -> anyhow::Result<()> {
    let mut stream = client
        .get_status(tonic::Request::new(grpc::GetStatusRequest {
            execution_id: Some(grpc::ExecutionId::from(execution_id.clone())),
            follow: opts.follow,
            send_finished_status: true,
        }))
        .await?
        .into_inner();
    while let Some(status) = stream.message().await? {
        clear_screen(stdout)?;
        println!("{execution_id}");

        let finished = print_status(status)?;
        if finished {
            // Do not print last backtrace on finished.
            return Ok(());
        }
        if !opts.no_backtrace {
            if let Some(backtrace_response) = fetch_backtrace(client, execution_id).await? {
                let mut seen_positions = hashbrown::HashSet::new();
                let wasm_backtrace = backtrace_response
                    .wasm_backtrace
                    .expect("`wasm_backtrace` is sent");
                println!(
                    "\nBacktrace (version {}):",
                    wasm_backtrace.version_min_including
                );
                for (i, frame) in wasm_backtrace.frames.into_iter().enumerate() {
                    println!("{i}: {}, function: {}", frame.module, frame.func_name);

                    for symbol in frame.symbols {
                        // Print location.
                        let location = match (&symbol.file, symbol.line, symbol.col) {
                            (Some(file), Some(line), Some(col)) => format!("{file}:{line}:{col}"),
                            (Some(file), Some(line), None) => format!("{file}:{line}"),
                            (Some(file), None, None) => file.clone(),
                            _ => "unknown location".to_string(),
                        };
                        stdout
                            .execute(SetForegroundColor(Color::Green))?
                            .execute(Print(format!("    at {location}")))?
                            .execute(ResetColor)?;

                        // Print function name if it's different from frameinfo
                        match &symbol.func_name {
                            Some(func_name) if *func_name != frame.func_name => {
                                println!(" - {func_name}");
                            }
                            _ => println!(),
                        }

                        // Print source file.
                        if let (Some(file), Some(line)) = (&symbol.file, symbol.line) {
                            let new_position = seen_positions.insert((file.clone(), line));
                            if new_position {
                                let source = {
                                    if let Some(source) = source_cache.get(file.as_str()) {
                                        Some(source)
                                    } else if let Ok(source) = client
                                        .get_backtrace_source(tonic::Request::new(
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
                                        source_cache
                                            .insert(file.clone(), source.into_inner().content);
                                        source_cache.get(file.as_str())
                                    } else {
                                        None
                                    }
                                };
                                if let Some(source) = source {
                                    print_backtrace_with_content(
                                        stdout,
                                        source.as_str(),
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
    }
    Ok(())
}

fn print_backtrace_with_content(
    stdout: &mut Stdout,
    file_content: &str,
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
        stdout.execute(Print(format!("{:>8} ", line_number + 1)))?;
        stdout.execute(ResetColor)?;
        stdout.execute(Print(" "))?;

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
                    stdout.execute(SetBackgroundColor(Color::DarkGrey))?;
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
) -> anyhow::Result<Option<GetBacktraceResponse>> {
    let backtrace_response = client
        .get_backtrace(tonic::Request::new(grpc::GetBacktraceRequest {
            execution_id: Some(grpc::ExecutionId::from(execution_id)),
            filter: Some(grpc::get_backtrace_request::Filter::Last(
                grpc::get_backtrace_request::Last {},
            )),
        }))
        .await;
    let backtrace_response = match backtrace_response {
        Err(status) if status.code() == tonic::Code::NotFound => {
            return Ok(None);
        }
        Err(err) => return Err(anyhow::Error::from(err)),
        Ok(ok) => ok.into_inner(),
    };
    Ok(Some(backtrace_response))
}
