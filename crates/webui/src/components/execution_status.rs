use crate::{
    components::{execution_detail::finished::FinishedEvent, execution_header::ExecutionLink},
    grpc::grpc_client::{
        self, ExecutionStatus as GExecutionStatus, ExecutionSummary, FinishedStatus, ResultKind,
        execution_status::{Finished, Locked, PendingAt},
        get_status_response,
    },
    util::trace_id,
};
use chrono::DateTime;
use futures::FutureExt as _;
use log::{debug, error, trace};
use std::ops::Deref;
use yew::prelude::*;

#[derive(Properties, PartialEq)]
pub struct ExecutionStatusProps {
    pub status: Option<grpc_client::execution_status::Status>,
    pub execution_id: grpc_client::ExecutionId,
    pub print_finished_status: bool,
}

fn status_as_message(
    status: Option<&grpc_client::execution_status::Status>,
) -> Option<get_status_response::Message> {
    status.map(|s| {
        get_status_response::Message::CurrentStatus(GExecutionStatus {
            status: Some(s.clone()),
        })
    })
}

#[function_component(ExecutionStatus)]
pub fn execution_status(
    ExecutionStatusProps {
        status,
        execution_id,
        print_finished_status,
    }: &ExecutionStatusProps,
) -> Html {
    let print_finished_status = *print_finished_status;
    let is_finished = matches!(
        status,
        Some(grpc_client::execution_status::Status::Finished(_))
    );
    // If the status was passed in props, store it inside `status_state`.
    let status_state = use_state(move || {
        debug!("ExecutionStatus {execution_id} use_state status:{status:?}");
        status_as_message(status.as_ref())
    });
    {
        // Subscribe to GetStatus if needed.
        let status_state = status_state.clone();
        let execution_id = execution_id.clone();
        let connectin_id = trace_id();
        use_effect_with(
            (execution_id.clone(), status.clone()),
            move |(execution_id, status)| {
                let execution_id = execution_id.clone();
                status_state.set(status_as_message(status.as_ref())); // use_state is not called when parameters change.
                let (cancel_tx, cancel_rx) = if !is_finished || print_finished_status {
                    let (tx, rx) = futures::channel::oneshot::channel();
                    (Some(tx), Some(rx))
                } else {
                    (None, None)
                };

                if let Some(cancel_rx) = cancel_rx {
                    debug!(
                        "[{connectin_id}] <ExecutionStatus /> Subscribing to status of {execution_id}"
                    );
                    wasm_bindgen_futures::spawn_local({
                        let status_state = status_state.clone();
                        let connection_id = connectin_id.clone();
                        let execution_id = execution_id.clone();
                        async move {
                            let base_url = "/api";
                            let mut execution_client =
                        grpc_client::execution_repository_client::ExecutionRepositoryClient::new(
                            tonic_web_wasm_client::Client::new(base_url.to_string()),
                        );
                            let mut response_stream = execution_client
                                .get_status(grpc_client::GetStatusRequest {
                                    execution_id: Some(execution_id),
                                    follow: true,
                                    send_finished_status: print_finished_status,
                                })
                                .await
                                .unwrap()
                                .into_inner();
                            let mut cancel_rx = cancel_rx.fuse();
                            loop {
                                let next_message = futures::select! {
                                    next_message = response_stream.message().fuse() => next_message,
                                    _ =  &mut cancel_rx => break,
                                };
                                match next_message {
                                    Ok(Some(status)) => {
                                        let status = status.message.expect(
                                            "GetStatusResponse.message is sent by the server",
                                        );
                                        trace!(
                                            "[{connection_id}] <ExecutionStatus /> Got {status:?}"
                                        );
                                        status_state.set(Some(status));
                                    }
                                    Ok(None) => break,
                                    Err(err) => {
                                        error!(
                                            "[{connection_id}] Error wile listening to status updates: {err:?}"
                                        );
                                        break;
                                    }
                                }
                            }
                            debug!("[{connection_id}] <ExecutionStatus /> Ended subscription");
                        }
                    })
                }
                move || {
                    debug!("Cleaning up {execution_id}");
                    if let Some(cancel_tx) = cancel_tx {
                        let res = cancel_tx.send(());
                        debug!("[{connectin_id}] <ExecutionStatus /> cacelling: {res:?}");
                    }
                    status_state.set(None);
                }
            },
        );
    }
    // Render `status_state`.
    match status_state.deref() {
        None => {
            html! {
                {"Loading..."}
            }
        }
        Some(get_status_response::Message::Summary(ExecutionSummary {
            current_status:
                Some(GExecutionStatus {
                    status: Some(status),
                }),
            ..
        }))
        | Some(get_status_response::Message::CurrentStatus(GExecutionStatus {
            status: Some(status),
        })) => status_to_string(status),
        Some(get_status_response::Message::FinishedStatus(FinishedStatus {
            created_at: _,
            scheduled_at: Some(scheduled_at),
            finished_at: Some(finished_at),
            result_detail: Some(result_detail),
        })) => {
            let finished_at = DateTime::from(*finished_at);
            let scheduled_at = DateTime::from(*scheduled_at);
            let since_scheduled = (finished_at - scheduled_at)
                .to_std()
                .expect("must be non-negative");
            html! {<>
                <FinishedEvent result_detail={result_detail.clone()} version={None} link={ExecutionLink::Trace} is_selected={false}/>
                <p>{format!("Execution completed in {since_scheduled:?}.")}</p>
            </>}
        }
        Some(unknown) => unreachable!("unexpected {unknown:?}"),
    }
}

fn status_to_string(status: &grpc_client::execution_status::Status) -> Html {
    match status {
        grpc_client::execution_status::Status::Locked(Locked {
            lock_expires_at, ..
        }) => html! {
            format!("Locked{}", convert_date(" until ", lock_expires_at.as_ref()))
        },
        grpc_client::execution_status::Status::PendingAt(PendingAt { scheduled_at }) => html! {
            format!("Pending{}", convert_date(" at ", scheduled_at.as_ref()))
        },
        grpc_client::execution_status::Status::BlockedByJoinSet(_) => {
            html! { "Blocked by join set"}
        }
        grpc_client::execution_status::Status::Finished(Finished { result_kind, .. }) => {
            match ResultKind::try_from(*result_kind)
                .expect("ResultKind must be convertible from i32")
            {
                ResultKind::Ok => html! {"Finished OK"},
                ResultKind::FallibleError => {
                    html! {"Finished with Err variant"}
                }
                ResultKind::Timeout => {
                    html! {"Finished with Timeout"}
                }
                ResultKind::ExecutionFailure => {
                    html! {"Execution failure"}
                }
                ResultKind::UnhandledChildExecutionError => {
                    html! {"Unhandled child execution error"}
                }
            }
        }
    }
}

fn convert_date(prefix: &str, date: Option<&::prost_wkt_types::Timestamp>) -> String {
    date.map(|date| {
        let date = DateTime::from(*date);
        format!("{prefix}{date:?}")
    })
    .unwrap_or_default()
}
