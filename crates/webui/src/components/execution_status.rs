use crate::grpc::grpc_client::{
    self,
    execution_status::{Finished, Locked, PendingAt},
    get_status_response, ExecutionStatus as GExecutionStatus, ExecutionSummary, FinishedStatus,
    ResultKind,
};
use chrono::DateTime;
use log::{error, trace};
use std::ops::Deref;
use yew::prelude::*;

#[derive(Properties, PartialEq)]
pub struct ExecutionStatusProps {
    pub status: Option<grpc_client::execution_status::Status>,
    pub execution_id: grpc_client::ExecutionId,
    #[prop_or_default]
    pub print_finished_status: bool,
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
        status.clone().map(|s| {
            get_status_response::Message::CurrentStatus(GExecutionStatus { status: Some(s) })
        })
    });
    {
        // Subscribe to GetStatus if needed.
        let status_state = status_state.clone();
        let execution_id = execution_id.clone();
        use_effect_with(execution_id.clone(), move |_| {
            if !is_finished || print_finished_status {
                trace!("<ExecutionStatus /> Subscribing to status of {execution_id}");
                wasm_bindgen_futures::spawn_local(async move {
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
                    loop {
                        match response_stream.message().await {
                            Ok(Some(status)) => {
                                let status = status
                                    .message
                                    .expect("GetStatusResponse.message is sent by the server");
                                trace!("<ExecutionStatus /> Got {status:?}");
                                status_state.set(Some(status));
                            }
                            Ok(None) => break,
                            Err(err) => {
                                error!("Error wile listening to status updates: {err:?}");
                                break;
                            }
                        }
                    }
                })
            }
        });
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
            let render_result = |return_value: &Option<prost_wkt_types::Any>| {
                let return_value = return_value
                    .as_ref()
                    .map(|return_value| String::from_utf8_lossy(&return_value.value));
                html! {
                    if let Some(return_value) = return_value {
                        {return_value}
                    } else {
                        {"None"}
                    }
                }
            };

            let result = match &result_detail.value {
                Some(grpc_client::result_detail::Value::Ok(grpc_client::result_detail::Ok {
                    return_value,
                })) => {
                    html! {<>
                        <span>{"OK"}</span>
                        <span style="color:green">
                            {render_result(return_value)}
                        </span>
                    </>}
                }
                Some(grpc_client::result_detail::Value::FallibleError(
                    grpc_client::result_detail::FallibleError { return_value },
                )) => {
                    html! {<>
                        <span>{"Err"}</span>
                        <span style="color:red">
                            {render_result(return_value)}
                        </span>
                    </>}
                }
                Some(grpc_client::result_detail::Value::Timeout(_)) => {
                    html! {<>
                        <span>{"Timeout"}</span>
                    </>}
                }
                Some(grpc_client::result_detail::Value::ExecutionFailure(
                    grpc_client::result_detail::ExecutionFailure { reason },
                )) => {
                    html! {<>
                        <span>{"Execution failure"}</span>
                        <span style="color:red">
                            {reason}
                        </span>
                    </>}
                }
                Some(grpc_client::result_detail::Value::NondeterminismDetected(
                    grpc_client::result_detail::NondeterminismDetected { reason },
                )) => {
                    html! {<>
                        <span>{"Nondeterminism detected"}</span>
                        <span style="color:red">
                            {reason}
                        </span>
                    </>}
                }
                other => unreachable!("unexpected variant {other:?}"),
            };

            let finished_at = DateTime::from(*finished_at);
            let scheduled_at = DateTime::from(*scheduled_at);
            let since_scheduled = (finished_at - scheduled_at)
                .to_std()
                .expect("must be non-negative");
            html! {<>
                <p>{result}</p>
                <p>{format!("Execution finished in {since_scheduled:?} after it was scheduled")}</p>
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
            match ResultKind::try_from(*result_kind).expect("TODO") {
                ResultKind::Ok => html! {"Finished OK"},
                ResultKind::Timeout => {
                    html! {"Finished with Timeout"}
                }
                ResultKind::NondeterminismDetected => {
                    html! {"Nondeterminism detected"}
                }
                ResultKind::ExecutionFailure => {
                    html! {"Execution failure"}
                }
                ResultKind::FallibleError => {
                    html! {"Finished with Err variant"}
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
