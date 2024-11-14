use crate::grpc::grpc_client::{
    self,
    execution_status::{Finished, Locked, PendingAt},
    get_status_response, ExecutionSummary, FinishedStatus, ResultKind,
};
use chrono::DateTime;
use log::debug;
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
    let status = status.as_ref();
    let is_finished = matches!(
        status,
        Some(grpc_client::execution_status::Status::Finished(_))
    );
    let status_state = use_state(move || status.map(status_to_string));
    {
        let status_state = status_state.clone();
        let execution_id = execution_id.clone();
        use_effect_with((), move |_x| {
            if !is_finished || print_finished_status {
                debug!("Subscribing to status of {execution_id}");
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
                    while let Some(status) = response_stream
                        .message()
                        .await
                        .expect("TODO error handling")
                    {
                        let status = status
                            .message
                            .expect("GetStatusResponse.message is sent by the server");
                        debug!("Got {status:?}");
                        status_state.set(Some(match status {
                            get_status_response::Message::Summary(ExecutionSummary {
                                current_status: Some(status),
                                ..
                            })
                            | get_status_response::Message::CurrentStatus(status) => {
                                status_to_string(
                                    &status
                                        .status
                                        .expect("ExecutionStatus.status is sent by the server"),
                                )
                            }
                            get_status_response::Message::FinishedStatus(FinishedStatus {
                                result,
                                created_at,
                                finished_at,
                                result_kind,
                            }) => {
                                let result_kind = ResultKind::try_from(result_kind)
                                    .expect("ResultKind must be known");
                                let result = result
                                    .map(|r| String::from_utf8(r.value).expect("must be UTF-8"))
                                    .unwrap_or("(no value)".to_string());
                                let finished_at = DateTime::from(
                                    finished_at.expect("finished_at is sent by the server"),
                                );
                                let created_at = DateTime::from(
                                    created_at.expect("created_at is sent by the server"),
                                );
                                let since_created = (finished_at - created_at)
                                    .to_std()
                                    .expect("must be non-negative");
                                html! {<>
                                    <p>
                                    {format!("{result_kind:?} {result}")}
                                    </p>
                                    <p>{format!("Execution took: {since_created:?}, created at: {created_at:?}, finished at: {finished_at}")}</p>
                                </>}
                            }
                            _ => unreachable!("?"),
                        }));
                    }
                })
            }
        });
    }
    if let Some(status) = status_state.deref() {
        status.clone()
    } else {
        html! {
            {"Loading..."}
        }
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
