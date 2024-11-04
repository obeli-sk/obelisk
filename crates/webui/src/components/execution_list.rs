use crate::grpc_client::{self, get_status_response, ExecutionSummary, GetStatusResponse};
use log::debug;
use std::ops::Deref;
use yew::prelude::*;

#[derive(Properties, PartialEq)]
pub struct ExecutionStatusProps {
    pub status: grpc_client::execution_status::Status,
    pub execution_id: grpc_client::ExecutionId,
}
#[function_component(ExecutionStatus)]
pub fn execution_status(
    ExecutionStatusProps {
        status,
        execution_id,
    }: &ExecutionStatusProps,
) -> Html {
    let status = status.clone();
    let is_finished = matches!(status, grpc_client::execution_status::Status::Finished(_));
    let status_state = use_state(move || status);
    {
        let status_state = status_state.clone();
        let execution_id = execution_id.clone();
        use_effect_with((), move |_x| {
            if !is_finished {
                debug!("Subscribing to status of {}", execution_id.id);
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
                            send_finished_status: false,
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
                        match status {
                            get_status_response::Message::Summary(ExecutionSummary {
                                current_status: Some(status),
                                ..
                            })
                            | get_status_response::Message::CurrentStatus(status) => {
                                status_state.set(
                                    status
                                        .status
                                        .expect("ExecutionStatus.status is sent by the server"),
                                );
                            }
                            _ => {
                                unreachable!("send_finished_status is set to false, server sends ExecutionSummary.currentStatus")
                            }
                        }
                    }

                    // status_state.set(Some(response.executions));
                })
            }
        });
    }

    html! {
        {format!("{:?}", status_state.deref())}
    }
}
