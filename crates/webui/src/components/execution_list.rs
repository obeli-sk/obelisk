use crate::grpc_client;
use yew::prelude::*;

#[derive(Properties, PartialEq)]
pub struct ExecutionStatusProps {
    pub status: grpc_client::ExecutionStatus,
}
#[function_component(ExecutionStatus)]
pub fn execution_status(ExecutionStatusProps { status }: &ExecutionStatusProps) -> Html {
    let status = status
        .status
        .clone()
        .expect("`current_status.status` is sent by the server");
    html! {
        {format!("{:?}", status)}
    }
}
