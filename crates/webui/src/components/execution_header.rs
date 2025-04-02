use crate::app::Route;
use crate::components::execution_status::ExecutionStatus;
use crate::grpc::execution_id::ExecutionIdExt;
use crate::grpc::grpc_client::ExecutionId;
use yew::prelude::*;

#[derive(Properties, PartialEq)]
pub struct ExecutionHeaderProps {
    pub execution_id: ExecutionId,
    pub route_fn: Callback<ExecutionId, Route>,
}
#[function_component(ExecutionHeader)]
pub fn execution_header(
    ExecutionHeaderProps {
        execution_id,
        route_fn,
    }: &ExecutionHeaderProps,
) -> Html {
    html! {
        <div class="execution-header">
            <h3>{ execution_id.render_execution_parts(false, route_fn.clone()) }</h3>
            <ExecutionStatus execution_id={execution_id.clone()} status={None} print_finished_status={true} />
        </div>
    }
}
