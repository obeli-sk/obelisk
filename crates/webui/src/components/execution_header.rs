use crate::app::Route;
use crate::components::execution_status::ExecutionStatus;
use crate::grpc::execution_id::ExecutionIdExt;
use crate::grpc::grpc_client::ExecutionId;
use yew::prelude::*;
use yew_router::prelude::Link;

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
            <div class="header-and-links">
                <h3>{ execution_id.render_execution_parts(false, route_fn.clone()) }</h3>

                <div class="execution-links">
                    <Link<Route> to={Route::ExecutionTrace { execution_id: execution_id.clone() }}>
                        {"Trace"}
                    </Link<Route>>
                    <Link<Route> to={Route::ExecutionDetail { execution_id: execution_id.clone() }}>
                        {"Log"}
                    </Link<Route>>
                    <Link<Route> to={Route::ExecutionDebugger { execution_id: execution_id.clone() }}>
                        {"Debug"}
                    </Link<Route>>
                </div>
            </div>

            <ExecutionStatus execution_id={execution_id.clone()} status={None} print_finished_status={true} />

        </div>
    }
}
