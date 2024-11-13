use crate::components::execution_status::ExecutionStatus;
use crate::grpc::grpc_client;
use yew::prelude::*;

#[derive(Properties, PartialEq)]
pub struct ExecutionDetailPageProps {
    pub execution_id: grpc_client::ExecutionId,
}
#[function_component(ExecutionDetailPage)]
pub fn execution_detail_page(
    ExecutionDetailPageProps { execution_id }: &ExecutionDetailPageProps,
) -> Html {
    // let app_state =
    //     use_context::<AppState>().expect("AppState context is set when starting the App");

    html! {
        <>
        <h3>{execution_id.to_string() }</h3>
        <ExecutionStatus execution_id={execution_id.clone()} status={None} />
    </>}
    // match FunctionFqn::from_str(ffqn).and_then(|ffqn| {
    //     app_state
    //         .submittable_ffqns_to_details
    //         .get(&ffqn)
    //         .ok_or("function not found")
    // }) {
    //     Ok(function_detail) => {
    //         let ffqn = FunctionFqn::from_fn_detail(function_detail);
    //         html! {<>
    //             <h3>{ ffqn.to_string() }</h3>
    //             <p><Link<Route> to={Route::ExecutionListByFfqn { ffqn: ffqn.to_string() }}>{"Go to execution list"}</Link<Route>></p>
    //             <h4><FunctionSignature params = {function_detail.params.clone()} return_type = {function_detail.return_type.clone()} /></h4>

    //         </>}
    //     }
    //     Err(err) => html! {
    //         <p>{ err }</p>
    //     },
    // }
}
