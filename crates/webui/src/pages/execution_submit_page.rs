use crate::{
    app::{AppState, Route},
    components::{execution_submit::ExecutionSubmitForm, function_signature::FunctionSignature},
    grpc::ffqn::FunctionFqn,
};
use yew::prelude::*;
use yew_router::prelude::Link;

#[derive(Properties, PartialEq)]
pub struct ExecutionSubmitPageProps {
    pub ffqn: FunctionFqn,
}
#[function_component(ExecutionSubmitPage)]
pub fn execution_submit_page(ExecutionSubmitPageProps { ffqn }: &ExecutionSubmitPageProps) -> Html {
    let app_state =
        use_context::<AppState>().expect("AppState context is set when starting the App");

    if let Some(function_detail) = app_state.submittable_ffqns_to_details.get(ffqn) {
        html! {<>
            <h3>{ ffqn.to_string() }</h3>
            <p><Link<Route> to={Route::ExecutionListByFfqn { ffqn: ffqn.clone() }}>{"Go to execution list"}</Link<Route>></p>
            <h4><FunctionSignature params = {function_detail.params.clone()} return_type = {function_detail.return_type.clone()} /></h4>
            <ExecutionSubmitForm {function_detail} />
        </>}
    } else {
        html! {
            <p>{"function not found"}</p>
        }
    }
}
