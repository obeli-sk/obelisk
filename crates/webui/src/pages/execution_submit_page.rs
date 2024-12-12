use crate::{
    app::AppState,
    components::{
        execution_submit::ExecutionSubmitForm, ffqn_with_links::FfqnWithLinks,
        function_signature::FunctionSignature,
    },
    grpc::ffqn::FunctionFqn,
};
use yew::prelude::*;

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
            <header>
                <h1>{"Execution submit"}</h1>
                <h2><FfqnWithLinks ffqn={ffqn.clone()} hide_submit={true}  />
                </h2>
            </header>

            <h4><FunctionSignature params = {function_detail.params.clone()} return_type = {function_detail.return_type.clone()} /></h4>
            <ExecutionSubmitForm {function_detail} />
        </>}
    } else {
        html! {
            <p>{"function not found"}</p>
        }
    }
}
