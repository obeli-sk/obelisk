use crate::{
    app::AppState,
    components::{execution_submit::ExecutionSubmitForm, function_signature::FunctionSignature},
    ffqn::FunctionFqn,
};
use std::str::FromStr;
use yew::prelude::*;

#[derive(Properties, PartialEq)]
pub struct ExecutionSubmitPageProps {
    pub ffqn: String,
}
#[function_component(ExecutionSubmitPage)]
pub fn execution_submit_page(ExecutionSubmitPageProps { ffqn }: &ExecutionSubmitPageProps) -> Html {
    let app_state =
        use_context::<AppState>().expect("AppState context is set when starting the App");

    match FunctionFqn::from_str(ffqn).and_then(|ffqn| {
        app_state
            .submittable_ffqns_to_details
            .get(&ffqn)
            .ok_or("function not found")
    }) {
        Ok(function_detail) => {
            let ffqn = FunctionFqn::from_fn_detail(function_detail);
            html! {<>
                <h3>{ ffqn.to_string() }</h3>
                <h4><FunctionSignature params = {function_detail.params.clone()} return_type = {function_detail.return_type.clone()} /></h4>
                <ExecutionSubmitForm {function_detail} />
            </>}
        }
        Err(err) => html! {
            <p>{ err }</p>
        },
    }
}