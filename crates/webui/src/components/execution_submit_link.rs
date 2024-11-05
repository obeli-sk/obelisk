use crate::{app::Route, grpc::ffqn::FunctionFqn};
use yew::prelude::*;
use yew_router::prelude::Link;
use yewprint::Icon;

#[derive(Properties, PartialEq)]
pub struct ExecutionSubmitLinkProps {
    pub ffqn: FunctionFqn,
}
#[function_component(ExecutionSubmitLink)]
pub fn execution_submit_link(ExecutionSubmitLinkProps { ffqn }: &ExecutionSubmitLinkProps) -> Html {
    html! {
        <Link<Route> to={Route::ExecutionSubmit { ffqn: ffqn.to_string() } }>
            <div style="display: inline-flex;">
                <Icon icon = { Icon::Play } class = ""/>
                {format!("{} ", ffqn.function_name)}
            </div>
        </Link<Route>>
    }
}
