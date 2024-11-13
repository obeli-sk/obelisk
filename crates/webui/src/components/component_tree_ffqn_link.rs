use crate::{app::Route, grpc::ffqn::FunctionFqn};
use yew::prelude::*;
use yew_router::prelude::Link;
use yewprint::Icon;

#[derive(Properties, PartialEq)]
pub struct ExecutionListByFfqnLinkProps {
    pub ffqn: FunctionFqn,
}
#[function_component(ComponentTreeFfqnLink)]
pub fn component_tree_ffqn_link(
    ExecutionListByFfqnLinkProps { ffqn }: &ExecutionListByFfqnLinkProps,
) -> Html {
    html! {
        <div style="display: inline-flex;">
            <Link<Route> to={Route::ExecutionListByFfqn { ffqn: ffqn.clone() } }>
                <Icon icon = { Icon::Search }/>
            </Link<Route>>
            <Link<Route> to={Route::ExecutionSubmit { ffqn: ffqn.clone() } }>
                <Icon icon = { Icon::Play }/>
            </Link<Route>>
            {format!("{} ", ffqn.function_name)}
        </div>
    }
}
