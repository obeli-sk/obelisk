use crate::{app::Route, grpc::ffqn::FunctionFqn};
use yew::prelude::*;
use yew_router::prelude::Link;
use yewprint::Icon;

#[derive(Properties, PartialEq)]
pub struct ExecutionListByFfqnLinkProps {
    pub ffqn: FunctionFqn,
}
#[function_component(ExecutionListByFfqnLink)]
pub fn execution_list_by_ffqn_link(
    ExecutionListByFfqnLinkProps { ffqn }: &ExecutionListByFfqnLinkProps,
) -> Html {
    html! {
        <Link<Route> to={Route::ExecutionListByFfqn { ffqn: ffqn.to_string() } }>
            <div style="display: inline-flex;">
                <Icon icon = { Icon::Filter } class = ""/>
                {format!("{} ", ffqn.function_name)}
            </div>
        </Link<Route>>
    }
}
