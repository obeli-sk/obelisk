use crate::{app::Route, grpc::ffqn::FunctionFqn};
use yew::prelude::*;
use yew_router::prelude::Link;
use yewprint::Icon;

#[derive(Properties, PartialEq)]
pub struct FfqnWithLinksProps {
    pub ffqn: FunctionFqn,
    #[prop_or_default]
    pub fully_qualified: bool,
}
#[function_component(FfqnWithLinks)]
pub fn ffqn_with_links(
    FfqnWithLinksProps {
        ffqn,
        fully_qualified,
    }: &FfqnWithLinksProps,
) -> Html {
    html! {
        <div style="display: inline-flex;">
            <Link<Route> to={Route::ExecutionListByFfqn { ffqn: ffqn.clone() } }>
                <Icon icon = { Icon::Search }/>
            </Link<Route>>
            <Link<Route> to={Route::ExecutionSubmit { ffqn: ffqn.clone() } }>
                <Icon icon = { Icon::Play }/>
            </Link<Route>>
            {format!("{} ", if *fully_qualified { ffqn.to_string() } else { ffqn.function_name.clone() })}
        </div>
    }
}
