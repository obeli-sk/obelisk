use crate::{app::Route, grpc::ffqn::FunctionFqn};
use yew::prelude::*;
use yew_router::prelude::Link;
use yewprint::Icon;

#[derive(Properties, PartialEq)]
pub struct FfqnWithLinksProps {
    pub ffqn: FunctionFqn,
    #[prop_or_default]
    pub fully_qualified: bool,
    #[prop_or_default]
    pub hide_submit: bool,
}
#[function_component(FfqnWithLinks)]
pub fn ffqn_with_links(
    FfqnWithLinksProps {
        ffqn,
        fully_qualified,
        hide_submit,
    }: &FfqnWithLinksProps,
) -> Html {
    let label = if *fully_qualified {
        ffqn.to_string()
    } else {
        ffqn.function_name.clone()
    };
    html! {
        <div style="display: inline-flex;">
            <Link<Route> to={Route::ExecutionListByFfqn { ffqn: ffqn.clone() } }>
                <Icon icon = { Icon::Search }/>
            </Link<Route>>
            if !hide_submit {
                <Link<Route> to={Route::ExecutionSubmit { ffqn: ffqn.clone() } }>
                    <Icon icon = { Icon::Play }/>
                </Link<Route>>
            }
            { label }
        </div>
    }
}
