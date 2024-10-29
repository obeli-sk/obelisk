use yew::prelude::*;

use crate::grpc_client;

#[derive(Properties, PartialEq)]
pub struct ComponentListProps {
    pub components: Vec<grpc_client::Component>,
    pub on_click: Callback<grpc_client::Component>,
}
#[function_component(ComponentList)]
pub fn component_list(
    ComponentListProps {
        components,
        on_click,
    }: &ComponentListProps,
) -> Html {
    let workflows = filter_by(components, "workflow", on_click);
    let activities = filter_by(components, "activity_wasm", on_click);
    let webhooks = filter_by(components, "webhook_wasm", on_click);

    html! {
        <div key={"workflows"}>
        <h3>{"Workflows"}</h3>
        {workflows}
        <h3>{"Activities"}</h3>
        {activities}
        <h3>{"Webhooks"}</h3>
        {webhooks}
        </div>
    }
}

fn filter_by(
    components: &[grpc_client::Component],
    r#type: &str,
    on_click: &Callback<grpc_client::Component>,
) -> Vec<Html> {
    components
        .into_iter()
        .filter(|component| component.r#type == r#type)
        .map(|component| {
            let on_select = {
                let on_click = on_click.clone();
                let component = component.clone();
                Callback::from(move |_| on_click.emit(component.clone()))
            };
            html! {
                    <p key={component.config_id.as_ref().unwrap().id.as_str()}
                        onclick={on_select}>{format!("{}", component.name)}</p>
            }
        })
        .collect::<Vec<_>>()
}
