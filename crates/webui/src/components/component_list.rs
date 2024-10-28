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
    let on_click = on_click.clone();
    components
        .iter()
        .map(|component| {
            let on_select = {
                let on_click = on_click.clone();
                let component = component.clone();
                Callback::from(move |_| on_click.emit(component.clone()))
            };
            html! {
                    <p key={component.config_id.as_ref().unwrap().id.as_str()}
                        onclick={on_select}>{format!("{}: {}", component.r#type, component.name)}</p>
            }
        })
        .collect()
}
