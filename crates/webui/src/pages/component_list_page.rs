use crate::{app::AppState, components::component_list::ComponentTree};
use yew::prelude::*;

#[function_component(ComponentListPage)]
pub fn component_list_page() -> Html {
    let extensions_state = use_state(|| false);
    // Save flipped `extensions_state`.
    let on_extensions_change = {
        let extensions_state = extensions_state.clone();
        Callback::from(move |event: MouseEvent| {
            event.prevent_default();
            extensions_state.set(!*extensions_state);
        })
    };
    let app_state =
        use_context::<AppState>().expect("AppState context is set when starting the App");

    html! {<>
        <h1>{ "Obelisk WebUI" }</h1>
        <div>
            <h3>{"Components"}</h3>
            <p>
                <input type="checkbox" checked={*extensions_state} onclick={&on_extensions_change} />
                <label onclick={&on_extensions_change}> { "Show function extensions" }</label>
            </p>
            <ComponentTree components={app_state.components} show_extensions={ *extensions_state }/>
        </div>
    </>}
}
