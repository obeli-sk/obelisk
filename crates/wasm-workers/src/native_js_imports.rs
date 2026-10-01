//! Host imports of the V8 JS runtimes, which have no WASM component to decode them from.
//!
//! Each world mirrors the imports of the corresponding Boa runtime crate's `wit/world.wit`.

use concepts::ComponentType;
use std::sync::LazyLock;
use utils::wasm_tools::WasmComponent;
use utils::wit::{
    WIT_OBELISK_LOG_PACKAGE, WIT_OBELISK_TYPES_PACKAGE, WIT_OBELISK_WEBHOOK_PACKAGE,
    WIT_OBELISK_WORKFLOW_PACKAGE,
};

const ACTIVITY_WORLD: &str = "package obelisk-activity:activity-js-runtime;
world any {
    import obelisk:log/log@1.0.0;
}";

const WORKFLOW_WORLD: &str = "package obelisk-workflow:workflow-js-runtime;
world any {
    import obelisk:workflow/workflow-support@7.0.0;
    import obelisk:workflow/workflow-support-backtrace@7.0.0;
    import obelisk:workflow/workflow-dynamic-support@7.0.0;
    import obelisk:workflow/workflow-dynamic-support-backtrace@7.0.0;
    import obelisk:log/log@1.0.0;
}";

const WEBHOOK_WORLD: &str = "package obelisk-webhook:webhook-js-runtime;
world any {
    import obelisk:webhook/webhook-support@7.0.0;
    import obelisk:webhook/webhook-dynamic-support@7.0.0;
    import obelisk:webhook/webhook-dynamic-support-backtrace@7.0.0;
    import obelisk:log/log@1.0.0;
}";

fn runtime_component(world: &str, deps: &[&str], component_type: ComponentType) -> WasmComponent {
    WasmComponent::new_from_wit_string_with_deps(world, deps, component_type)
        .expect("embedded runtime WIT must be valid")
}

pub(crate) static ACTIVITY: LazyLock<WasmComponent> = LazyLock::new(|| {
    runtime_component(
        ACTIVITY_WORLD,
        &[WIT_OBELISK_LOG_PACKAGE[2]],
        ComponentType::Activity,
    )
});

pub(crate) static WORKFLOW: LazyLock<WasmComponent> = LazyLock::new(|| {
    runtime_component(
        WORKFLOW_WORLD,
        &[
            WIT_OBELISK_TYPES_PACKAGE[2],
            WIT_OBELISK_LOG_PACKAGE[2],
            WIT_OBELISK_WORKFLOW_PACKAGE[2],
        ],
        ComponentType::Workflow,
    )
});

pub(crate) static WEBHOOK: LazyLock<WasmComponent> = LazyLock::new(|| {
    runtime_component(
        WEBHOOK_WORLD,
        &[
            WIT_OBELISK_TYPES_PACKAGE[2],
            WIT_OBELISK_LOG_PACKAGE[2],
            WIT_OBELISK_WEBHOOK_PACKAGE[2],
        ],
        ComponentType::WebhookEndpoint,
    )
});

#[cfg(test)]
mod tests {
    use super::*;

    fn obelisk_imports(component: &WasmComponent) -> hashbrown::HashMap<String, serde_json::Value> {
        component
            .imported_functions()
            .iter()
            .filter(|metadata| metadata.ffqn.ifc_fqn.namespace() == "obelisk")
            .map(|metadata| {
                (
                    metadata.ffqn.to_string(),
                    serde_json::to_value(metadata).unwrap(),
                )
            })
            .collect()
    }

    #[rstest::rstest(native, wasm_path, component_type,
        case(&ACTIVITY, activity_js_runtime_builder::ACTIVITY_JS_RUNTIME, ComponentType::Activity),
        case(&WORKFLOW, workflow_js_runtime_builder::WORKFLOW_JS_RUNTIME, ComponentType::Workflow),
        case(&WEBHOOK, webhook_js_runtime_builder::WEBHOOK_JS_RUNTIME, ComponentType::WebhookEndpoint),
    )]
    fn imports_cover_boa_runtime(
        native: &LazyLock<WasmComponent>,
        wasm_path: &str,
        component_type: ComponentType,
    ) {
        let boa = WasmComponent::new(wasm_path, component_type).unwrap();
        // The Boa component drops the functions it does not call, the WIT world keeps them all.
        let native = obelisk_imports(native);
        for (ffqn, boa_metadata) in obelisk_imports(&boa) {
            assert_eq!(Some(&boa_metadata), native.get(&ffqn), "{ffqn}");
        }
    }
}
