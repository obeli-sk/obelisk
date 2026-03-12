//! Inline stub activity: synthesize WIT from config (no WASM file needed).
//!
//! Mirrors how `activity_js_worker` synthesizes a WIT for JS activities, but
//! produces a stub component that never actually executes — the caller supplies
//! the return value via the `stub()` RPC.

use concepts::{ComponentId, ComponentType, FunctionFqn, ParameterType, ReturnTypeExtendable};
use utils::wasm_tools::WasmComponent;

use crate::registry::{ComponentConfig, ComponentConfigImportable};

/// Build a [`ComponentConfig`] for an inline-defined stub activity.
///
/// The function synthesizes a WIT string from `ffqn`, `params`, and `return_type`
/// (same approach as JS activities), then creates a virtual `WasmComponent` from
/// the WIT alone — no real WASM binary is required.
pub fn compile_activity_stub_inline(
    component_id: ComponentId,
    ffqn: &FunctionFqn,
    params: &[ParameterType],
    return_type: &ReturnTypeExtendable,
) -> Result<ComponentConfig, utils::wasm_tools::DecodeError> {
    let wit = crate::js_wit_builder::synthesize_wit(ffqn, params, return_type, "stub-activity");
    let wasm_component = WasmComponent::new_from_wit_string(&wit, ComponentType::ActivityStub)?;
    let wit_text = wasm_component
        .wit()
        .inspect_err(|err| tracing::warn!("Cannot get wit - {err:?}"))
        .ok();
    let exports_ext = wasm_component.exim.get_exports(true).to_vec();
    let exports_hierarchy_ext = wasm_component.exim.get_exports_hierarchy_ext().to_vec();
    let component_config_importable = ComponentConfigImportable {
        exports_ext,
        exports_hierarchy_ext,
    };
    Ok(ComponentConfig {
        component_id,
        imports: vec![],
        workflow_or_activity_config: Some(component_config_importable),
        wit: wit_text,
        workflow_replay_info: None,
        source: None,
    })
}
