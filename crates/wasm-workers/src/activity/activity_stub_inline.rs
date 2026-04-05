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
    let wasm_component = WasmComponent::new_from_fn_signature(
        ffqn,
        params,
        return_type,
        ComponentType::ActivityStub,
        "stub-activity",
    )?;
    let wit_text_with_extensions = wasm_component.wit();
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
        wit: wit_text_with_extensions,
        workflow_replay_info: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use concepts::{
        ComponentType, ParameterType, ReturnType, StrVariant,
        component_id::{ComponentDigest, Digest},
    };

    fn make_return_type() -> ReturnTypeExtendable {
        let tw = val_json::type_wrapper::parse_wit_type("result<string, string>").unwrap();
        let ReturnType::Extendable(rt) =
            ReturnType::detect(tw, StrVariant::Static("result<string, string>"))
        else {
            unreachable!()
        };
        rt
    }

    #[test]
    fn wit_includes_obelisk_extension_packages() {
        let component_id = ComponentId::new(
            ComponentType::ActivityStub,
            StrVariant::Static("test_stub"),
            ComponentDigest(Digest([0u8; 32])),
        )
        .unwrap();
        let ffqn = FunctionFqn::new_static("ns:pkg/ifc", "my-fn");
        let params = vec![ParameterType {
            name: StrVariant::Static("id"),
            type_wrapper: val_json::type_wrapper::TypeWrapper::U64,
            wit_type: StrVariant::Static("u64"),
        }];

        let config =
            compile_activity_stub_inline(component_id, &ffqn, &params, &make_return_type())
                .expect("compile must succeed");

        // The rebuilt WIT includes extension packages absent from the raw synthesized string.
        insta::assert_snapshot!(config.wit);
    }
}
