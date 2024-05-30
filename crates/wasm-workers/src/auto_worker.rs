//! Worker that acts as wrapper for `activity_worker` or `workflow_worker`.
//! Apply following heuristic to distinguish between an activity and workflow:
//! * Read all imported functions of the component's world
//! * If there are no imports except for standard WASI -> Activity
//! * Otherwise -> Workflow

use crate::{WasmComponent, WasmFileError};
use concepts::{ComponentType, FunctionMetadata, IfcFqnName, StrVariant};
use std::ops::Deref;
use wasmtime::Engine;

pub struct DetectedComponent {
    pub component_type: ComponentType,
    pub exports: Vec<FunctionMetadata>,
    pub imports: Vec<FunctionMetadata>,
}

impl DetectedComponent {
    pub fn new(wasm_path: &StrVariant, engine: &Engine) -> Result<Self, WasmFileError> {
        let wasm_component = WasmComponent::new(wasm_path, engine)?;
        let component_type =
            if supported_wasi_imports(wasm_component.exim.imports.iter().map(|pif| &pif.ifc_fqn)) {
                ComponentType::WasmActivity
            } else {
                ComponentType::WasmWorkflow
            };
        Ok(Self {
            component_type,
            exports: wasm_component.exported_functions().collect(),
            imports: wasm_component.imported_functions().collect(),
        })
    }
}

fn supported_wasi_imports<'a>(mut imported_packages: impl Iterator<Item = &'a IfcFqnName>) -> bool {
    // FIXME Fail if both wasi and host imports are present
    imported_packages.all(|ifc| ifc.deref().starts_with("wasi:"))
}

#[cfg(test)]
mod tests {
    use crate::auto_worker::DetectedComponent;
    use crate::{workflow_worker::workflow_engine, EngineConfig};
    use concepts::ComponentType;
    use concepts::StrVariant;
    use test_utils::set_up;

    #[rstest::rstest]
    #[case(
        test_programs_fibo_activity_builder::TEST_PROGRAMS_FIBO_ACTIVITY,
        ComponentType::WasmActivity
    )]
    #[case(
        test_programs_fibo_workflow_builder::TEST_PROGRAMS_FIBO_WORKFLOW,
        ComponentType::WasmWorkflow
    )]
    #[tokio::test]
    async fn detection(#[case] file: &'static str, #[case] expected: ComponentType) {
        set_up();
        let detected = DetectedComponent::new(
            &StrVariant::Static(file),
            &workflow_engine(EngineConfig::default()),
        )
        .unwrap();
        assert_eq!(expected, detected.component_type);
    }
}
