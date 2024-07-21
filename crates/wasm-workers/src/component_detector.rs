//! Read the wasm component, iterate over all its exported interfaces.
//! If the interface contains the string `workflow`, assume the component defines a workflow.
//! Otherwise it defines an activity.
//! Mixing is not allowed.

use crate::WasmFileError;
use concepts::ContentDigest;
use concepts::{ComponentType, FunctionMetadata};
use std::path::Path;
use utils::wasm_tools::WasmComponent;
use wasmtime::Engine;

pub struct ComponentDetector {
    pub component_type: ComponentType,
    pub exports: Vec<FunctionMetadata>,
    pub imports: Vec<FunctionMetadata>,
}

impl ComponentDetector {
    #[must_use]
    pub fn get_engine() -> Engine {
        let mut wasmtime_config = wasmtime::Config::new();
        wasmtime_config.wasm_component_model(true);
        Engine::new(&wasmtime_config).unwrap()
    }

    // TODO: Allow workflows and activities in the same world.
    pub fn new<P: AsRef<Path>>(wasm_path: P, engine: &Engine) -> Result<Self, WasmFileError> {
        let wasm_path = wasm_path.as_ref();
        let wasm_component = WasmComponent::new(wasm_path, engine)
            .map_err(|err| WasmFileError::DecodeError(wasm_path.to_owned(), err))?;
        let types = wasm_component
            .exim
            .exports
            .iter()
            .map(|pkg_ifc_fns| {
                if pkg_ifc_fns.ifc_fqn.contains("workflow") {
                    ComponentType::WasmWorkflow
                } else {
                    ComponentType::WasmActivity
                }
            })
            .collect::<hashbrown::HashSet<_>>();
        let component_type = match types.into_iter().collect::<Vec<_>>().as_slice() {
            [item] => Ok(*item),
            [] => Err(WasmFileError::NoExportedInterfaces),
            _ => Err(WasmFileError::MixedWorkflowsAndActivities),
        }?;
        Ok(Self {
            component_type,
            exports: wasm_component.exported_functions().collect(),
            imports: wasm_component.imported_functions().collect(),
        })
    }
}

#[cfg(not(madsim))]
pub async fn file_hash<P: AsRef<Path>>(path: P) -> Result<ContentDigest, std::io::Error> {
    let hash_base16 = match wasm_pkg_common::digest::ContentDigest::sha256_from_file(path).await? {
        wasm_pkg_common::digest::ContentDigest::Sha256 { hex } => hex,
    };
    Ok(ContentDigest::new(concepts::HashType::Sha256, hash_base16))
}
#[cfg(madsim)]
pub async fn file_hash<P: AsRef<Path>>(_path: P) -> Result<ContentDigest, std::io::Error> {
    Ok(ContentDigest::new(
        concepts::HashType::Sha256,
        ulid::Ulid::new().to_string(),
    ))
}

#[cfg(test)]
mod tests {
    use crate::component_detector::ComponentDetector;
    use crate::engines::{EngineConfig, Engines};
    use concepts::ComponentType;

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
        let detected = ComponentDetector::new(
            file,
            &Engines::get_workflow_engine(EngineConfig::on_demand()).unwrap(),
        )
        .unwrap();
        assert_eq!(expected, detected.component_type);
    }
}
