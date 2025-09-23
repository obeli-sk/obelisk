use concepts::{
    ComponentId, ComponentRetryConfig, FnName, FunctionFqn, FunctionMetadata, FunctionRegistry,
    IfcFqnName, PackageIfcFns, ParameterTypes, RETURN_TYPE_DUMMY,
};
use indexmap::IndexMap;
use std::{sync::Arc, time::Duration};

use crate::RunnableComponent;

pub struct TestingFnRegistry {
    ffqn_to_fn_details:
        hashbrown::HashMap<FunctionFqn, (FunctionMetadata, ComponentId, ComponentRetryConfig)>,
    export_hierarchy: Vec<PackageIfcFns>,
}

impl TestingFnRegistry {
    #[must_use]
    pub fn new_from_components(
        wasm_components: Vec<(RunnableComponent, ComponentId)>,
    ) -> Arc<dyn FunctionRegistry> {
        let mut ffqn_to_fn_details = hashbrown::HashMap::new();
        let mut export_hierarchy: hashbrown::HashMap<
            IfcFqnName,
            IndexMap<FnName, FunctionMetadata>,
        > = hashbrown::HashMap::new();
        for (runnable_component, component_id) in wasm_components {
            for exported_function in runnable_component.wasm_component.exim.get_exports(true) {
                let ffqn = exported_function.ffqn.clone();
                ffqn_to_fn_details.insert(
                    ffqn.clone(),
                    (
                        exported_function.clone(),
                        component_id.clone(),
                        ComponentRetryConfig {
                            max_retries: 0,
                            retry_exp_backoff: Duration::ZERO,
                        },
                    ),
                );

                let index_map = export_hierarchy.entry(ffqn.ifc_fqn.clone()).or_default();
                index_map.insert(ffqn.function_name.clone(), exported_function.clone());
            }
        }
        let export_hierarchy = export_hierarchy
            .into_iter()
            .map(|(ifc_fqn, fns)| PackageIfcFns {
                extension: ifc_fqn.is_extension(),
                ifc_fqn,
                fns,
            })
            .collect();
        Arc::from(TestingFnRegistry {
            ffqn_to_fn_details,
            export_hierarchy,
        })
    }
}

impl FunctionRegistry for TestingFnRegistry {
    fn get_by_exported_function(
        &self,
        ffqn: &FunctionFqn,
    ) -> Option<(FunctionMetadata, ComponentId, ComponentRetryConfig)> {
        self.ffqn_to_fn_details.get(ffqn).cloned()
    }

    fn all_exports(&self) -> &[PackageIfcFns] {
        &self.export_hierarchy
    }
}

#[must_use]
pub fn fn_registry_dummy(ffqns: &[FunctionFqn]) -> Arc<dyn FunctionRegistry> {
    let component_id = ComponentId::dummy_activity();
    let mut ffqn_to_fn_details = hashbrown::HashMap::new();
    let mut export_hierarchy: hashbrown::HashMap<IfcFqnName, IndexMap<FnName, FunctionMetadata>> =
        hashbrown::HashMap::new();
    for ffqn in ffqns {
        let fn_metadata = FunctionMetadata {
            ffqn: ffqn.clone(),
            parameter_types: ParameterTypes::default(),
            return_type: RETURN_TYPE_DUMMY,
            extension: None,
            submittable: true,
        };
        ffqn_to_fn_details.insert(
            ffqn.clone(),
            (
                fn_metadata.clone(),
                component_id.clone(),
                ComponentRetryConfig::ZERO,
            ),
        );
        let index_map = export_hierarchy.entry(ffqn.ifc_fqn.clone()).or_default();
        index_map.insert(ffqn.function_name.clone(), fn_metadata);
    }
    let export_hierarchy = export_hierarchy
        .into_iter()
        .map(|(ifc_fqn, fns)| PackageIfcFns {
            extension: ifc_fqn.is_extension(),
            ifc_fqn,
            fns,
        })
        .collect();
    Arc::new(TestingFnRegistry {
        ffqn_to_fn_details,
        export_hierarchy,
    })
}
