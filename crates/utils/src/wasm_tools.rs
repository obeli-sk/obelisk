use concepts::{FnName, FunctionFqn, IfcFqnName};
use indexmap::IndexMap;
use std::sync::Arc;
use tracing::debug;
use val_json::{type_wrapper::TypeConversionError, type_wrapper::TypeWrapper};
use wasmtime::{
    component::{types::ComponentItem, Component},
    Engine,
};

#[derive(Debug, thiserror::Error)]
pub enum DecodeError {
    #[error("multi-value result is not supported in {0}")]
    MultiValueResultNotSupported(FunctionFqn),
    #[error("unsupported type in {ffqn} - {err}")]
    TypeNotSupported {
        err: TypeConversionError,
        ffqn: FunctionFqn,
    },
}

#[derive(Debug, Clone)]
pub struct ExIm {
    pub exports: Vec<PackageIfcFns>,
    pub imports: Vec<PackageIfcFns>,
}

#[derive(Debug, Clone)]
pub struct PackageIfcFns {
    pub ifc_fqn: IfcFqnName,
    pub fns: IndexMap<FnName, (Arc<[TypeWrapper]>, Option<TypeWrapper>)>,
}

impl ExIm {
    fn new(exports: Vec<PackageIfcFns>, imports: Vec<PackageIfcFns>) -> Self {
        Self { exports, imports }
    }

    fn flatten(
        input: &[PackageIfcFns],
    ) -> impl Iterator<Item = (FunctionFqn, &[TypeWrapper], &Option<TypeWrapper>)> {
        input.iter().flat_map(|ifc| {
            ifc.fns.iter().map(|(fun, (params, result))| {
                (
                    FunctionFqn {
                        ifc_fqn: ifc.ifc_fqn.clone(),
                        function_name: fun.clone(),
                    },
                    params.as_ref(),
                    result,
                )
            })
        })
    }

    pub fn exported_functions(
        &self,
    ) -> impl Iterator<Item = (FunctionFqn, &[TypeWrapper], &Option<TypeWrapper>)> {
        Self::flatten(&self.exports)
    }

    pub fn imported_functions(
        &self,
    ) -> impl Iterator<Item = (FunctionFqn, &[TypeWrapper], &Option<TypeWrapper>)> {
        Self::flatten(&self.imports)
    }
}

fn exported<'a>(
    iterator: impl ExactSizeIterator<Item = (&'a str, ComponentItem)> + 'a,
    engine: &Engine,
) -> Result<Vec<PackageIfcFns>, DecodeError> {
    let mut vec = Vec::new();
    for (ifc_fqn, item) in iterator {
        if let ComponentItem::ComponentInstance(instance) = item {
            let exports = instance.exports(engine);
            let mut fns = IndexMap::new();
            for (function_name, export) in exports {
                if let ComponentItem::ComponentFunc(func) = export {
                    let params = func
                        .params()
                        .map(TypeWrapper::try_from)
                        .collect::<Result<Arc<_>, _>>()
                        .map_err(|err| DecodeError::TypeNotSupported {
                            err,
                            ffqn: FunctionFqn::new_arc(
                                Arc::from(ifc_fqn),
                                Arc::from(function_name),
                            ),
                        })?;
                    let mut results = func.results();
                    let result = if results.len() <= 1 {
                        results
                            .next()
                            .map(TypeWrapper::try_from)
                            .transpose()
                            .map_err(|err| DecodeError::TypeNotSupported {
                                err,
                                ffqn: FunctionFqn::new_arc(
                                    Arc::from(ifc_fqn),
                                    Arc::from(function_name),
                                ),
                            })
                    } else {
                        Err(DecodeError::MultiValueResultNotSupported(
                            FunctionFqn::new_arc(Arc::from(ifc_fqn), Arc::from(function_name)),
                        ))
                    }?;
                    fns.insert(FnName::new_arc(Arc::from(function_name)), (params, result));
                } else {
                    debug!("Ignoring export - not a ComponentFunc: {export:?}");
                }
            }
            vec.push(PackageIfcFns {
                ifc_fqn: IfcFqnName::new_arc(Arc::from(ifc_fqn)),
                fns,
            });
        } else {
            panic!("not a ComponentInstance: {item:?}")
        }
    }
    Ok(vec)
}

pub fn decode(component: &Component, engine: &Engine) -> Result<ExIm, DecodeError> {
    let component_type = component.component_type();
    let exports = exported(component_type.exports(engine), engine)?;
    let imports = exported(component_type.imports(engine), engine)?;
    Ok(ExIm::new(exports, imports))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use wasmtime::{component::Component, Engine};

    fn engine() -> Arc<Engine> {
        let mut wasmtime_config = wasmtime::Config::new();
        wasmtime_config.wasm_component_model(true);
        wasmtime_config.async_support(true);
        Arc::new(Engine::new(&wasmtime_config).unwrap())
    }

    #[test]
    fn test() {
        test_utils::set_up();
        let wasm_path = test_programs_fibo_workflow_builder::TEST_PROGRAMS_FIBO_WORKFLOW;
        let engine = engine();
        let component = Component::from_file(&engine, wasm_path).unwrap();
        let exim = super::decode(&component, &engine).unwrap();
        insta::assert_debug_snapshot!(&exim);
    }
}
