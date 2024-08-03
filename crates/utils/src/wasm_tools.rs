use concepts::{FnName, FunctionFqn, FunctionMetadata, IfcFqnName, ParameterTypes, ReturnType};
use indexmap::IndexMap;
use std::{path::Path, sync::Arc};
use tracing::{debug, error};
use val_json::{type_wrapper::TypeConversionError, type_wrapper::TypeWrapper};
use wasmtime::{
    component::{types::ComponentItem, Component, ComponentExportIndex},
    Engine,
};
use wit_parser::{decoding::DecodedWasm, Resolve, WorldItem, WorldKey};

#[derive(derivative::Derivative)]
#[derivative(Debug)]
pub struct WasmComponent {
    #[derivative(Debug = "ignore")]
    pub component: Component,
    pub exim: ExIm,
}

impl WasmComponent {
    pub fn new<P: AsRef<Path>>(wasm_path: P, engine: &Engine) -> Result<Self, DecodeError> {
        let wasm_path = wasm_path.as_ref();
        let wasm_file = std::fs::File::open(wasm_path).map_err(|err| {
            error!("Cannot read the file {wasm_path:?} - {err:?}");
            DecodeError::CannotReadComponent(err.to_string())
        })?;
        let component = {
            let stopwatch = std::time::Instant::now();
            let component = Component::from_file(engine, wasm_path).map_err(|err| {
                error!("Cannot read component {wasm_path:?} - {err:?}");
                DecodeError::CannotReadComponent(err.to_string())
            })?;
            debug!("Parsed with wasmtime in {:?}", stopwatch.elapsed());
            component
        };
        let (exported_ffqns_to_param_names, imported_ffqns_to_param_names) = {
            let stopwatch = std::time::Instant::now();
            let decoded = wit_parser::decoding::decode_reader(wasm_file).unwrap();
            let DecodedWasm::Component(resolve, world_id) = decoded else {
                error!("Must be a wasi component");
                return Err(DecodeError::CannotReadComponent(
                    "must be a wasi component".to_string(),
                ));
            };
            let world = resolve.worlds.get(world_id).expect("world must exist");
            let exported_ffqns_to_param_names =
                wit_parsed_ffqn_to_param_names(&resolve, world.exports.iter())?;
            let imported_ffqns_to_param_names =
                wit_parsed_ffqn_to_param_names(&resolve, world.imports.iter())?;
            debug!("Parsed with wit_parser in {:?}", stopwatch.elapsed());
            (exported_ffqns_to_param_names, imported_ffqns_to_param_names)
        };
        let exim = decode(
            &component,
            engine,
            &exported_ffqns_to_param_names,
            &imported_ffqns_to_param_names,
        )?;
        Ok(Self { component, exim })
    }

    pub fn exported_functions(&self) -> impl Iterator<Item = FunctionMetadata> + '_ {
        self.exim.exported_functions()
    }

    pub fn imported_functions(&self) -> impl Iterator<Item = FunctionMetadata> + '_ {
        self.exim.imported_functions()
    }

    pub fn index_exported_functions(
        &self,
    ) -> Result<hashbrown::HashMap<FunctionFqn, ComponentExportIndex>, DecodeError> {
        let mut exported_ffqn_to_index = hashbrown::HashMap::new();
        for (ffqn, _params, _ret) in self.exported_functions() {
            let Some((_, ifc_export_index)) = self.component.export_index(None, &ffqn.ifc_fqn)
            else {
                error!("Cannot find exported interface `{}`", ffqn.ifc_fqn);
                return Err(DecodeError::CannotReadComponent(format!(
                    "cannot find exported interface {ffqn}"
                )));
            };
            let Some((_, fn_export_index)) = self
                .component
                .export_index(Some(&ifc_export_index), &ffqn.function_name)
            else {
                error!("Cannot find exported function {ffqn}");
                return Err(DecodeError::CannotReadComponent(format!(
                    "cannot find exported function {ffqn}"
                )));
            };
            exported_ffqn_to_index.insert(ffqn, fn_export_index);
        }
        Ok(exported_ffqn_to_index)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum DecodeError {
    #[error("cannot read wasm component - {0}")]
    CannotReadComponent(String),
    #[error("multi-value result is not supported in {0}")]
    MultiValueResultNotSupported(FunctionFqn),
    #[error("unsupported type in {ffqn} - {err}")]
    TypeNotSupported {
        err: TypeConversionError,
        ffqn: FunctionFqn,
    },
    #[error("parameter cardinality mismatch in {0}")]
    ParameterCardinalityMismatch(FunctionFqn),
    #[error("empty package")]
    EmptyPackage,
    #[error("empty interface")]
    EmptyInterface,
}

#[derive(Debug, Clone)]
pub struct ExIm {
    pub exports: Vec<PackageIfcFns>,
    pub imports: Vec<PackageIfcFns>,
}

#[derive(Debug, Clone)]
pub struct PackageIfcFns {
    pub ifc_fqn: IfcFqnName,
    pub fns: IndexMap<FnName, (ParameterTypes, ReturnType)>,
}

impl ExIm {
    fn new(exports: Vec<PackageIfcFns>, imports: Vec<PackageIfcFns>) -> Self {
        Self { exports, imports }
    }

    fn flatten(input: &[PackageIfcFns]) -> impl Iterator<Item = FunctionMetadata> + '_ {
        input.iter().flat_map(|ifc| {
            ifc.fns.iter().map(|(fun, (param_types, result))| {
                (
                    FunctionFqn {
                        ifc_fqn: ifc.ifc_fqn.clone(),
                        function_name: fun.clone(),
                    },
                    param_types.clone(),
                    result.clone(),
                )
            })
        })
    }

    pub fn exported_functions(&self) -> impl Iterator<Item = FunctionMetadata> + '_ {
        Self::flatten(&self.exports)
    }

    pub fn imported_functions(&self) -> impl Iterator<Item = FunctionMetadata> + '_ {
        Self::flatten(&self.imports)
    }
}

fn enrich_function_params<'a>(
    iterator: impl ExactSizeIterator<Item = (&'a str, ComponentItem)> + 'a,
    engine: &Engine,
    ffqns_to_param_names: &hashbrown::HashMap<FunctionFqn, Vec<String>>,
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
                        .collect::<Result<Vec<_>, _>>()
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
                    let function_name: Arc<str> = Arc::from(function_name);
                    let ffqn = FunctionFqn::new_arc(Arc::from(ifc_fqn), function_name.clone());
                    let params = if let Some(param_names) = ffqns_to_param_names.get(&ffqn) {
                        if param_names.len() != params.len() {
                            return Err(DecodeError::ParameterCardinalityMismatch(
                                FunctionFqn::new_arc(Arc::from(ifc_fqn), function_name),
                            ));
                        }
                        ParameterTypes(param_names.iter().cloned().zip(params).collect())
                    } else {
                        ParameterTypes(
                            params
                                .into_iter()
                                .map(|ty| ("unknown".to_string(), ty))
                                .collect(),
                        )
                    };
                    fns.insert(FnName::new_arc(function_name), (params, result));
                } else {
                    debug!("Ignoring export - not a ComponentFunc: {export:?}");
                }
            }
            vec.push(PackageIfcFns {
                ifc_fqn: IfcFqnName::new_arc(Arc::from(ifc_fqn)),
                fns,
            });
        } else {
            return Err(DecodeError::CannotReadComponent(format!(
                "not a ComponentInstance: {item:?}"
            )));
        }
    }
    Ok(vec)
}

fn decode(
    component: &Component,
    engine: &Engine,
    exported_ffqns_to_param_names: &hashbrown::HashMap<FunctionFqn, Vec<String>>,
    imported_ffqns_to_param_names: &hashbrown::HashMap<FunctionFqn, Vec<String>>,
) -> Result<ExIm, DecodeError> {
    let component_type = component.component_type();
    let exports = enrich_function_params(
        component_type.exports(engine),
        engine,
        exported_ffqns_to_param_names,
    )?;
    let imports = enrich_function_params(
        component_type.imports(engine),
        engine,
        imported_ffqns_to_param_names,
    )?;
    Ok(ExIm::new(exports, imports))
}

fn wit_parsed_ffqn_to_param_names<'a>(
    resolve: &'a Resolve,
    iter: impl Iterator<Item = (&'a WorldKey, &'a WorldItem)>,
) -> Result<hashbrown::HashMap<FunctionFqn, Vec<String>>, DecodeError> {
    let mut res_map = hashbrown::HashMap::new();
    for (_, item) in iter {
        if let wit_parser::WorldItem::Interface {
            id: ifc_id,
            stability: _,
        } = item
        {
            let ifc = resolve.interfaces.get(*ifc_id).unwrap();
            let Some(package) = ifc
                .package
                .and_then(|pkg| resolve.packages.get(pkg))
                .map(|p| &p.name)
            else {
                return Err(DecodeError::EmptyPackage);
            };
            let Some(ifc_name) = ifc.name.as_deref() else {
                return Err(DecodeError::EmptyInterface);
            };
            let ifc_fqn = if let Some(version) = &package.version {
                format!(
                    "{namespace}:{name}/{ifc_name}@{version}",
                    namespace = package.namespace,
                    name = package.name
                )
            } else {
                format!("{package}/{ifc_name}")
            };
            let ifc_fqn: Arc<str> = Arc::from(ifc_fqn);
            for (function_name, function) in ifc.functions.iter() {
                let ffqn =
                    FunctionFqn::new_arc(ifc_fqn.clone(), Arc::from(function_name.to_string()));
                let param_names = function
                    .params
                    .iter()
                    .map(|(param_name, _)| param_name)
                    .cloned()
                    .collect();
                res_map.insert(ffqn, param_names);
            }
        }
    }
    Ok(res_map)
}

#[cfg(test)]
mod tests {
    use super::wit_parsed_ffqn_to_param_names;
    use crate::wasm_tools::WasmComponent;
    use rstest::rstest;
    use std::{path::PathBuf, sync::Arc};
    use wasmtime::Engine;
    use wit_parser::decoding::DecodedWasm;

    fn engine() -> Arc<Engine> {
        let mut wasmtime_config = wasmtime::Config::new();
        wasmtime_config.wasm_component_model(true);
        wasmtime_config.async_support(true);
        Arc::new(Engine::new(&wasmtime_config).unwrap())
    }

    #[rstest]
    #[test]
    #[case(test_programs_fibo_workflow_builder::TEST_PROGRAMS_FIBO_WORKFLOW)]
    #[case(test_programs_http_get_workflow_builder::TEST_PROGRAMS_HTTP_GET_WORKFLOW)]
    fn exports_imports(#[case] wasm_path: &str) {
        let wasm_path = PathBuf::from(wasm_path);
        let wasm_file = wasm_path.file_name().unwrap().to_string_lossy();
        test_utils::set_up();
        let engine = engine();
        let component = WasmComponent::new(&wasm_path, &engine).unwrap();
        let exports = component
            .exported_functions()
            .map(|(ffqn, params, ret)| (ffqn.to_string(), (params, ret)))
            .collect::<hashbrown::HashMap<_, _>>();
        insta::with_settings!({sort_maps => true, snapshot_suffix => format!("{wasm_file}_exports")}, {insta::assert_json_snapshot!(exports)});
        let imports = component
            .imported_functions()
            .map(|(ffqn, params, ret)| (ffqn.to_string(), (params, ret)))
            .collect::<hashbrown::HashMap<_, _>>();
        insta::with_settings!({sort_maps => true, snapshot_suffix => format!("{wasm_file}_imports")}, {insta::assert_json_snapshot!(imports)});
    }

    #[test]
    fn test_params() {
        let wasm_path =
            PathBuf::from(test_programs_fibo_workflow_builder::TEST_PROGRAMS_FIBO_WORKFLOW);
        let wasm_file = wasm_path.file_name().unwrap().to_string_lossy();
        let file = std::fs::File::open(&wasm_path).unwrap();
        let decoded = wit_parser::decoding::decode_reader(file).unwrap();
        let DecodedWasm::Component(resolve, world_id) = decoded else {
            panic!();
        };
        let world = resolve.worlds.get(world_id).expect("world must exist");
        let exports = wit_parsed_ffqn_to_param_names(&resolve, world.exports.iter())
            .unwrap()
            .into_iter()
            .map(|(ffqn, val)| (ffqn.to_string(), val))
            .collect::<hashbrown::HashMap<_, _>>();
        insta::with_settings!({sort_maps => true, snapshot_suffix => format!("{wasm_file}_exports")}, {insta::assert_json_snapshot!(exports)});

        let imports = wit_parsed_ffqn_to_param_names(&resolve, world.imports.iter())
            .unwrap()
            .into_iter()
            .map(|(ffqn, val)| (ffqn.to_string(), (val, ffqn.ifc_fqn, ffqn.function_name)))
            .collect::<hashbrown::HashMap<_, _>>();
        insta::with_settings!({sort_maps => true, snapshot_suffix => format!("{wasm_file}_imports")}, {insta::assert_json_snapshot!(imports)});
    }
}
