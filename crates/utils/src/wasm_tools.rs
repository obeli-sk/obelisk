use crate::wit_printer::WitPrinter;
use concepts::{
    FnName, FunctionFqn, FunctionMetadata, IfcFqnName, PackageIfcFns, ParameterType,
    ParameterTypes, ReturnType, StrVariant,
};
use indexmap::{indexmap, IndexMap};
use std::{borrow::Cow, path::Path, sync::Arc};
use tracing::{debug, error, trace};
use val_json::type_wrapper::{TypeConversionError, TypeWrapper};
use wasmtime::{
    component::{types::ComponentItem, Component, ComponentExportIndex},
    Engine,
};
use wit_parser::{decoding::DecodedWasm, Resolve, Results, WorldItem, WorldKey};

pub const SUFFIX_PKG_EXT: &str = "-obelisk-ext";

#[derive(derivative::Derivative)]
#[derivative(Debug)]
pub struct WasmComponent {
    #[derivative(Debug = "ignore")]
    pub wasmtime_component: Component,
    pub exim: ExIm,
}

impl WasmComponent {
    pub fn new<P: AsRef<Path>>(wasm_path: P, engine: &Engine) -> Result<Self, DecodeError> {
        let wasm_path = wasm_path.as_ref();
        let wasm_file = std::fs::File::open(wasm_path).map_err(|err| {
            error!("Cannot read the file {wasm_path:?} - {err:?}");
            DecodeError::CannotReadComponent(err.to_string())
        })?;
        trace!("Decoding using wasmtime");
        let wasmtime_component = {
            let stopwatch = std::time::Instant::now();
            let component = Component::from_file(engine, wasm_path).map_err(|err| {
                error!("Cannot read component {wasm_path:?} - {err:?}");
                DecodeError::CannotReadComponent(err.to_string())
            })?;
            debug!("Parsed with wasmtime in {:?}", stopwatch.elapsed());
            component
        };
        trace!("Decoding using wit_parser");
        let (exported_ffqns_to_wit_meta, imported_ffqns_to_wit_meta) = {
            let stopwatch = std::time::Instant::now();
            let decoded = wit_parser::decoding::decode_reader(wasm_file).map_err(|err| {
                error!("Cannot read component {wasm_path:?} - {err:?}");
                DecodeError::CannotReadComponent(err.to_string())
            })?;
            let DecodedWasm::Component(resolve, world_id) = decoded else {
                error!("Must be a wasi component");
                return Err(DecodeError::CannotReadComponent(
                    "must be a wasi component".to_string(),
                ));
            };
            let world = resolve.worlds.get(world_id).expect("world must exist");
            let exported_ffqns_to_wit_meta =
                wit_parsed_ffqn_to_wit_parsed_fn_metadata(&resolve, world.exports.iter())?;
            let imported_ffqns_to_wit_meta =
                wit_parsed_ffqn_to_wit_parsed_fn_metadata(&resolve, world.imports.iter())?;
            debug!("Parsed with wit_parser in {:?}", stopwatch.elapsed());
            (exported_ffqns_to_wit_meta, imported_ffqns_to_wit_meta)
        };
        let exim = ExIm::decode(
            &wasmtime_component,
            engine,
            exported_ffqns_to_wit_meta,
            imported_ffqns_to_wit_meta,
        )?;
        Ok(Self {
            wasmtime_component,
            exim,
        })
    }

    #[must_use]
    pub fn exported_functions(&self, extensions: bool) -> &[FunctionMetadata] {
        if extensions {
            &self.exim.exports_flat
        } else {
            &self.exim.exports_flat_noext
        }
    }

    #[must_use]
    pub fn imported_functions(&self) -> &[FunctionMetadata] {
        &self.exim.imports_flat
    }

    pub fn index_exported_functions(
        &self,
    ) -> Result<hashbrown::HashMap<FunctionFqn, ComponentExportIndex>, DecodeError> {
        let mut exported_ffqn_to_index = hashbrown::HashMap::new();
        for FunctionMetadata { ffqn, .. } in &self.exim.exports_flat_noext {
            let Some((_, ifc_export_index)) =
                self.wasmtime_component.export_index(None, &ffqn.ifc_fqn)
            else {
                error!("Cannot find exported interface `{}`", ffqn.ifc_fqn);
                return Err(DecodeError::CannotReadComponent(format!(
                    "cannot find exported interface {ffqn}"
                )));
            };
            let Some((_, fn_export_index)) = self
                .wasmtime_component
                .export_index(Some(&ifc_export_index), &ffqn.function_name)
            else {
                error!("Cannot find exported function {ffqn}");
                return Err(DecodeError::CannotReadComponent(format!(
                    "cannot find exported function {ffqn}"
                )));
            };
            exported_ffqn_to_index.insert(ffqn.clone(), fn_export_index);
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
    #[error("invalid package `{0}`, {SUFFIX_PKG_EXT} is reserved")]
    ReservedPackageSuffix(String),
}

#[derive(Debug, Clone)]
pub struct ExIm {
    pub exports_hierarchy: Vec<PackageIfcFns>,
    pub imports_hierarchy: Vec<PackageIfcFns>,
    pub exports_flat_noext: Vec<FunctionMetadata>,
    pub exports_flat: Vec<FunctionMetadata>,
    pub imports_flat: Vec<FunctionMetadata>,
}

impl ExIm {
    fn decode(
        component: &Component,
        engine: &Engine,
        exported_ffqns_to_wit_parsed_meta: hashbrown::HashMap<
            FunctionFqn,
            WitParsedFunctionMetadata,
        >,
        imported_ffqns_to_wit_parsed_meta: hashbrown::HashMap<
            FunctionFqn,
            WitParsedFunctionMetadata,
        >,
    ) -> Result<ExIm, DecodeError> {
        let component_type = component.component_type();
        let mut exports_hierarchy = enrich_function_params(
            component_type.exports(engine),
            engine,
            exported_ffqns_to_wit_parsed_meta,
        )?;
        // Verify that there is no -obelisk-ext export
        for PackageIfcFns { ifc_fqn, fns: _ } in &exports_hierarchy {
            if ifc_fqn.package_name().ends_with(SUFFIX_PKG_EXT) {
                return Err(DecodeError::ReservedPackageSuffix(
                    ifc_fqn.package_name().to_string(),
                ));
            }
        }

        let exports_flat_noext = Self::flatten(&exports_hierarchy);
        Self::enrich_exports_with_extensions(&mut exports_hierarchy);
        let exports_flat = Self::flatten(&exports_hierarchy);
        let imports_hierarchy = enrich_function_params(
            component_type.imports(engine),
            engine,
            imported_ffqns_to_wit_parsed_meta,
        )?;
        let imports_flat = Self::flatten(&imports_hierarchy);
        Ok(Self {
            exports_hierarchy,
            imports_hierarchy,
            exports_flat_noext,
            exports_flat,
            imports_flat,
        })
    }

    #[expect(clippy::too_many_lines)]
    fn enrich_exports_with_extensions(exports_hierarchy: &mut Vec<PackageIfcFns>) {
        // initialize values for reuse
        let execution_id_type_wrapper =
            TypeWrapper::Record(indexmap! {"id".into() => TypeWrapper::String});
        let join_set_id_type_wrapper = TypeWrapper::Borrow;

        let return_type_execution_id = Some(ReturnType {
            type_wrapper: execution_id_type_wrapper.clone(),
            wit_type: Some(concepts::StrVariant::Static(
                "/* use obelisk:types/execution.{execution-id} */ execution-id",
            )),
        });
        let param_type_join_set = ParameterType {
            type_wrapper: join_set_id_type_wrapper.clone(),
            name: Some(StrVariant::Static("join-set-id")),
            wit_type: Some(StrVariant::Static(
                "/* use obelisk:types/execution.{join-set-id} */ join-set-id",
            )),
        };
        let duration_type_wrapper = TypeWrapper::Variant(indexmap! {
            Box::from("milliseconds") => Some(TypeWrapper::U64),
            Box::from("seconds") => Some(TypeWrapper::U64),
            Box::from("minutes") => Some(TypeWrapper::U32),
            Box::from("hours") => Some(TypeWrapper::U32),
            Box::from("days") => Some(TypeWrapper::U32),
        });
        let param_type_scheduled_at = ParameterType {
            type_wrapper: TypeWrapper::Variant(indexmap! {
                Box::from("now") => None,
                Box::from("at") => Some(TypeWrapper::Record(indexmap! {
                    Box::from("seconds") => TypeWrapper::U64,
                    Box::from("nanoseconds") => TypeWrapper::U32,
                })),
                Box::from("in") => Some(duration_type_wrapper),
            }),
            name: Some(StrVariant::Static("scheduled-at")),
            wit_type: Some(StrVariant::Static(
                "/* use obelisk:types/time.{schedule-at} */ schedule-at",
            )),
        };

        let mut extensions = Vec::with_capacity(exports_hierarchy.len());
        for PackageIfcFns { ifc_fqn, fns } in exports_hierarchy.iter() {
            let obelisk_extended_ifc = IfcFqnName::from_parts(
                ifc_fqn.namespace(),
                &format!("{}{SUFFIX_PKG_EXT}", ifc_fqn.package_name()),
                ifc_fqn.ifc_name(),
                ifc_fqn.version(),
            );
            let mut extension_fns = IndexMap::new();
            let mut insert = |fn_metadata: FunctionMetadata| {
                //(fun, (param_types, return_type))
                extension_fns.insert(
                    fn_metadata.ffqn.function_name,
                    (fn_metadata.parameter_types, fn_metadata.return_type),
                );
            };
            for (fun, (param_types, return_type)) in fns {
                let exported_fn_metadata = FunctionMetadata {
                    ffqn: FunctionFqn {
                        ifc_fqn: ifc_fqn.clone(),
                        function_name: fun.clone(),
                    },
                    parameter_types: param_types.clone(),
                    return_type: return_type.clone(),
                };

                // -submit(join-set-id: join-set-id, original params) -> execution id
                let fn_submit = FunctionMetadata {
                    ffqn: FunctionFqn {
                        ifc_fqn: obelisk_extended_ifc.clone(),
                        function_name: FnName::new_string(format!(
                            "{}-submit",
                            exported_fn_metadata.ffqn.function_name
                        )),
                    },
                    parameter_types: {
                        let mut params =
                            Vec::with_capacity(exported_fn_metadata.parameter_types.len() + 1);
                        params.push(param_type_join_set.clone());
                        params.extend_from_slice(&exported_fn_metadata.parameter_types.0);
                        ParameterTypes(params)
                    },
                    return_type: return_type_execution_id.clone(),
                };
                insert(fn_submit);

                // -await-next(join-set-id: join-set-id) ->  result<(execution_id, original_return_type), execution-error>
                let fn_await_next = FunctionMetadata {
                    ffqn: FunctionFqn {
                        ifc_fqn: obelisk_extended_ifc.clone(),
                        function_name: FnName::new_string(format!(
                            "{}-await-next",
                            exported_fn_metadata.ffqn.function_name
                        )),
                    },
                    parameter_types: ParameterTypes(vec![param_type_join_set.clone()]),
                    return_type: {
                        let error_tuple = Some(Box::new(TypeWrapper::Tuple(Box::new([
                            execution_id_type_wrapper.clone(),
                            TypeWrapper::Variant(indexmap! {
                                Box::from("permanent-failure") => Some(TypeWrapper::String),
                                Box::from("permanent-timeout") => None,
                                Box::from("nondeterminism") => None,
                            }),
                        ]))));
                        let (ok_part, type_wrapper) = if let Some(original_ret) =
                            exported_fn_metadata.return_type
                        {
                            (
                                // Use ReturnType::display to serialize wit_type or fallback
                                Cow::Owned(format!("tuple</* use obelisk:types/execution.{{execution-id}} */ execution-id, {original_ret}>")),
                                TypeWrapper::Result {
                                    ok: Some(Box::new(TypeWrapper::Tuple(Box::new([
                                        execution_id_type_wrapper.clone(),
                                        original_ret.type_wrapper,
                                    ])))), // (execution-id, original_ret)
                                    err: error_tuple,
                                },
                            )
                        } else {
                            (
                                Cow::Borrowed(
                                    "/* use obelisk:types/execution.{execution-id} */ execution-id",
                                ),
                                TypeWrapper::Result {
                                    ok: Some(Box::new(execution_id_type_wrapper.clone())),
                                    err: error_tuple,
                                },
                            )
                        };
                        Some(ReturnType { type_wrapper, wit_type: Some(StrVariant::from(format!(
                            // result<{ok_part}, tuple<execution_id, execution_error>>
                            "result<{ok_part}, tuple</* use obelisk:types/execution.{{execution-id}} */ execution-id, /* use obelisk:types/execution.{{execution-error}} */ execution-error>>"
                        ))) })
                    },
                };
                insert(fn_await_next);
                // -schedule(schedule: schedule-at, original params) -> string (execution id)
                let fn_schedule = FunctionMetadata {
                    ffqn: FunctionFqn {
                        ifc_fqn: obelisk_extended_ifc.clone(),
                        function_name: FnName::new_string(format!(
                            "{}-schedule",
                            exported_fn_metadata.ffqn.function_name
                        )),
                    },
                    parameter_types: {
                        let mut params =
                            Vec::with_capacity(exported_fn_metadata.parameter_types.len() + 1);
                        params.push(param_type_scheduled_at.clone());
                        params.extend_from_slice(&exported_fn_metadata.parameter_types.0);
                        ParameterTypes(params)
                    },
                    return_type: return_type_execution_id.clone(),
                };
                insert(fn_schedule);
            }
            extensions.push((obelisk_extended_ifc, extension_fns));
        }
        for (obelisk_extended_ifc, extension_fns) in extensions {
            exports_hierarchy.push(PackageIfcFns {
                ifc_fqn: obelisk_extended_ifc,
                fns: extension_fns,
            });
        }
    }

    fn flatten(input: &[PackageIfcFns]) -> Vec<FunctionMetadata> {
        input
            .iter()
            .flat_map(|pif| {
                pif.fns
                    .iter()
                    .map(|(fun, (param_types, return_type))| FunctionMetadata {
                        ffqn: FunctionFqn {
                            ifc_fqn: pif.ifc_fqn.clone(),
                            function_name: fun.clone(),
                        },
                        parameter_types: param_types.clone(),
                        return_type: return_type.clone(),
                    })
            })
            .collect()
    }
}

// Attempt to merge parameter names obtained using the wit-parser passed as `ffqns_to_wit_parsed_meta`
fn enrich_function_params<'a>(
    iterator: impl ExactSizeIterator<Item = (&'a str /* ifc_fqn */, ComponentItem)> + 'a,
    engine: &Engine,
    mut ffqns_to_wit_parsed_meta: hashbrown::HashMap<FunctionFqn, WitParsedFunctionMetadata>,
) -> Result<Vec<PackageIfcFns>, DecodeError> {
    let mut vec = Vec::new();
    for (ifc_fqn, item) in iterator {
        let ifc_fqn: Arc<str> = Arc::from(ifc_fqn);
        if let ComponentItem::ComponentInstance(instance) = item {
            let exports = instance.exports(engine);
            let mut fns = IndexMap::new();
            for (function_name, export) in exports {
                if let ComponentItem::ComponentFunc(func) = export {
                    let function_name: Arc<str> = Arc::from(function_name);
                    let ffqn = FunctionFqn::new_arc(ifc_fqn.clone(), function_name.clone());
                    let (wit_meta_params, wit_meta_res) = ffqns_to_wit_parsed_meta
                        .remove(&ffqn)
                        .map(|meta| (Some(meta.params), meta.return_type.map(StrVariant::from)))
                        .unwrap_or_default();

                    let mut return_type = func.results();
                    let return_type = if return_type.len() <= 1 {
                        match return_type.next().map(TypeWrapper::try_from).transpose() {
                            Ok(type_wrapper) => type_wrapper.map(|type_wrapper| ReturnType {
                                type_wrapper,
                                wit_type: wit_meta_res,
                            }),
                            Err(err) => {
                                return Err(DecodeError::TypeNotSupported {
                                    err,
                                    ffqn: FunctionFqn::new_arc(ifc_fqn, function_name),
                                })
                            }
                        }
                    } else {
                        return Err(DecodeError::MultiValueResultNotSupported(
                            FunctionFqn::new_arc(ifc_fqn, function_name),
                        ));
                    };
                    let params = match func
                        .params()
                        .map(TypeWrapper::try_from)
                        .collect::<Result<Vec<_>, _>>()
                    {
                        Ok(params) => params,
                        Err(err) => {
                            return Err(DecodeError::TypeNotSupported {
                                err,
                                ffqn: FunctionFqn::new_arc(ifc_fqn, function_name),
                            })
                        }
                    };
                    let params = if let Some(wit_meta_params) = wit_meta_params {
                        if wit_meta_params.len() != params.len() {
                            return Err(DecodeError::ParameterCardinalityMismatch(
                                FunctionFqn::new_arc(ifc_fqn, function_name),
                            ));
                        }
                        ParameterTypes(
                            wit_meta_params
                                .into_iter()
                                .zip(params)
                                .map(|(name_wit, type_wrapper)| ParameterType {
                                    name: Some(StrVariant::from(name_wit.name)),
                                    wit_type: name_wit.wit_type.map(StrVariant::from),
                                    type_wrapper,
                                })
                                .collect(),
                        )
                    } else {
                        ParameterTypes(
                            params
                                .into_iter()
                                .map(|type_wrapper| ParameterType {
                                    name: None,
                                    wit_type: None,
                                    type_wrapper,
                                })
                                .collect(),
                        )
                    };
                    fns.insert(FnName::new_arc(function_name), (params, return_type));
                } else {
                    debug!("Ignoring export - not a ComponentFunc: {export:?}");
                }
            }
            vec.push(PackageIfcFns {
                ifc_fqn: IfcFqnName::new_arc(ifc_fqn),
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

#[cfg_attr(test, derive(serde::Serialize))]
struct WitParsedFunctionMetadata {
    params: Vec<ParameterNameWitType>,
    return_type: Option<String>,
}

#[cfg_attr(test, derive(serde::Serialize))]
struct ParameterNameWitType {
    name: String,
    wit_type: Option<String>,
}

fn wit_parsed_ffqn_to_wit_parsed_fn_metadata<'a>(
    resolve: &'a Resolve,
    iter: impl Iterator<Item = (&'a WorldKey, &'a WorldItem)>,
) -> Result<hashbrown::HashMap<FunctionFqn, WitParsedFunctionMetadata>, DecodeError> {
    let mut res_map = hashbrown::HashMap::new();
    for (_, item) in iter {
        if let wit_parser::WorldItem::Interface {
            id: ifc_id,
            stability: _,
        } = item
        {
            let ifc = resolve
                .interfaces
                .get(*ifc_id)
                .expect("`iter` must be derived from `resolve`");
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
            for (function_name, function) in &ifc.functions {
                let ffqn =
                    FunctionFqn::new_arc(ifc_fqn.clone(), Arc::from(function_name.to_string()));
                let params = function
                    .params
                    .iter()
                    .map(|(param_name, param_ty)| {
                        let mut printer = WitPrinter::default();
                        ParameterNameWitType {
                            name: param_name.to_string(),
                            wit_type: printer
                                .print_type_name(resolve, param_ty)
                                .ok()
                                .map(|()| String::from(printer)),
                        }
                    })
                    .collect();
                let return_type = if let Results::Anon(return_type) = function.results {
                    let mut printer = WitPrinter::default();
                    printer
                        .print_type_name(resolve, &return_type)
                        .ok()
                        .map(|()| String::from(printer))
                } else {
                    None
                };
                res_map.insert(
                    ffqn,
                    WitParsedFunctionMetadata {
                        params,
                        return_type,
                    },
                );
            }
        }
    }
    Ok(res_map)
}

#[cfg(test)]
mod tests {
    use super::wit_parsed_ffqn_to_wit_parsed_fn_metadata;
    use crate::wasm_tools::WasmComponent;
    use concepts::FunctionMetadata;
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
            .exported_functions(false)
            .iter()
            .map(
                |FunctionMetadata {
                     ffqn,
                     parameter_types,
                     return_type,
                 }| (ffqn.to_string(), (parameter_types, return_type)),
            )
            .collect::<hashbrown::HashMap<_, _>>();

        insta::with_settings!({sort_maps => true, snapshot_suffix => format!("{wasm_file}_exports_noext")}, {insta::assert_json_snapshot!(exports)});

        let exports = component
            .exported_functions(true)
            .iter()
            .map(
                |FunctionMetadata {
                     ffqn,
                     parameter_types,
                     return_type,
                 }| (ffqn.to_string(), (parameter_types, return_type)),
            )
            .collect::<hashbrown::HashMap<_, _>>();

        insta::with_settings!({sort_maps => true, snapshot_suffix => format!("{wasm_file}_exports_ext")}, {insta::assert_json_snapshot!(exports)});

        let imports = component
            .imported_functions()
            .iter()
            .map(
                |FunctionMetadata {
                     ffqn,
                     parameter_types,
                     return_type,
                 }| (ffqn.to_string(), (parameter_types, return_type)),
            )
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
        let exports = wit_parsed_ffqn_to_wit_parsed_fn_metadata(&resolve, world.exports.iter())
            .unwrap()
            .into_iter()
            .map(|(ffqn, val)| (ffqn.to_string(), val))
            .collect::<hashbrown::HashMap<_, _>>();
        insta::with_settings!({sort_maps => true, snapshot_suffix => format!("{wasm_file}_exports")}, {insta::assert_json_snapshot!(exports)});

        let imports = wit_parsed_ffqn_to_wit_parsed_fn_metadata(&resolve, world.imports.iter())
            .unwrap()
            .into_iter()
            .map(|(ffqn, val)| (ffqn.to_string(), (val, ffqn.ifc_fqn, ffqn.function_name)))
            .collect::<hashbrown::HashMap<_, _>>();
        insta::with_settings!({sort_maps => true, snapshot_suffix => format!("{wasm_file}_imports")}, {insta::assert_json_snapshot!(imports)});
    }
}
