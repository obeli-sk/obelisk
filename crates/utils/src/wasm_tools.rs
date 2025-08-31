use crate::sha256sum::calculate_sha256_file;
use anyhow::Context;
use concepts::{
    ComponentType, FnName, FunctionExtension, FunctionFqn, FunctionMetadata, IfcFqnName,
    PackageIfcFns, ParameterType, ParameterTypes, ReturnType, SUFFIX_PKG_EXT, SUFFIX_PKG_SCHEDULE,
    SUFFIX_PKG_STUB, StrVariant,
};
use indexmap::{IndexMap, indexmap};
use std::{
    borrow::Cow,
    path::{Path, PathBuf},
    sync::Arc,
};
use tracing::{debug, error, info, trace};
use val_json::type_wrapper::{TypeConversionError, TypeWrapper};
use wit_component::{ComponentEncoder, WitPrinter};
use wit_parser::{Resolve, WorldItem, WorldKey, decoding::DecodedWasm};

pub const EXTENSION_FN_SUFFIX_SCHEDULE: &str = "-schedule";

pub const HTTP_HANDLER_FFQN: FunctionFqn =
    FunctionFqn::new_static("wasi:http/incoming-handler", "handle");

#[derive(derive_more::Debug)]
pub struct WasmComponent {
    #[debug(skip)]
    pub wasmtime_component: wasmtime::component::Component,
    pub exim: ExIm,
    #[debug(skip)]
    decoded: DecodedWasm,
    component_type: ComponentType,
}

impl WasmComponent {
    pub async fn convert_core_module_to_component(
        wasm_path: &Path,
        output_parent: &Path,
    ) -> Result<Option<PathBuf>, anyhow::Error> {
        use tokio::io::AsyncReadExt;

        let mut wasm_file = tokio::fs::File::open(wasm_path)
            .await
            .with_context(|| format!("cannot open {wasm_path:?}"))
            .inspect_err(|err| {
                error!("{err:?}");
            })?;

        let mut header_vec = Vec::new();
        loop {
            let mut header = [0_u8; 8];
            let n = wasm_file.read(&mut header).await?;
            if n == 0 {
                break;
            }
            header_vec.extend(header[..n].iter());
            if header_vec.len() >= 8 {
                // `is_component` and `is_core_wasm` need only first 8 bytes.
                break;
            }
        }
        if wasmparser::Parser::is_component(&header_vec) {
            return Ok(None);
        }
        if !wasmparser::Parser::is_core_wasm(&header_vec) {
            error!("Not a WASM Component or a Core WASM Module: {wasm_file:?}");
            anyhow::bail!("not a WASM Component or a Core WASM Module");
        }
        let content_digest = calculate_sha256_file(wasm_path).await?;
        let output_file = output_parent.join(format!(
            "{hash_type}_{content_digest}.wasm",
            hash_type = content_digest.hash_type(),
            content_digest = content_digest.digest_base16(),
        ));
        // already transformed?
        if output_file.exists() {
            debug!("Found the transformed WASM Component {output_file:?}");
            return Ok(Some(output_file));
        }

        let wasm = tokio::fs::read(wasm_path).await?;
        let mut encoder = ComponentEncoder::default().validate(true);
        encoder = encoder.module(&wasm)?;
        let component_contents = encoder
            .encode()
            .with_context(|| {
                format!(
                    "failed to transform a WASM Component from the Core WASM Module {wasm_path:?}"
                )
            })
            .inspect_err(|err| error!("{err:?}"))?;

        tokio::fs::write(&output_file, component_contents)
            .await
            .with_context(|| {
                format!("cannot write the transformed WASM Component to {output_file:?}")
            })
            .inspect_err(|err| error!("{err:?}"))?;
        info!("Transformed Core WASM Module to WASM Component {output_file:?}");
        Ok(Some(output_file))
    }

    /// Attempt to open the WASM file, parse it using `wit_parser`.
    /// Check that the file is a WASM Component.
    pub fn verify_wasm<P: AsRef<Path>>(wasm_path: P) -> Result<(), DecodeError> {
        let wasm_path = wasm_path.as_ref();
        Self::decode_using_wit_parser_inner(
            wasm_path, false, // does not matter
        )?;
        Ok(())
    }

    fn decode_using_wit_parser(
        wasm_path: &Path,
        component_type: ComponentType,
    ) -> Result<(WitParsed, DecodedWasm), DecodeError> {
        Self::decode_using_wit_parser_inner(
            wasm_path,
            component_type != ComponentType::ActivityStub, // not submittable, stub executions must be created by workflows
        )
    }

    fn decode_using_wit_parser_inner(
        wasm_path: &Path,
        submittable_exports: bool, // whether exported functions + `-schedule` extensions should be submittable
    ) -> Result<(WitParsed, DecodedWasm), DecodeError> {
        trace!("Decoding using wit_parser");

        let wasm_file = std::fs::File::open(wasm_path)
            .with_context(|| format!("cannot open {wasm_path:?}"))
            .map_err(|err| {
                error!("Cannot read the file {wasm_path:?} - {err:?}");
                DecodeError::CannotReadComponent { source: err }
            })?;
        let stopwatch = std::time::Instant::now();
        let decoded = wit_parser::decoding::decode_reader(wasm_file).map_err(|err| {
            error!("Cannot read {wasm_path:?} using wit_parser - {err:?}");
            DecodeError::CannotReadComponent { source: err }
        })?;

        let (resolve, world_id) = match &decoded {
            DecodedWasm::Component(resolve, world_id) => (resolve, world_id),
            DecodedWasm::WitPackage(..) => {
                error!("Input file must not be a WIT package");
                return Err(DecodeError::CannotReadComponentWithReason {
                    reason: "must not be a WIT package".to_string(),
                });
            }
        };

        let world = resolve.worlds.get(*world_id).expect("world must exist");
        let exports = populate_ifcs(
            resolve,
            world.exports.iter(),
            if submittable_exports {
                ProcessingKind::ExportsSubmittable
            } else {
                ProcessingKind::ExportsOfActivityStub
            },
        )?;
        let imports = populate_ifcs(resolve, world.imports.iter(), ProcessingKind::Imports)?;
        debug!("Parsed with wit_parser in {:?}", stopwatch.elapsed());
        trace!("Exports: {exports:?}, imports: {imports:?}");
        Ok((WitParsed { imports, exports }, decoded))
    }

    pub fn new<P: AsRef<Path>>(
        wasm_path: P,
        engine: &wasmtime::Engine,
        component_type: ComponentType,
    ) -> Result<Self, DecodeError> {
        let wasm_path = wasm_path.as_ref();

        trace!("Decoding using wasmtime");
        let wasmtime_component = {
            let stopwatch = std::time::Instant::now();
            let wasmtime_component = wasmtime::component::Component::from_file(engine, wasm_path)
                .map_err(|err| {
                error!("Cannot parse {wasm_path:?} using wasmtime - {err:?}");
                DecodeError::CannotReadComponent { source: err }
            })?;
            debug!("Parsed with wasmtime in {:?}", stopwatch.elapsed());
            wasmtime_component
        };
        let (wit_parsed, decoded) = Self::decode_using_wit_parser(wasm_path, component_type)?;

        let exim = ExIm::decode(wit_parsed, component_type)?;

        Ok(Self {
            wasmtime_component,
            exim,
            decoded,
            component_type,
        })
    }

    pub fn wit(&self) -> Result<String, anyhow::Error> {
        crate::wit::wit(self.component_type, &self.decoded, &self.exim)
    }

    #[must_use]
    pub fn exported_functions(&self, extensions: bool) -> &[FunctionMetadata] {
        if extensions {
            &self.exim.exports_flat_ext
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
    ) -> Result<
        hashbrown::HashMap<FunctionFqn, wasmtime::component::ComponentExportIndex>,
        DecodeError,
    > {
        let mut exported_ffqn_to_index = hashbrown::HashMap::new();
        for FunctionMetadata { ffqn, .. } in &self.exim.exports_flat_noext {
            let Some(ifc_export_index) = self
                .wasmtime_component
                .get_export_index(None, &ffqn.ifc_fqn)
            else {
                error!("Cannot find exported interface `{}`", ffqn.ifc_fqn);
                return Err(DecodeError::CannotReadComponentWithReason {
                    reason: format!("cannot find exported interface {ffqn}"),
                });
            };
            let Some(fn_export_index) = self
                .wasmtime_component
                .get_export_index(Some(&ifc_export_index), &ffqn.function_name)
            else {
                error!("Cannot find exported function {ffqn}");
                return Err(DecodeError::CannotReadComponentWithReason {
                    reason: format!("cannot find exported function {ffqn}"),
                });
            };
            exported_ffqn_to_index.insert(ffqn.clone(), fn_export_index);
        }
        Ok(exported_ffqn_to_index)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum DecodeError {
    #[error("cannot read the WASM Component")]
    CannotReadComponent {
        #[source]
        source: anyhow::Error,
    },
    #[error("cannot read the WASM Component - {reason}")]
    CannotReadComponentWithReason { reason: String },
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
    exports_hierarchy_ext: Vec<PackageIfcFns>,
    exports_flat_noext: Vec<FunctionMetadata>,
    exports_flat_ext: Vec<FunctionMetadata>,
    pub imports_flat: Vec<FunctionMetadata>,
}

impl ExIm {
    #[must_use]
    pub fn get_exports(&self, extensions: bool) -> &[FunctionMetadata] {
        if extensions {
            &self.exports_flat_ext
        } else {
            &self.exports_flat_noext
        }
    }

    #[must_use]
    pub fn get_exports_hierarchy_ext(&self) -> &[PackageIfcFns] {
        &self.exports_hierarchy_ext
    }

    pub fn get_exports_hierarchy_noext(&self) -> impl Iterator<Item = &PackageIfcFns> {
        self.exports_hierarchy_ext
            .iter()
            .filter(|pif| !pif.extension)
    }

    fn decode(wit_parsed: WitParsed, component_type: ComponentType) -> Result<ExIm, DecodeError> {
        let mut exports_hierarchy_ext = wit_parsed.exports;
        // Verify that there is no -obelisk-ext export
        for PackageIfcFns {
            ifc_fqn,
            fns: _,
            extension,
        } in &exports_hierarchy_ext
        {
            assert!(
                !extension,
                "the list must not be enriched with extensions yet"
            );
            if ifc_fqn.is_extension() {
                return Err(DecodeError::ReservedPackageSuffix(
                    ifc_fqn.package_name().to_string(),
                ));
            }
        }

        let exports_flat_noext = Self::flatten(&exports_hierarchy_ext);
        Self::enrich_exports_with_extensions(&mut exports_hierarchy_ext, component_type);
        let exports_flat_ext = Self::flatten(&exports_hierarchy_ext);
        let imports_hierarchy = wit_parsed.imports;
        let imports_flat = Self::flatten(&imports_hierarchy);
        Ok(Self {
            exports_hierarchy_ext,
            exports_flat_noext,
            exports_flat_ext,
            imports_flat,
        })
    }

    fn enrich_exports_with_extensions(
        exports_hierarchy: &mut Vec<PackageIfcFns>,
        component_type: ComponentType,
    ) {
        if component_type == ComponentType::WebhookEndpoint {
            return;
        }
        // initialize values for reuse
        let execution_id_type_wrapper =
            TypeWrapper::Record(indexmap! {"id".into() => TypeWrapper::String});
        let delay_id_type_wrapper =
            TypeWrapper::Record(indexmap! {"id".into() => TypeWrapper::String});
        let join_set_id_type_wrapper = TypeWrapper::Borrow;

        let return_type_execution_id = Some(ReturnType {
            type_wrapper: execution_id_type_wrapper.clone(),
            wit_type: concepts::StrVariant::Static("execution-id"),
        });
        let param_type_execution_id = ParameterType {
            type_wrapper: execution_id_type_wrapper.clone(),
            name: StrVariant::Static("execution-id"),
            wit_type: StrVariant::Static("execution-id"),
        };
        let param_type_join_set = ParameterType {
            type_wrapper: join_set_id_type_wrapper.clone(),
            name: StrVariant::Static("join-set-id"),
            wit_type: StrVariant::Static("borrow<join-set-id>"),
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
            name: StrVariant::Static("scheduled-at"),
            wit_type: StrVariant::Static("schedule-at"),
        };

        let function_type_wrapper = TypeWrapper::Record(indexmap! {
            Box::from("interface-name") => TypeWrapper::String,
            Box::from("function-name") => TypeWrapper::String,
        });

        let response_id = TypeWrapper::Variant(indexmap! {
            Box::from("execution-id") => Some(execution_id_type_wrapper.clone()),
            Box::from("delay-id") => Some(delay_id_type_wrapper.clone()),
        });

        // record execution-failed
        let execution_failed_type_wrapper = TypeWrapper::Record(indexmap! {
            Box::from("execution-id") => execution_id_type_wrapper.clone()
        });

        // record function-mismatch
        let function_mismatch_type_wrapper = TypeWrapper::Record(indexmap! {
            Box::from("specified-function") => function_type_wrapper.clone(),
            Box::from("actual-function") => TypeWrapper::Option(Box::from(function_type_wrapper)),
            Box::from("actual-id") => response_id,
        });

        // await-next-extension-error
        let await_next_extension_error_type_wrapper = TypeWrapper::Variant(indexmap! {
            Box::from("execution-failed") => Some(execution_failed_type_wrapper.clone()),
            Box::from("all-processed") => None,
            Box::from("function-mismatch") => Some(function_mismatch_type_wrapper.clone()),
        });

        // get-extension-error
        let get_extension_error_type_wrapper = TypeWrapper::Variant(indexmap! {
            Box::from("execution-failed") => Some(execution_failed_type_wrapper.clone()),
            Box::from("function-mismatch") => Some(function_mismatch_type_wrapper.clone()),
            Box::from("not-found-in-processed-responses") => None,
        });

        // stub-error
        let stub_error_type_wrapper = TypeWrapper::Variant(indexmap! {
            Box::from("conflict") => None,
        });

        let mut extensions = Vec::new();
        let mut schedules = Vec::new();
        let mut stubs = Vec::new();
        for PackageIfcFns {
            ifc_fqn,
            fns,
            extension,
        } in exports_hierarchy.iter()
        {
            assert!(!extension);
            let obelisk_ext_ifc = IfcFqnName::from_parts(
                ifc_fqn.namespace(),
                &format!("{}{SUFFIX_PKG_EXT}", ifc_fqn.package_name()),
                ifc_fqn.ifc_name(),
                ifc_fqn.version(),
            );
            let obelisk_schedule_ifc = IfcFqnName::from_parts(
                ifc_fqn.namespace(),
                &format!("{}{SUFFIX_PKG_SCHEDULE}", ifc_fqn.package_name()),
                ifc_fqn.ifc_name(),
                ifc_fqn.version(),
            );
            let obelisk_stub_ifc = IfcFqnName::from_parts(
                ifc_fqn.namespace(),
                &format!("{}{SUFFIX_PKG_STUB}", ifc_fqn.package_name()),
                ifc_fqn.ifc_name(),
                ifc_fqn.version(),
            );

            let mut extension_fns = IndexMap::new();
            let mut schedule_fns = IndexMap::new();
            let mut insert_ext = |fn_metadata: FunctionMetadata| {
                extension_fns.insert(fn_metadata.ffqn.function_name.clone(), fn_metadata);
            };
            let mut insert_schedule = |fn_metadata: FunctionMetadata| {
                schedule_fns.insert(fn_metadata.ffqn.function_name.clone(), fn_metadata);
            };

            let mut stub_fns = IndexMap::new();
            for (
                fun,
                FunctionMetadata {
                    ffqn: _,
                    parameter_types,
                    return_type,
                    extension,
                    submittable: original_submittable,
                },
            ) in fns
            {
                assert!(
                    extension.is_none(),
                    "`exports_hierarchy` must not contain extensions"
                );
                let exported_fn_metadata = FunctionMetadata {
                    ffqn: FunctionFqn {
                        ifc_fqn: ifc_fqn.clone(),
                        function_name: fun.clone(),
                    },
                    parameter_types: parameter_types.clone(),
                    return_type: return_type.clone(),
                    extension: None,
                    submittable: false,
                };

                // -submit(join-set-id: join-set-id, original params) -> execution id
                let fn_submit = FunctionMetadata {
                    ffqn: FunctionFqn {
                        ifc_fqn: obelisk_ext_ifc.clone(),
                        function_name: FnName::from(format!(
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
                    extension: Some(FunctionExtension::Submit),
                    submittable: false,
                };
                insert_ext(fn_submit);

                // -await-next(join-set-id: join-set-id) ->  result<(execution_id, original_return_type), await-next-extension-error>
                let fn_await_next = FunctionMetadata {
                    ffqn: FunctionFqn {
                        ifc_fqn: obelisk_ext_ifc.clone(),
                        function_name: FnName::from(format!(
                            "{}-await-next",
                            exported_fn_metadata.ffqn.function_name
                        )),
                    },
                    parameter_types: ParameterTypes(vec![param_type_join_set.clone()]),
                    return_type: {
                        let (ok_part, ok_type_wrapper) =
                            if let Some(original_ret) = &exported_fn_metadata.return_type {
                                (
                                    Cow::Owned(format!("tuple<execution-id, {original_ret}>")),
                                    // (execution-id, original_ret)
                                    TypeWrapper::Tuple(Box::new([
                                        execution_id_type_wrapper.clone(),
                                        original_ret.type_wrapper.clone(),
                                    ])),
                                )
                            } else {
                                (
                                    Cow::Borrowed("execution-id"),
                                    execution_id_type_wrapper.clone(),
                                )
                            };
                        Some(ReturnType {
                            type_wrapper: TypeWrapper::Result {
                                ok: Some(Box::new(ok_type_wrapper)),
                                err: Some(Box::new(
                                    await_next_extension_error_type_wrapper.clone(),
                                )),
                            },
                            wit_type: StrVariant::from(format!(
                                "result<{ok_part}, await-next-extension-error>"
                            )),
                        })
                    },
                    extension: Some(FunctionExtension::AwaitNext),
                    submittable: false,
                };
                insert_ext(fn_await_next);

                // -get(execution-id) -> result<original_return_type, get-extension-error>
                // or
                // -get(execution-id) -> result<_,  get-extension-error>
                let fn_get = FunctionMetadata {
                    ffqn: FunctionFqn {
                        ifc_fqn: obelisk_ext_ifc.clone(),
                        function_name: FnName::from(format!(
                            "{}-get",
                            exported_fn_metadata.ffqn.function_name
                        )),
                    },
                    parameter_types: ParameterTypes(vec![param_type_execution_id.clone()]),
                    return_type: {
                        let (ok_wit, ok_type_wrapper) =
                            if let Some(original_ret) = &exported_fn_metadata.return_type {
                                (
                                    &original_ret.wit_type,
                                    Some(Box::new(original_ret.type_wrapper.clone())),
                                )
                            } else {
                                (&StrVariant::Static("_"), None)
                            };
                        Some(ReturnType {
                            type_wrapper: TypeWrapper::Result {
                                ok: ok_type_wrapper,
                                err: Some(Box::new(get_extension_error_type_wrapper.clone())),
                            },
                            wit_type: StrVariant::from(format!(
                                "result<{ok_wit}, get-extension-error>"
                            )),
                        })
                    },
                    extension: Some(FunctionExtension::Get),
                    submittable: false,
                };
                insert_ext(fn_get);

                if component_type != ComponentType::ActivityStub {
                    assert!(
                        original_submittable,
                        "original exported function must be submittable for components different from activity stubs"
                    );
                    // -schedule(schedule: schedule-at, original params) -> string (execution id)
                    let fn_schedule = FunctionMetadata {
                        ffqn: FunctionFqn {
                            ifc_fqn: obelisk_schedule_ifc.clone(),
                            function_name: FnName::from(format!(
                                "{}{EXTENSION_FN_SUFFIX_SCHEDULE}",
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
                        extension: Some(FunctionExtension::Schedule),
                        submittable: true,
                    };
                    insert_schedule(fn_schedule);
                }

                if component_type == ComponentType::ActivityStub {
                    // -stub(execution-id: execution-id, original retval) -> result<_, stub-error>
                    let fn_stub = FunctionMetadata {
                        ffqn: FunctionFqn {
                            ifc_fqn: obelisk_stub_ifc.clone(),
                            function_name: FnName::from(format!(
                                "{}-stub",
                                exported_fn_metadata.ffqn.function_name
                            )),
                        },
                        parameter_types: {
                            let mut params = vec![param_type_execution_id.clone()];
                            if let Some(original_ret) = &exported_fn_metadata.return_type {
                                params.push(ParameterType {
                                    type_wrapper: TypeWrapper::Result {
                                        ok: Some(Box::new(original_ret.type_wrapper.clone())),
                                        err: None,
                                    },
                                    name: StrVariant::Static("execution-result"),
                                    wit_type: format!("result<{}>", original_ret.wit_type).into(),
                                });
                            } else {
                                params.push(ParameterType {
                                    type_wrapper: TypeWrapper::Result {
                                        ok: None,
                                        err: None,
                                    },
                                    name: StrVariant::Static("execution-result"),
                                    wit_type: "result".into(),
                                });
                            }
                            ParameterTypes(params)
                        },
                        return_type: Some(ReturnType {
                            type_wrapper: TypeWrapper::Result {
                                ok: None,
                                err: Some(Box::new(stub_error_type_wrapper.clone())),
                            },
                            wit_type: StrVariant::Static("result<_, stub-error>"),
                        }),
                        extension: Some(FunctionExtension::Stub),
                        submittable: false,
                    };
                    stub_fns.insert(fn_stub.ffqn.function_name.clone(), fn_stub);
                }
            }
            extensions.push((obelisk_ext_ifc, extension_fns));
            schedules.push((obelisk_schedule_ifc, schedule_fns));
            stubs.push((obelisk_stub_ifc, stub_fns));
        }
        for (ifc_fqn, fns) in extensions {
            exports_hierarchy.push(PackageIfcFns {
                ifc_fqn,
                fns,
                extension: true,
            });
        }
        for (ifc_fqn, fns) in schedules {
            exports_hierarchy.push(PackageIfcFns {
                ifc_fqn,
                fns,
                extension: true,
            });
        }
        for (ifc_fqn, fns) in stubs {
            exports_hierarchy.push(PackageIfcFns {
                ifc_fqn,
                fns,
                extension: true,
            });
        }
    }

    fn flatten(input: &[PackageIfcFns]) -> Vec<FunctionMetadata> {
        input
            .iter()
            .flat_map(|pif| pif.fns.values().cloned())
            .collect()
    }
}

struct WitParsed {
    imports: Vec<PackageIfcFns>,
    exports: Vec<PackageIfcFns>,
}

#[derive(Clone, Copy)]
enum ProcessingKind {
    Imports,
    ExportsOfActivityStub,
    ExportsSubmittable,
}
impl ProcessingKind {
    fn is_submittable_export(&self) -> bool {
        matches!(self, ProcessingKind::ExportsSubmittable)
    }
}

fn populate_ifcs<'a>(
    resolve: &'a Resolve,
    iter: impl Iterator<Item = (&'a WorldKey, &'a WorldItem)>,
    processing_kind: ProcessingKind,
) -> Result<Vec<PackageIfcFns>, DecodeError> {
    let mut vec = Vec::new();
    let submittable = processing_kind.is_submittable_export();
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
            let mut fns = IndexMap::new();
            for (function_name, function) in &ifc.functions {
                let ffqn =
                    FunctionFqn::new_arc(ifc_fqn.clone(), Arc::from(function_name.to_string()));
                let parameter_types = ParameterTypes({
                    let mut params = Vec::new();
                    for (param_name, param_ty) in &function.params {
                        let mut printer = WitPrinter::default();
                        let item = ParameterType {
                            type_wrapper: match TypeWrapper::from_wit_parser_type(resolve, param_ty)
                            {
                                Ok(ok) => ok,
                                Err(err) => {
                                    return Err(DecodeError::TypeNotSupported { err, ffqn });
                                }
                            },
                            name: StrVariant::from(param_name.to_string()),
                            wit_type: printer
                                .print_type_name(resolve, param_ty)
                                .ok()
                                .map_or(StrVariant::Static("unknown"), |()| {
                                    StrVariant::from(printer.output.to_string())
                                }),
                        };
                        params.push(item);
                    }
                    params
                });
                let return_type = if let Some(return_type) = function.result {
                    let mut printer = WitPrinter::default();
                    let wit_type = printer
                        .print_type_name(resolve, &return_type)
                        .ok()
                        .map_or(StrVariant::Static("unknown"), |()| {
                            StrVariant::from(printer.output.to_string())
                        });
                    Some(ReturnType {
                        type_wrapper: match TypeWrapper::from_wit_parser_type(resolve, &return_type)
                        {
                            Ok(ok) => ok,
                            Err(err) => {
                                return Err(DecodeError::TypeNotSupported { err, ffqn });
                            }
                        },
                        wit_type,
                    })
                } else {
                    None
                };
                fns.insert(
                    ffqn.function_name.clone(),
                    FunctionMetadata {
                        ffqn,
                        parameter_types,
                        return_type,
                        extension: None,
                        submittable,
                    },
                );
            }
            vec.push(PackageIfcFns {
                ifc_fqn: IfcFqnName::new_arc(ifc_fqn),
                fns,
                extension: false,
            });
        }
    }
    Ok(vec)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::populate_ifcs;
    use crate::wasm_tools::{ProcessingKind, WasmComponent};
    use concepts::ComponentType;
    use rstest::rstest;
    use std::{path::PathBuf, sync::Arc};
    use wasmtime::Engine;
    use wit_parser::decoding::DecodedWasm;

    pub(crate) fn engine() -> Arc<Engine> {
        let mut wasmtime_config = wasmtime::Config::new();
        wasmtime_config.wasm_component_model(true);
        wasmtime_config.async_support(true);
        Arc::new(Engine::new(&wasmtime_config).unwrap())
    }

    #[derive(Debug, Clone, Copy)]
    enum ExIm {
        Exports,
        ExportsExtended,
        Imports,
    }

    #[rstest]
    #[case(test_programs_fibo_workflow_builder::TEST_PROGRAMS_FIBO_WORKFLOW)]
    #[case(test_programs_http_get_workflow_builder::TEST_PROGRAMS_HTTP_GET_WORKFLOW)]
    fn exports_imports(
        #[case] wasm_path: &str,
        #[values(ExIm::Exports, ExIm::ExportsExtended, ExIm::Imports)] exim: ExIm,
    ) {
        test_utils::set_up();

        let wasm_path = PathBuf::from(wasm_path);
        let wasm_file = wasm_path.file_name().unwrap().to_string_lossy();
        let engine = engine();
        let component = WasmComponent::new(&wasm_path, &engine, ComponentType::Workflow).unwrap();
        match exim {
            ExIm::Exports => {
                let exports = component
                    .exported_functions(false)
                    .iter()
                    .map(|fn_metadata| (fn_metadata.ffqn.to_string(), fn_metadata))
                    .collect::<hashbrown::HashMap<_, _>>();
                insta::with_settings!({sort_maps => true, snapshot_suffix => format!("{wasm_file}_exports_noext")}, {insta::assert_json_snapshot!(exports)});
            }
            ExIm::ExportsExtended => {
                let exports = component
                    .exported_functions(true)
                    .iter()
                    .map(|fn_metadata| (fn_metadata.ffqn.to_string(), fn_metadata))
                    .collect::<hashbrown::HashMap<_, _>>();
                insta::with_settings!({sort_maps => true, snapshot_suffix => format!("{wasm_file}_exports_ext")}, {insta::assert_json_snapshot!(exports)});
            }
            ExIm::Imports => {
                let imports = component
                    .imported_functions()
                    .iter()
                    .map(|fn_metadata| (fn_metadata.ffqn.to_string(), fn_metadata))
                    .collect::<hashbrown::HashMap<_, _>>();
                insta::with_settings!({sort_maps => true, snapshot_suffix => format!("{wasm_file}_imports")}, {insta::assert_json_snapshot!(imports)});
            }
        }
    }

    #[rstest]
    fn test_params(#[values(true, false)] exports: bool) {
        let wasm_path =
            PathBuf::from(test_programs_fibo_workflow_builder::TEST_PROGRAMS_FIBO_WORKFLOW);
        let wasm_file = wasm_path.file_name().unwrap().to_string_lossy();
        let file = std::fs::File::open(&wasm_path).unwrap();
        let decoded = wit_parser::decoding::decode_reader(file).unwrap();
        let DecodedWasm::Component(resolve, world_id) = decoded else {
            panic!();
        };
        let world = resolve.worlds.get(world_id).expect("world must exist");
        if exports {
            let exports = populate_ifcs(
                &resolve,
                world.exports.iter(),
                ProcessingKind::ExportsSubmittable,
            )
            .unwrap()
            .into_iter()
            .flat_map(|ifc| ifc.fns)
            .map(|(_fn_name, fn_metadata)| (fn_metadata.ffqn.to_string(), fn_metadata))
            .collect::<hashbrown::HashMap<_, _>>();
            insta::with_settings!({sort_maps => true,  snapshot_suffix => format!("{wasm_file}_exports")}, {insta::assert_json_snapshot!(exports)});
        } else {
            let imports = populate_ifcs(&resolve, world.imports.iter(), ProcessingKind::Imports)
                .unwrap()
                .into_iter()
                .flat_map(|ifc| ifc.fns)
                .map(|(_fn_name, fn_metadata)| (fn_metadata.ffqn.to_string(), fn_metadata))
                .collect::<hashbrown::HashMap<_, _>>();
            insta::with_settings!({ sort_maps => true,  snapshot_suffix => format!("{wasm_file}_imports")}, {insta::assert_json_snapshot!(imports)});
        }
    }
}
