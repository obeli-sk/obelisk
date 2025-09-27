use crate::{sha256sum::calculate_sha256_file, wit::from_wit_package_name_to_pkg_fqn};
use anyhow::Context;
use concepts::{
    ComponentType, FnName, FunctionExtension, FunctionFqn, FunctionMetadata, IfcFqnName,
    PackageIfcFns, ParameterType, ParameterTypes, PkgFqn, ReturnType, ReturnTypeNonExtendable,
    SUFFIX_FN_AWAIT_NEXT, SUFFIX_FN_GET, SUFFIX_FN_INVOKE, SUFFIX_FN_SCHEDULE, SUFFIX_FN_STUB,
    SUFFIX_FN_SUBMIT, SUFFIX_PKG_EXT, SUFFIX_PKG_SCHEDULE, SUFFIX_PKG_STUB, StrVariant,
};
use indexmap::{IndexMap, indexmap};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};
use strum::IntoEnumIterator as _;
use tracing::{debug, error, info, trace, warn};
use val_json::type_wrapper::{TypeConversionError, TypeWrapper};
use wit_component::{ComponentEncoder, WitPrinter};
use wit_parser::{InterfaceId, PackageId, Resolve, World, WorldKey, decoding::DecodedWasm};

#[derive(derive_more::Debug)]
pub struct WasmComponent {
    pub exim: ExIm,
    resolve: Resolve,
    main_pkg_id: PackageId,
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
    pub fn verify_wasm(wasm_path: impl AsRef<Path>) -> Result<(), DecodeError> {
        let wasm_path = wasm_path.as_ref();
        Self::decode_using_wit_parser_inner(
            wasm_path, false, // `submittable_exports` parameter does not matter
        )?;
        Ok(())
    }

    pub fn new(
        wasm_path: impl AsRef<Path>,
        component_type: ComponentType,
    ) -> Result<Self, DecodeError> {
        let wasm_path = wasm_path.as_ref();
        let (exim_lite, resolve, main_pkg_id) =
            Self::decode_using_wit_parser(wasm_path, component_type)?;

        let exim = ExIm::decode(exim_lite, component_type)?;
        let (resolve, main_pkg_id) = crate::wit::rebuild_resolve(&exim, resolve, main_pkg_id)
            .map_err(DecodeError::RebuildResolveError)?;
        Ok(Self {
            exim,
            resolve,
            main_pkg_id,
        })
    }

    pub fn new_from_wit_folder(
        path: impl AsRef<Path>,
        component_type: ComponentType,
    ) -> Result<Self, DecodeError> {
        let mut resolve = Resolve::default();
        let (main_pkg_id, _) = resolve
            .push_dir(path)
            .map_err(|source| DecodeError::WitDirectoryParsingError { source })?;
        let world_id = resolve
            .select_world(&[main_pkg_id], None)
            .map_err(|source| DecodeError::WorldSelectionError { source })?;
        let world = resolve.worlds.get(world_id).expect("world must exist");
        let exim_lite =
            Self::create_exim_lite(&resolve, world, has_submittable_exports(component_type))?;

        let exim = ExIm::decode(exim_lite, component_type)?;
        let (resolve, main_pkg_id) = crate::wit::rebuild_resolve(&exim, resolve, main_pkg_id)
            .map_err(DecodeError::RebuildResolveError)?;
        Ok(Self {
            exim,
            resolve,
            main_pkg_id,
        })
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

    pub fn wit(&self) -> Result<String, anyhow::Error> {
        crate::wit::wit(&self.resolve, self.main_pkg_id)
    }

    pub fn exported_extension_wits(
        &self,
    ) -> Result<hashbrown::HashMap<PkgFqn, String>, anyhow::Error> {
        let mut pkgs_to_wits = hashbrown::HashMap::new();

        let exported_ext_packages: hashbrown::HashSet<_> = self
            .exim
            .exports_hierarchy_ext
            .iter()
            .filter_map(|pkg_ifc_fns| {
                if pkg_ifc_fns.extension {
                    Some(pkg_ifc_fns.ifc_fqn.pkg_fqn_name().to_string())
                } else {
                    None
                }
            })
            .collect();

        for (package_id, package) in self
            .resolve
            .packages
            .iter()
            .filter(|(_, package)| exported_ext_packages.contains(&package.name.to_string()))
        {
            let pkg_fqn = from_wit_package_name_to_pkg_fqn(&package.name);

            let mut printer = wit_component::WitPrinter::default();
            printer.print(&self.resolve, package_id, &[]).unwrap();
            let wit = printer.output.to_string();

            pkgs_to_wits.insert(pkg_fqn, wit);
        }
        Ok(pkgs_to_wits)
    }

    fn decode_using_wit_parser(
        wasm_path: &Path,
        component_type: ComponentType,
    ) -> Result<(ExImLite, Resolve, PackageId), DecodeError> {
        Self::decode_using_wit_parser_inner(wasm_path, has_submittable_exports(component_type))
    }

    fn decode_using_wit_parser_inner(
        wasm_path: &Path,
        submittable_exports: bool, // whether exported functions + `-schedule` extensions should be submittable
    ) -> Result<(ExImLite, Resolve, PackageId), DecodeError> {
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
        let main_package = decoded.package();
        let DecodedWasm::Component(resolve, world_id) = decoded else {
            error!("Input file must be a WASM Component");
            return Err(DecodeError::CannotReadComponentWithReason {
                reason: "must be a WASM Component".to_string(),
            });
        };
        let world = resolve.worlds.get(world_id).expect("world must exist");
        let exim_lite = Self::create_exim_lite(&resolve, world, submittable_exports)?;
        debug!("Parsed with wit_parser in {:?}", stopwatch.elapsed());
        trace!("{exim_lite:?}");
        Ok((exim_lite, resolve, main_package))
    }

    fn create_exim_lite(
        resolve: &Resolve,
        world: &World,
        submittable_exports: bool,
    ) -> Result<ExImLite, DecodeError> {
        let exports = populate_ifcs_with_compatible_fns(
            resolve,
            world_interfaces(world, ExOrIm::Exports),
            if submittable_exports {
                ProcessingKind::ExportsSubmittable
            } else {
                ProcessingKind::ExportsOfActivityStub
            },
        )?;
        let imports = populate_ifcs_with_compatible_fns(
            resolve,
            world_interfaces(world, ExOrIm::Imports),
            ProcessingKind::Imports,
        )?;
        Ok(ExImLite { imports, exports })
    }
}

#[derive(Clone, Copy, PartialEq)]
pub(crate) enum ExOrIm {
    Exports,
    Imports,
}

fn world_interfaces(world: &World, exorim: ExOrIm) -> impl Iterator<Item = InterfaceId> {
    match exorim {
        ExOrIm::Exports => &world.exports,
        ExOrIm::Imports => &world.imports,
    }
    .keys()
    .filter_map(|world_key| {
        if let WorldKey::Interface(ifc_id) = world_key {
            Some(*ifc_id)
        } else {
            None
        }
    })
}

fn has_submittable_exports(component_type: ComponentType) -> bool {
    component_type != ComponentType::ActivityStub // not submittable, stub executions must be created by workflows
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
    #[error("invalid package `{0}`, {SUFFIX_PKG_EXT} is reserved")]
    ReservedPackageSuffix(String),
    #[error("cannot parse the WIT directory")]
    WitDirectoryParsingError {
        #[source]
        source: anyhow::Error,
    },
    #[error("cannot select the default world")]
    WorldSelectionError {
        #[source]
        source: anyhow::Error,
    },
    #[error("cannot rebuild resolve - {0}")]
    RebuildResolveError(anyhow::Error),
    #[error("component is exporting obelisk extended package `{0}`")]
    ExportingExt(String),
    #[error("wrong extension import {0}")]
    WrongExtImport(FunctionFqn),
}

#[derive(Debug, Clone)]
pub struct ExIm {
    pub(crate) exports_hierarchy_ext: Vec<PackageIfcFns>,
    exports_flat_noext: Vec<FunctionMetadata>,
    exports_flat_ext: Vec<FunctionMetadata>,
    pub imports_flat: Vec<FunctionMetadata>,
    pub(crate) imports_hierarchy: Vec<PackageIfcFns>,
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

    fn decode(exim_lite: ExImLite, component_type: ComponentType) -> Result<ExIm, DecodeError> {
        let mut exports_hierarchy_ext = exim_lite.exports;
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
        let imports_hierarchy = exim_lite.imports;
        let imports_flat = Self::flatten(&imports_hierarchy);
        Ok(Self {
            exports_hierarchy_ext,
            exports_flat_noext,
            exports_flat_ext,
            imports_flat,
            imports_hierarchy,
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

        let return_type_execution_id = ReturnType::NonExtendable(ReturnTypeNonExtendable {
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
        let param_type_invoke_label = ParameterType {
            type_wrapper: TypeWrapper::String,
            name: "label".into(),
            wit_type: "string".into(),
        };
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

        // record function-mismatch
        let function_mismatch_type_wrapper = TypeWrapper::Record(indexmap! {
            Box::from("specified-function") => function_type_wrapper.clone(),
            Box::from("actual-function") => TypeWrapper::Option(Box::from(function_type_wrapper)),
            Box::from("actual-id") => response_id,
        });

        // await-next-extension-error
        let await_next_extension_error_type_wrapper = TypeWrapper::Variant(indexmap! {
            Box::from("all-processed") => None,
            Box::from("function-mismatch") => Some(function_mismatch_type_wrapper.clone()),
        });

        // get-extension-error
        let get_extension_error_type_wrapper = TypeWrapper::Variant(indexmap! {
            Box::from("function-mismatch") => Some(function_mismatch_type_wrapper.clone()),
            Box::from("not-found-in-processed-responses") => None,
        });

        // invoke-extension-error
        let invoke_extension_error_type_wrapper = TypeWrapper::Variant(indexmap! {
            Box::from("invalid-name") => Some(TypeWrapper::String),
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
                let ReturnType::Extendable(return_type) = return_type else {
                    unreachable!(
                        "all ExImLite exported functions must have their return type validated"
                    )
                };

                // -submit(join-set-id: join-set-id, original params) -> execution id
                let fn_submit = FunctionMetadata {
                    ffqn: FunctionFqn {
                        ifc_fqn: obelisk_ext_ifc.clone(),
                        function_name: FnName::from(format!(
                            "{}{SUFFIX_FN_SUBMIT}",
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
                            "{}{SUFFIX_FN_AWAIT_NEXT}",
                            exported_fn_metadata.ffqn.function_name
                        )),
                    },
                    parameter_types: ParameterTypes(vec![param_type_join_set.clone()]),
                    return_type: {
                        let ok_part = format!(
                            "tuple<execution-id, {original_ret}>",
                            original_ret = exported_fn_metadata.return_type
                        );
                        // (execution-id, original_ret)
                        let ok_type_wrapper = TypeWrapper::Tuple(Box::new([
                            execution_id_type_wrapper.clone(),
                            TypeWrapper::from(return_type.type_wrapper_tl.clone()),
                        ]));
                        ReturnType::detect(
                            TypeWrapper::Result {
                                ok: Some(Box::new(ok_type_wrapper)),
                                err: Some(Box::new(
                                    await_next_extension_error_type_wrapper.clone(),
                                )),
                            },
                            StrVariant::from(format!(
                                "result<{ok_part}, await-next-extension-error>"
                            )),
                        )
                    },
                    extension: Some(FunctionExtension::AwaitNext),
                    submittable: false,
                };
                insert_ext(fn_await_next);

                // -get(execution-id) -> result<original_return_type, get-extension-error>
                let fn_get = FunctionMetadata {
                    ffqn: FunctionFqn {
                        ifc_fqn: obelisk_ext_ifc.clone(),
                        function_name: FnName::from(format!(
                            "{}{SUFFIX_FN_GET}",
                            exported_fn_metadata.ffqn.function_name
                        )),
                    },
                    parameter_types: ParameterTypes(vec![param_type_execution_id.clone()]),
                    return_type: {
                        ReturnType::detect(
                            TypeWrapper::Result {
                                ok: Some(Box::new(TypeWrapper::from(
                                    return_type.type_wrapper_tl.clone(),
                                ))),
                                err: Some(Box::new(get_extension_error_type_wrapper.clone())),
                            },
                            StrVariant::from(format!(
                                "result<{}, get-extension-error>",
                                return_type.wit_type
                            )),
                        )
                    },
                    extension: Some(FunctionExtension::Get),
                    submittable: false,
                };
                insert_ext(fn_get);

                // -invoke(label: string, original params) -> result<original reslut, invoke-extension-error>
                let fn_invoke = FunctionMetadata {
                    ffqn: FunctionFqn {
                        ifc_fqn: obelisk_ext_ifc.clone(),
                        function_name: FnName::from(format!(
                            "{}{SUFFIX_FN_INVOKE}",
                            exported_fn_metadata.ffqn.function_name
                        )),
                    },
                    parameter_types: {
                        let mut params =
                            Vec::with_capacity(exported_fn_metadata.parameter_types.len() + 1);
                        params.push(param_type_invoke_label.clone());
                        params.extend_from_slice(&exported_fn_metadata.parameter_types.0);
                        ParameterTypes(params)
                    },
                    return_type: ReturnType::detect(
                        TypeWrapper::Result {
                            ok: Some(Box::new(TypeWrapper::from(
                                return_type.type_wrapper_tl.clone(),
                            ))),
                            err: Some(Box::new(invoke_extension_error_type_wrapper.clone())),
                        },
                        StrVariant::from(format!(
                            "result<{}, invoke-extension-error>",
                            return_type.wit_type
                        )),
                    ),
                    extension: Some(FunctionExtension::Invoke),
                    submittable: false,
                };
                insert_ext(fn_invoke);

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
                                "{}{SUFFIX_FN_SCHEDULE}",
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
                                "{}{SUFFIX_FN_STUB}",
                                exported_fn_metadata.ffqn.function_name
                            )),
                        },
                        parameter_types: {
                            let mut params = vec![param_type_execution_id.clone()];

                            params.push(ParameterType {
                                type_wrapper: TypeWrapper::from(
                                    return_type.type_wrapper_tl.clone(),
                                ),
                                name: StrVariant::Static("execution-result"),
                                wit_type: return_type.wit_type.clone(),
                            });

                            ParameterTypes(params)
                        },
                        return_type: ReturnType::detect(
                            TypeWrapper::Result {
                                ok: None,
                                err: Some(Box::new(stub_error_type_wrapper.clone())),
                            },
                            StrVariant::Static("result<_, stub-error>"),
                        ),
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
            if !fns.is_empty() {
                exports_hierarchy.push(PackageIfcFns {
                    ifc_fqn,
                    fns,
                    extension: true,
                });
            }
        }
        for (ifc_fqn, fns) in schedules {
            if !fns.is_empty() {
                exports_hierarchy.push(PackageIfcFns {
                    ifc_fqn,
                    fns,
                    extension: true,
                });
            }
        }
        for (ifc_fqn, fns) in stubs {
            if !fns.is_empty() {
                exports_hierarchy.push(PackageIfcFns {
                    ifc_fqn,
                    fns,
                    extension: true,
                });
            }
        }
    }

    fn flatten(input: &[PackageIfcFns]) -> Vec<FunctionMetadata> {
        input
            .iter()
            .flat_map(|pif| pif.fns.values().cloned())
            .collect()
    }
}

// Only contains functions with result types of ResultType::Compatible
#[derive(Debug)]
struct ExImLite {
    imports: Vec<PackageIfcFns>,
    // Only no-ext functions in exports, guarded by DecodeError::ExportingExt
    exports: Vec<PackageIfcFns>,
}

#[derive(Clone, Copy, PartialEq)]
enum ProcessingKind {
    Imports,
    ExportsOfActivityStub,
    ExportsSubmittable,
}
impl ProcessingKind {
    fn is_export(&self) -> bool {
        matches!(
            self,
            ProcessingKind::ExportsSubmittable | ProcessingKind::ExportsOfActivityStub
        )
    }
}

fn populate_ifcs_with_compatible_fns(
    resolve: &Resolve,
    ifc_ids: impl Iterator<Item = InterfaceId>,
    processing_kind: ProcessingKind,
) -> Result<Vec<PackageIfcFns>, DecodeError> {
    let mut vec = Vec::new();
    let submittable = processing_kind == ProcessingKind::ExportsSubmittable;
    for ifc in ifc_ids
        .map(|ifc_id| {
            resolve
                .interfaces
                .get(ifc_id)
                .expect("`iter` must be derived from `resolve`")
        })
        // skip inline interfaces
        .filter(|ifc| ifc.name.is_some())
    {
        let Some(package) = ifc
            .package
            .and_then(|pkg| resolve.packages.get(pkg))
            .map(|p| &p.name)
        else {
            unreachable!("interface's package cannot be empty");
        };
        let Some(ifc_name) = ifc.name.as_deref() else {
            unreachable!("inline interfaces already filterd out");
        };
        let pkg_fqn = PkgFqn {
            namespace: package.namespace.clone(),
            package_name: package.name.clone(),
            version: package
                .version
                .as_ref()
                .map(std::string::ToString::to_string),
        };
        let package_ext = pkg_fqn.split_ext().map(|(_, pkg_ext)| pkg_ext);
        if processing_kind.is_export() && package_ext.is_some() {
            return Err(DecodeError::ExportingExt(pkg_fqn.to_string()));
        }
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
            let ffqn = FunctionFqn::new_arc(ifc_fqn.clone(), Arc::from(function_name.to_string()));
            let return_type = if let Some(return_type) = function.result {
                let mut printer = WitPrinter::default();
                let wit_type = printer
                    .print_type_name(resolve, &return_type)
                    .ok()
                    .map_or(StrVariant::Static("unknown"), |()| {
                        StrVariant::from(printer.output.to_string())
                    });
                Some(ReturnType::detect(
                    match TypeWrapper::from_wit_parser_type(resolve, &return_type) {
                        Ok(ok) => ok,
                        Err(err) => {
                            return Err(DecodeError::TypeNotSupported { err, ffqn });
                        }
                    },
                    wit_type,
                ))
            } else {
                None
            };

            let return_type_valid = matches!(return_type, Some(ReturnType::Extendable(_)));

            match (return_type, processing_kind.is_export()) {
                (Some(return_type @ ReturnType::Extendable(_)), true)
                | (Some(return_type), false) => {
                    let ffqn =
                        FunctionFqn::new_arc(ifc_fqn.clone(), Arc::from(function_name.to_string()));
                    let parameter_types = ParameterTypes({
                        let mut params = Vec::new();
                        for (param_name, param_ty) in &function.params {
                            let mut printer = WitPrinter::default();
                            let item = ParameterType {
                                type_wrapper: match TypeWrapper::from_wit_parser_type(
                                    resolve, param_ty,
                                ) {
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

                    fns.insert(
                        ffqn.function_name.clone(),
                        FunctionMetadata {
                            parameter_types,
                            return_type,
                            extension: {
                                let guessed_fn_extension = FunctionExtension::iter()
                                    .find(|fn_ext| function_name.ends_with(fn_ext.suffix()));
                                match (package_ext, guessed_fn_extension) {
                                    (None, _) => None,
                                    (Some(pkg_ext), Some(fn_ext)) if fn_ext.belongs_to(pkg_ext) => {
                                        Some(fn_ext)
                                    }
                                    _ => return Err(DecodeError::WrongExtImport(ffqn)),
                                }
                            },
                            ffqn,
                            submittable,
                        },
                    );
                }
                (Some(return_type), true)
                    if processing_kind.is_export()
                        && !return_type_valid
                        && !ifc_fqn.starts_with("wasi:http/incoming-handler@") =>
                {
                    // Warn if this is export and a function return type is not compatible.
                    // Mute warnings for `incoming-handlers`, exported by webhooks.
                    warn!("Ignoring export {ffqn} with unsupported return type {return_type:?}");
                }
                _ => {}
            }
        }
        if !fns.is_empty() {
            vec.push(PackageIfcFns {
                ifc_fqn: IfcFqnName::new_arc(ifc_fqn),
                fns,
                extension: package_ext.is_some(),
            });
        }
    }
    Ok(vec)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::populate_ifcs_with_compatible_fns;
    use crate::wasm_tools::{ExOrIm, ProcessingKind, WasmComponent, world_interfaces};
    use concepts::ComponentType;
    use rstest::rstest;
    use std::path::PathBuf;
    use wit_parser::decoding::DecodedWasm;

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
        let component = WasmComponent::new(&wasm_path, ComponentType::Workflow).unwrap();
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
            let exports = populate_ifcs_with_compatible_fns(
                &resolve,
                world_interfaces(world, ExOrIm::Exports),
                ProcessingKind::ExportsSubmittable,
            )
            .unwrap()
            .into_iter()
            .flat_map(|ifc| ifc.fns)
            .map(|(_fn_name, fn_metadata)| (fn_metadata.ffqn.to_string(), fn_metadata))
            .collect::<hashbrown::HashMap<_, _>>();
            insta::with_settings!({sort_maps => true,  snapshot_suffix => format!("{wasm_file}_exports")}, {insta::assert_json_snapshot!(exports)});
        } else {
            let imports = populate_ifcs_with_compatible_fns(
                &resolve,
                world_interfaces(world, ExOrIm::Imports),
                ProcessingKind::Imports,
            )
            .unwrap()
            .into_iter()
            .flat_map(|ifc| ifc.fns)
            .map(|(_fn_name, fn_metadata)| (fn_metadata.ffqn.to_string(), fn_metadata))
            .collect::<hashbrown::HashMap<_, _>>();
            insta::with_settings!({ sort_maps => true,  snapshot_suffix => format!("{wasm_file}_imports")}, {insta::assert_json_snapshot!(imports)});
        }
    }

    #[rstest]
    #[case("fibo/activity", ComponentType::ActivityWasm)]
    #[case("fibo/workflow", ComponentType::Workflow)]
    #[case("fibo/webhook", ComponentType::WebhookEndpoint)]
    #[case("stub/activity", ComponentType::ActivityStub)]
    fn test_wit_folder_parsing(#[case] path: &'static str, #[case] component_type: ComponentType) {
        use std::fmt::Write;

        let workspace_dir = PathBuf::from(
            std::env::var("CARGO_WORKSPACE_DIR")
                .as_deref()
                .unwrap_or("."),
        )
        .canonicalize()
        .unwrap();
        let wasm_component = WasmComponent::new_from_wit_folder(
            workspace_dir
                .join("crates/testing/test-programs")
                .join(path)
                .join("wit"),
            component_type,
        )
        .unwrap();

        let mut pkgs_to_wits: Vec<_> = wasm_component
            .exported_extension_wits()
            .unwrap()
            .into_iter()
            .map(|(pkg_fqn, wit)| (pkg_fqn.to_string(), wit))
            .collect();
        pkgs_to_wits.sort_by(|(pkg_fqn, _), (pkg_fqn2, _)| pkg_fqn.cmp(pkg_fqn2));

        let mut snapshot = String::new();
        for (pkg_fqn, wit) in pkgs_to_wits {
            write!(&mut snapshot, "{pkg_fqn}\n{wit}\n\n").unwrap();
        }

        insta::with_settings!({  snapshot_suffix => format!("{path}")}, {insta::assert_snapshot!(snapshot)});
    }
}
