use crate::wasm_tools::ExIm;
use anyhow::Context;
use concepts::{ComponentType, FnName, IfcFqnName, PkgFqn, SUFFIX_PKG_EXT, SUFFIX_PKG_STUB};
use const_format::formatcp;
use hashbrown::HashMap;
use id_arena::Arena;
use indexmap::IndexMap;
use semver::{BuildMetadata, Prerelease, Version};
use std::{ops::Deref, path::PathBuf};
use tracing::{error, warn};
use wit_component::{DecodedWasm, WitPrinter};
use wit_parser::{
    Function, FunctionKind, Handle, Interface, InterfaceId, PackageId, PackageName, Resolve, Type,
    TypeDef, TypeDefKind, TypeOwner, UnresolvedPackageGroup,
};

const OBELISK_TYPES_VERSION_MAJOR: u64 = 1;
const OBELISK_TYPES_VERSION_MINOR: u64 = 1;
const OBELISK_TYPES_VERSION_PATCH: u64 = 0;
const OBELISK_TYPES_VERSION: &str = formatcp!(
    "{OBELISK_TYPES_VERSION_MAJOR}.{OBELISK_TYPES_VERSION_MINOR}.{OBELISK_TYPES_VERSION_PATCH}"
);
const OBELISK_TYPES_PACKAGE_NAME: &str = formatcp!("obelisk:types@{OBELISK_TYPES_VERSION}");

const OBELISK_TYPES_PACKAGE_NO_NESTING: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/wit/obelisk_types@1.1.0/types@1.1.0.wit"
));

pub(crate) fn wit(
    component_type: ComponentType,
    decoded: &DecodedWasm,
    exim: &ExIm,
) -> Result<String, anyhow::Error> {
    let resolve = decoded.resolve();
    let mut ids = resolve
        .packages
        .iter()
        .map(|(id, _)| id)
        // The main package would show as a nested package as well
        .filter(|id| *id != decoded.package())
        .collect::<Vec<_>>();
    ids.sort();
    let mut printer = WitPrinter::default();
    printer.print(resolve, decoded.package(), &ids)?;
    let wit = printer.output.to_string();
    add_ext_exports(wit, exim, component_type)
}

// Replace obelisk:types from the actual WASM file because it may not contain all types we are going to need in exported functions.
fn replace_obelisk_types(wit: &str) -> String {
    // Replace last character of the first line from ; to {
    let types_nesting = {
        let mut types_nesting = OBELISK_TYPES_PACKAGE_NO_NESTING.replacen(';', "{", 1);
        types_nesting.push('}');
        types_nesting
    };

    const TYPES_NESTED_PACKAGE_FIRST_LINE: &str =
        formatcp!("package {OBELISK_TYPES_PACKAGE_NAME} {{");
    let wit = remove_nested_package(wit, TYPES_NESTED_PACKAGE_FIRST_LINE);
    let wit = format!("{wit}\n{types_nesting}");
    wit
}

fn add_ext_exports(
    wit: String,
    exim: &ExIm,
    component_type: ComponentType,
) -> Result<String, anyhow::Error> {
    if component_type == ComponentType::WebhookEndpoint {
        return Ok(wit);
    }
    let wit = replace_obelisk_types(&wit);
    let group = UnresolvedPackageGroup::parse(PathBuf::new(), &wit)?;
    let mut resolve = Resolve::new();
    let main_id = resolve.push_group(group)?;
    let exported_pkg_to_ifc_to_details_map = get_exported_pkg_to_ifc_to_details_map_noext(exim);

    // Find necessary handles
    // Get obelisk:types
    let obelisk_types_package_name = PackageName {
        namespace: "obelisk".to_string(),
        name: "types".to_string(),
        version: Some(Version {
            major: OBELISK_TYPES_VERSION_MAJOR,
            minor: OBELISK_TYPES_VERSION_MINOR,
            patch: OBELISK_TYPES_VERSION_PATCH,
            pre: Prerelease::EMPTY,
            build: BuildMetadata::EMPTY,
        }),
    };
    let obelisk_types_pkg_id =
        if let Some(id) = resolve.package_names.get(&obelisk_types_package_name) {
            *id
        } else {
            let pkg = wit_parser::Package {
                name: obelisk_types_package_name,
                docs: wit_parser::Docs::default(),
                interfaces: IndexMap::default(),
                worlds: IndexMap::default(),
            };
            let package_name = pkg.name.clone();
            let ext_pkg_id = resolve.packages.alloc(pkg);
            resolve.package_names.insert(package_name, ext_pkg_id);
            ext_pkg_id
        };
    // Get obelisk:types/time@VERSION
    let time_ifc_id = *resolve.packages[obelisk_types_pkg_id]
        .interfaces
        .get("time")
        .expect("`time` interface was added");
    let time_ifc = &resolve.interfaces[time_ifc_id];

    let (execution_ifc_id, execution_ifc) = find_interface(
        &IfcFqnName::from_parts("obelisk", "types", "execution", Some(OBELISK_TYPES_VERSION)),
        &resolve,
        &resolve.interfaces,
    )
    .expect(formatcp!("{OBELISK_TYPES_PACKAGE_NAME} must be found"));
    let type_id_execution_id = {
        // obelisk:types/execution@VERSION.{execution-id}
        let actual_type_id = *execution_ifc
            .types
            .get("execution-id")
            .expect("`execution-id` must exist");
        // Create a reference to the type.
        resolve.types.alloc(TypeDef {
            name: None,
            kind: TypeDefKind::Type(Type::Id(actual_type_id)),
            owner: TypeOwner::Interface(execution_ifc_id),
            docs: wit_parser::Docs::default(),
            stability: wit_parser::Stability::default(),
        })
    };
    let (type_id_join_set_id, type_id_join_set_id_borrow_handle) = {
        // obelisk:types/execution@VERSION.{join-set-id}
        let actual_type_id = *execution_ifc
            .types
            .get("join-set-id")
            .expect("`join-set-id` must exist");
        // Create a reference to the type.
        let type_id_join_set_id = resolve.types.alloc(TypeDef {
            name: Some("join-set-id".to_string()),
            kind: TypeDefKind::Type(Type::Id(actual_type_id)),
            owner: TypeOwner::Interface(execution_ifc_id),
            docs: wit_parser::Docs::default(),
            stability: wit_parser::Stability::default(),
        });
        // Create a Handle::Borrow to the reference.
        let type_id_join_set_id_borrow_handle = resolve.types.alloc(TypeDef {
            name: None,
            kind: TypeDefKind::Handle(Handle::Borrow(type_id_join_set_id)),
            owner: TypeOwner::Interface(execution_ifc_id),
            docs: wit_parser::Docs::default(),
            stability: wit_parser::Stability::default(),
        });
        (type_id_join_set_id, type_id_join_set_id_borrow_handle)
    };
    let type_id_execution_error = {
        // obelisk:types/execution.{execution-error}
        let actual_type_id = *execution_ifc
            .types
            .get("execution-error")
            .expect("`execution-error` must exist");
        // Create a reference to the type.
        resolve.types.alloc(TypeDef {
            name: None,
            kind: TypeDefKind::Type(Type::Id(actual_type_id)),
            owner: TypeOwner::Interface(execution_ifc_id),
            docs: wit_parser::Docs::default(),
            stability: wit_parser::Stability::default(),
        })
    };
    let type_id_stub_error = {
        // obelisk:types/execution.{stub-error}
        let actual_type_id = *execution_ifc
            .types
            .get("stub-error")
            .expect("`stub-error` must exist");
        // Create a reference to the type.
        resolve.types.alloc(TypeDef {
            name: None,
            kind: TypeDefKind::Type(Type::Id(actual_type_id)),
            owner: TypeOwner::Interface(execution_ifc_id),
            docs: wit_parser::Docs::default(),
            stability: wit_parser::Stability::default(),
        })
    };
    let type_id_await_next_err_part = resolve.types.alloc(TypeDef {
        name: None,
        kind: TypeDefKind::Tuple(wit_parser::Tuple {
            types: vec![
                Type::Id(type_id_execution_id),
                Type::Id(type_id_execution_error),
            ],
        }),
        owner: TypeOwner::None,
        docs: wit_parser::Docs::default(),
        stability: wit_parser::Stability::default(),
    });
    let type_id_schedule_at = {
        // obelisk:types/time.{schedule-at}
        let actual_type_id = *time_ifc
            .types
            .get("schedule-at")
            .expect("`schedule-at` must exist");
        // Create a reference to the type.
        resolve.types.alloc(TypeDef {
            name: None,
            kind: TypeDefKind::Type(Type::Id(actual_type_id)),
            owner: TypeOwner::Interface(execution_ifc_id),
            docs: wit_parser::Docs::default(),
            stability: wit_parser::Stability::default(),
        })
    };

    for (pkg_fqn, ifc_to_fns) in exported_pkg_to_ifc_to_details_map {
        // Get or create the -obelisk-ext variant of the exported package.
        let obelisk_ext_pkg_id = get_or_create_package(&pkg_fqn, SUFFIX_PKG_EXT, &mut resolve)?;
        // Get or create the -obelisk-stub variant of the exported package.
        let obelisk_stub_pkg_id = if component_type == ComponentType::ActivityStub {
            Some(get_or_create_package(
                &pkg_fqn,
                SUFFIX_PKG_STUB,
                &mut resolve,
            )?)
        } else {
            None
        };

        for (ifc_fqn, fns) in ifc_to_fns {
            let (original_ifc_id, original_ifc) =
                find_interface(&ifc_fqn, &resolve, &resolve.interfaces)
                    .with_context(|| format!("cannot find interface {ifc_fqn}"))?;

            let mut ext_ifc = {
                let mut types = IndexMap::new();
                types.insert("execution-id".to_string(), type_id_execution_id);
                types.insert("join-set-id".to_string(), type_id_join_set_id);
                types.insert("schedule-at".to_string(), type_id_schedule_at);
                types.insert("execution-error".to_string(), type_id_execution_error);
                copy_original_types(
                    original_ifc_id,
                    original_ifc,
                    &mut types,
                    &mut resolve.types,
                );
                Interface {
                    name: Some(ifc_fqn.ifc_name().to_string()),
                    types,
                    functions: IndexMap::default(),
                    docs: wit_parser::Docs::default(),
                    stability: wit_parser::Stability::default(),
                    package: Some(obelisk_ext_pkg_id),
                }
            };
            let mut stub_ifc = if let Some(obelisk_stub_pkg_id) = obelisk_stub_pkg_id {
                let mut types = IndexMap::new();
                types.insert("stub-error".to_string(), type_id_stub_error);
                copy_original_types(
                    original_ifc_id,
                    original_ifc,
                    &mut types,
                    &mut resolve.types,
                );
                Some((
                    obelisk_stub_pkg_id,
                    Interface {
                        name: Some(ifc_fqn.ifc_name().to_string()),
                        types,
                        functions: IndexMap::default(),
                        docs: wit_parser::Docs::default(),
                        stability: wit_parser::Stability::default(),
                        package: Some(obelisk_stub_pkg_id),
                    },
                ))
            } else {
                None
            };
            for fn_name in fns {
                let original_fn = original_ifc
                    .functions
                    .get(fn_name.deref())
                    .with_context(|| format!("cannot find function {ifc_fqn}.{fn_name}"))?;
                // -submit: func(join-set-id: borrow<join-set-id>, <params>) -> execution-id;
                {
                    let fn_name = format!("{fn_name}-submit");
                    let mut params = vec![(
                        "join-set-id".to_string(),
                        Type::Id(type_id_join_set_id_borrow_handle),
                    )];
                    params.extend_from_slice(&original_fn.params);
                    let fn_ext = Function {
                        name: fn_name.clone(),
                        kind: FunctionKind::Freestanding,
                        params,
                        result: Some(Type::Id(type_id_execution_id)),
                        docs: wit_parser::Docs::default(),
                        stability: wit_parser::Stability::default(),
                    };
                    ext_ifc.functions.insert(fn_name, fn_ext);
                }
                // -await-next: func(join-set-id: borrow<join-set-id>) ->
                //  result<tuple<execution-id, <return-type>>, tuple<execution-id, execution-error>>;
                // or if the function does not return anything:
                //  result<execution-id, tuple<execution-id, execution-error>>;
                {
                    let fn_name = format!("{fn_name}-await-next");
                    let params = vec![(
                        "join-set-id".to_string(),
                        Type::Id(type_id_join_set_id_borrow_handle),
                    )];
                    let result = if let Some(actual_return_type_id) = &original_fn.result {
                        let type_id_await_next_ok_part_tuple = resolve.types.alloc(TypeDef {
                            name: None,
                            kind: TypeDefKind::Tuple(wit_parser::Tuple {
                                types: vec![Type::Id(type_id_execution_id), *actual_return_type_id],
                            }),
                            owner: TypeOwner::None,
                            docs: wit_parser::Docs::default(),
                            stability: wit_parser::Stability::default(),
                        });
                        let type_id_result = resolve.types.alloc(TypeDef {
                            name: None,
                            kind: TypeDefKind::Result(wit_parser::Result_ {
                                ok: Some(Type::Id(type_id_await_next_ok_part_tuple)),
                                err: Some(Type::Id(type_id_await_next_err_part)),
                            }),
                            owner: TypeOwner::None,
                            docs: wit_parser::Docs::default(),
                            stability: wit_parser::Stability::default(),
                        });
                        Some(Type::Id(type_id_result))
                    } else {
                        let type_id_result = resolve.types.alloc(TypeDef {
                            name: None,
                            kind: TypeDefKind::Result(wit_parser::Result_ {
                                ok: Some(Type::Id(type_id_execution_id)),
                                err: Some(Type::Id(type_id_await_next_err_part)),
                            }),
                            owner: TypeOwner::None,
                            docs: wit_parser::Docs::default(),
                            stability: wit_parser::Stability::default(),
                        });
                        Some(Type::Id(type_id_result))
                    };
                    let fn_ext = Function {
                        name: fn_name.clone(),
                        kind: FunctionKind::Freestanding,
                        params,
                        result,
                        docs: wit_parser::Docs::default(),
                        stability: wit_parser::Stability::default(),
                    };
                    ext_ifc.functions.insert(fn_name, fn_ext);
                }
                // -schedule: func(schedule-at: schedule-at, <params>) -> execution-id;
                {
                    let fn_name = format!("{fn_name}-schedule");
                    let mut params =
                        vec![("schedule-at".to_string(), Type::Id(type_id_schedule_at))];
                    params.extend_from_slice(&original_fn.params);
                    let fn_ext = Function {
                        name: fn_name.clone(),
                        kind: FunctionKind::Freestanding,
                        params,
                        result: Some(Type::Id(type_id_execution_id)),
                        docs: wit_parser::Docs::default(),
                        stability: wit_parser::Stability::default(),
                    };
                    ext_ifc.functions.insert(fn_name, fn_ext);
                }
                // -stub: func(execution_id: execution-id, <retval>) -> result<_, stub-error>;
                if let Some((_, stub_ifc)) = &mut stub_ifc {
                    let fn_name = format!("{fn_name}-stub");
                    let mut params =
                        vec![("execution-id".to_string(), Type::Id(type_id_execution_id))];
                    if let Some(actual_return_type_id) = &original_fn.result {
                        params.push(("return-value".to_string(), actual_return_type_id.clone()));
                    }

                    let fn_stub = Function {
                        name: fn_name.clone(),
                        kind: FunctionKind::Freestanding,
                        params,
                        result: {
                            // TODO: can be externalized?
                            let type_id_result = resolve.types.alloc(TypeDef {
                                name: None,
                                kind: TypeDefKind::Result(wit_parser::Result_ {
                                    ok: None,
                                    err: Some(Type::Id(type_id_stub_error)),
                                }),
                                owner: TypeOwner::None,
                                docs: wit_parser::Docs::default(),
                                stability: wit_parser::Stability::default(),
                            });
                            Some(Type::Id(type_id_result))
                        },
                        docs: wit_parser::Docs::default(),
                        stability: wit_parser::Stability::default(),
                    };
                    stub_ifc.functions.insert(fn_name, fn_stub);
                }
            }
            {
                // Add the `-obelisk-ext` Interface to `resolve`.
                let ext_ifc_id = resolve.interfaces.alloc(ext_ifc);
                resolve
                    .packages
                    .get_mut(obelisk_ext_pkg_id)
                    .expect("found or inserted already")
                    .interfaces
                    .insert(ifc_fqn.ifc_name().to_string(), ext_ifc_id);
            }
            if let Some((obelisk_stub_pkg_id, stub_ifc)) = stub_ifc {
                // Add the `-obelisk-stub` Interface to `resolve`.
                let stub_ifc_id = resolve.interfaces.alloc(stub_ifc);
                resolve
                    .packages
                    .get_mut(obelisk_stub_pkg_id)
                    .expect("found or inserted already")
                    .interfaces
                    .insert(ifc_fqn.ifc_name().to_string(), stub_ifc_id);
            }
        }
    }

    let ids = resolve
        .packages
        .iter()
        .map(|(id, _)| id)
        // The main package would show as a nested package as well
        .filter(|id| *id != main_id)
        .collect::<Vec<_>>();

    let mut printer = WitPrinter::default();
    printer.print(&resolve, main_id, &ids)?;
    Ok(printer.output.to_string())
}

fn copy_original_types(
    original_ifc_id: id_arena::Id<Interface>,
    original_ifc: &Interface,
    types: &mut IndexMap<String, id_arena::Id<TypeDef>>,
    resolve_types: &mut Arena<TypeDef>,
) {
    for (original_type_name, original_type_id) in &original_ifc.types {
        // Create a reference to the type.
        let reference_type_def = resolve_types.alloc(TypeDef {
            name: None,
            kind: TypeDefKind::Type(Type::Id(*original_type_id)),
            owner: TypeOwner::Interface(original_ifc_id),
            docs: wit_parser::Docs::default(),
            stability: wit_parser::Stability::default(),
        });
        types.insert(original_type_name.clone(), reference_type_def);
    }
}

fn get_or_create_package(
    pkg_fqn: &PkgFqn,
    suffix: &'static str,
    resolve: &mut Resolve,
) -> Result<PackageId, semver::Error> {
    let new_pkg_fqn = PkgFqn {
        namespace: pkg_fqn.namespace.clone(),
        package_name: format!("{}{suffix}", pkg_fqn.package_name),
        version: pkg_fqn.version.clone(),
    };
    if let Some((pkg_id, _)) = resolve
        .packages
        .iter()
        .find(|(_, found_pkg)| from_wit_package_name_to_pkg_fqn(&found_pkg.name) == new_pkg_fqn)
    {
        Ok(pkg_id)
    } else {
        let pkg = wit_parser::Package {
            name: from_pkg_fqn_to_wit_package_name(new_pkg_fqn)?,
            docs: wit_parser::Docs::default(),
            interfaces: IndexMap::default(),
            worlds: IndexMap::default(),
        };
        let package_name = pkg.name.clone();
        let pkg_id = resolve.packages.alloc(pkg);
        resolve.package_names.insert(package_name, pkg_id);
        Ok(pkg_id)
    }
}

fn get_exported_pkg_to_ifc_to_details_map_noext(
    exim: &ExIm,
) -> HashMap<PkgFqn, HashMap<IfcFqnName, Vec<FnName>>> {
    let mut exported_pkg_to_ifc_to_details_map: HashMap<PkgFqn, HashMap<IfcFqnName, Vec<FnName>>> =
        HashMap::new();
    for pkg_ifc_fns in exim.get_exports_hierarchy_noext() {
        let inner_map = exported_pkg_to_ifc_to_details_map
            .entry(pkg_ifc_fns.ifc_fqn.pkg_fqn_name())
            .or_default();
        inner_map.insert(
            pkg_ifc_fns.ifc_fqn.clone(),
            pkg_ifc_fns.fns.keys().cloned().collect(),
        );
    }
    exported_pkg_to_ifc_to_details_map
}

fn find_interface<'a>(
    ifc_fqn: &IfcFqnName,
    resolve: &'_ Resolve,
    interfaces: &'a Arena<Interface>,
) -> Option<(InterfaceId, &'a Interface)> {
    let pkg_id = *resolve
        .package_names
        .get(&try_from_ifc_fqn_name(ifc_fqn).ok()?)?;
    let ifc_id = *resolve.packages[pkg_id]
        .interfaces
        .get(ifc_fqn.ifc_name())?;
    interfaces.get(ifc_id).map(|ifc| (ifc_id, ifc))
}

fn remove_nested_package(wit_string: &str, nested_package_to_remove: &str) -> String {
    // Find the start of the namespace
    let Some(nested_package_start) = wit_string.find(nested_package_to_remove) else {
        return wit_string.to_string();
    };

    // Find the opening brace after the namespace
    let Some(open_brace_index) = wit_string[nested_package_start..]
        .find('{')
        .map(|idx| nested_package_start + idx)
    else {
        panic!("nested namespace must contain '{{'");
    };

    // Track brace nesting to find the matching closing brace
    let mut brace_count = 1;
    let mut current_index = open_brace_index + 1;

    while current_index < wit_string.len() && brace_count > 0 {
        match wit_string.chars().nth(current_index) {
            Some('{') => brace_count += 1,
            Some('}') => brace_count -= 1,
            _ => {}
        }
        current_index += 1;
    }

    // If we didn't find the matching brace, return the original string
    if brace_count != 0 {
        warn!("Cannot remove the nested package {nested_package_to_remove}");
        return wit_string.to_string();
    }

    // Remove the package and its contents
    format!(
        "{}\n{}",
        wit_string[..nested_package_start].trim(),
        wit_string[current_index..].trim()
    )
}

fn try_from_ifc_fqn_name(ifc_fqn: &IfcFqnName) -> Result<PackageName, anyhow::Error> {
    Ok(PackageName {
        namespace: ifc_fqn.namespace().to_string(),
        name: ifc_fqn.package_name().to_string(),
        version: ifc_fqn
            .version()
            .map(semver::Version::parse)
            .transpose()
            .inspect_err(|err| {
                error!(
                    "cannot parse the version `{:?}` - {err:?}",
                    ifc_fqn.version()
                );
            })?,
    })
}

fn from_wit_package_name_to_pkg_fqn(package_name: &PackageName) -> PkgFqn {
    PkgFqn {
        namespace: package_name.namespace.clone(),
        package_name: package_name.name.clone(),
        version: package_name.version.as_ref().map(ToString::to_string),
    }
}

fn from_pkg_fqn_to_wit_package_name(pkg_fqn: PkgFqn) -> Result<PackageName, semver::Error> {
    Ok(PackageName {
        namespace: pkg_fqn.namespace,
        name: pkg_fqn.package_name,
        version: pkg_fqn
            .version
            .as_ref()
            .map(|v| v.parse())
            .transpose()
            .inspect_err(|err| error!("Cannot convert version {:?} - {err:?}", pkg_fqn.version))?,
    })
}

#[cfg(test)]
mod tests {
    use crate::wasm_tools::WasmComponent;
    use crate::wasm_tools::tests::engine;
    use concepts::ComponentType;
    use rstest::rstest;
    use std::path::PathBuf;

    #[rstest]
    #[test]
    #[case(
        test_programs_fibo_workflow_builder::TEST_PROGRAMS_FIBO_WORKFLOW,
        ComponentType::Workflow
    )]
    #[case(
        test_programs_fibo_activity_builder::TEST_PROGRAMS_FIBO_ACTIVITY,
        ComponentType::ActivityWasm
    )]
    #[case(
        test_programs_fibo_webhook_builder::TEST_PROGRAMS_FIBO_WEBHOOK,
        ComponentType::WebhookEndpoint
    )]
    #[case(
        test_programs_http_get_activity_builder::TEST_PROGRAMS_HTTP_GET_ACTIVITY,
        ComponentType::ActivityWasm
    )]
    #[case(
        test_programs_stub_activity_builder::TEST_PROGRAMS_STUB_ACTIVITY,
        ComponentType::ActivityStub
    )]
    fn wit_should_contain_extensions(
        #[case] wasm_path: &'static str,
        #[case] component_type: ComponentType,
    ) {
        test_utils::set_up();

        let engine = engine();
        let component = WasmComponent::new(wasm_path, &engine, component_type).unwrap();
        let wasm_path = PathBuf::from(wasm_path);
        let wasm_file = wasm_path.file_name().unwrap().to_string_lossy();
        let wit = component.wit().unwrap();
        insta::with_settings!({sort_maps => true, snapshot_suffix => format!("{wasm_file}_wit")}, {insta::assert_snapshot!(wit)});
    }
}
