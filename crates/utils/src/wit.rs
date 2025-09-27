use crate::wasm_tools::{ExIm, ExOrIm};
use concepts::{FnName, FunctionExtension, FunctionMetadata, IfcFqnName, PackageExtension, PkgFqn};
use const_format::formatcp;
use id_arena::Arena;
use indexmap::IndexMap;
use semver::{BuildMetadata, Prerelease, Version};
use std::path::PathBuf;
use tracing::{error, warn};
use wit_component::WitPrinter;
use wit_parser::{
    Function, FunctionKind, Handle, Interface, InterfaceId, PackageId, PackageName, Resolve,
    Stability, Type, TypeDef, TypeDefKind, TypeOwner, UnresolvedPackageGroup, WorldItem, WorldKey,
};

const OBELISK_TYPES_VERSION_MAJOR: u64 = 3;
const OBELISK_TYPES_VERSION_MINOR: u64 = 0;
const OBELISK_TYPES_VERSION_PATCH: u64 = 0;
const OBELISK_TYPES_VERSION: &str = formatcp!(
    "{OBELISK_TYPES_VERSION_MAJOR}.{OBELISK_TYPES_VERSION_MINOR}.{OBELISK_TYPES_VERSION_PATCH}"
);
const OBELISK_TYPES_PACKAGE_NAME: &str = formatcp!("obelisk:types@{OBELISK_TYPES_VERSION}");

pub const WIT_OBELISK_ACTIVITY_PACKAGE_PROCESS: [&str; 3] = [
    "obelisk_activity@1.0.0",
    "process.wit",
    include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/wit/obelisk_activity@1.0.0/process.wit"
    )),
];
pub const WIT_OBELISK_LOG_PACKAGE: [&str; 3] = [
    "obelisk_log@1.0.0",
    "log.wit",
    include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/wit/obelisk_log@1.0.0/log.wit"
    )),
];
const WIT_OBELISK_TYPES_PACKAGE_CONTENT: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/wit/obelisk_types@3.0.0/types.wit"
));
pub const WIT_OBELISK_TYPES_PACKAGE: [&str; 3] = [
    "obelisk_types@3.0.0",
    "types.wit",
    WIT_OBELISK_TYPES_PACKAGE_CONTENT,
];
pub const WIT_OBELISK_WORKFLOW_PACKAGE: [&str; 3] = [
    "obelisk_workflow@3.0.0",
    "workflow-support.wit",
    include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/wit/obelisk_workflow@3.0.0/workflow-support.wit"
    )),
];

pub(crate) fn wit(resolve: &Resolve, main_package: PackageId) -> Result<String, anyhow::Error> {
    // print all packages, with the main package as root, others as nested.
    let ids = packages_except_main(resolve, main_package, false);
    let mut printer = WitPrinter::default();
    printer.print(resolve, main_package, &ids)?;
    let wit = printer.output.to_string();
    Ok(wit)
}

#[expect(
    clippy::needless_pass_by_value,
    reason = "original resolve is consumed; the enriched resolve is returned"
)]
pub(crate) fn rebuild_resolve(
    exim: &ExIm,
    resolve: Resolve,
    main_package: PackageId,
) -> Result<(Resolve, PackageId), anyhow::Error> {
    let ids = packages_except_main(&resolve, main_package, true);
    let mut printer = WitPrinter::default();
    printer.print(&resolve, main_package, &ids)?;
    let wit = printer.output.to_string();

    let (mut resolve, main_pkg_id) = {
        let wit = replace_obelisk_types(&wit);
        let group = UnresolvedPackageGroup::parse(PathBuf::new(), &wit)?;
        let mut resolve = Resolve::new();
        let main_pkg_id = resolve.push_group(group)?;
        (resolve, main_pkg_id)
    };
    let world_id = resolve
        .select_world(&[main_pkg_id], None)
        .expect("default world must be found");
    let added_interfaces = add_extended_interfaces(exim, &mut resolve)?;
    resolve
        .worlds
        .get_mut(world_id)
        .expect("id belongs to this resolve")
        .exports
        .extend(added_interfaces.into_iter().map(|ifc_id| {
            (
                WorldKey::Interface(ifc_id),
                WorldItem::Interface {
                    id: ifc_id,
                    stability: Stability::Unknown,
                },
            )
        }));

    Ok((resolve, main_pkg_id))
}

fn add_extended_interfaces(
    exim: &ExIm,
    resolve: &mut Resolve,
) -> Result<Vec<InterfaceId>, semver::Error> {
    let mut added_interfaces = Vec::new();
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
        resolve,
        &resolve.interfaces,
    )
    .expect(formatcp!("{OBELISK_TYPES_PACKAGE_NAME} must be found"));
    // obelisk:types/execution@VERSION.{execution-id}
    let type_id_execution_id = {
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
    // obelisk:types/execution@VERSION.{join-set-id}
    let (type_id_join_set_id, type_id_join_set_id_borrow_handle) = {
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
    // obelisk:types/execution.{await-next-extension-error}
    let type_id_await_next_extension_error = {
        let actual_type_id = *execution_ifc
            .types
            .get("await-next-extension-error")
            .expect("`await-next-extension-error` must exist");
        // Create a reference to the type.
        resolve.types.alloc(TypeDef {
            name: None,
            kind: TypeDefKind::Type(Type::Id(actual_type_id)),
            owner: TypeOwner::Interface(execution_ifc_id),
            docs: wit_parser::Docs::default(),
            stability: wit_parser::Stability::default(),
        })
    };
    // obelisk:types/execution.{get-extension-error}
    let type_id_get_extension_error = {
        let actual_type_id = *execution_ifc
            .types
            .get("get-extension-error")
            .expect("`get-extension-error` must exist");
        // Create a reference to the type.
        resolve.types.alloc(TypeDef {
            name: None,
            kind: TypeDefKind::Type(Type::Id(actual_type_id)),
            owner: TypeOwner::Interface(execution_ifc_id),
            docs: wit_parser::Docs::default(),
            stability: wit_parser::Stability::default(),
        })
    };
    // obelisk:types/execution.{stub-error}
    let type_id_stub_error = {
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
    let type_id_await_next_err_part = type_id_await_next_extension_error;
    // obelisk:types/time.{schedule-at}
    let type_id_schedule_at = {
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

    for (pkg_fqn, ifc_to_fns) in get_ext_pkg_to_ifc_to_details_map(exim, ExOrIm::Exports) {
        let (orig_pkg_fqn, pkg_ext) = pkg_fqn
            .split_ext()
            .expect("`get_pkg_to_ifc_to_details_map` filtered by ext packages");
        let pkg_id = get_or_create_package(pkg_fqn, resolve)?;
        let (orig_pkg_id, _) = resolve
            .packages
            .iter()
            .find(|(_, found_pkg)| {
                from_wit_package_name_to_pkg_fqn(&found_pkg.name) == orig_pkg_fqn
            })
            .unwrap_or_else(|| {
                panic!("original package must be not found in resolve: {orig_pkg_fqn}")
            });

        for (ifc_fqn, fns) in ifc_to_fns {
            let orig_pkg = resolve.packages.get(orig_pkg_id).expect("id is fresh");

            let orig_ifc_id = *orig_pkg
                .interfaces
                .get(ifc_fqn.ifc_name())
                .unwrap_or_else(|| {
                    panic!("interface must be found in original resolve: {ifc_fqn}")
                });
            let orig_ifc = resolve
                .interfaces
                .get(orig_ifc_id)
                .expect("orig_ifc obtained from orig_resolve");

            let mut types = copy_or_refer_original_types(orig_ifc_id, orig_ifc, &mut resolve.types);
            match pkg_ext {
                PackageExtension::ObeliskExt => {
                    types.insert("execution-id".to_string(), type_id_execution_id);
                    types.insert("join-set-id".to_string(), type_id_join_set_id);
                    types.insert(
                        "await-next-extension-error".to_string(),
                        type_id_await_next_extension_error,
                    );
                    types.insert(
                        "get-extension-error".to_string(),
                        type_id_get_extension_error,
                    );
                }
                PackageExtension::ObeliskSchedule => {
                    types.insert("execution-id".to_string(), type_id_execution_id);
                    types.insert("schedule-at".to_string(), type_id_schedule_at);
                }
                PackageExtension::ObeliskStub => {
                    types.insert("execution-id".to_string(), type_id_execution_id);
                    types.insert("stub-error".to_string(), type_id_stub_error);
                }
            }

            let mut ifc = Interface {
                name: Some(ifc_fqn.ifc_name().to_string()),
                types,
                functions: IndexMap::default(),
                docs: wit_parser::Docs::default(),
                stability: Stability::default(),
                package: Some(pkg_id),
            };
            for (fn_name, fn_meta) in fns {
                let (prefix, fn_ext) = fn_meta.split_extension().expect("filtered by ext package");
                let original_fn = orig_ifc.functions.get(prefix).unwrap_or_else(|| {
                    panic!("original function {prefix} must be found based on {fn_meta:?}")
                });
                let (params, result) = match fn_ext {
                    FunctionExtension::Submit => {
                        // -submit: func(join-set-id: borrow<join-set-id>, <params>) -> execution-id;
                        assert_eq!(pkg_ext, PackageExtension::ObeliskExt);
                        let mut params = vec![(
                            generate_param_name("join-set-id", &original_fn.params),
                            Type::Id(type_id_join_set_id_borrow_handle),
                        )];
                        params.extend_from_slice(&original_fn.params);

                        (params, Some(Type::Id(type_id_execution_id)))
                    }
                    FunctionExtension::AwaitNext => {
                        // -await-next: func(join-set-id: borrow<join-set-id>) ->
                        //  result<tuple<execution-id, return-type>, await-next-extension-error>;
                        // or if the function does not return anything:
                        //  result<execution-id, await-next-extension-error>;
                        assert_eq!(pkg_ext, PackageExtension::ObeliskExt);
                        let params = vec![(
                            "join-set-id".to_string(),
                            Type::Id(type_id_join_set_id_borrow_handle),
                        )];
                        let result = if let Some(actual_return_type_id) = &original_fn.result {
                            let type_id_await_next_ok_part_tuple = resolve.types.alloc(TypeDef {
                                name: None,
                                kind: TypeDefKind::Tuple(wit_parser::Tuple {
                                    types: vec![
                                        Type::Id(type_id_execution_id),
                                        *actual_return_type_id,
                                    ],
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
                        (params, result)
                    }
                    FunctionExtension::Schedule => {
                        // -schedule: func(schedule-at: schedule-at, <params>) -> execution-id;
                        assert_eq!(pkg_ext, PackageExtension::ObeliskSchedule);
                        let schedule_at_param_name =
                            generate_param_name("schedule-at", &original_fn.params);
                        let mut params = vec![(
                            schedule_at_param_name.to_string(),
                            Type::Id(type_id_schedule_at),
                        )];
                        params.extend_from_slice(&original_fn.params);
                        let result = Some(Type::Id(type_id_execution_id));
                        (params, result)
                    }
                    FunctionExtension::Stub => {
                        // -stub: func(execution_id: execution-id, original retval) -> result<_, stub-error>;
                        assert_eq!(pkg_ext, PackageExtension::ObeliskStub);
                        let mut params =
                            vec![("execution-id".to_string(), Type::Id(type_id_execution_id))];
                        let Some(return_type) = &original_fn.result else {
                            unreachable!(
                                "return types of exported functions are validated in ExImLite"
                            )
                        };
                        let return_type_id = Type::Id(resolve.types.alloc(TypeDef {
                            name: None,
                            kind: TypeDefKind::Type(*return_type),
                            owner: TypeOwner::None,
                            docs: wit_parser::Docs::default(),
                            stability: wit_parser::Stability::default(),
                        }));
                        params.push(("execution-result".to_string(), return_type_id));

                        let result = {
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
                        };
                        (params, result)
                    }
                    FunctionExtension::Get => {
                        // -get(execution-id) -> result<originalreturn type, get-extension-error>
                        assert_eq!(pkg_ext, PackageExtension::ObeliskExt);
                        let params =
                            vec![("execution-id".to_string(), Type::Id(type_id_execution_id))];
                        let result = Some(Type::Id(resolve.types.alloc(TypeDef {
                            name: None,
                            kind: TypeDefKind::Result(wit_parser::Result_ {
                                ok: original_fn.result, // return type or None
                                err: Some(Type::Id(type_id_get_extension_error)),
                            }),
                            owner: TypeOwner::None,
                            docs: wit_parser::Docs::default(),
                            stability: wit_parser::Stability::default(),
                        })));
                        (params, result)
                    }
                    // TODO: Add a name as parameter
                    FunctionExtension::Invoke => {
                        // -invoke(original param) -> original return type
                        assert_eq!(pkg_ext, PackageExtension::ObeliskExt);
                        let params = original_fn.params.clone();
                        let result = original_fn.result;
                        (params, result)
                    }
                };
                let wit_fun = Function {
                    name: fn_name.to_string(),
                    kind: FunctionKind::Freestanding,
                    params,
                    result,
                    docs: wit_parser::Docs::default(),
                    stability: Stability::default(),
                };
                ifc.functions.insert(fn_name.to_string(), wit_fun);
            }
            // Add Interface to `resolve`.
            let ifc_id = resolve.interfaces.alloc(ifc);
            resolve
                .packages
                .get_mut(pkg_id)
                .expect("found or inserted already")
                .interfaces
                .insert(ifc_fqn.ifc_name().to_string(), ifc_id);

            // Add the interface
            added_interfaces.push(ifc_id);
        }
    }
    Ok(added_interfaces)
}

pub(crate) fn packages_except_main(
    resolve: &Resolve,
    main_package: PackageId,
    sorted: bool,
) -> Vec<PackageId> {
    let mut packages = resolve
        .packages
        .iter()
        .map(|(id, _)| id)
        // The main package would show as a nested package as well
        .filter(|id| *id != main_package)
        .collect::<Vec<_>>();
    if sorted {
        packages.sort();
    }
    packages
}

// Replace obelisk:types from the actual WASM file because it may not contain all types we are going to need in exported functions.
#[expect(clippy::items_after_statements)]
fn replace_obelisk_types(wit: &str) -> String {
    // Replace last character of the first line from ; to {
    let types_nesting = {
        let mut types_nesting = WIT_OBELISK_TYPES_PACKAGE_CONTENT.replacen(';', "{", 1);
        types_nesting.push('}');
        types_nesting
    };
    const TYPES_NESTED_PACKAGE_FIRST_LINE: &str =
        formatcp!("package {OBELISK_TYPES_PACKAGE_NAME} {{");
    let wit = remove_nested_package(wit, TYPES_NESTED_PACKAGE_FIRST_LINE);
    let wit = format!("{wit}\n{types_nesting}");
    wit
}

fn generate_param_name(param_name: &str, params: &[(String, Type)]) -> String {
    let orig_param_names: hashbrown::HashSet<&str> =
        params.iter().map(|(name, _)| name.as_str()).collect();
    if orig_param_names.contains(param_name) {
        for my_char in 'a'..='z' {
            let name = format!("{param_name}-{my_char}");
            if !orig_param_names.contains(name.as_str()) {
                return name;
            }
        }
        warn!("Parameter name `{param_name}` collides with other params {orig_param_names:?}");
    }
    param_name.to_string()
}

fn copy_or_refer_original_types(
    orig_ifc_id: InterfaceId,
    orig_ifc: &Interface,
    resolve_types: &mut Arena<TypeDef>,
) -> IndexMap<String, id_arena::Id<TypeDef>> {
    let mut target_types = IndexMap::new();
    // Copy all imports from original to ext interface. Declared types like records will be referenced instead.
    for (name, orig_type_id) in &orig_ifc.types {
        let type_def = resolve_types
            .get(*orig_type_id)
            .expect("type def must be found in resolve");

        let allocated_type_id = match type_def.kind {
            TypeDefKind::Type(_) => resolve_types.alloc(type_def.clone()),
            _ => {
                // Create a reference to the type.
                resolve_types.alloc(TypeDef {
                    name: None,
                    kind: TypeDefKind::Type(Type::Id(*orig_type_id)),
                    owner: TypeOwner::Interface(orig_ifc_id),
                    docs: wit_parser::Docs::default(),
                    stability: wit_parser::Stability::default(),
                })
            }
        };
        target_types.insert(name.clone(), allocated_type_id);
    }
    target_types
}

fn get_or_create_package(
    pkg_fqn: PkgFqn,
    resolve: &mut Resolve,
) -> Result<PackageId, semver::Error> {
    if let Some((pkg_id, _)) = resolve
        .packages
        .iter()
        .find(|(_, found_pkg)| from_wit_package_name_to_pkg_fqn(&found_pkg.name) == pkg_fqn)
    {
        Ok(pkg_id)
    } else {
        let pkg = wit_parser::Package {
            name: from_pkg_fqn_to_wit_package_name(pkg_fqn)?,
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

fn get_ext_pkg_to_ifc_to_details_map(
    exim: &ExIm,
    exorim: ExOrIm,
) -> IndexMap<PkgFqn, IndexMap<IfcFqnName, IndexMap<FnName, FunctionMetadata>>> {
    // Consistent iteration order so that the WIT output is deterministic.
    // Interfaces are sorted already.
    let mut pkg_to_ifc_to_details_map: IndexMap<PkgFqn, IndexMap<IfcFqnName, IndexMap<FnName, _>>> =
        IndexMap::new();
    for pkg_ifc_fns in match exorim {
        ExOrIm::Exports => &exim.exports_hierarchy_ext,
        ExOrIm::Imports => &exim.imports_hierarchy,
    } {
        if pkg_ifc_fns.ifc_fqn.pkg_fqn_name().is_extension() {
            let inner_map = pkg_to_ifc_to_details_map
                .entry(pkg_ifc_fns.ifc_fqn.pkg_fqn_name())
                .or_default();
            inner_map.insert(pkg_ifc_fns.ifc_fqn.clone(), pkg_ifc_fns.fns.clone());
        }
    }
    pkg_to_ifc_to_details_map
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

pub(crate) fn from_wit_package_name_to_pkg_fqn(package_name: &PackageName) -> PkgFqn {
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
    use concepts::ComponentType;
    use rstest::rstest;
    use std::path::PathBuf;
    use wit_component::WitPrinter;
    use wit_parser::{Resolve, UnresolvedPackageGroup};

    #[rstest]
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
        test_programs_http_get_workflow_builder::TEST_PROGRAMS_HTTP_GET_WORKFLOW,
        ComponentType::Workflow
    )]
    #[case(
        test_programs_stub_activity_builder::TEST_PROGRAMS_STUB_ACTIVITY,
        ComponentType::ActivityStub
    )]
    #[case(
        test_programs_stub_workflow_builder::TEST_PROGRAMS_STUB_WORKFLOW,
        ComponentType::Workflow
    )]
    #[case(
        test_programs_sleep_activity_builder::TEST_PROGRAMS_SLEEP_ACTIVITY,
        ComponentType::Workflow
    )]
    #[case(
        test_programs_sleep_workflow_builder::TEST_PROGRAMS_SLEEP_WORKFLOW,
        ComponentType::Workflow
    )]
    fn wit_should_contain_extensions(
        #[case] wasm_path: &'static str,
        #[case] component_type: ComponentType,
    ) {
        test_utils::set_up();

        let component = WasmComponent::new(wasm_path, component_type).unwrap();
        let wasm_path = PathBuf::from(wasm_path);
        let wasm_file = wasm_path.file_name().unwrap().to_string_lossy();
        let wit = component.wit().unwrap();
        // Verify that the generated WIT parses.
        let group = UnresolvedPackageGroup::parse(PathBuf::new(), &wit).unwrap();
        let mut resolve = Resolve::new();
        let main_id = resolve.push_group(group).unwrap();
        let ids = resolve
            .packages
            .iter()
            .map(|(id, _)| id)
            // The main package would show as a nested package as well
            .filter(|id| *id != main_id)
            .collect::<Vec<_>>();
        let mut printer = WitPrinter::default();
        printer.print(&resolve, main_id, &ids).unwrap(); // verify it parses
        // store original WIT string in snapshots, because that is the `wit()` output.
        insta::with_settings!({sort_maps => true, snapshot_suffix => format!("{wasm_file}_wit")}, {insta::assert_snapshot!(wit)});
    }
}
