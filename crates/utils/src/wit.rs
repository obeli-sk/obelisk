use std::{ops::Deref, path::PathBuf};

use crate::wasm_tools::{ComponentExportsType, ExIm};
use anyhow::bail;
use concepts::{FnName, IfcFqnName, PkgFqn, SUFFIX_PKG_EXT};
use hashbrown::HashMap;
use id_arena::Arena;
use indexmap::IndexMap;
use tracing::{error, warn};
use wit_component::{DecodedWasm, WitPrinter};
use wit_parser::{
    Function, FunctionKind, Handle, Interface, InterfaceId, PackageName, Resolve, Results, Type,
    TypeDef, TypeDefKind, TypeOwner, UnresolvedPackageGroup,
};

// Keep in sync with the wit file
const OBELISK_TYPES_PACKAGE: &str = "
    package obelisk:types {
      interface time {
        variant duration {
          milliseconds(u64),
          seconds(u64),
          minutes(u32),
          hours(u32),
          days(u32),
        }
        record datetime {
          seconds: u64,
          nanoseconds: u32,
        }
        variant schedule-at {
          now,
          at(datetime),
          in(duration),
        }
      }
      interface execution {
          resource join-set-id {
          }
          record execution-id {
              id: string,
          }

          record delay-id {
              id: string,
          }

          variant execution-error {
              permanent-failure(string),
              permanent-timeout,
              nondeterminism,
          }
      }
    }
    ";

pub(crate) fn wit(
    enrich: ComponentExportsType,
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
    match enrich {
        ComponentExportsType::Enrichable => add_ext_exports(&wit, exim),
        ComponentExportsType::Plain => Ok(wit),
    }
}

#[expect(clippy::too_many_lines)]
fn add_ext_exports(wit: &str, exim: &ExIm) -> Result<String, anyhow::Error> {
    let wit = remove_nested_package(wit, "package obelisk:types {");
    let wit = format!("{wit}\n{OBELISK_TYPES_PACKAGE}");

    let group = UnresolvedPackageGroup::parse(PathBuf::new(), &wit)?;
    let mut resolve = Resolve::new();
    let main_id = resolve.push_group(group)?;
    let exported_pkg_to_ifc_to_details_map = get_exported_pkg_to_ifc_to_details_map(exim);

    // Find necessary handles
    // Get obelisk:types
    let obelisk_types_package_name = PackageName {
        namespace: "obelisk".to_string(),
        name: "types".to_string(),
        version: None,
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
    // Get obelisk:types/time
    let time_ifc_id = *resolve.packages[obelisk_types_pkg_id]
        .interfaces
        .get("time")
        .expect("`time` interface was added");
    let time_ifc = &resolve.interfaces[time_ifc_id];

    let (execution_ifc_id, execution_ifc) = find_interface(
        &IfcFqnName::from_parts("obelisk", "types", "execution", None),
        &resolve,
        &resolve.interfaces,
    )
    .expect("`obelisk:types/execution` must be found");
    let type_id_execution_id = {
        // obelisk:types/execution.{execution-id}
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
        // obelisk:types/execution.{join-set-id}
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
        let pkg_ext_fqn = PkgFqn {
            namespace: pkg_fqn.namespace.clone(),
            package_name: format!("{}{SUFFIX_PKG_EXT}", pkg_fqn.package_name),
            version: pkg_fqn.version.clone(),
        };

        // Get or create the -obelisk-ext variant of the exported package.
        let ext_pkg_id =
            if let Some((pkg_ext_id, _)) = resolve.packages.iter().find(|(_, found_pkg)| {
                from_wit_package_name_to_pkg_fqn(&found_pkg.name) == pkg_ext_fqn
            }) {
                pkg_ext_id
            } else {
                let pkg_ext = wit_parser::Package {
                    name: from_pkg_fqn_to_wit_package_name(pkg_ext_fqn)?,
                    docs: wit_parser::Docs::default(),
                    interfaces: IndexMap::default(),
                    worlds: IndexMap::default(),
                };
                let package_name = pkg_ext.name.clone();
                let ext_pkg_id = resolve.packages.alloc(pkg_ext);
                resolve.package_names.insert(package_name, ext_pkg_id);
                ext_pkg_id
            };

        for (ifc_fqn, fns) in ifc_to_fns {
            let (_, original_ifc) =
                find_interface(&ifc_fqn, &resolve, &resolve.interfaces).expect("TODO");
            let mut types = original_ifc.types.clone();
            types.insert("execution-id".to_string(), type_id_execution_id);
            types.insert("join-set-id".to_string(), type_id_join_set_id);
            types.insert("schedule-at".to_string(), type_id_schedule_at);
            types.insert("execution-error".to_string(), type_id_execution_error);

            let mut ext_ifc = Interface {
                name: Some(ifc_fqn.ifc_name().to_string()),
                types,
                functions: IndexMap::default(),
                docs: wit_parser::Docs::default(),
                stability: wit_parser::Stability::default(),
                package: Some(ext_pkg_id),
            };
            for fn_name in fns {
                let original_fn = original_ifc.functions.get(fn_name.deref()).expect("TODO");
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
                        results: Results::Anon(Type::Id(type_id_execution_id)),
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
                    let results = match &original_fn.results {
                        Results::Anon(actual_return_type_id) => {
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
                            Results::Anon(Type::Id(type_id_result))
                        }
                        Results::Named(vec) if vec.is_empty() => {
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
                            Results::Anon(Type::Id(type_id_result))
                        }
                        Results::Named(_) => {
                            bail!(
                                "named results are unsupported {fn_name} - {:?}",
                                original_fn
                            )
                        }
                    };
                    let fn_ext = Function {
                        name: fn_name.clone(),
                        kind: FunctionKind::Freestanding,
                        params,
                        results,
                        docs: wit_parser::Docs::default(),
                        stability: wit_parser::Stability::default(),
                    };
                    ext_ifc.functions.insert(fn_name, fn_ext);
                }
                // -schedule  -schedule: func(schedule-at: schedule-at, <params>) -> execution-id;
                {
                    let fn_name = format!("{fn_name}-schedule");
                    let mut params =
                        vec![("schedule-at".to_string(), Type::Id(type_id_schedule_at))];
                    params.extend_from_slice(&original_fn.params);
                    let fn_ext = Function {
                        name: fn_name.clone(),
                        kind: FunctionKind::Freestanding,
                        params,
                        results: Results::Anon(Type::Id(type_id_execution_id)),
                        docs: wit_parser::Docs::default(),
                        stability: wit_parser::Stability::default(),
                    };
                    ext_ifc.functions.insert(fn_name, fn_ext);
                }
            }
            let ext_ifc_id = resolve.interfaces.alloc(ext_ifc);
            resolve
                .packages
                .get_mut(ext_pkg_id)
                .expect("found or inserted already")
                .interfaces
                .insert(ifc_fqn.ifc_name().to_string(), ext_ifc_id);
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

fn get_exported_pkg_to_ifc_to_details_map(
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
