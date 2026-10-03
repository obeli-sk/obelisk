//! Shared JS import extraction and resolution for Boa-based runtimes.
//!
//! Used by both `workflow_js_worker` and `webhook_trigger` to parse JS source,
//! extract ES module imports, validate them against the function registry,
//! and expand each referenced interface into the full set of function bindings
//! the synthetic module needs to expose.

use boa_engine::ast::declaration::{ExportEntry, ImportName, ReExportImportName};
use concepts::{
    ComponentType, FnName, FunctionMetadata, FunctionRegistry, IfcFqnName, PackageIfcFns,
};
use std::collections::hash_map::Entry;
use std::collections::{BTreeSet, HashMap};
use std::ops::Deref;
use std::str::FromStr;
use std::sync::LazyLock;
use utils::wasm_tools::WasmComponent;
use utils::wit::{
    WIT_OBELISK_TYPES_PACKAGE, WIT_OBELISK_WEBHOOK_PACKAGE, WIT_OBELISK_WORKFLOW_PACKAGE,
};

#[derive(Debug)]
pub struct BuiltinModule {
    specifier: &'static str,
    exports: &'static [&'static str],
    /// Listed as imports of a component whose code imports this module.
    listed_imports: Option<&'static LazyLock<Vec<FunctionMetadata>>>,
}

/// Functions of an embedded `obelisk` interface.
fn interface_functions(
    pkg: &str,
    ifc_fqn: &str,
    component_type: ComponentType,
) -> Vec<FunctionMetadata> {
    let world = format!("package any:any; world any {{ import {ifc_fqn}; }}");
    WasmComponent::new_from_wit_string_with_deps(
        &world,
        &[WIT_OBELISK_TYPES_PACKAGE[2], pkg],
        component_type,
    )
    .expect("embedded WIT must be valid")
    .imported_functions()
    .iter()
    .filter(|function| function.ffqn.ifc_fqn.deref() == ifc_fqn)
    .cloned()
    .collect()
}

static WORKFLOW_DYNAMIC_SUPPORT: LazyLock<Vec<FunctionMetadata>> = LazyLock::new(|| {
    interface_functions(
        WIT_OBELISK_WORKFLOW_PACKAGE[2],
        "obelisk:workflow/workflow-dynamic-support@7.0.0",
        ComponentType::Workflow,
    )
});

static WEBHOOK_DYNAMIC_SUPPORT: LazyLock<Vec<FunctionMetadata>> = LazyLock::new(|| {
    interface_functions(
        WIT_OBELISK_WEBHOOK_PACKAGE[2],
        "obelisk:webhook/webhook-dynamic-support@7.0.0",
        ComponentType::WebhookEndpoint,
    )
});

pub const WORKFLOW_BUILTIN_MODULES: &[BuiltinModule] = &[
    BuiltinModule {
        specifier: "obelisk:workflow@1.0.0",
        exports: &[
            "executionIdCurrent",
            "executionIdGenerate",
            "createJoinSet",
            "sleep",
            "randomU64",
            "randomU64Inclusive",
            "randomString",
            "getResult",
            "stub",
            "JoinSetExhaustedError",
            "ChildError",
        ],
        listed_imports: None,
    },
    BuiltinModule {
        specifier: "obelisk:workflow-dynamic@1.0.0",
        exports: &["call", "schedule", "submit"],
        listed_imports: Some(&WORKFLOW_DYNAMIC_SUPPORT),
    },
];
pub const WEBHOOK_BUILTIN_MODULES: &[BuiltinModule] = &[
    BuiltinModule {
        specifier: "obelisk:webhook@1.0.0",
        exports: &[
            "executionIdGenerate",
            "executionIdCurrent",
            "getStatus",
            "get",
            "tryGet",
            "ChildError",
        ],
        listed_imports: None,
    },
    BuiltinModule {
        specifier: "obelisk:webhook-dynamic@1.0.0",
        exports: &["call", "schedule"],
        listed_imports: Some(&WEBHOOK_DYNAMIC_SUPPORT),
    },
];

/// Convert a JS camelCase name to WIT kebab-case.
fn camel_to_kebab(s: &str) -> String {
    let mut result = String::with_capacity(s.len() + 4);
    for (i, ch) in s.char_indices() {
        if ch.is_uppercase() {
            if i > 0 {
                result.push('-');
            }
            for lower in ch.to_lowercase() {
                result.push(lower);
            }
        } else {
            result.push(ch);
        }
    }
    result
}

/// A function imported via `import { ... } from 'ns:pkg/ifc'`, with both its
/// JS-side camelCase name and the resolved WIT kebab-case name.
#[derive(Clone, Debug)]
pub(crate) struct NamedFnImport {
    pub js_name: String,
    pub wit_name: String,
}

/// Convert a WIT kebab-case name to JS camelCase.
///
/// Examples: `"account-info"` → `"accountInfo"`, `"add"` → `"add"`,
/// `"add-submit"` → `"addSubmit"`.
fn kebab_to_camel(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    let mut capitalize_next = false;
    for ch in s.chars() {
        if ch == '-' {
            capitalize_next = true;
        } else if capitalize_next {
            for upper in ch.to_uppercase() {
                result.push(upper);
            }
            capitalize_next = false;
        } else {
            result.push(ch);
        }
    }
    result
}

/// Parse JS source, validate every non-`obelisk:` import against the function
/// registry, and return the deduped map of referenced interfaces with their
/// resolved `PackageIfcFns` entry from the registry. The registry already
/// lists `-obelisk-*` extension interfaces separately with their suffixed
/// function names, so an `import` from `pkg-obelisk-ext/ifc` resolves to the
/// extension entry directly — no per-flavor synthesis on this side.
///
/// Named imports are checked function-by-function so typos surface at link
/// time. Namespace imports (`import * as`) and star re-exports bind the whole interface.
fn extract_and_verify<'a, 'm>(
    js_code: &str,
    all_exports: &'a [PackageIfcFns],
    builtin_modules: &'m [BuiltinModule],
) -> Result<ExtractedImports<'a, 'm>, String> {
    let mut interner = boa_engine::interner::Interner::new();
    let mut parser = boa_engine::parser::Parser::new(boa_engine::Source::from_bytes(js_code));
    let scope = boa_engine::ast::scope::Scope::new_global();
    let module = parser
        .parse_module(&scope, &mut interner)
        .map_err(|e| format!("import extraction parse error: {e}"))?;

    let mut referenced: HashMap<IfcFqnName, ReferencedIfc> = HashMap::new();
    let mut used_builtin_modules = Vec::new();

    let named_imports = module
        .items()
        .import_entries()
        .into_iter()
        .filter_map(|entry| {
            if let ImportName::Name(name) = entry.import_name() {
                Some((entry.module_request(), name))
            } else {
                None
            }
        })
        .chain(
            module
                .items()
                .export_entries()
                .into_iter()
                .filter_map(|entry| {
                    if let ExportEntry::ReExport(entry) = entry
                        && let ReExportImportName::Name(name) = entry.import_name()
                    {
                        Some((entry.module_request(), name))
                    } else {
                        None
                    }
                }),
        )
        .collect::<Vec<_>>();
    let namespace_requests = module
        .items()
        .import_entries()
        .into_iter()
        .filter_map(|entry| {
            matches!(entry.import_name(), ImportName::Namespace).then(|| entry.module_request())
        })
        .chain(
            module
                .items()
                .export_entries()
                .into_iter()
                .filter_map(|entry| match entry {
                    ExportEntry::StarReExport { module_request, .. } => Some(module_request),
                    ExportEntry::ReExport(entry)
                        if matches!(entry.import_name(), ReExportImportName::Star) =>
                    {
                        Some(entry.module_request())
                    }
                    _ => None,
                }),
        )
        .collect::<Vec<_>>();
    for request in module.items().requests() {
        let specifier = interner
            .resolve_expect(request)
            .utf8()
            .ok_or_else(|| "import specifier is not valid UTF-8".to_string())?;

        if specifier.starts_with("./") || specifier.starts_with("../") {
            continue;
        }

        if specifier.starts_with("obelisk:") {
            if let Some(module) = builtin_modules
                .iter()
                .find(|module| module.specifier == specifier)
            {
                for (_, sym) in named_imports
                    .iter()
                    .filter(|(module, _)| *module == request)
                {
                    let name = interner.resolve_expect(*sym).utf8().ok_or_else(|| {
                        format!("imported name from `{specifier}` is not valid UTF-8")
                    })?;
                    if !module.exports.contains(&name) {
                        return Err(format!(
                            "export `{name}` not found in Obelisk JavaScript module `{specifier}`"
                        ));
                    }
                }
                used_builtin_modules.push(module);
                continue;
            }
            return Err(format!(
                "unsupported Obelisk JavaScript module `{specifier}`"
            ));
        }

        let ifc_fqn = IfcFqnName::from_str(specifier).map_err(|e| {
            format!(
                "import specifier `{specifier}` is not a WIT interface FQN \
                 (`ns:pkg/ifc` or `ns:pkg/ifc@ver`): {e}"
            )
        })?;
        let ifc = all_exports
            .iter()
            .find(|pkg| pkg.ifc_fqn == ifc_fqn)
            .ok_or_else(|| format!("interface `{ifc_fqn}` not found for import"))?;

        let mut named_fns = BTreeSet::new();
        for (_, sym) in named_imports
            .iter()
            .filter(|(module, _)| *module == request)
        {
            let js_name = interner
                .resolve_expect(*sym)
                .utf8()
                .ok_or_else(|| format!("imported name from `{specifier}` is not valid UTF-8"))?;
            named_fns.insert(verify_named_import(js_name, ifc)?);
        }
        let referenced_ifc = ReferencedIfc {
            ifc,
            named_fns: (!named_fns.is_empty() && !namespace_requests.contains(&request))
                .then_some(named_fns),
        };
        ReferencedIfc::insert(&mut referenced, ifc_fqn, referenced_ifc);
    }
    Ok(ExtractedImports {
        interfaces: referenced,
        builtin_modules: used_builtin_modules,
    })
}

pub(crate) struct JsDispatchPolicy {
    pub(crate) dynamic: bool,
    targets: std::collections::HashSet<concepts::FunctionFqn>,
}

impl JsDispatchPolicy {
    pub(crate) fn new(
        files: &std::collections::BTreeMap<String, String>,
        imports: &HashMap<IfcFqnName, Vec<NamedFnImport>>,
        dynamic_module: &str,
    ) -> Result<Self, String> {
        use worker_common::js_imports::{
            EXT_SUFFIX, SCHEDULE_SUFFIX, STUB_SUFFIX, strip_specifier_suffix,
        };
        let dynamic = boa_common::imports::declared_modules(files.values().map(String::as_str))?
            .contains(dynamic_module);
        let mut targets = std::collections::HashSet::new();
        for (specifier, functions) in imports {
            let specifier = specifier.to_string();
            let extension = [
                (SCHEDULE_SUFFIX, &["-schedule"][..]),
                (EXT_SUFFIX, &["-submit", "-await-next", "-get"][..]),
                (STUB_SUFFIX, &["-stub"][..]),
            ]
            .into_iter()
            .find_map(|(suffix, names)| {
                strip_specifier_suffix(&specifier, suffix).map(|base| (base, names))
            });
            for function in functions {
                let (interface, name) = if let Some((base, suffixes)) = &extension {
                    let name = suffixes
                        .iter()
                        .find_map(|suffix| function.wit_name.strip_suffix(suffix))
                        .ok_or_else(|| {
                            format!("invalid extension function: {}", function.wit_name)
                        })?;
                    (base.as_str(), name)
                } else {
                    (specifier.as_str(), function.wit_name.as_str())
                };
                targets.insert(
                    format!("{interface}.{name}")
                        .parse()
                        .map_err(|err| format!("invalid target: {err}"))?,
                );
            }
        }
        Ok(Self { dynamic, targets })
    }

    pub(crate) fn check(&self, target: &str) -> Result<(), String> {
        let target = target
            .parse::<concepts::FunctionFqn>()
            .map_err(|err| format!("invalid target: {err}"))?;
        if self.dynamic || self.targets.contains(&target) {
            Ok(())
        } else {
            Err(format!(
                "undeclared target `{target}`; import its interface or the dynamic module"
            ))
        }
    }
}

#[derive(Debug)]
struct ExtractedImports<'a, 'm> {
    interfaces: HashMap<IfcFqnName, ReferencedIfc<'a>>,
    builtin_modules: Vec<&'m BuiltinModule>,
}

#[derive(Debug)]
struct ReferencedIfc<'a> {
    ifc: &'a PackageIfcFns,
    /// WIT names of the functions imported by name. `None` declares the whole interface:
    /// a namespace import, a star re-export, or a declaration without bindings.
    named_fns: Option<BTreeSet<String>>,
}

impl ReferencedIfc<'_> {
    fn insert(map: &mut HashMap<IfcFqnName, Self>, ifc_fqn: IfcFqnName, referenced: Self) {
        match map.entry(ifc_fqn) {
            Entry::Occupied(mut occupied) => {
                match (&mut occupied.get_mut().named_fns, referenced.named_fns) {
                    (Some(ours), Some(theirs)) => ours.extend(theirs),
                    (named_fns, _) => *named_fns = None,
                }
            }
            Entry::Vacant(vacant) => {
                vacant.insert(referenced);
            }
        }
    }

    fn imported_functions(&self) -> impl Iterator<Item = (&FnName, &FunctionMetadata)> {
        self.ifc.fns.iter().filter(|(fn_name, _)| {
            self.named_fns
                .as_ref()
                .is_none_or(|named| named.contains::<str>(fn_name))
        })
    }
}

/// Verify that a JS-side named import resolves to a real function on the
/// interface. The registry's extension entries already carry suffixed
/// function names (e.g. `add-submit`), so we just kebab-case the JS name and
/// look it up directly.
fn verify_named_import(js_name: &str, ifc: &PackageIfcFns) -> Result<String, String> {
    let wit_name = camel_to_kebab(js_name);
    if !ifc.fns.contains_key(wit_name.as_str()) {
        return Err(format!(
            "function `{ifc_fqn}.{wit_name}` (imported as `{js_name}`) not found",
            ifc_fqn = ifc.ifc_fqn,
        ));
    }
    Ok(wit_name)
}

/// Extract and merge the imports of all files of a component.
fn extract_and_verify_files<'a, 'm>(
    js_files: impl IntoIterator<Item = &'a str>,
    all_exports: &'m [PackageIfcFns],
    builtin_modules: &'m [BuiltinModule],
) -> Result<ExtractedImports<'m, 'm>, String> {
    let mut merged = ExtractedImports {
        interfaces: HashMap::new(),
        builtin_modules: Vec::new(),
    };
    for js_code in js_files {
        let extracted = extract_and_verify(js_code, all_exports, builtin_modules)?;
        for (ifc_fqn, referenced) in extracted.interfaces {
            ReferencedIfc::insert(&mut merged.interfaces, ifc_fqn, referenced);
        }
        for module in extracted.builtin_modules {
            if !merged
                .builtin_modules
                .iter()
                .any(|merged| merged.specifier == module.specifier)
            {
                merged.builtin_modules.push(module);
            }
        }
    }
    Ok(merged)
}

/// Resolve JS imports of all files of a component against the function registry.
///
/// Returns one entry per referenced interface with the functions its synthetic module exposes:
/// those imported by name, or all of them when the whole interface is declared.
///
/// The key preserves the original specifier (including any `-obelisk-*`
/// package suffix) so the runtime can register the module under the same
/// name JS uses.
pub(crate) fn resolve_js_imports<'a>(
    js_files: impl IntoIterator<Item = &'a str>,
    fn_registry: &dyn FunctionRegistry,
    builtin_modules: &[BuiltinModule],
) -> Result<HashMap<IfcFqnName, Vec<NamedFnImport>>, String> {
    let extracted = extract_and_verify_files(js_files, fn_registry.all_exports(), builtin_modules)?;
    Ok(extracted
        .interfaces
        .into_iter()
        .map(|(ifc_fqn, referenced)| (ifc_fqn, expand_interface(&referenced)))
        .collect())
}

/// Imports of a JS component as read from its code: the functions its synthetic modules expose,
/// plus the dynamic support interface when the code uses dynamic calls.
pub fn js_component_imports<'a>(
    js_files: impl IntoIterator<Item = &'a str>,
    all_exports: &[PackageIfcFns],
    builtin_modules: &[BuiltinModule],
) -> Result<Vec<FunctionMetadata>, String> {
    let extracted = extract_and_verify_files(js_files, all_exports, builtin_modules)?;
    let mut imports = extracted
        .interfaces
        .values()
        .flat_map(|referenced| referenced.imported_functions().map(|(_, f)| f.clone()))
        .chain(
            extracted
                .builtin_modules
                .into_iter()
                .filter_map(|module| module.listed_imports)
                .flat_map(|functions| functions.iter().cloned()),
        )
        .collect::<Vec<_>>();
    imports.sort_by(|a, b| {
        (a.ffqn.ifc_fqn.deref(), a.ffqn.function_name.deref())
            .cmp(&(b.ffqn.ifc_fqn.deref(), b.ffqn.function_name.deref()))
    });
    Ok(imports)
}

/// Build the list of `NamedFnImport` entries the synthetic module for
/// this interface should expose. Extension interfaces already carry their
/// suffixed function names in `ifc.fns`, so a straight kebab→camel mapping
/// is all that's needed.
fn expand_interface(referenced: &ReferencedIfc) -> Vec<NamedFnImport> {
    referenced
        .imported_functions()
        .map(|(fn_name, _)| {
            let wit_name = fn_name.to_string();
            let js_name = kebab_to_camel(&wit_name);
            NamedFnImport { js_name, wit_name }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing_fn_registry::fn_registry_dummy;
    use concepts::FunctionFqn;

    #[test]
    fn component_imports_list_imported_functions_and_dynamic_support() {
        let deployment_registry = fn_registry_dummy(&[
            FunctionFqn::new_static("app:act/api", "get"),
            FunctionFqn::new_static("app:act/api", "put"),
            FunctionFqn::new_static("app:other/api", "unused"),
        ]);
        let ffqns = |js_files: &[&str]| {
            js_component_imports(
                js_files.iter().copied(),
                deployment_registry.all_exports(),
                WORKFLOW_BUILTIN_MODULES,
            )
            .unwrap()
            .into_iter()
            .map(|function| function.ffqn.to_string())
            .collect::<Vec<_>>()
        };

        assert_eq!(
            ffqns(&[
                "import { get } from 'app:act/api';",
                "import { sleep } from 'obelisk:workflow@1.0.0';",
            ]),
            ["app:act/api.get"]
        );
        let all = ["app:act/api.get", "app:act/api.put"];
        assert_eq!(ffqns(&["import * as api from 'app:act/api';"]), all);
        assert_eq!(ffqns(&["export * from 'app:act/api';"]), all);
        assert_eq!(
            ffqns(&[
                "import { get } from 'app:act/api';",
                "import * as api from 'app:act/api';",
            ]),
            all
        );
        assert_eq!(
            ffqns(&[
                "import { get } from 'app:act/api';",
                "export { put } from 'app:act/api';",
            ]),
            all
        );
        assert_eq!(ffqns(&["import 'app:act/api';"]), all);
        assert_eq!(ffqns(&["import {} from 'app:act/api';"]), all);
        assert_eq!(
            ffqns(&[
                "import { get } from 'app:act/api';",
                "import 'app:act/api';",
            ]),
            all
        );
        let resolved = resolve_js_imports(
            ["import { get } from 'app:act/api';"],
            deployment_registry.as_ref(),
            WORKFLOW_BUILTIN_MODULES,
        )
        .unwrap();
        assert_eq!(
            resolved[&IfcFqnName::from_str("app:act/api").unwrap()]
                .iter()
                .map(|function| function.js_name.as_str())
                .collect::<Vec<_>>(),
            ["get"]
        );
        let dynamic = ffqns(&["import { call } from 'obelisk:workflow-dynamic@1.0.0';"]);
        assert!(!dynamic.is_empty());
        assert!(
            dynamic
                .iter()
                .all(|ffqn| ffqn.starts_with("obelisk:workflow/workflow-dynamic-support@7.0.0."))
        );
    }

    #[test]
    fn dynamic_support_is_not_empty() {
        assert!(!WORKFLOW_DYNAMIC_SUPPORT.is_empty());
        assert!(!WEBHOOK_DYNAMIC_SUPPORT.is_empty());
    }

    #[test]
    fn import_policy_counts_all_static_module_declarations() {
        let registry = fn_registry_dummy(&[FunctionFqn::new_static("app:act/api", "get")]);
        for (modules, specifier, dynamic_ifc) in [
            (
                WORKFLOW_BUILTIN_MODULES,
                "obelisk:workflow-dynamic@1.0.0",
                "obelisk:workflow/workflow-dynamic-support@7.0.0",
            ),
            (
                WEBHOOK_BUILTIN_MODULES,
                "obelisk:webhook-dynamic@1.0.0",
                "obelisk:webhook/webhook-dynamic-support@7.0.0",
            ),
        ] {
            for declaration in [
                format!("import '{specifier}';"),
                format!("import {{}} from '{specifier}';"),
                format!("export {{call as invoke}} from '{specifier}';"),
                format!("export * from '{specifier}';"),
                format!("export * as dynamic from '{specifier}';"),
            ] {
                let sources = [
                    "import './helper.js'; import 'app:act/api';",
                    declaration.as_str(),
                ];
                let imports =
                    js_component_imports(sources, registry.all_exports(), modules).unwrap();
                assert!(
                    imports
                        .iter()
                        .any(|function| function.ffqn.ifc_fqn.to_string() == dynamic_ifc),
                    "{declaration}"
                );
                assert!(
                    imports
                        .iter()
                        .any(|function| function.ffqn.to_string() == "app:act/api.get")
                );
                let files = sources
                    .into_iter()
                    .enumerate()
                    .map(|(i, source)| (format!("{i}.js"), source.to_string()))
                    .collect();
                let policy = JsDispatchPolicy::new(&files, &HashMap::new(), specifier).unwrap();
                assert!(policy.dynamic);
            }
            let imports = js_component_imports(
                [format!("export default () => import('{specifier}');").as_str()],
                registry.all_exports(),
                modules,
            )
            .unwrap();
            assert!(imports.is_empty());
            let err = extract_and_verify(
                &format!("export {{unknown}} from '{specifier}';"),
                registry.all_exports(),
                modules,
            )
            .unwrap_err();
            assert!(err.contains("export `unknown` not found"));
        }
        let err = extract_and_verify(
            "export { missing } from 'app:act/api';",
            registry.all_exports(),
            WORKFLOW_BUILTIN_MODULES,
        )
        .unwrap_err();
        assert!(err.contains("function `app:act/api.missing`"));
    }

    #[test]
    fn accepts_supported_builtin_module() {
        let imports = extract_and_verify(
            "import * as obelisk from 'obelisk:workflow@1.0.0';",
            &[],
            WORKFLOW_BUILTIN_MODULES,
        )
        .unwrap();
        assert!(imports.interfaces.is_empty());
    }

    #[test]
    fn rejects_unsupported_builtin_version() {
        let err = extract_and_verify(
            "import * as obelisk from 'obelisk:workflow@2.0.0';",
            &[],
            WORKFLOW_BUILTIN_MODULES,
        )
        .unwrap_err();
        assert!(err.contains("unsupported Obelisk JavaScript module"));
    }

    #[test]
    fn rejects_unknown_builtin_export() {
        let err = extract_and_verify(
            "import { call } from 'obelisk:workflow@1.0.0';",
            &[],
            WORKFLOW_BUILTIN_MODULES,
        )
        .unwrap_err();
        assert!(err.contains("export `call` not found"));
    }
}
