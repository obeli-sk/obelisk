//! Shared JS import extraction and resolution for Boa-based runtimes.
//!
//! Used by both `workflow_js_worker` and `webhook_trigger` to parse JS source,
//! extract ES module imports, validate them against the function registry,
//! and expand each referenced interface into the full set of function bindings
//! the synthetic module needs to expose.

use boa_engine::ast::declaration::ImportName;
use concepts::{FunctionMetadata, FunctionRegistry, IfcFqnName, PackageIfcFns};
use std::collections::{HashMap, HashSet};
use std::ops::Deref;
use std::str::FromStr;

#[derive(Debug)]
pub struct BuiltinModule {
    specifier: &'static str,
    exports: &'static [&'static str],
    /// `obelisk` runtime interfaces backing this module, listed as imports only when the code imports it.
    runtime_ifc_names: &'static [&'static str],
}

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
        runtime_ifc_names: &[],
    },
    BuiltinModule {
        specifier: "obelisk:workflow-dynamic@1.0.0",
        exports: &["call", "schedule"],
        runtime_ifc_names: &[
            "workflow-dynamic-support",
            "workflow-dynamic-support-backtrace",
        ],
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
        runtime_ifc_names: &[],
    },
    BuiltinModule {
        specifier: "obelisk:webhook-dynamic@1.0.0",
        exports: &["call", "schedule"],
        runtime_ifc_names: &[
            "webhook-dynamic-support",
            "webhook-dynamic-support-backtrace",
        ],
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
/// time. Namespace imports (`import * as`) just contribute their interface.
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

    let mut referenced: HashMap<IfcFqnName, &PackageIfcFns> = HashMap::new();
    let mut used_builtin_modules = Vec::new();

    for entry in module.items().import_entries() {
        let specifier = interner
            .resolve_expect(entry.module_request())
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
                if let ImportName::Name(sym) = entry.import_name() {
                    let name = interner.resolve_expect(sym).utf8().ok_or_else(|| {
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

        if let ImportName::Name(sym) = entry.import_name() {
            let js_name = interner
                .resolve_expect(sym)
                .utf8()
                .ok_or_else(|| format!("imported name from `{specifier}` is not valid UTF-8"))?;
            verify_named_import(js_name, ifc)?;
        }

        referenced.entry(ifc_fqn).or_insert(ifc);
    }
    Ok(ExtractedImports {
        interfaces: referenced,
        builtin_modules: used_builtin_modules,
    })
}

#[derive(Debug)]
struct ExtractedImports<'a, 'm> {
    interfaces: HashMap<IfcFqnName, &'a PackageIfcFns>,
    builtin_modules: Vec<&'m BuiltinModule>,
}

/// Verify that a JS-side named import resolves to a real function on the
/// interface. The registry's extension entries already carry suffixed
/// function names (e.g. `add-submit`), so we just kebab-case the JS name and
/// look it up directly.
fn verify_named_import(js_name: &str, ifc: &PackageIfcFns) -> Result<(), String> {
    let wit_name = camel_to_kebab(js_name);
    if !ifc.fns.contains_key(wit_name.as_str()) {
        return Err(format!(
            "function `{ifc_fqn}.{wit_name}` (imported as `{js_name}`) not found",
            ifc_fqn = ifc.ifc_fqn,
        ));
    }
    Ok(())
}

/// Resolve JS imports against the function registry.
///
/// Returns one entry per referenced interface, with every function the
/// synthetic module needs to expose so both `import { x }` and `import * as
/// ns` from the same specifier work uniformly.
///
/// The key preserves the original specifier (including any `-obelisk-*`
/// package suffix) so the runtime can register the module under the same
/// name JS uses.
pub(crate) fn resolve_js_imports(
    js_code: &str,
    fn_registry: &dyn FunctionRegistry,
    builtin_modules: &[BuiltinModule],
) -> Result<HashMap<IfcFqnName, Vec<NamedFnImport>>, String> {
    let all_exports = fn_registry.all_exports();
    let referenced = extract_and_verify(js_code, all_exports, builtin_modules)?;
    Ok(referenced
        .interfaces
        .into_iter()
        .map(|(ifc_fqn, ifc)| (ifc_fqn, expand_interface(ifc)))
        .collect())
}

/// Functions a JS component can call, listed as its imports: every function of each interface
/// its code imports, plus the imports of its runtime except for the interfaces backing builtin
/// modules the code does not import.
pub fn js_component_imports<'a>(
    js_files: impl IntoIterator<Item = &'a str>,
    runtime_imports: &[FunctionMetadata],
    all_exports: &[PackageIfcFns],
    builtin_modules: &[BuiltinModule],
) -> Result<Vec<FunctionMetadata>, String> {
    let mut interfaces = HashMap::new();
    let mut used_builtin_modules = HashSet::new();
    for js_code in js_files {
        let extracted = extract_and_verify(js_code, all_exports, builtin_modules)?;
        interfaces.extend(extracted.interfaces);
        used_builtin_modules.extend(extracted.builtin_modules.iter().map(|m| m.specifier));
    }
    let hidden_runtime_ifc_names = builtin_modules
        .iter()
        .filter(|module| !used_builtin_modules.contains(module.specifier))
        .flat_map(|module| module.runtime_ifc_names.iter().copied())
        .collect::<HashSet<_>>();
    let mut imports = runtime_imports
        .iter()
        .filter(|function| {
            let ifc_fqn = &function.ffqn.ifc_fqn;
            ifc_fqn.namespace() != "obelisk"
                || !hidden_runtime_ifc_names.contains(ifc_fqn.ifc_name())
        })
        .cloned()
        .chain(
            interfaces
                .into_values()
                .flat_map(|ifc| ifc.fns.values().cloned()),
        )
        .collect::<Vec<_>>();
    imports.sort_by(|a, b| {
        (a.ffqn.ifc_fqn.deref(), a.ffqn.function_name.deref())
            .cmp(&(b.ffqn.ifc_fqn.deref(), b.ffqn.function_name.deref()))
    });
    Ok(imports)
}

/// Build the full list of `NamedFnImport` entries the synthetic module for
/// this interface should expose. Extension interfaces already carry their
/// suffixed function names in `ifc.fns`, so a straight kebab→camel mapping
/// is all that's needed.
fn expand_interface(ifc: &PackageIfcFns) -> Vec<NamedFnImport> {
    ifc.fns
        .keys()
        .map(|fn_name| {
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
    fn component_imports_list_imported_interfaces_and_used_runtime_support() {
        let runtime_registry = fn_registry_dummy(&[
            FunctionFqn::new_static("obelisk:workflow/workflow-support@7.0.0", "join-next"),
            FunctionFqn::new_static("obelisk:workflow/workflow-dynamic-support@7.0.0", "call"),
            FunctionFqn::new_static(
                "obelisk:workflow/workflow-dynamic-support-backtrace@7.0.0",
                "call",
            ),
        ]);
        let runtime_imports = runtime_registry
            .all_exports()
            .iter()
            .flat_map(|ifc| ifc.fns.values().cloned())
            .collect::<Vec<_>>();
        let deployment_registry = fn_registry_dummy(&[
            FunctionFqn::new_static("app:act/api", "get"),
            FunctionFqn::new_static("app:act/api", "put"),
            FunctionFqn::new_static("app:other/api", "unused"),
        ]);
        let ffqns = |js_files: &[&str]| {
            js_component_imports(
                js_files.iter().copied(),
                &runtime_imports,
                deployment_registry.all_exports(),
                WORKFLOW_BUILTIN_MODULES,
            )
            .unwrap()
            .into_iter()
            .map(|function| function.ffqn.to_string())
            .collect::<Vec<_>>()
        };

        assert_eq!(
            ffqns(&["import { get } from 'app:act/api';"]),
            [
                "app:act/api.get",
                "app:act/api.put",
                "obelisk:workflow/workflow-support@7.0.0.join-next",
            ]
        );
        assert_eq!(
            ffqns(&[
                "import { sleep } from 'obelisk:workflow@1.0.0';",
                "import { call } from 'obelisk:workflow-dynamic@1.0.0';",
            ]),
            [
                "obelisk:workflow/workflow-dynamic-support-backtrace@7.0.0.call",
                "obelisk:workflow/workflow-dynamic-support@7.0.0.call",
                "obelisk:workflow/workflow-support@7.0.0.join-next",
            ]
        );
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
