//! Shared JS import extraction and resolution for Boa-based runtimes.
//!
//! Used by both `workflow_js_worker` and `webhook_trigger` to parse JS source,
//! extract ES module imports, validate them against the function registry,
//! and expand each referenced interface into the full set of function bindings
//! the synthetic module needs to expose.

use boa_engine::ast::declaration::ImportName;
use concepts::{FunctionRegistry, IfcFqnName, PackageIfcFns};
use std::collections::HashMap;
use std::str::FromStr;

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
#[derive(Debug)]
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
fn extract_and_verify<'a>(
    js_code: &str,
    all_exports: &'a [PackageIfcFns],
) -> Result<HashMap<IfcFqnName, &'a PackageIfcFns>, String> {
    let mut interner = boa_engine::interner::Interner::new();
    let mut parser = boa_engine::parser::Parser::new(boa_engine::Source::from_bytes(js_code));
    let scope = boa_engine::ast::scope::Scope::new_global();
    let module = parser
        .parse_module(&scope, &mut interner)
        .map_err(|e| format!("import extraction parse error: {e}"))?;

    let mut referenced: HashMap<IfcFqnName, &PackageIfcFns> = HashMap::new();

    for entry in module.items().import_entries() {
        let specifier = interner
            .resolve_expect(entry.module_request())
            .utf8()
            .ok_or_else(|| "import specifier is not valid UTF-8".to_string())?;

        let ifc_fqn = IfcFqnName::from_str(specifier).map_err(|e| {
            format!(
                "import specifier `{specifier}` is not a WIT interface FQN \
                 (`ns:pkg/ifc` or `ns:pkg/ifc@ver`): {e}"
            )
        })?;
        if ifc_fqn.is_namespace_obelisk() {
            continue;
        }

        let ifc = all_exports
            .iter()
            .find(|pkg| pkg.ifc_fqn == ifc_fqn)
            .ok_or_else(|| format!("interface `{ifc_fqn}` not found for import"))?;

        if let ImportName::Name(sym) = entry.import_name() {
            let js_name = interner
                .resolve_expect(sym)
                .utf8()
                .ok_or_else(|| {
                    format!("imported name from `{specifier}` is not valid UTF-8")
                })?;
            verify_named_import(js_name, ifc)?;
        }

        referenced.entry(ifc_fqn).or_insert(ifc);
    }
    Ok(referenced)
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
) -> Result<HashMap<IfcFqnName, Vec<NamedFnImport>>, String> {
    let all_exports = fn_registry.all_exports();
    let referenced = extract_and_verify(js_code, all_exports)?;
    Ok(referenced
        .into_iter()
        .map(|(ifc_fqn, ifc)| (ifc_fqn, expand_interface(ifc)))
        .collect())
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
