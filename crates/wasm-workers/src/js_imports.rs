//! Shared JS import extraction and resolution for Boa-based runtimes.
//!
//! Used by both `workflow_js_worker` and `webhook_trigger` to parse JS source,
//! extract ES module imports, and resolve them against the function registry.

use boa_engine::ast::declaration::ImportName;
use concepts::FunctionRegistry;
use std::collections::HashMap;

/// Suffix appended to WIT package names for schedule imports.
const SCHEDULE_SUFFIX: &str = "-obelisk-schedule";
/// Suffix appended to WIT package names for extension imports (submit/awaitNext/get).
const EXT_SUFFIX: &str = "-obelisk-ext";
/// Suffix appended to WIT package names for stub imports.
const STUB_SUFFIX: &str = "-obelisk-stub";

/// Strip an obelisk suffix from a WIT specifier's package name.
///
/// Specifier format: `"ns:pkg-obelisk-schedule/ifc"` → `"ns:pkg/ifc"`.
/// Returns `Some(base_specifier)` if the suffix was found, `None` otherwise.
fn strip_specifier_suffix(specifier: &str, suffix: &str) -> Option<String> {
    let slash_pos = specifier.find('/')?;
    let pkg_part = &specifier[..slash_pos];
    let ifc_part = &specifier[slash_pos..];
    pkg_part
        .strip_suffix(suffix)
        .map(|base_pkg| format!("{base_pkg}{ifc_part}"))
}

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

/// Import kind extracted from JS source.
#[derive(Debug)]
enum JsImportKind {
    /// `import { a, b } from 'spec'` — list of (`js_name`, `wit_name`) pairs.
    Named(Vec<(String, String)>),
    /// `import * as ns from 'spec'`.
    Namespace,
}

/// Convert a WIT kebab-case name to JS camelCase.
///
/// Examples: `"account-info"` → `"accountInfo"`, `"add"` → `"add"`.
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

/// Extract import specifiers and their imported names from JS source code.
///
/// Returns a map from specifier to `JsImportKind`.
/// Imports from the `obelisk:` namespace are skipped (those are host-provided APIs).
fn extract_js_imports(js_code: &str) -> Result<HashMap<String, JsImportKind>, String> {
    let mut interner = boa_engine::interner::Interner::new();
    let mut parser = boa_engine::parser::Parser::new(boa_engine::Source::from_bytes(js_code));
    let scope = boa_engine::ast::scope::Scope::new_global();
    let module = parser
        .parse_module(&scope, &mut interner)
        .map_err(|e| format!("import extraction parse error: {e}"))?;

    let mut imports: HashMap<String, JsImportKind> = HashMap::new();

    for entry in module.items().import_entries() {
        let specifier = interner
            .resolve_expect(entry.module_request())
            .utf8()
            .unwrap_or("")
            .to_string();

        // Skip obelisk: namespace (host-provided)
        if specifier.starts_with("obelisk:") {
            continue;
        }

        match entry.import_name() {
            ImportName::Name(sym) => {
                let js_name = interner
                    .resolve_expect(sym)
                    .utf8()
                    .unwrap_or("")
                    .to_string();
                let wit_name = camel_to_kebab(&js_name);
                match imports.entry(specifier) {
                    std::collections::hash_map::Entry::Occupied(mut e) => {
                        if let JsImportKind::Named(funcs) = e.get_mut() {
                            funcs.push((js_name, wit_name));
                        }
                    }
                    std::collections::hash_map::Entry::Vacant(e) => {
                        e.insert(JsImportKind::Named(vec![(js_name, wit_name)]));
                    }
                }
            }
            ImportName::Namespace => {
                imports.insert(specifier, JsImportKind::Namespace);
            }
        }
    }
    Ok(imports)
}

/// Resolve JS imports against the function registry.
///
/// Parses JS source to extract imports, then validates each import against the
/// registry. For namespace imports (`import *`), resolves all functions for the
/// interface. Handles `-obelisk-schedule` suffix by stripping it for lookup and
/// adjusting function names.
///
/// Returns the fully resolved imports map: specifier → [(`js_name`, `wit_name`)].
pub fn resolve_js_imports(
    js_code: &str,
    fn_registry: &dyn FunctionRegistry,
) -> Result<HashMap<String, Vec<(String, String)>>, String> {
    let raw_imports = extract_js_imports(js_code)?;
    if raw_imports.is_empty() {
        return Ok(HashMap::new());
    }

    let all_exports = fn_registry.all_exports();
    let mut resolved: HashMap<String, Vec<(String, String)>> = HashMap::new();

    for (specifier, kind) in raw_imports {
        // Check for obelisk suffixes that change the proxy type.
        // For suffixed specifiers, look up the base interface for validation.
        let schedule_base = strip_specifier_suffix(&specifier, SCHEDULE_SUFFIX);
        let ext_base = strip_specifier_suffix(&specifier, EXT_SUFFIX);
        let stub_base = strip_specifier_suffix(&specifier, STUB_SUFFIX);
        let is_schedule = schedule_base.is_some();
        let is_ext = ext_base.is_some();
        let is_stub = stub_base.is_some();
        let lookup_specifier = schedule_base
            .as_deref()
            .or(ext_base.as_deref())
            .or(stub_base.as_deref())
            .unwrap_or(&specifier);

        // Find the interface in the registry using the base specifier
        let ifc = all_exports
            .iter()
            .find(|pkg| &*pkg.ifc_fqn == lookup_specifier);

        // Interface must exist at link time for all import kinds.
        let ifc =
            ifc.ok_or_else(|| format!("interface '{lookup_specifier}' not found for import"))?;

        match kind {
            JsImportKind::Named(funcs) => {
                if is_schedule {
                    // For schedule imports, strip the `-schedule` suffix from
                    // wit_name before validating against the base interface.
                    for (js_name, wit_name) in &funcs {
                        let base_wit = wit_name.strip_suffix("-schedule").unwrap_or(wit_name);
                        if !ifc.fns.keys().any(|k| &**k == base_wit) {
                            return Err(format!(
                                "function '{js_name}' (base '{base_wit}') not found in interface '{lookup_specifier}'"
                            ));
                        }
                    }
                } else if is_ext {
                    // For ext imports, strip `-submit`, `-await-next`, or `-get`
                    // suffix from wit_name before validating against the base interface.
                    for (js_name, wit_name) in &funcs {
                        let base_wit = wit_name
                            .strip_suffix("-submit")
                            .or_else(|| wit_name.strip_suffix("-await-next"))
                            .or_else(|| wit_name.strip_suffix("-get"))
                            .ok_or_else(|| {
                                format!(
                                    "ext import '{js_name}' ('{wit_name}') must end with \
                                     Submit, AwaitNext, or Get"
                                )
                            })?;
                        if !ifc.fns.keys().any(|k| &**k == base_wit) {
                            return Err(format!(
                                "function '{js_name}' (base '{base_wit}') not found in interface '{lookup_specifier}'"
                            ));
                        }
                    }
                } else if is_stub {
                    // For stub imports, strip the `-stub` suffix from
                    // wit_name before validating against the base interface.
                    for (js_name, wit_name) in &funcs {
                        let base_wit = wit_name.strip_suffix("-stub").unwrap_or(wit_name);
                        if !ifc.fns.keys().any(|k| &**k == base_wit) {
                            return Err(format!(
                                "function '{js_name}' (base '{base_wit}') not found in interface '{lookup_specifier}'"
                            ));
                        }
                    }
                } else {
                    // For direct call imports, validate each function exists
                    for (js_name, wit_name) in &funcs {
                        if !ifc.fns.keys().any(|k| &**k == wit_name) {
                            return Err(format!(
                                "function '{js_name}' ('{wit_name}') not found in interface '{specifier}'"
                            ));
                        }
                    }
                }
                resolved.insert(specifier, funcs);
            }
            JsImportKind::Namespace => {
                let mut funcs: Vec<(String, String)> = Vec::new();
                for fn_name in ifc.fns.keys() {
                    let wit_name = fn_name.to_string();
                    let js_name = kebab_to_camel(&wit_name);
                    if is_schedule {
                        // Add Schedule suffix: "fibo" → "fiboSchedule" / "fibo-schedule"
                        funcs.push((format!("{js_name}Schedule"), format!("{wit_name}-schedule")));
                    } else if is_ext {
                        // Generate three functions per base function
                        funcs.push((format!("{js_name}Submit"), format!("{wit_name}-submit")));
                        funcs.push((
                            format!("{js_name}AwaitNext"),
                            format!("{wit_name}-await-next"),
                        ));
                        funcs.push((format!("{js_name}Get"), format!("{wit_name}-get")));
                    } else if is_stub {
                        // Add Stub suffix: "myFunc" → "myFuncStub" / "my-func-stub"
                        funcs.push((format!("{js_name}Stub"), format!("{wit_name}-stub")));
                    } else {
                        funcs.push((js_name, wit_name));
                    }
                }
                resolved.insert(specifier, funcs);
            }
        }
    }
    Ok(resolved)
}
