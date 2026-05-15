//! ES module import support for Obelisk's native Boa JS runtimes.
//!
//! Provides shared logic for registering synthetic ES modules that proxy
//! WIT host function calls. Each runtime (workflow, webhook) supplies its
//! own proxy factory — this module handles suffix detection, name stripping,
//! and `SyntheticModule` / `MapModuleLoader` wiring.

use boa_engine::module::{MapModuleLoader, SyntheticModuleInitializer};
use boa_engine::{Context, JsString, JsValue, Module, js_string, object::JsObject};
use std::collections::HashMap;

/// Suffix appended to WIT package names for schedule imports.
pub const SCHEDULE_SUFFIX: &str = "-obelisk-schedule";
/// Suffix appended to WIT package names for extension imports (submit/awaitNext/get).
pub const EXT_SUFFIX: &str = "-obelisk-ext";
/// Suffix appended to WIT package names for stub imports.
pub const STUB_SUFFIX: &str = "-obelisk-stub";

/// Strip an obelisk suffix from a WIT specifier's package name.
///
/// Specifier format: `"ns:pkg-obelisk-schedule/ifc"` → `"ns:pkg/ifc"`.
/// Returns `Some(base_specifier)` if the suffix was found, `None` otherwise.
pub fn strip_specifier_suffix(specifier: &str, suffix: &str) -> Option<String> {
    let slash_pos = specifier.find('/')?;
    let pkg_part = &specifier[..slash_pos];
    let ifc_part = &specifier[slash_pos..];
    pkg_part
        .strip_suffix(suffix)
        .map(|base_pkg| format!("{base_pkg}{ifc_part}"))
}

/// The kind of proxy to create for an imported function.
pub enum ProxyKind<'a> {
    /// Direct call: `import { add } from 'ns:pkg/ifc'`
    DirectCall {
        interface_name: &'a str,
        function_name: &'a str,
    },
    /// Schedule: `import { addSchedule } from 'ns:pkg-obelisk-schedule/ifc'`
    Schedule {
        interface_name: &'a str,
        function_name: &'a str,
    },
    /// Extension submit: `import { addSubmit } from 'ns:pkg-obelisk-ext/ifc'`
    ExtSubmit {
        interface_name: &'a str,
        function_name: &'a str,
    },
    /// Extension awaitNext: `import { addAwaitNext } from 'ns:pkg-obelisk-ext/ifc'`
    ExtAwaitNext,
    /// Extension get: `import { addGet } from 'ns:pkg-obelisk-ext/ifc'`
    ExtGet,
    /// Stub: `import { fooStub } from 'ns:pkg-obelisk-stub/ifc'`
    Stub {
        interface_name: &'a str,
        function_name: &'a str,
    },
}

/// Build a [`MapModuleLoader`] with [`SyntheticModule`]s for each imported specifier.
///
/// Each imported JS name becomes a proxy `JsValue` (typically a `NativeFunction`)
/// created by `create_proxy`. The specifier suffix determines the [`ProxyKind`]:
///
/// - No suffix → [`ProxyKind::DirectCall`]
/// - `-obelisk-schedule` → [`ProxyKind::Schedule`] (base specifier + base function name)
/// - `-obelisk-ext` → [`ProxyKind::ExtSubmit`] / [`ProxyKind::ExtAwaitNext`] / [`ProxyKind::ExtGet`]
/// - `-obelisk-stub` → [`ProxyKind::Stub`] (base specifier + base function name)
pub fn register_import_modules(
    imports: &HashMap<String, Vec<(String, String)>>,
    loader: &MapModuleLoader,
    context: &mut Context,
    create_proxy: &dyn Fn(ProxyKind, &mut Context) -> JsValue,
) {
    for (specifier, funcs) in imports {
        let mut export_names: Vec<JsString> = Vec::new();

        // Detect obelisk suffix to determine the proxy kind.
        let schedule_base = strip_specifier_suffix(specifier, SCHEDULE_SUFFIX);
        let ext_base = strip_specifier_suffix(specifier, EXT_SUFFIX);
        let stub_base = strip_specifier_suffix(specifier, STUB_SUFFIX);

        // Create all proxy functions and store them in a plain JsObject for the
        // SyntheticModuleInitializer to retrieve during evaluation.
        let exports_obj = JsObject::with_null_proto();
        for (js_name, wit_name) in funcs {
            let proxy = if let Some(base_specifier) = &schedule_base {
                // Schedule proxy: strip `-schedule` from wit_name to get base function name
                let base_fn = wit_name.strip_suffix("-schedule").unwrap_or(wit_name);
                create_proxy(
                    ProxyKind::Schedule {
                        interface_name: base_specifier,
                        function_name: base_fn,
                    },
                    context,
                )
            } else if let Some(base_specifier) = &ext_base {
                // Extension proxy: determine type from wit_name suffix
                if let Some(base_fn) = wit_name.strip_suffix("-submit") {
                    create_proxy(
                        ProxyKind::ExtSubmit {
                            interface_name: base_specifier,
                            function_name: base_fn,
                        },
                        context,
                    )
                } else if wit_name.ends_with("-await-next") {
                    create_proxy(ProxyKind::ExtAwaitNext, context)
                } else if wit_name.ends_with("-get") {
                    create_proxy(ProxyKind::ExtGet, context)
                } else {
                    // Unknown ext suffix — fall back to direct call on base specifier
                    create_proxy(
                        ProxyKind::DirectCall {
                            interface_name: base_specifier,
                            function_name: wit_name,
                        },
                        context,
                    )
                }
            } else if let Some(base_specifier) = &stub_base {
                // Stub proxy: strip `-stub` from wit_name to get base function name
                let base_fn = wit_name.strip_suffix("-stub").unwrap_or(wit_name);
                create_proxy(
                    ProxyKind::Stub {
                        interface_name: base_specifier,
                        function_name: base_fn,
                    },
                    context,
                )
            } else {
                create_proxy(
                    ProxyKind::DirectCall {
                        interface_name: specifier,
                        function_name: wit_name,
                    },
                    context,
                )
            };
            let js_name_str = js_string!(js_name.as_str());
            exports_obj
                .set(js_name_str.clone(), proxy, false, context)
                .expect("set on plain object must work");
            export_names.push(js_name_str);
        }

        let synth = Module::synthetic(
            &export_names,
            SyntheticModuleInitializer::from_copy_closure_with_captures(
                |module, (obj, names): &(JsObject, Vec<JsString>), ctx| {
                    for name in names {
                        let val = obj.get(name.clone(), ctx)?;
                        module.set_export(name, val)?;
                    }
                    Ok(())
                },
                (exports_obj, export_names.clone()),
            ),
            None,
            None,
            context,
        );

        loader.insert(specifier, synth);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strip_schedule_suffix() {
        assert_eq!(
            strip_specifier_suffix("testing:fibo-obelisk-schedule/fibo", SCHEDULE_SUFFIX),
            Some("testing:fibo/fibo".to_string())
        );
    }

    #[test]
    fn strip_ext_suffix() {
        assert_eq!(
            strip_specifier_suffix("testing:integration-obelisk-ext/activity", EXT_SUFFIX),
            Some("testing:integration/activity".to_string())
        );
    }

    #[test]
    fn strip_stub_suffix() {
        assert_eq!(
            strip_specifier_suffix(
                "testing:stub-activity-obelisk-stub/activity",
                STUB_SUFFIX
            ),
            Some("testing:stub-activity/activity".to_string())
        );
    }

    #[test]
    fn strip_suffix_not_present() {
        assert_eq!(
            strip_specifier_suffix("testing:integration/activity", SCHEDULE_SUFFIX),
            None
        );
    }

    #[test]
    fn strip_suffix_no_slash() {
        assert_eq!(strip_specifier_suffix("no-slash", SCHEDULE_SUFFIX), None);
    }
}
