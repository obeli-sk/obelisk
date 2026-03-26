//! WIT synthesis for JS workers (activity and workflow).
//!
//! Named types (`record`, `variant`, `enum`, `flags`) cannot appear inline in WIT
//! function signatures; they must be declared at interface level with a name.
//! [`TypeDeclCollector`] traverses a [`TypeWrapper`] tree depth-first, assigns
//! sequential generated names (`t0`, `t1`, …), and accumulates the declarations.
//! [`synthesize_wit`] uses it to produce a complete, valid WIT string.

use std::fmt::Write;

use concepts::{FunctionFqn, ParameterType, ReturnTypeExtendable};
use val_json::type_wrapper::TypeWrapper;

/// Collects named type declarations (record, variant, enum, flags) that cannot be
/// used inline in WIT function signatures and must be declared at interface level.
/// Assigns sequential generated names `t0`, `t1`, etc. in depth-first post-order.
pub(crate) struct TypeDeclCollector {
    counter: usize,
    /// Maps `TypeWrapper` Display string → generated name (for deduplication).
    seen: std::collections::HashMap<String, String>,
    /// WIT declarations ready to be inserted into the interface body, in order.
    declarations: Vec<String>,
}

impl TypeDeclCollector {
    pub(crate) fn new() -> Self {
        Self {
            counter: 0,
            seen: std::collections::HashMap::new(),
            declarations: Vec::new(),
        }
    }

    /// Returns the WIT reference string for `ty`.
    ///
    /// Primitive and container types (`list`, `option`, `result`, `tuple`) are returned
    /// as inline strings. Named types (`record`, `variant`, `enum`, `flags`) are declared
    /// at interface level and referenced by their generated name.
    pub(crate) fn wit_ref(&mut self, ty: &TypeWrapper) -> String {
        match ty {
            TypeWrapper::Record(fields) => {
                let key = format!("{ty}");
                if let Some(name) = self.seen.get(&key) {
                    return name.clone();
                }
                // Process fields depth-first so inner named types are declared first.
                let n = fields.len();
                let fields_str =
                    fields
                        .iter()
                        .enumerate()
                        .fold(String::new(), |mut s, (i, (k, v))| {
                            let comma = if i + 1 < n { "," } else { "" };
                            writeln!(
                                s,
                                "        {}: {}{}",
                                k.as_kebab_str(),
                                self.wit_ref(v),
                                comma
                            )
                            .unwrap();
                            s
                        });
                let name = format!("t{}", self.counter);
                self.counter += 1;
                self.seen.insert(key, name.clone());
                self.declarations
                    .push(format!("    record {name} {{\n{fields_str}    }}"));
                name
            }
            TypeWrapper::Variant(cases) => {
                let key = format!("{ty}");
                if let Some(name) = self.seen.get(&key) {
                    return name.clone();
                }
                let n = cases.len();
                let cases_str: String = cases
                    .iter()
                    .enumerate()
                    .map(|(i, (k, payload))| {
                        let comma = if i + 1 < n { "," } else { "" };
                        match payload {
                            Some(p) => format!(
                                "        {}({}){}\n",
                                k.as_kebab_str(),
                                self.wit_ref(p),
                                comma
                            ),
                            None => format!("        {}{}\n", k.as_kebab_str(), comma),
                        }
                    })
                    .collect();
                let name = format!("t{}", self.counter);
                self.counter += 1;
                self.seen.insert(key, name.clone());
                self.declarations
                    .push(format!("    variant {name} {{\n{cases_str}    }}"));
                name
            }
            TypeWrapper::Enum(cases) => {
                let key = format!("{ty}");
                if let Some(name) = self.seen.get(&key) {
                    return name.clone();
                }
                let n = cases.len();
                let cases_str = cases
                    .iter()
                    .enumerate()
                    .fold(String::new(), |mut s, (i, k)| {
                        let comma = if i + 1 < n { "," } else { "" };
                        writeln!(s, "        {}{}", k.as_kebab_str(), comma).unwrap();
                        s
                    });
                let name = format!("t{}", self.counter);
                self.counter += 1;
                self.seen.insert(key, name.clone());
                self.declarations
                    .push(format!("    enum {name} {{\n{cases_str}    }}"));
                name
            }
            TypeWrapper::Flags(flags) => {
                let key = format!("{ty}");
                if let Some(name) = self.seen.get(&key) {
                    return name.clone();
                }
                let n = flags.len();
                let flags_str = flags
                    .iter()
                    .enumerate()
                    .fold(String::new(), |mut s, (i, k)| {
                        let comma = if i + 1 < n { "," } else { "" };
                        writeln!(s, "        {}{}", k.as_kebab_str(), comma).unwrap();
                        s
                    });
                let name = format!("t{}", self.counter);
                self.counter += 1;
                self.seen.insert(key, name.clone());
                self.declarations
                    .push(format!("    flags {name} {{\n{flags_str}    }}"));
                name
            }
            // Container types: recurse but stay inline.
            TypeWrapper::List(inner) => format!("list<{}>", self.wit_ref(inner)),
            TypeWrapper::Option(inner) => format!("option<{}>", self.wit_ref(inner)),
            TypeWrapper::Tuple(items) => {
                let refs: Vec<String> = items.iter().map(|t| self.wit_ref(t)).collect();
                format!("tuple<{}>", refs.join(", "))
            }
            TypeWrapper::Result { ok, err } => match (ok, err) {
                (None, None) => "result".to_string(),
                (Some(ok), None) => format!("result<{}>", self.wit_ref(ok)),
                (None, Some(err)) => format!("result<_, {}>", self.wit_ref(err)),
                (Some(ok), Some(err)) => {
                    let ok_ref = self.wit_ref(ok);
                    let err_ref = self.wit_ref(err);
                    format!("result<{ok_ref}, {err_ref}>")
                }
            },
            // Primitive types.
            _ => format!("{ty}"),
        }
    }
}

/// Synthesize a WIT string for a JS worker's user-facing interface.
///
/// Named types (`record`, `variant`, `enum`, `flags`) that appear in params or the return
/// type cannot be used inline in WIT function signatures; they are extracted and declared
/// at interface level with generated names `t0`, `t1`, etc. in depth-first post-order.
///
/// `world_name` is the WIT world exported by the component (e.g. `"js-activity"` or
/// `"js-workflow"`).
///
/// The generated WIT is parsed by `WasmComponent::new_from_wit_string` which runs
/// `ExIm::decode` and `rebuild_resolve`, producing the same extension metadata
/// and printable WIT that standard WASM components get.
pub(crate) fn synthesize_wit(
    ffqn: &FunctionFqn,
    params: &[ParameterType],
    return_type: &ReturnTypeExtendable,
    world_name: &str,
) -> String {
    let ifc_fqn = &ffqn.ifc_fqn;
    let namespace = ifc_fqn.namespace();
    let package_name = ifc_fqn.package_name();
    let ifc_name = ifc_fqn.ifc_name();
    let fn_name = &ffqn.function_name;
    let version_suffix = ifc_fqn.version().map_or(String::new(), |v| format!("@{v}"));

    let mut collector = TypeDeclCollector::new();

    let wit_params: Vec<String> = params
        .iter()
        .map(|p| format!("{}: {}", p.name, collector.wit_ref(&p.type_wrapper)))
        .collect();

    // Convert TypeWrapperTopLevel → TypeWrapper::Result for uniform traversal.
    let return_tw = TypeWrapper::from(return_type.type_wrapper_tl.clone());
    let return_wit_type = collector.wit_ref(&return_tw);

    let type_decls: String = collector
        .declarations
        .iter()
        .fold(String::new(), |mut s, d| {
            write!(s, "{d}\n\n").unwrap();
            s
        });

    format!(
        "package {namespace}:{package_name}{version_suffix};\n\
         \n\
         interface {ifc_name} {{\n\
         {type_decls}\
         \x20   {fn_name}: func({params}) -> {return_wit_type};\n\
         }}\n\
         \n\
         world {world_name} {{\n\
         \x20   export {ifc_name};\n\
         }}\n",
        params = wit_params.join(", "),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use concepts::{ReturnTypeExtendable, StrVariant, TypeWrapperTopLevel};
    use utils::wasm_tools::WasmComponent;
    use val_json::type_wrapper::{TypeKey, TypeWrapper, indexmap::IndexMap};

    fn make_return_type(ok: TypeWrapper) -> ReturnTypeExtendable {
        ReturnTypeExtendable {
            type_wrapper_tl: TypeWrapperTopLevel {
                ok: Some(Box::new(ok)),
                err: Some(Box::new(TypeWrapper::String)),
            },
            wit_type: StrVariant::Static("(test return type)"),
        }
    }

    #[test]
    fn record_generates_named_type_before_function() {
        use concepts::ParameterType;
        let ffqn = FunctionFqn::new_static("test:pkg/ifc", "make-point");
        let params = vec![ParameterType {
            type_wrapper: TypeWrapper::String,
            name: StrVariant::Static("label"),
            wit_type: StrVariant::Static("string"),
        }];
        let fields: IndexMap<TypeKey, TypeWrapper> = [
            (TypeKey::new_kebab("x"), TypeWrapper::U32),
            (TypeKey::new_kebab("y"), TypeWrapper::U32),
        ]
        .into_iter()
        .collect();
        let return_type = make_return_type(TypeWrapper::Record(fields));

        let wit = synthesize_wit(&ffqn, &params, &return_type, "js-activity");

        assert!(
            wit.contains("record t0 {"),
            "expected record declaration:\n{wit}"
        );
        assert!(wit.contains("x: u32"), "expected field x: u32:\n{wit}");
        assert!(wit.contains("y: u32"), "expected field y: u32:\n{wit}");
        assert!(
            wit.contains("func(label: string) -> result<t0, string>"),
            "expected named type reference in function:\n{wit}"
        );
        assert!(
            !wit.contains("record {"),
            "inline record must not appear:\n{wit}"
        );
        WasmComponent::new_from_wit_string(&wit, concepts::ComponentType::Activity)
            .expect("synthesized WIT must be valid");
    }

    #[test]
    fn nested_record_declares_inner_first() {
        let inner_fields: IndexMap<TypeKey, TypeWrapper> =
            [(TypeKey::new_kebab("z"), TypeWrapper::F32)]
                .into_iter()
                .collect();
        let outer_fields: IndexMap<TypeKey, TypeWrapper> = [
            (
                TypeKey::new_kebab("inner"),
                TypeWrapper::Record(inner_fields),
            ),
            (TypeKey::new_kebab("tag"), TypeWrapper::String),
        ]
        .into_iter()
        .collect();
        let ffqn = FunctionFqn::new_static("test:pkg/ifc", "nested");
        let return_type = make_return_type(TypeWrapper::Record(outer_fields));

        let wit = synthesize_wit(&ffqn, &[], &return_type, "js-activity");

        let pos_t0 = wit.find("record t0").expect("t0 declaration");
        let pos_t1 = wit.find("record t1").expect("t1 declaration");
        assert!(
            pos_t0 < pos_t1,
            "inner record t0 must be declared before outer t1"
        );
        assert!(
            wit.contains("func() -> result<t1, string>"),
            "outer named:\n{wit}"
        );
        WasmComponent::new_from_wit_string(&wit, concepts::ComponentType::Activity)
            .expect("synthesized WIT must be valid");
    }

    #[test]
    fn duplicate_type_uses_same_name() {
        use concepts::ParameterType;
        let fields: IndexMap<TypeKey, TypeWrapper> = [(TypeKey::new_kebab("v"), TypeWrapper::U32)]
            .into_iter()
            .collect();
        let rec = TypeWrapper::Record(fields);
        let ffqn = FunctionFqn::new_static("test:pkg/ifc", "deduplicated");
        let params = vec![ParameterType {
            type_wrapper: rec.clone(),
            name: StrVariant::Static("input"),
            wit_type: StrVariant::Static("(unused)"),
        }];
        let return_type = make_return_type(rec);

        let wit = synthesize_wit(&ffqn, &params, &return_type, "js-activity");

        // Only one declaration: t0
        assert_eq!(
            wit.matches("record t0").count(),
            1,
            "single declaration:\n{wit}"
        );
        assert!(!wit.contains("t1"), "no second name:\n{wit}");
        assert!(wit.contains("input: t0"), "param uses t0:\n{wit}");
        assert!(wit.contains("result<t0, string>"), "return uses t0:\n{wit}");
        WasmComponent::new_from_wit_string(&wit, concepts::ComponentType::Activity)
            .expect("synthesized WIT must be valid");
    }

    #[test]
    fn enum_and_flags_generate_named_types() {
        use val_json::type_wrapper::indexmap::IndexSet;
        let cases: IndexSet<TypeKey> = ["a", "b", "c"]
            .iter()
            .map(|s| TypeKey::new_kebab(*s))
            .collect();
        let flags: IndexSet<TypeKey> = ["read", "write"]
            .iter()
            .map(|s| TypeKey::new_kebab(*s))
            .collect();
        // Use enum as ok type; flags in a list param
        use concepts::ParameterType;
        let ffqn = FunctionFqn::new_static("test:pkg/ifc", "multi");
        let params = vec![ParameterType {
            type_wrapper: TypeWrapper::Flags(flags),
            name: StrVariant::Static("perms"),
            wit_type: StrVariant::Static("(unused)"),
        }];
        let return_type = make_return_type(TypeWrapper::Enum(cases));

        let wit = synthesize_wit(&ffqn, &params, &return_type, "js-activity");

        assert!(wit.contains("flags t0 {"), "flags declaration:\n{wit}");
        assert!(wit.contains("enum t1 {"), "enum declaration:\n{wit}");
        assert!(wit.contains("perms: t0"), "param uses t0:\n{wit}");
        assert!(wit.contains("result<t1, string>"), "return uses t1:\n{wit}");
        WasmComponent::new_from_wit_string(&wit, concepts::ComponentType::Activity)
            .expect("synthesized WIT must be valid");
    }

    #[test]
    fn world_name_is_used() {
        let ffqn = FunctionFqn::new_static("test:pkg/ifc", "fn");
        let return_type = ReturnTypeExtendable {
            type_wrapper_tl: TypeWrapperTopLevel {
                ok: None,
                err: None,
            },
            wit_type: StrVariant::Static("result"),
        };
        let wit = synthesize_wit(&ffqn, &[], &return_type, "js-workflow");
        assert!(wit.contains("world js-workflow {"), "world name:\n{wit}");
    }
}
