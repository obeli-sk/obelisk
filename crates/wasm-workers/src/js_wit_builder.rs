//! WIT synthesis for JS workers (activity and workflow).
//!
//! Builds a `wit_parser::Resolve` from a JS function's signature metadata, then
//! uses `WitPrinter` to generate valid WIT text. Delegates generic
//! `TypeWrapper`-to-`TypeDef` allocation to [`utils::wit_builder::allocate_type`].

use concepts::{FunctionFqn, ParameterType, ReturnTypeExtendable};
use hashbrown::HashMap;
use indexmap::IndexMap;
use semver::Version;
use utils::wit_builder::allocate_type;
use val_json::type_wrapper::TypeWrapper;
use wit_component::WitPrinter;
use wit_parser::{
    Function, FunctionKind, Interface, Package, PackageName, Param, Resolve, World, WorldItem,
    WorldKey,
};

/// Build a complete `Resolve` from a JS function's signature.
///
/// Returns `(resolve, main_package_id)` ready for `WitPrinter::print`.
pub fn build_resolve(
    ffqn: &FunctionFqn,
    params: &[ParameterType],
    return_type: &ReturnTypeExtendable,
    world_name: &str,
) -> (Resolve, wit_parser::PackageId) {
    let ifc_fqn = &ffqn.ifc_fqn;
    let namespace = ifc_fqn.namespace();
    let ifc_name = ifc_fqn.ifc_name();
    let fn_name = &ffqn.function_name;

    let version = ifc_fqn.version().and_then(|v| Version::parse(v).ok());
    let pkg = Package {
        name: PackageName {
            namespace: namespace.to_string(),
            name: ifc_fqn.package_name().to_string(),
            version,
        },
        docs: wit_parser::Docs::default(),
        interfaces: IndexMap::default(),
        worlds: IndexMap::default(),
    };

    let mut resolve = Resolve::new();
    let pkg_id = resolve.packages.alloc(pkg);
    resolve
        .package_names
        .insert(resolve.packages[pkg_id].name.clone(), pkg_id);

    // Create the interface.
    let iface = Interface {
        name: Some(ifc_name.to_string()),
        types: IndexMap::new(),
        functions: IndexMap::new(),
        docs: wit_parser::Docs::default(),
        stability: wit_parser::Stability::default(),
        package: Some(pkg_id),
        span: wit_parser::Span::default(),
        clone_of: None,
    };
    let ifc_id = resolve.interfaces.alloc(iface);
    resolve
        .packages
        .get_mut(pkg_id)
        .unwrap()
        .interfaces
        .insert(ifc_name.to_string(), ifc_id);

    // Convert all params + return type, which allocates TypeDefs (named + anonymous).
    let mut dedup = HashMap::new();
    let wit_params: Vec<Param> = params
        .iter()
        .map(|p| {
            let ty = allocate_type(&mut resolve, ifc_id, &p.type_wrapper, &mut dedup);
            Param {
                name: String::from(p.name.as_ref()),
                ty,
                span: wit_parser::Span::default(),
            }
        })
        .collect();

    let return_tw = TypeWrapper::from(return_type.type_wrapper_tl.clone());
    let result_type = allocate_type(&mut resolve, ifc_id, &return_tw, &mut dedup);

    // Collect named types into interface's types map.
    let mut types = IndexMap::new();
    for (type_id, type_def) in resolve.types.iter() {
        if type_def.owner == wit_parser::TypeOwner::Interface(ifc_id) {
            if let Some(name) = &type_def.name {
                types.insert(name.clone(), type_id);
            }
        }
    }
    resolve.interfaces.get_mut(ifc_id).unwrap().types = types;

    // Add the function.
    let wit_fn = Function {
        name: fn_name.to_string(),
        kind: FunctionKind::Freestanding,
        params: wit_params,
        result: Some(result_type),
        docs: wit_parser::Docs::default(),
        stability: wit_parser::Stability::default(),
        span: wit_parser::Span::default(),
    };
    resolve
        .interfaces
        .get_mut(ifc_id)
        .unwrap()
        .functions
        .insert(fn_name.to_string(), wit_fn);

    // Create the world exporting the interface.
    let world = World {
        name: world_name.to_string(),
        docs: wit_parser::Docs::default(),
        imports: IndexMap::default(),
        exports: IndexMap::from_iter([(
            WorldKey::Interface(ifc_id),
            WorldItem::Interface {
                id: ifc_id,
                stability: wit_parser::Stability::Unknown,
                span: wit_parser::Span::default(),
            },
        )]),
        package: Some(pkg_id),
        span: wit_parser::Span::default(),
        includes: vec![],
        stability: wit_parser::Stability::Unknown,
    };
    let world_id = resolve.worlds.alloc(world);
    resolve
        .packages
        .get_mut(pkg_id)
        .unwrap()
        .worlds
        .insert(world_name.to_string(), world_id);

    (resolve, pkg_id)
}

/// Synthesize a WIT string for a JS worker's user-facing interface.
///
/// Named types (`record`, `variant`, `enum`, `flags`) that appear in params or the
/// return type are declared at interface level with generated names `t0`, `t1`, etc.
///
/// `world_name` is the WIT world exported by the component (e.g. `"js-activity"` or
/// `"js-workflow"`).
///
/// The generated WIT is parsed by `WasmComponent::new_from_wit_string` which runs
/// `ExIm::decode` and `rebuild_resolve`, producing the same extension metadata
/// and printable WIT that standard WASM components get.
pub fn synthesize_wit(
    ffqn: &FunctionFqn,
    params: &[ParameterType],
    return_type: &ReturnTypeExtendable,
    world_name: &str,
) -> String {
    let (resolve, pkg_id) = build_resolve(ffqn, params, return_type, world_name);
    let mut printer = WitPrinter::default();
    printer
        .print(&resolve, pkg_id, &[])
        .expect("WIT printing must succeed");
    printer.output.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use concepts::{ReturnTypeExtendable, StrVariant, TypeWrapperTopLevel};
    use val_json::type_wrapper::indexmap::IndexMap as TWIndexMap;
    use val_json::type_wrapper::indexmap::IndexSet;
    use val_json::type_wrapper::{TypeKey, TypeWrapper};

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
        let ffqn = FunctionFqn::new_static("test:pkg/ifc", "make-point");
        let params = vec![ParameterType {
            type_wrapper: TypeWrapper::String,
            name: StrVariant::Static("label"),
            wit_type: StrVariant::Static("string"),
        }];
        let fields: TWIndexMap<TypeKey, TypeWrapper> = [
            (TypeKey::new_kebab("x"), TypeWrapper::U32),
            (TypeKey::new_kebab("y"), TypeWrapper::U32),
        ]
        .into_iter()
        .collect();
        let return_type = make_return_type(TypeWrapper::Record(fields));

        let wit = synthesize_wit(&ffqn, &params, &return_type, "js-activity");
        insta::assert_snapshot!(wit);
    }

    #[test]
    fn nested_record_declares_inner_first() {
        let inner_fields: TWIndexMap<TypeKey, TypeWrapper> =
            [(TypeKey::new_kebab("z"), TypeWrapper::F32)]
                .into_iter()
                .collect();
        let outer_fields: TWIndexMap<TypeKey, TypeWrapper> = [
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
        insta::assert_snapshot!(wit);
    }

    #[test]
    fn duplicate_type_uses_same_name() {
        let fields: TWIndexMap<TypeKey, TypeWrapper> =
            [(TypeKey::new_kebab("v"), TypeWrapper::U32)]
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
        insta::assert_snapshot!(wit);
    }

    #[test]
    fn enum_and_flags_generate_named_types() {
        use concepts::ParameterType;
        let cases: IndexSet<TypeKey> = ["a", "b", "c"]
            .iter()
            .map(|s| TypeKey::new_kebab(*s))
            .collect();
        let flags: IndexSet<TypeKey> = ["read", "write"]
            .iter()
            .map(|s| TypeKey::new_kebab(*s))
            .collect();
        let ffqn = FunctionFqn::new_static("test:pkg/ifc", "multi");
        let params = vec![ParameterType {
            type_wrapper: TypeWrapper::Flags(flags),
            name: StrVariant::Static("perms"),
            wit_type: StrVariant::Static("(unused)"),
        }];
        let return_type = make_return_type(TypeWrapper::Enum(cases));

        let wit = synthesize_wit(&ffqn, &params, &return_type, "js-activity");
        insta::assert_snapshot!(wit);
    }

    #[test]
    fn variant_with_optional_none() {
        let cases: TWIndexMap<TypeKey, Option<TypeWrapper>> = [
            (TypeKey::new_kebab("just"), Some(TypeWrapper::String)),
            (TypeKey::new_kebab("none"), None),
        ]
        .into_iter()
        .collect();
        let ffqn = FunctionFqn::new_static("test:pkg/ifc", "opt-variant");
        let return_type = make_return_type(TypeWrapper::Variant(cases));

        let wit = synthesize_wit(&ffqn, &[], &return_type, "js-activity");
        insta::assert_snapshot!(wit);
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
