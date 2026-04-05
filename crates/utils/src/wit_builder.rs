//! Generic WIT builder: allocates [`val_json::type_wrapper::TypeWrapper`] values
//! into a [`wit_parser::Resolve`].
//!
//! Named types (`record`, `variant`, `enum`, `flags`) that cannot appear inline
//! in WIT function signatures are allocated as `TypeDef` entries at the
//! interface level with generated names (`t0`, `t1`, …).

use hashbrown::HashMap;
use val_json::type_wrapper::TypeWrapper;
use wit_parser::{
    Enum, EnumCase, Field, Flag, Flags, Resolve, Result_, Type, TypeDef, TypeDefKind, TypeOwner,
};

/// Allocates a `Type` into the resolve, creating named `TypeDef` entries for
/// types that cannot be used inline (record, variant, enum, flags).
///
/// The `dedup` map keys on `TypeWrapper`'s Display string → allocated `TypeId`.
pub fn allocate_type(
    resolve: &mut Resolve,
    interface_id: wit_parser::InterfaceId,
    tw: &TypeWrapper,
    dedup: &mut HashMap<String /* typewrapper.to_string() */, wit_parser::TypeId>,
) -> Type {
    match tw {
        TypeWrapper::Record(fields) => {
            let key = format!("{tw}");
            if let Some(&id) = dedup.get(&key) {
                return Type::Id(id);
            }
            let wit_fields: Vec<Field> = fields
                .iter()
                .map(|(k, v)| {
                    let ty = allocate_type(resolve, interface_id, v, dedup);
                    Field {
                        name: k.as_kebab_str().to_string(),
                        ty,
                        docs: wit_parser::Docs::default(),
                        span: wit_parser::Span::default(),
                    }
                })
                .collect();
            let name = format!("t{}", dedup.len());
            let type_def = resolve.types.alloc(TypeDef {
                name: Some(name.clone()),
                kind: TypeDefKind::Record(wit_parser::Record { fields: wit_fields }),
                owner: TypeOwner::Interface(interface_id),
                docs: wit_parser::Docs::default(),
                stability: wit_parser::Stability::default(),
                span: wit_parser::Span::default(),
            });
            dedup.insert(key, type_def);
            Type::Id(type_def)
        }
        TypeWrapper::Variant(cases) => {
            let key = format!("{tw}");
            if let Some(&id) = dedup.get(&key) {
                return Type::Id(id);
            }
            let wit_cases: Vec<wit_parser::Case> = cases
                .iter()
                .map(|(k, payload)| {
                    let ty = payload
                        .as_ref()
                        .map(|p| allocate_type(resolve, interface_id, p, dedup));
                    wit_parser::Case {
                        name: k.as_kebab_str().to_string(),
                        ty,
                        docs: wit_parser::Docs::default(),
                        span: wit_parser::Span::default(),
                    }
                })
                .collect();
            let name = format!("t{}", dedup.len());
            let type_def = resolve.types.alloc(TypeDef {
                name: Some(name.clone()),
                kind: TypeDefKind::Variant(wit_parser::Variant { cases: wit_cases }),
                owner: TypeOwner::Interface(interface_id),
                docs: wit_parser::Docs::default(),
                stability: wit_parser::Stability::default(),
                span: wit_parser::Span::default(),
            });
            dedup.insert(key, type_def);
            Type::Id(type_def)
        }
        TypeWrapper::Enum(cases) => {
            let key = format!("{tw}");
            if let Some(&id) = dedup.get(&key) {
                return Type::Id(id);
            }
            let wit_cases: Vec<EnumCase> = cases
                .iter()
                .map(|k| EnumCase {
                    name: k.as_kebab_str().to_string(),
                    docs: wit_parser::Docs::default(),
                    span: wit_parser::Span::default(),
                })
                .collect();
            let name = format!("t{}", dedup.len());
            let type_def = resolve.types.alloc(TypeDef {
                name: Some(name.clone()),
                kind: TypeDefKind::Enum(Enum { cases: wit_cases }),
                owner: TypeOwner::Interface(interface_id),
                docs: wit_parser::Docs::default(),
                stability: wit_parser::Stability::default(),
                span: wit_parser::Span::default(),
            });
            dedup.insert(key, type_def);
            Type::Id(type_def)
        }
        TypeWrapper::Flags(flags) => {
            let key = format!("{tw}");
            if let Some(&id) = dedup.get(&key) {
                return Type::Id(id);
            }
            let wit_flags = flags
                .iter()
                .map(|k| Flag {
                    name: k.as_kebab_str().to_string(),
                    docs: wit_parser::Docs::default(),
                    span: wit_parser::Span::default(),
                })
                .collect();
            let name = format!("t{}", dedup.len());
            let type_def = resolve.types.alloc(TypeDef {
                name: Some(name.clone()),
                kind: TypeDefKind::Flags(Flags { flags: wit_flags }),
                owner: TypeOwner::Interface(interface_id),
                docs: wit_parser::Docs::default(),
                stability: wit_parser::Stability::default(),
                span: wit_parser::Span::default(),
            });
            dedup.insert(key, type_def);
            Type::Id(type_def)
        }
        // Container types — anonymous (no name, TypeOwner::None).
        TypeWrapper::List(inner) => {
            let inner_ty = allocate_type(resolve, interface_id, inner, dedup);
            Type::Id(resolve.types.alloc(TypeDef {
                name: None,
                kind: TypeDefKind::List(inner_ty),
                owner: TypeOwner::None,
                docs: wit_parser::Docs::default(),
                stability: wit_parser::Stability::default(),
                span: wit_parser::Span::default(),
            }))
        }
        TypeWrapper::Option(inner) => {
            let inner_ty = allocate_type(resolve, interface_id, inner, dedup);
            Type::Id(resolve.types.alloc(TypeDef {
                name: None,
                kind: TypeDefKind::Option(inner_ty),
                owner: TypeOwner::None,
                docs: wit_parser::Docs::default(),
                stability: wit_parser::Stability::default(),
                span: wit_parser::Span::default(),
            }))
        }
        TypeWrapper::Tuple(items) => {
            let types = items
                .iter()
                .map(|t| allocate_type(resolve, interface_id, t, dedup))
                .collect();
            Type::Id(resolve.types.alloc(TypeDef {
                name: None,
                kind: TypeDefKind::Tuple(wit_parser::Tuple { types }),
                owner: TypeOwner::None,
                docs: wit_parser::Docs::default(),
                stability: wit_parser::Stability::default(),
                span: wit_parser::Span::default(),
            }))
        }
        TypeWrapper::Result { ok, err } => {
            let ok = ok
                .as_ref()
                .map(|t| allocate_type(resolve, interface_id, t, dedup));
            let err = err
                .as_ref()
                .map(|t| allocate_type(resolve, interface_id, t, dedup));
            Type::Id(resolve.types.alloc(TypeDef {
                name: None,
                kind: TypeDefKind::Result(Result_ { ok, err }),
                owner: TypeOwner::None,
                docs: wit_parser::Docs::default(),
                stability: wit_parser::Stability::default(),
                span: wit_parser::Span::default(),
            }))
        }
        // Primitives.
        TypeWrapper::Bool => Type::Bool,
        TypeWrapper::S8 => Type::S8,
        TypeWrapper::U8 => Type::U8,
        TypeWrapper::S16 => Type::S16,
        TypeWrapper::U16 => Type::U16,
        TypeWrapper::S32 => Type::S32,
        TypeWrapper::U32 => Type::U32,
        TypeWrapper::S64 => Type::S64,
        TypeWrapper::U64 => Type::U64,
        TypeWrapper::F32 => Type::F32,
        TypeWrapper::F64 => Type::F64,
        TypeWrapper::Char => Type::Char,
        TypeWrapper::String => Type::String,
        TypeWrapper::Own | TypeWrapper::Borrow => {
            panic!("resource types (own/borrow) are not supported in WIT synthesis")
        }
    }
}
