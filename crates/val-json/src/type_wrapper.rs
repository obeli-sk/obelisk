pub use indexmap;
use indexmap::{IndexMap, IndexSet};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

// TODO: Consider replacing IndexMap with ordermap - https://github.com/indexmap-rs/indexmap/issues/153#issuecomment-2189804150
#[derive(Clone, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TypeWrapper {
    Bool,
    S8,
    U8,
    S16,
    U16,
    S32,
    U32,
    S64,
    U64,
    F32,
    F64,
    Char,
    String,
    Own,
    Borrow,
    Record(IndexMap<Box<str>, TypeWrapper>), // TODO: use ordermap, ordering of keys matter!
    Variant(IndexMap<Box<str>, Option<TypeWrapper>>), // TODO: use ordermap, ordering of keys matter!
    List(Box<TypeWrapper>),
    Tuple(Box<[TypeWrapper]>),
    Enum(IndexSet<Box<str>>),
    Option(Box<TypeWrapper>),
    Result {
        ok: Option<Box<TypeWrapper>>,
        err: Option<Box<TypeWrapper>>,
    },
    Flags(IndexSet<Box<str>>),
    Map(MapKeyType, Box<TypeWrapper>),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Hash)]
#[serde(rename_all = "snake_case")]
pub enum MapKeyType {
    Bool,
    S8,
    U8,
    S16,
    U16,
    S32,
    U32,
    S64,
    U64,
    Char,
    String,
}

impl PartialEq for TypeWrapper {
    #[expect(clippy::match_same_arms)]
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Bool, Self::Bool) => true,
            (Self::Bool, _) => false, // Avoids catch-all arm
            (Self::S8, Self::S8) => true,
            (Self::S8, _) => false,
            (Self::U8, Self::U8) => true,
            (Self::U8, _) => false,
            (Self::S16, Self::S16) => true,
            (Self::S16, _) => false,
            (Self::U16, Self::U16) => true,
            (Self::U16, _) => false,
            (Self::S32, Self::S32) => true,
            (Self::S32, _) => false,
            (Self::U32, Self::U32) => true,
            (Self::U32, _) => false,
            (Self::S64, Self::S64) => true,
            (Self::S64, _) => false,
            (Self::U64, Self::U64) => true,
            (Self::U64, _) => false,
            (Self::F32, Self::F32) => true,
            (Self::F32, _) => false,
            (Self::F64, Self::F64) => true,
            (Self::F64, _) => false,
            (Self::Char, Self::Char) => true,
            (Self::Char, _) => false,
            (Self::String, Self::String) => true,
            (Self::String, _) => false,
            (Self::Own, Self::Own) => true,
            (Self::Own, _) => false,
            (Self::Borrow, Self::Borrow) => true,
            (Self::Borrow, _) => false,

            (Self::Record(left_map), Self::Record(right_map)) => {
                // IndexMap equality only if ordering is the same!
                left_map.as_slice() == right_map.as_slice()
            }
            (Self::Record(_), _) => false,
            (Self::Variant(left_map), Self::Variant(right_map)) => {
                // IndexMap equality only if ordering is the same!
                left_map.as_slice() == right_map.as_slice()
            }
            (Self::Variant(_), _) => false,

            // Other types that do not have this problem.
            (Self::List(l0), Self::List(r0)) => l0 == r0,
            (Self::List(_), _) => false,
            (Self::Tuple(l0), Self::Tuple(r0)) => l0 == r0,
            (Self::Tuple(_), _) => false,
            (Self::Enum(l0), Self::Enum(r0)) => l0 == r0,
            (Self::Enum(_), _) => false,
            (Self::Option(l0), Self::Option(r0)) => l0 == r0,
            (Self::Option(_), _) => false,
            (
                Self::Result {
                    ok: l_ok,
                    err: l_err,
                },
                Self::Result {
                    ok: r_ok,
                    err: r_err,
                },
            ) => l_ok == r_ok && l_err == r_err,
            (Self::Result { .. }, _) => false,
            (Self::Flags(l0), Self::Flags(r0)) => l0 == r0,
            (Self::Flags(_), _) => false,
            (Self::Map(left_key, left_val), Self::Map(right_key, right_val)) => {
                left_key == right_key && left_val == right_val
            }
            (Self::Map(_, _), _) => false,
        }
    }
}

impl Debug for TypeWrapper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Bool => write!(f, "Bool"),
            Self::S8 => write!(f, "S8"),
            Self::U8 => write!(f, "U8"),
            Self::S16 => write!(f, "S16"),
            Self::U16 => write!(f, "U16"),
            Self::S32 => write!(f, "S32"),
            Self::U32 => write!(f, "U32"),
            Self::S64 => write!(f, "S64"),
            Self::U64 => write!(f, "U64"),
            Self::F32 => write!(f, "F32"),
            Self::F64 => write!(f, "F64"),
            Self::Char => write!(f, "Char"),
            Self::String => write!(f, "String"),
            Self::List(arg0) => f.debug_tuple("List").field(arg0).finish(),
            Self::Record(arg0) => f.debug_tuple("Record").field(arg0).finish(),
            Self::Tuple(arg0) => f.debug_tuple("Tuple").field(arg0).finish(),
            Self::Variant(arg0) => f.debug_tuple("Variant").field(arg0).finish(),
            Self::Enum(arg0) => f.debug_tuple("Enum").field(arg0).finish(),
            Self::Option(arg0) => f.debug_tuple("Option").field(arg0).finish(),
            Self::Result {
                ok: Some(ok),
                err: Some(err),
            } => f
                .debug_struct("Result")
                .field("ok", ok)
                .field("err", err)
                .finish(),
            Self::Result {
                ok: None,
                err: Some(err),
            } => f.debug_struct("Result").field("err", err).finish(),
            Self::Result {
                ok: Some(ok),
                err: None,
            } => f.debug_struct("Result").field("ok", ok).finish(),
            Self::Result {
                ok: None,
                err: None,
            } => f.debug_struct("Result").finish(),
            Self::Flags(flags) => f.debug_tuple("Flags").field(flags).finish(),
            Self::Own => f.debug_tuple("Own").finish(),
            Self::Borrow => f.debug_tuple("Borrow").finish(),
            Self::Map(key_type, val_type) => f
                .debug_tuple("Map")
                .field(key_type)
                .field(val_type)
                .finish(),
        }
    }
}

#[derive(thiserror::Error, Debug, Clone)]
pub enum TypeConversionError {
    #[error("unsupported type {0}")]
    UnsupportedType(&'static str),
    #[error("{0}")]
    Invalid(String),
}

#[cfg(feature = "wasmtime")]
impl TryFrom<wasmtime::component::Type> for TypeWrapper {
    type Error = TypeConversionError;

    fn try_from(value: wasmtime::component::Type) -> Result<Self, Self::Error> {
        use wasmtime::component::Type;
        match value {
            Type::Bool => Ok(Self::Bool),
            Type::S8 => Ok(Self::S8),
            Type::U8 => Ok(Self::U8),
            Type::S16 => Ok(Self::S16),
            Type::U16 => Ok(Self::U16),
            Type::S32 => Ok(Self::S32),
            Type::U32 => Ok(Self::U32),
            Type::S64 => Ok(Self::S64),
            Type::U64 => Ok(Self::U64),
            Type::Float32 => Ok(Self::F32),
            Type::Float64 => Ok(Self::F64),
            Type::Char => Ok(Self::Char),
            Type::String => Ok(Self::String),
            Type::Option(option) => Ok(Self::Option(Box::new(Self::try_from(option.ty())?))),
            Type::Result(result) => {
                let transform = |ty: Option<Type>| {
                    ty.map(Self::try_from)
                        .transpose()
                        .map(|option| option.map(Box::new))
                };
                Ok(Self::Result {
                    ok: transform(result.ok())?,
                    err: transform(result.err())?,
                })
            }
            Type::List(list) => Ok(Self::List(Box::new(Self::try_from(list.ty())?))),
            Type::Record(record) => {
                let map = record
                    .fields()
                    .map(|field| Self::try_from(field.ty).map(|ty| (Box::from(field.name), ty)))
                    .collect::<Result<_, _>>()?;
                Ok(Self::Record(map))
            }
            Type::Variant(variant) => {
                let map = variant
                    .cases()
                    .map(|case| {
                        if let Some(ty) = case.ty {
                            Self::try_from(ty).map(|ty| (Box::from(case.name), Some(ty)))
                        } else {
                            Ok((Box::from(case.name), None))
                        }
                    })
                    .collect::<Result<_, _>>()?;
                Ok(Self::Variant(map))
            }
            Type::Tuple(tuple) => Ok(Self::Tuple(
                tuple
                    .types()
                    .map(Self::try_from)
                    .collect::<Result<_, _>>()?,
            )),
            Type::Enum(names) => Ok(Self::Enum(names.names().map(Box::from).collect())),
            Type::Borrow(_) => Ok(Self::Borrow),
            Type::Own(_) => Ok(Self::Own),
            Type::Flags(flags) => Ok(Self::Flags(flags.names().map(Box::from).collect())),
            Type::Future(_) => Err(TypeConversionError::UnsupportedType("future")),
            Type::Stream(_) => Err(TypeConversionError::UnsupportedType("stream")),
            Type::ErrorContext => Err(TypeConversionError::UnsupportedType("error-context")),
        }
    }
}

#[cfg(feature = "wit-parser")]
impl MapKeyType {
    pub fn from_wit_parser_type(ty: &wit_parser::Type) -> Result<MapKeyType, TypeConversionError> {
        use wit_parser::Type;

        match ty {
            Type::Bool => Ok(MapKeyType::Bool),
            Type::U8 => Ok(MapKeyType::U8),
            Type::U16 => Ok(MapKeyType::U16),
            Type::U32 => Ok(MapKeyType::U32),
            Type::U64 => Ok(MapKeyType::U64),
            Type::S8 => Ok(MapKeyType::S8),
            Type::S16 => Ok(MapKeyType::S16),
            Type::S32 => Ok(MapKeyType::S32),
            Type::S64 => Ok(MapKeyType::S64),
            Type::Char => Ok(MapKeyType::Char),
            Type::String => Ok(MapKeyType::String),
            other => Err(TypeConversionError::Invalid(format!(
                "invalid map key type {other:?}"
            ))),
        }
    }
}

#[cfg(feature = "wit-parser")]
impl TypeWrapper {
    pub fn from_wit_parser_type(
        resolve: &wit_parser::Resolve,
        ty: &wit_parser::Type,
    ) -> Result<TypeWrapper, TypeConversionError> {
        use wit_parser::{Type, TypeDefKind};

        match ty {
            Type::Bool => Ok(TypeWrapper::Bool),
            Type::U8 => Ok(TypeWrapper::U8),
            Type::U16 => Ok(TypeWrapper::U16),
            Type::U32 => Ok(TypeWrapper::U32),
            Type::U64 => Ok(TypeWrapper::U64),
            Type::S8 => Ok(TypeWrapper::S8),
            Type::S16 => Ok(TypeWrapper::S16),
            Type::S32 => Ok(TypeWrapper::S32),
            Type::S64 => Ok(TypeWrapper::S64),
            Type::F32 => Ok(TypeWrapper::F32),
            Type::F64 => Ok(TypeWrapper::F64),
            Type::Char => Ok(TypeWrapper::Char),
            Type::String => Ok(TypeWrapper::String),

            Type::ErrorContext => Err(TypeConversionError::UnsupportedType("error-context")),

            Type::Id(id) => {
                let ty = &resolve.types[*id];

                match &ty.kind {
                    TypeDefKind::Handle(wit_parser::Handle::Own(_)) => Ok(TypeWrapper::Own),
                    TypeDefKind::Handle(wit_parser::Handle::Borrow(_)) => Ok(TypeWrapper::Borrow),
                    TypeDefKind::Resource => {
                        Err(TypeConversionError::UnsupportedType("resource type"))
                    }
                    TypeDefKind::Tuple(tuple) => Ok(TypeWrapper::Tuple(
                        tuple
                            .types
                            .iter()
                            .map(|ty| TypeWrapper::from_wit_parser_type(resolve, ty))
                            .collect::<Result<_, _>>()?,
                    )),
                    TypeDefKind::Option(inner) => Ok(TypeWrapper::Option(Box::new(
                        TypeWrapper::from_wit_parser_type(resolve, inner)?,
                    ))),
                    TypeDefKind::Result(wit_parser::Result_ { ok, err }) => {
                        Ok(TypeWrapper::Result {
                            ok: ok
                                .as_ref()
                                .map(|inner| TypeWrapper::from_wit_parser_type(resolve, inner))
                                .transpose()?
                                .map(Box::new),
                            err: err
                                .as_ref()
                                .map(|inner| TypeWrapper::from_wit_parser_type(resolve, inner))
                                .transpose()?
                                .map(Box::new),
                        })
                    }
                    TypeDefKind::Record(record) => {
                        let map = record
                            .fields
                            .iter()
                            .map(|field| {
                                TypeWrapper::from_wit_parser_type(resolve, &field.ty)
                                    .map(|ty| (Box::from(field.name.clone()), ty))
                            })
                            .collect::<Result<_, _>>()?;
                        Ok(TypeWrapper::Record(map))
                    }
                    TypeDefKind::Flags(flags) => Ok(TypeWrapper::Flags(
                        flags
                            .flags
                            .iter()
                            .map(|f| Box::from(f.name.clone()))
                            .collect(),
                    )),
                    TypeDefKind::Enum(en) => Ok(TypeWrapper::Enum(
                        en.cases
                            .iter()
                            .map(|case| Box::from(case.name.clone()))
                            .collect(),
                    )),
                    TypeDefKind::Variant(variant) => {
                        let map = variant
                            .cases
                            .iter()
                            .map(|case| {
                                if let Some(ty) = &case.ty {
                                    TypeWrapper::from_wit_parser_type(resolve, ty)
                                        .map(|ty| (Box::from(case.name.clone()), Some(ty)))
                                } else {
                                    Ok((Box::from(case.name.clone()), None))
                                }
                            })
                            .collect::<Result<_, _>>()?;
                        Ok(TypeWrapper::Variant(map))
                    }
                    TypeDefKind::List(inner) => Ok(TypeWrapper::List(Box::new(
                        TypeWrapper::from_wit_parser_type(resolve, inner)?,
                    ))),
                    TypeDefKind::Map(key_type, value_type) => Ok(TypeWrapper::Map(
                        MapKeyType::from_wit_parser_type(key_type)?,
                        TypeWrapper::from_wit_parser_type(resolve, value_type).map(Box::new)?,
                    )),
                    TypeDefKind::FixedSizeList(_inner, _size) => {
                        Err(TypeConversionError::UnsupportedType("FixedSizeList")) // TODO
                    }
                    TypeDefKind::Type(inner) => TypeWrapper::from_wit_parser_type(resolve, inner),
                    TypeDefKind::Future(_) => Err(TypeConversionError::UnsupportedType("Future")),
                    TypeDefKind::Stream(_) => Err(TypeConversionError::UnsupportedType("Stream")),
                    TypeDefKind::Unknown => unreachable!(),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::TypeWrapper;
    use assert_matches::assert_matches;
    use indexmap::indexmap;
    use itertools::Itertools;

    #[test]
    fn deserialize_type_u64() {
        let json = r#"["u64"]"#;
        let actual: Vec<TypeWrapper> = serde_json::from_str(json).unwrap();
        assert_eq!(vec![TypeWrapper::U64], actual);
    }

    #[test]
    fn deser_should_preserve_its_attribute_order() {
        let json = r#"
            {
                "record": {
                    "logins": "string",
                    "cursor": "string"
                }
            }
        "#;
        let deser: TypeWrapper = serde_json::from_str(json).unwrap();
        let fields = assert_matches!(deser, TypeWrapper::Record(fields) => fields);
        let expected = indexmap! {
            Box::from("logins") => TypeWrapper::String,
            Box::from("cursor") => TypeWrapper::String,
        };
        assert_eq!(expected, fields);
        assert_eq!(
            vec!["logins", "cursor"],
            fields
                .keys()
                .map(std::string::ToString::to_string)
                .collect_vec()
        );
    }
}
