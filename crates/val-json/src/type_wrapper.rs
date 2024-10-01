pub use indexmap;
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
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
    List(Box<TypeWrapper>),
    Record(IndexMap<Box<str>, TypeWrapper>),
    Tuple(Vec<TypeWrapper>),
    Variant(IndexMap<Box<str>, Option<TypeWrapper>>),
    Enum(Vec<Box<str>>),
    Option(Box<TypeWrapper>),
    Result {
        ok: Option<Box<TypeWrapper>>,
        err: Option<Box<TypeWrapper>>,
    },
    Flags(Vec<Box<str>>),
    Own(ResourceTypeWrapper),
    Borrow(ResourceTypeWrapper),
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
            Self::Flags(arg0) => f.debug_tuple("Flags").field(arg0).finish(),
            Self::Own(arg0) => f.debug_tuple("Own").field(arg0).finish(),
            Self::Borrow(arg0) => f.debug_tuple("Borrow").field(arg0).finish(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceTypeWrapper {}

#[derive(thiserror::Error, Debug, Clone)]
pub enum TypeConversionError {
    #[error("unsupported type {0}")]
    UnsupportedType(String),
}

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
            Type::Borrow(_) => {
                // TODO finish
                Ok(Self::Borrow(ResourceTypeWrapper {}))
            }
            Type::Own(_) => {
                // TODO finish
                Ok(Self::Own(ResourceTypeWrapper {}))
            }
            Type::Flags(flags) => Ok(Self::Flags(flags.names().map(Box::from).collect())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::TypeWrapper;

    #[test]
    fn deserialize_type_u64() {
        let json = r#"["U64"]"#;
        let actual: Vec<TypeWrapper> = serde_json::from_str(json).unwrap();
        assert_eq!(vec![TypeWrapper::U64], actual);
    }
}
