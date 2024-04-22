use indexmap::IndexMap;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TypeWrapper {
    // TODO: serde using wit syntax.
    Bool,
    S8,
    U8,
    S16,
    U16,
    S32,
    U32,
    S64,
    U64,
    Float32,
    Float64,
    Char,
    String,
    List(Box<TypeWrapper>),
    Record(IndexMap<Box<str>, TypeWrapper>),
    // Tuple(Tuple),
    // Variant(Variant),
    // Enum(Enum),
    Option(Box<TypeWrapper>),
    Result {
        ok: Option<Box<TypeWrapper>>,
        err: Option<Box<TypeWrapper>>,
    },
    // Flags(Flags),
    // Own(ResourceType),
    // Borrow(ResourceType),
}

#[derive(thiserror::Error, Debug, Clone)]
pub enum TypeConversionError {
    #[error("unsupported type {0}")]
    UnsupportedType(String),
}

impl TryFrom<wit_parser::Type> for TypeWrapper {
    type Error = TypeConversionError;

    fn try_from(value: wit_parser::Type) -> Result<Self, Self::Error> {
        match value {
            wit_parser::Type::Bool => Ok(Self::Bool),
            wit_parser::Type::Char => Ok(Self::Char),
            wit_parser::Type::F32 => Ok(Self::Float32),
            wit_parser::Type::F64 => Ok(Self::Float64),
            wit_parser::Type::S16 => Ok(Self::S16),
            wit_parser::Type::S32 => Ok(Self::S32),
            wit_parser::Type::S64 => Ok(Self::S64),
            wit_parser::Type::S8 => Ok(Self::S8),
            wit_parser::Type::String => Ok(Self::String),
            wit_parser::Type::U16 => Ok(Self::U16),
            wit_parser::Type::U32 => Ok(Self::U32),
            wit_parser::Type::U64 => Ok(Self::U64),
            wit_parser::Type::U8 => Ok(Self::U8),
            wit_parser::Type::Id(_) => {
                Err(TypeConversionError::UnsupportedType(format!("{value:?}")))
            }
        }
    }
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
            Type::Float32 => Ok(Self::Float32),
            Type::Float64 => Ok(Self::Float64),
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
                let vec = record
                    .fields()
                    .map(|field| Self::try_from(field.ty).map(|ty| (Box::from(field.name), ty)))
                    .collect::<Result<_, _>>()?;
                Ok(Self::Record(vec))
            }
            _ => Err(TypeConversionError::UnsupportedType(format!("{value:?}"))),
        }
    }
}

impl TryFrom<&wasmtime::component::Val> for TypeWrapper {
    type Error = TypeConversionError;

    fn try_from(value: &wasmtime::component::Val) -> Result<Self, Self::Error> {
        use wasmtime::component::Type;
        use wasmtime::component::Val;

        match value {
            Val::Bool(_) => Ok(Self::Bool),
            Val::Char(_) => Ok(Self::Char),
            Val::Float32(_) => Ok(Self::Float32),
            Val::Float64(_) => Ok(Self::Float64),
            Val::S16(_) => Ok(Self::S16),
            Val::S32(_) => Ok(Self::S32),
            Val::S64(_) => Ok(Self::S64),
            Val::S8(_) => Ok(Self::S8),
            Val::String(_) => Ok(Self::String),
            Val::U16(_) => Ok(Self::U16),
            Val::U32(_) => Ok(Self::U32),
            Val::U64(_) => Ok(Self::U64),
            Val::U8(_) => Ok(Self::U8),
            Val::Option(option) => {
                let inner = option.ty().ty();
                let inner = Self::try_from(inner)?;
                Ok(Self::Option(inner.into()))
            }
            Val::Result(result) => {
                let res = result.ty();
                let transform = |ty: Option<Type>| {
                    ty.map(Self::try_from)
                        .transpose()
                        .map(|option| option.map(Box::new))
                };
                let ok = transform(res.ok())?;
                let err = transform(res.err())?;
                Ok(Self::Result { ok, err })
            }
            Val::List(list) => {
                let inner = list.ty().ty();
                let inner = Self::try_from(inner)?;
                Ok(Self::List(inner.into()))
            }
            _ => Err(TypeConversionError::UnsupportedType(format!("{value:?}"))),
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
