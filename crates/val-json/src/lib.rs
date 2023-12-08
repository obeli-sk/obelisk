mod deser;

use serde::{
    ser::{Serialize, Serializer},
    Deserialize,
};

pub use deser::deserialize_sequence;

#[derive(Debug, Clone)]
pub enum ValWrapper {
    Bool(bool),
    S8(i8),
    U8(u8),
    S16(i16),
    U16(u16),
    S32(i32),
    U32(u32),
    S64(i64),
    U64(u64),
    Float32(f32),
    Float64(f64),
    Char(char),
    String(Box<str>),
    // List(List),
    // Record(Record),
    // Tuple(Tuple),
    // Variant(Variant),
    // Enum(Enum),
    // Option(OptionVal),
    // Result(ResultVal),
    // Flags(Flags),
    // Resource(ResourceAny),
}

impl From<ValWrapper> for wasmtime::component::Val {
    fn from(value: ValWrapper) -> Self {
        match value {
            ValWrapper::Bool(v) => Self::Bool(v),
            ValWrapper::S8(v) => Self::S8(v),
            ValWrapper::U8(v) => Self::U8(v),
            ValWrapper::S16(v) => Self::S16(v),
            ValWrapper::U16(v) => Self::U16(v),
            ValWrapper::S32(v) => Self::S32(v),
            ValWrapper::U32(v) => Self::U32(v),
            ValWrapper::S64(v) => Self::S64(v),
            ValWrapper::U64(v) => Self::U64(v),
            ValWrapper::Float32(v) => Self::Float32(v),
            ValWrapper::Float64(v) => Self::Float64(v),
            ValWrapper::Char(v) => Self::Char(v),
            ValWrapper::String(v) => Self::String(v),
        }
    }
}

/*
impl TryFrom<wasmtime::component::Val> for ValWrapper {
    type Error = UnsupportedTypeError;

    fn try_from(value: wasmtime::component::Val) -> Result<Self, Self::Error> {
        match value {
            wasmtime::component::Val::Bool(v) => Ok(Self::Bool(v)),
            wasmtime::component::Val::S8(v) => Ok(Self::S8(v)),
            wasmtime::component::Val::U8(v) => Ok(Self::U8(v)),
            wasmtime::component::Val::S16(v) => Ok(Self::S16(v)),
            wasmtime::component::Val::U16(v) => Ok(Self::U16(v)),
            wasmtime::component::Val::S32(v) => Ok(Self::S32(v)),
            wasmtime::component::Val::U32(v) => Ok(Self::U32(v)),
            wasmtime::component::Val::S64(v) => Ok(Self::S64(v)),
            wasmtime::component::Val::U64(v) => Ok(Self::U64(v)),
            wasmtime::component::Val::Float32(v) => Ok(Self::Float32(v)),
            wasmtime::component::Val::Float64(v) => Ok(Self::Float64(v)),
            wasmtime::component::Val::Char(v) => Ok(Self::Char(v)),
            wasmtime::component::Val::String(v) => Ok(Self::String(v)),
            wasmtime::component::Val::List(_) => {
                Err(UnsupportedTypeError::UnsupportedType(format!("{value:?}")))
            }
            wasmtime::component::Val::Record(_) => {
                Err(UnsupportedTypeError::UnsupportedType(format!("{value:?}")))
            }
            wasmtime::component::Val::Tuple(_) => {
                Err(UnsupportedTypeError::UnsupportedType(format!("{value:?}")))
            }
            wasmtime::component::Val::Variant(_) => {
                Err(UnsupportedTypeError::UnsupportedType(format!("{value:?}")))
            }
            wasmtime::component::Val::Enum(_) => {
                Err(UnsupportedTypeError::UnsupportedType(format!("{value:?}")))
            }
            wasmtime::component::Val::Option(_) => {
                Err(UnsupportedTypeError::UnsupportedType(format!("{value:?}")))
            }
            wasmtime::component::Val::Result(_) => {
                Err(UnsupportedTypeError::UnsupportedType(format!("{value:?}")))
            }
            wasmtime::component::Val::Flags(_) => {
                Err(UnsupportedTypeError::UnsupportedType(format!("{value:?}")))
            }
            wasmtime::component::Val::Resource(_) => {
                Err(UnsupportedTypeError::UnsupportedType(format!("{value:?}")))
            }
        }
    }
}
*/

impl PartialEq for ValWrapper {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            // IEEE 754 equality considers NaN inequal to NaN and negative zero
            // equal to positive zero, however we do the opposite here, because
            // this logic is used by testing and fuzzing, which want to know
            // whether two values are semantically the same, rather than
            // numerically equal.
            (Self::Float32(l), Self::Float32(r)) => {
                (*l != 0.0 && l == r)
                    || (*l == 0.0 && l.to_bits() == r.to_bits())
                    || (l.is_nan() && r.is_nan())
            }
            (Self::Float32(_), _) => false,
            (Self::Float64(l), Self::Float64(r)) => {
                (*l != 0.0 && l == r)
                    || (*l == 0.0 && l.to_bits() == r.to_bits())
                    || (l.is_nan() && r.is_nan())
            }
            (Self::Float64(_), _) => false,

            (Self::Bool(l), Self::Bool(r)) => l == r,
            (Self::Bool(_), _) => false,
            (Self::S8(l), Self::S8(r)) => l == r,
            (Self::S8(_), _) => false,
            (Self::U8(l), Self::U8(r)) => l == r,
            (Self::U8(_), _) => false,
            (Self::S16(l), Self::S16(r)) => l == r,
            (Self::S16(_), _) => false,
            (Self::U16(l), Self::U16(r)) => l == r,
            (Self::U16(_), _) => false,
            (Self::S32(l), Self::S32(r)) => l == r,
            (Self::S32(_), _) => false,
            (Self::U32(l), Self::U32(r)) => l == r,
            (Self::U32(_), _) => false,
            (Self::S64(l), Self::S64(r)) => l == r,
            (Self::S64(_), _) => false,
            (Self::U64(l), Self::U64(r)) => l == r,
            (Self::U64(_), _) => false,
            (Self::Char(l), Self::Char(r)) => l == r,
            (Self::Char(_), _) => false,
            (Self::String(l), Self::String(r)) => l == r,
            (Self::String(_), _) => false,
        }
    }
}

impl Eq for ValWrapper {}

#[derive(Deserialize, Debug, Clone, PartialEq, Eq)]
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
    Float32,
    Float64,
    Char,
    String,
    // List(List),
    // Record(Record),
    // Tuple(Tuple),
    // Variant(Variant),
    // Enum(Enum),
    // Option(OptionType),
    // Result(ResultType),
    // Flags(Flags),
    // Own(ResourceType),
    // Borrow(ResourceType),
}

/*
impl From<TypeWrapper> for wasmtime::component::Type {
    fn from(value: TypeWrapper) -> Self {
        match value {
            TypeWrapper::Bool => Self::Bool,
            TypeWrapper::S8 => Self::S8,
            TypeWrapper::U8 => Self::U8,
            TypeWrapper::S16 => Self::S16,
            TypeWrapper::U16 => Self::U16,
            TypeWrapper::S32 => Self::S32,
            TypeWrapper::U32 => Self::U32,
            TypeWrapper::S64 => Self::S64,
            TypeWrapper::U64 => Self::U64,
            TypeWrapper::Float32 => Self::Float32,
            TypeWrapper::Float64 => Self::Float64,
            TypeWrapper::Char => Self::Char,
            TypeWrapper::String => Self::String,
        }
    }
}
*/

#[derive(thiserror::Error, Debug)]
pub enum UnsupportedTypeError {
    #[error("unsupported type {0}")]
    UnsupportedType(String),
}

impl TryFrom<wit_parser::Type> for TypeWrapper {
    type Error = UnsupportedTypeError;

    fn try_from(value: wit_parser::Type) -> Result<Self, Self::Error> {
        match value {
            wit_parser::Type::Bool => Ok(Self::Bool),
            wit_parser::Type::U8 => Ok(Self::U8),
            wit_parser::Type::U16 => Ok(Self::U16),
            wit_parser::Type::U32 => Ok(Self::U32),
            wit_parser::Type::U64 => Ok(Self::U64),
            wit_parser::Type::S8 => Ok(Self::S8),
            wit_parser::Type::S16 => Ok(Self::S16),
            wit_parser::Type::S32 => Ok(Self::S32),
            wit_parser::Type::S64 => Ok(Self::S64),
            wit_parser::Type::Float32 => Ok(Self::Float32),
            wit_parser::Type::Float64 => Ok(Self::Float64),
            wit_parser::Type::Char => Ok(Self::Char),
            wit_parser::Type::String => Ok(Self::String),
            wit_parser::Type::Id(_) => {
                Err(UnsupportedTypeError::UnsupportedType(format!("{value:?}")))
            }
        }
    }
}

/*
impl TryFrom<wasmtime::component::Type> for TypeWrapper {
    type Error = UnsupportedTypeError;

    fn try_from(value: wasmtime::component::Type) -> Result<Self, Self::Error> {
        match value {
            wasmtime::component::Type::Bool => Ok(Self::Bool),
            wasmtime::component::Type::S8 => Ok(Self::S8),
            wasmtime::component::Type::U8 => Ok(Self::U8),
            wasmtime::component::Type::S16 => Ok(Self::S16),
            wasmtime::component::Type::U16 => Ok(Self::U16),
            wasmtime::component::Type::S32 => Ok(Self::S32),
            wasmtime::component::Type::U32 => Ok(Self::U32),
            wasmtime::component::Type::S64 => Ok(Self::S64),
            wasmtime::component::Type::U64 => Ok(Self::U64),
            wasmtime::component::Type::Float32 => Ok(Self::Float32),
            wasmtime::component::Type::Float64 => Ok(Self::Float64),
            wasmtime::component::Type::Char => Ok(Self::Char),
            wasmtime::component::Type::String => Ok(Self::String),
            wasmtime::component::Type::List(_) => {
                Err(UnsupportedTypeError::UnsupportedType(format!("{value:?}")))
            }
            wasmtime::component::Type::Record(_) => {
                Err(UnsupportedTypeError::UnsupportedType(format!("{value:?}")))
            }
            wasmtime::component::Type::Tuple(_) => {
                Err(UnsupportedTypeError::UnsupportedType(format!("{value:?}")))
            }
            wasmtime::component::Type::Variant(_) => {
                Err(UnsupportedTypeError::UnsupportedType(format!("{value:?}")))
            }
            wasmtime::component::Type::Enum(_) => {
                Err(UnsupportedTypeError::UnsupportedType(format!("{value:?}")))
            }
            wasmtime::component::Type::Option(_) => {
                Err(UnsupportedTypeError::UnsupportedType(format!("{value:?}")))
            }
            wasmtime::component::Type::Result(_) => {
                Err(UnsupportedTypeError::UnsupportedType(format!("{value:?}")))
            }
            wasmtime::component::Type::Flags(_) => {
                Err(UnsupportedTypeError::UnsupportedType(format!("{value:?}")))
            }
            wasmtime::component::Type::Own(_) => {
                Err(UnsupportedTypeError::UnsupportedType(format!("{value:?}")))
            }
            wasmtime::component::Type::Borrow(_) => {
                Err(UnsupportedTypeError::UnsupportedType(format!("{value:?}")))
            }
        }
    }
}
*/

impl Serialize for ValWrapper {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            ValWrapper::Bool(v) => serializer.serialize_bool(*v),
            ValWrapper::S8(v) => serializer.serialize_i8(*v),
            ValWrapper::U8(v) => serializer.serialize_u8(*v),
            ValWrapper::S16(v) => serializer.serialize_i16(*v),
            ValWrapper::U16(v) => serializer.serialize_u16(*v),
            ValWrapper::S32(v) => serializer.serialize_i32(*v),
            ValWrapper::U32(v) => serializer.serialize_u32(*v),
            ValWrapper::S64(v) => serializer.serialize_i64(*v),
            ValWrapper::U64(v) => serializer.serialize_u64(*v),
            ValWrapper::Float32(v) => serializer.serialize_f32(*v),
            ValWrapper::Float64(v) => serializer.serialize_f64(*v),
            ValWrapper::Char(v) => serializer.serialize_char(*v),
            ValWrapper::String(v) => serializer.serialize_str(v),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::TypeWrapper;

    /*
    use crate::ValWrapper;
    use serde_json::json;
    #[test]
    fn serialize_bool_to_json() {
        let expected = true;
        let val = wasmtime::component::Val::Bool(true);
        let wrapper = ValWrapper::try_from(val).unwrap();
        let json = serde_json::to_value(wrapper).unwrap();
        assert_eq!(json, json!(expected));
    }
    */

    #[test]
    fn deserialize_types() {
        let json = r#"["U64"]"#;
        let actual: Vec<TypeWrapper> = serde_json::from_str(json).unwrap();
        assert_eq!(vec![TypeWrapper::U64], actual);
    }
}
