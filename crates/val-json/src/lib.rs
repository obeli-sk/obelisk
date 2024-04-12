mod deser;

use serde::{
    ser::{Serialize, Serializer},
    Deserialize,
};
mod core;
pub mod wast_val;
pub mod wast_val_ser;

#[derive(Debug, Clone)]
enum ValWrapper {
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
    // Result(Result<Option<Box<ValWrapper>>, Option<Box<ValWrapper>>>),
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

impl PartialEq for ValWrapper {
    #[allow(clippy::match_same_arms)]
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

    #[test]
    fn deserialize_type_u64() {
        let json = r#"["U64"]"#;
        let actual: Vec<TypeWrapper> = serde_json::from_str(json).unwrap();
        assert_eq!(vec![TypeWrapper::U64], actual);
    }
}
