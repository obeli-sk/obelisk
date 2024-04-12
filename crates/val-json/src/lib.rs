use serde::{Deserialize, Serialize};

mod core;
pub mod wast_val;
pub mod wast_val_ser;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
