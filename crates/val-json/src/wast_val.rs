use crate::type_wrapper::TypeConversionError;
use crate::type_wrapper::TypeWrapper;
use indexmap::IndexMap;
use serde::Serialize;
use std::fmt::Debug;

/// Expression that can be used inside of `invoke` expressions for core wasm
/// functions.
#[derive(Debug, Clone)]
#[allow(missing_docs)]
pub enum WastVal {
    Bool(bool),
    U8(u8),
    S8(i8),
    U16(u16),
    S16(i16),
    U32(u32),
    S32(i32),
    U64(u64),
    S64(i64),
    F32(f32),
    F64(f64),
    Char(char),
    String(String),
    List(Vec<WastVal>),
    Record(IndexMap<String, WastVal>), // TODO: Consider replacing IndexMap with ordermap - https://github.com/indexmap-rs/indexmap/issues/153#issuecomment-2189804150
    Tuple(Vec<WastVal>),
    Variant(String, Option<Box<WastVal>>),
    Enum(String),
    Option(Option<Box<WastVal>>),
    Result(Result<Option<Box<WastVal>>, Option<Box<WastVal>>>),
    Flags(Vec<String>),
    // TODO: Add Map(IndexMap<MapKey, WastVal>), when wasmtime supports it
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct WastValWithType {
    pub r#type: TypeWrapper,
    pub value: WastVal,
}

// TODO: only for feature test
impl TryFrom<WastVal> for WastValWithType {
    type Error = &'static str;

    fn try_from(value: WastVal) -> Result<Self, Self::Error> {
        let r#type = match value {
            WastVal::Bool(_) => Ok(TypeWrapper::Bool),
            WastVal::U8(_) => Ok(TypeWrapper::U8),
            WastVal::S8(_) => Ok(TypeWrapper::S8),
            WastVal::U16(_) => Ok(TypeWrapper::U16),
            WastVal::S16(_) => Ok(TypeWrapper::S16),
            WastVal::U32(_) => Ok(TypeWrapper::U32),
            WastVal::S32(_) => Ok(TypeWrapper::S32),
            WastVal::U64(_) => Ok(TypeWrapper::U64),
            WastVal::S64(_) => Ok(TypeWrapper::S64),
            WastVal::F32(_) => Ok(TypeWrapper::F32),
            WastVal::F64(_) => Ok(TypeWrapper::F64),
            WastVal::Char(_) => Ok(TypeWrapper::Char),
            WastVal::String(_) => Ok(TypeWrapper::String),
            _ => Err("only primitive types are supported"),
        }?;
        Ok(Self { r#type, value })
    }
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum WastValWithTypeConversionError {
    #[error(transparent)]
    WastValConversionError(#[from] WastValConversionError),
    #[error(transparent)]
    TypeConversionError(#[from] TypeConversionError),
}

#[derive(Debug, Clone, thiserror::Error)]
#[error("conversion of the type {0} is not supported")]
pub struct WastValConversionError(&'static str);

#[cfg(feature = "wasmtime")]
impl TryFrom<wasmtime::component::Val> for WastVal {
    type Error = WastValConversionError;

    fn try_from(value: wasmtime::component::Val) -> Result<Self, Self::Error> {
        use wasmtime::component::Val;

        match value {
            Val::Bool(v) => Ok(Self::Bool(v)),
            Val::S8(v) => Ok(Self::S8(v)),
            Val::U8(v) => Ok(Self::U8(v)),
            Val::S16(v) => Ok(Self::S16(v)),
            Val::U16(v) => Ok(Self::U16(v)),
            Val::S32(v) => Ok(Self::S32(v)),
            Val::U32(v) => Ok(Self::U32(v)),
            Val::S64(v) => Ok(Self::S64(v)),
            Val::U64(v) => Ok(Self::U64(v)),
            Val::Float32(v) => Ok(Self::F32(v)),
            Val::Float64(v) => Ok(Self::F64(v)),
            Val::Char(v) => Ok(Self::Char(v)),
            Val::String(v) => Ok(Self::String(v)),
            Val::List(v) => Ok(Self::List(
                v.into_iter()
                    .map(WastVal::try_from)
                    .collect::<Result<_, _>>()?,
            )),
            Val::Record(v) => Ok(Self::Record(
                v.into_iter()
                    .map(|(name, v)| WastVal::try_from(v).map(|v| (name, v)))
                    .collect::<Result<_, _>>()?,
            )),
            Val::Tuple(v) => Ok(Self::Tuple(
                v.into_iter()
                    .map(WastVal::try_from)
                    .collect::<Result<_, _>>()?,
            )),
            Val::Variant(str, v) => Ok(Self::Variant(
                str,
                v.map(|v| WastVal::try_from(*v)).transpose()?.map(Box::new),
            )),
            Val::Enum(v) => Ok(Self::Enum(v)),
            Val::Option(v) => Ok(Self::Option(
                v.map(|v| WastVal::try_from(*v)).transpose()?.map(Box::new),
            )),
            Val::Result(v) => Ok(Self::Result(match v {
                Ok(v) => Ok(v.map(|v| WastVal::try_from(*v)).transpose()?.map(Box::new)),
                Err(v) => Err(v.map(|v| WastVal::try_from(*v)).transpose()?.map(Box::new)),
            })),
            Val::Flags(v) => Ok(Self::Flags(v)),
            Val::Resource(_) => Err(WastValConversionError("resource")),
            Val::Future(_) => Err(WastValConversionError("future")),
            Val::Stream(_) => Err(WastValConversionError("stream")),
            Val::ErrorContext(_) => Err(WastValConversionError("error-context")),
        }
    }
}

impl PartialEq for WastVal {
    #[expect(clippy::match_same_arms)]
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            // IEEE 754 equality considers NaN inequal to NaN and negative zero
            // equal to positive zero, however we do the opposite here, because
            // this logic is used by testing and fuzzing, which want to know
            // whether two values are semantically the same, rather than
            // numerically equal.
            (Self::F32(l), Self::F32(r)) => {
                (*l != 0.0 && l == r)
                    || (*l == 0.0 && l.to_bits() == r.to_bits())
                    || (l.is_nan() && r.is_nan())
            }
            (Self::F32(_), _) => false,
            (Self::F64(l), Self::F64(r)) => {
                (*l != 0.0 && l == r)
                    || (*l == 0.0 && l.to_bits() == r.to_bits())
                    || (l.is_nan() && r.is_nan())
            }
            (Self::F64(_), _) => false,

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
            (Self::List(l), Self::List(r)) => l == r,
            (Self::List(_), _) => false,
            (Self::Record(left_map), Self::Record(right_map)) => {
                left_map.as_slice() == right_map.as_slice()
            }
            (Self::Record(_), _) => false,
            (Self::Tuple(l), Self::Tuple(r)) => l == r,
            (Self::Tuple(_), _) => false,
            (Self::Variant(l0, l1), Self::Variant(r0, r1)) => l0 == r0 && l1 == r1,
            (Self::Variant(_, _), _) => false,
            (Self::Enum(l), Self::Enum(r)) => l == r,
            (Self::Enum(_), _) => false,
            (Self::Option(l), Self::Option(r)) => l == r,
            (Self::Option(_), _) => false,
            (Self::Result(l), Self::Result(r)) => l == r,
            (Self::Result(_), _) => false,
            (Self::Flags(l), Self::Flags(r)) => l == r,
            (Self::Flags(_), _) => false,
        }
    }
}

impl Eq for WastVal {}

#[cfg(feature = "wasmtime")]
impl WastVal {
    #[must_use]
    pub fn as_val(&self) -> wasmtime::component::Val {
        use wasmtime::component::Val;

        match self {
            Self::Bool(b) => Val::Bool(*b),
            Self::U8(b) => Val::U8(*b),
            Self::S8(b) => Val::S8(*b),
            Self::U16(b) => Val::U16(*b),
            Self::S16(b) => Val::S16(*b),
            Self::U32(b) => Val::U32(*b),
            Self::S32(b) => Val::S32(*b),
            Self::U64(b) => Val::U64(*b),
            Self::S64(b) => Val::S64(*b),
            Self::F32(b) => Val::Float32(*b),
            Self::F64(b) => Val::Float64(*b),
            Self::Char(b) => Val::Char(*b),
            Self::String(s) => Val::String(s.clone()),
            Self::List(vals) => {
                let vals = vals.iter().map(Self::as_val).collect();
                Val::List(vals)
            }
            Self::Record(vals) => {
                let mut fields = Vec::new();
                for (name, v) in vals {
                    fields.push((name.clone(), v.as_val()));
                }
                Val::Record(fields)
            }
            Self::Tuple(vals) => Val::Tuple(vals.iter().map(Self::as_val).collect()),
            Self::Enum(name) => Val::Enum(name.clone()),
            Self::Variant(name, payload) => {
                let payload = Self::payload_val(payload.as_deref());
                Val::Variant(name.clone(), payload)
            }
            Self::Option(v) => Val::Option(v.as_ref().map(|v| Box::new(v.as_val()))),
            Self::Result(v) => Val::Result(match v {
                Ok(v) => Ok(Self::payload_val(v.as_deref())),
                Err(v) => Err(Self::payload_val(v.as_deref())),
            }),
            Self::Flags(v) => Val::Flags(v.iter().map(std::string::ToString::to_string).collect()),
        }
    }

    fn payload_val(v: Option<&WastVal>) -> Option<Box<wasmtime::component::Val>> {
        v.map(|v| Box::new(v.as_val()))
    }
}

#[cfg(feature = "wasmtime")]
impl From<WastVal> for wasmtime::component::Val {
    fn from(value: WastVal) -> Self {
        value.as_val()
    }
}
