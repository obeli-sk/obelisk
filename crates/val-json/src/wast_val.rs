use crate::{core, TypeWrapper};
use anyhow::{anyhow, bail, Context};
use serde::Serialize;
use std::collections::{BTreeSet, HashMap};
use std::fmt::Debug;
use std::ops::Deref;
use wasmtime::component::{Type, Val};
use wast::core::NanPattern;

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
    Float32(f32),
    Float64(f64),
    Char(char),
    String(Box<str>),
    List(Vec<WastVal>),
    Record(Vec<(Box<str>, WastVal)>),
    Tuple(Vec<WastVal>),
    Variant(Box<str>, Option<Box<WastVal>>),
    Enum(Box<str>),
    Option(Option<Box<WastVal>>),
    Result(Result<Option<Box<WastVal>>, Option<Box<WastVal>>>),
    Flags(Vec<Box<str>>),
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct WastValWithType {
    pub(crate) r#type: TypeWrapper,
    pub(crate) val: WastVal,
}

#[derive(Debug, thiserror::Error)]
#[error("conversion of the resource type is not supported")]
pub struct ConversionError;

impl TryFrom<Val> for WastVal {
    type Error = ConversionError;

    fn try_from(value: Val) -> Result<Self, Self::Error> {
        Ok(match value {
            Val::Bool(v) => Self::Bool(v),
            Val::S8(v) => Self::S8(v),
            Val::U8(v) => Self::U8(v),
            Val::S16(v) => Self::S16(v),
            Val::U16(v) => Self::U16(v),
            Val::S32(v) => Self::S32(v),
            Val::U32(v) => Self::U32(v),
            Val::S64(v) => Self::S64(v),
            Val::U64(v) => Self::U64(v),
            Val::Float32(v) => Self::Float32(v),
            Val::Float64(v) => Self::Float64(v),
            Val::Char(v) => Self::Char(v),
            Val::String(v) => Self::String(v),
            Val::List(v) => Self::List(
                v.deref()
                    .iter()
                    .map(|v| WastVal::try_from(v.clone()))
                    .collect::<Result<_, _>>()?,
            ),
            Val::Record(v) => Self::Record(
                v.fields()
                    .map(|(name, v)| {
                        WastVal::try_from(v.clone()).map(|v| (Box::<str>::from(name), v))
                    })
                    .collect::<Result<_, _>>()?,
            ),
            Val::Tuple(v) => Self::Tuple(
                v.values()
                    .iter()
                    .map(|v| WastVal::try_from(v.clone()))
                    .collect::<Result<_, _>>()?,
            ),
            Val::Variant(v) => Self::Variant(
                Box::from(v.discriminant()),
                v.payload()
                    .map(|v| WastVal::try_from(v.clone()).map(Box::new))
                    .transpose()?,
            ),
            Val::Enum(v) => Self::Enum(Box::from(v.discriminant())),
            Val::Option(v) => Self::Option(
                v.value()
                    .map(|v| WastVal::try_from(v.clone()).map(Box::new))
                    .transpose()?,
            ),
            Val::Result(v) => {
                let res = match v.value() {
                    Ok(v) => Ok(v
                        .map(|v| WastVal::try_from(v.clone()).map(Box::new))
                        .transpose()?),
                    Err(v) => Err(v
                        .map(|v| WastVal::try_from(v.clone()).map(Box::new))
                        .transpose()?),
                };
                Self::Result(res)
            }
            Val::Flags(v) => Self::Flags(v.flags().map(Box::from).collect()),
            Val::Resource(_) => return Err(ConversionError),
        })
    }
}

impl PartialEq for WastVal {
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
            (Self::List(l), Self::List(r)) => l == r,
            (Self::List(_), _) => false,
            (Self::Record(l), Self::Record(r)) => l == r,
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

#[allow(clippy::too_many_lines)]
#[allow(clippy::manual_let_else)]
pub fn val(v: &WastVal, ty: &Type) -> anyhow::Result<Val> {
    Ok(match v {
        WastVal::Bool(b) => Val::Bool(*b),
        WastVal::U8(b) => Val::U8(*b),
        WastVal::S8(b) => Val::S8(*b),
        WastVal::U16(b) => Val::U16(*b),
        WastVal::S16(b) => Val::S16(*b),
        WastVal::U32(b) => Val::U32(*b),
        WastVal::S32(b) => Val::S32(*b),
        WastVal::U64(b) => Val::U64(*b),
        WastVal::S64(b) => Val::S64(*b),
        WastVal::Float32(b) => Val::Float32(*b),
        WastVal::Float64(b) => Val::Float64(*b),
        WastVal::Char(b) => Val::Char(*b),
        WastVal::String(s) => Val::String(s.to_string().into()),
        WastVal::List(vals) => match ty {
            Type::List(t) => {
                let element = t.ty();
                let vals = vals
                    .iter()
                    .map(|v| val(v, &element))
                    .collect::<anyhow::Result<Vec<_>>>()?;
                t.new_val(vals.into())?
            }
            _ => bail!("expected a list value"),
        },
        WastVal::Record(fields) => match ty {
            Type::Record(t) => {
                let mut fields_by_name = HashMap::new();
                for (name, val) in fields {
                    let prev = fields_by_name.insert(name.clone(), val);
                    if prev.is_some() {
                        bail!("field `{name}` specified twice");
                    }
                }
                let mut values = Vec::new();
                for field in t.fields() {
                    let name = field.name;
                    let v = fields_by_name
                        .remove(name)
                        .ok_or_else(|| anyhow!("field `{name}` not specified"))?;
                    values.push((name, val(v, &field.ty)?));
                }
                if let Some((field, _)) = fields_by_name.iter().next() {
                    bail!("extra field `{field}` specified");
                }
                t.new_val(values)?
            }
            _ => bail!("expected a record value"),
        },
        WastVal::Tuple(vals) => match ty {
            Type::Tuple(t) => {
                if vals.len() != t.types().len() {
                    bail!("expected {} values got {}", t.types().len(), vals.len());
                }
                t.new_val(
                    vals.iter()
                        .zip(t.types())
                        .map(|(v, ty)| val(v, &ty))
                        .collect::<anyhow::Result<Vec<_>>>()?
                        .into(),
                )?
            }
            _ => bail!("expected a tuple value"),
        },
        WastVal::Enum(name) => match ty {
            Type::Enum(t) => t.new_val(name)?,
            _ => bail!("expected an enum value"),
        },
        WastVal::Variant(name, payload) => match ty {
            Type::Variant(t) => {
                let case = match t.cases().find(|c| c.name == name.deref()) {
                    Some(case) => case,
                    None => bail!("no case named `{}", name),
                };
                let payload = payload_val(case.name, payload.as_deref(), case.ty.as_ref())?;
                t.new_val(name, payload)?
            }
            _ => bail!("expected a variant value"),
        },
        WastVal::Option(v) => match ty {
            Type::Option(t) => {
                let v = match v {
                    Some(v) => Some(val(v, &t.ty())?),
                    None => None,
                };
                t.new_val(v)?
            }
            _ => bail!("expected an option value"),
        },
        WastVal::Result(v) => match ty {
            Type::Result(t) => {
                let v = match v {
                    Ok(v) => Ok(payload_val("ok", v.as_deref(), t.ok().as_ref())?),
                    Err(v) => Err(payload_val("err", v.as_deref(), t.err().as_ref())?),
                };
                t.new_val(v)?
            }
            _ => bail!("expected an expected value"),
        },
        WastVal::Flags(v) => match ty {
            Type::Flags(t) => {
                t.new_val(v.iter().map(AsRef::as_ref).collect::<Vec<_>>().as_slice())?
            }
            _ => bail!("expected a flags value"),
        },
    })
}

fn payload_val(name: &str, v: Option<&WastVal>, ty: Option<&Type>) -> anyhow::Result<Option<Val>> {
    match (v, ty) {
        (Some(v), Some(ty)) => Ok(Some(val(v, ty)?)),
        (None, None) => Ok(None),
        (Some(_), None) => bail!("expected payload for case `{name}`"),
        (None, Some(_)) => bail!("unexpected payload for case `{name}`"),
    }
}

#[allow(clippy::too_many_lines)]
pub fn match_val(expected: &WastVal, actual: &Val) -> anyhow::Result<()> {
    match expected {
        WastVal::Bool(e) => match actual {
            Val::Bool(a) => match_debug(a, e),
            _ => mismatch(expected, actual),
        },
        WastVal::U8(e) => match actual {
            Val::U8(a) => core::match_int(a, e),
            _ => mismatch(expected, actual),
        },
        WastVal::S8(e) => match actual {
            Val::S8(a) => core::match_int(a, e),
            _ => mismatch(expected, actual),
        },
        WastVal::U16(e) => match actual {
            Val::U16(a) => core::match_int(a, e),
            _ => mismatch(expected, actual),
        },
        WastVal::S16(e) => match actual {
            Val::S16(a) => core::match_int(a, e),
            _ => mismatch(expected, actual),
        },
        WastVal::U32(e) => match actual {
            Val::U32(a) => core::match_int(a, e),
            _ => mismatch(expected, actual),
        },
        WastVal::S32(e) => match actual {
            Val::S32(a) => core::match_int(a, e),
            _ => mismatch(expected, actual),
        },
        WastVal::U64(e) => match actual {
            Val::U64(a) => core::match_int(a, e),
            _ => mismatch(expected, actual),
        },
        WastVal::S64(e) => match actual {
            Val::S64(a) => core::match_int(a, e),
            _ => mismatch(expected, actual),
        },
        WastVal::Float32(e) => match actual {
            Val::Float32(a) => core::match_f32(
                a.to_bits(),
                &NanPattern::Value(wast::token::Float32 { bits: e.to_bits() }),
            ),
            _ => mismatch(expected, actual),
        },
        WastVal::Float64(e) => match actual {
            Val::Float64(a) => core::match_f64(
                a.to_bits(),
                &NanPattern::Value(wast::token::Float64 { bits: e.to_bits() }),
            ),
            _ => mismatch(expected, actual),
        },
        WastVal::Char(e) => match actual {
            Val::Char(a) => match_debug(a, e),
            _ => mismatch(expected, actual),
        },
        WastVal::String(e) => match actual {
            Val::String(a) => match_debug(&a[..], e.as_ref()),
            _ => mismatch(expected, actual),
        },
        WastVal::List(e) => match actual {
            Val::List(a) => {
                if e.len() != a.len() {
                    bail!("expected {} values got {}", e.len(), a.len());
                }
                for (i, (expected, actual)) in e.iter().zip(a.iter()).enumerate() {
                    match_val(expected, actual)
                        .with_context(|| format!("failed to match list element {i}"))?;
                }
                Ok(())
            }
            _ => mismatch(expected, actual),
        },
        WastVal::Record(e) => match actual {
            Val::Record(a) => {
                let mut fields_by_name = HashMap::new();
                for (name, val) in e {
                    let prev = fields_by_name.insert(name.clone(), val);
                    if prev.is_some() {
                        bail!("field `{name}` specified twice");
                    }
                }
                for (name, actual) in a.fields() {
                    let expected = fields_by_name
                        .remove(name)
                        .ok_or_else(|| anyhow!("field `{name}` not specified"))?;
                    match_val(expected, actual)
                        .with_context(|| format!("failed to match field `{name}`"))?;
                }
                if let Some((field, _)) = fields_by_name.iter().next() {
                    bail!("extra field `{field}` specified");
                }
                Ok(())
            }
            _ => mismatch(expected, actual),
        },
        WastVal::Tuple(e) => match actual {
            Val::Tuple(a) => {
                if e.len() != a.values().len() {
                    bail!(
                        "expected {}-tuple, found {}-tuple",
                        e.len(),
                        a.values().len()
                    );
                }
                for (i, (expected, actual)) in e.iter().zip(a.values()).enumerate() {
                    match_val(expected, actual)
                        .with_context(|| format!("failed to match tuple element {i}"))?;
                }
                Ok(())
            }
            _ => mismatch(expected, actual),
        },
        WastVal::Variant(name, e) => match actual {
            Val::Variant(a) => {
                if a.discriminant() != name.as_ref() {
                    bail!("expected discriminant `{name}` got `{}`", a.discriminant());
                }
                match_payload_val(name, e.as_deref(), a.payload())
            }
            _ => mismatch(expected, actual),
        },
        WastVal::Enum(name) => match actual {
            Val::Enum(a) => {
                if a.discriminant() == name.as_ref() {
                    Ok(())
                } else {
                    bail!("expected discriminant `{name}` got `{}`", a.discriminant());
                }
            }
            _ => mismatch(expected, actual),
        },
        WastVal::Option(e) => match actual {
            Val::Option(a) => match (e, a.value()) {
                (None, None) => Ok(()),
                (Some(expected), Some(actual)) => match_val(expected, actual),
                (None, Some(_)) => bail!("expected `none`, found `some`"),
                (Some(_), None) => bail!("expected `some`, found `none`"),
            },
            _ => mismatch(expected, actual),
        },
        WastVal::Result(e) => match actual {
            Val::Result(a) => match (e, a.value()) {
                (Ok(_), Err(_)) => bail!("expected `ok`, found `err`"),
                (Err(_), Ok(_)) => bail!("expected `err`, found `ok`"),
                (Err(e), Err(a)) => match_payload_val("err", e.as_deref(), a),
                (Ok(e), Ok(a)) => match_payload_val("ok", e.as_deref(), a),
            },
            _ => mismatch(expected, actual),
        },
        WastVal::Flags(e) => match actual {
            Val::Flags(a) => {
                let expected = e.iter().map(AsRef::as_ref).collect::<BTreeSet<_>>();
                let actual = a.flags().collect::<BTreeSet<_>>();
                match_debug(&actual, &expected)
            }
            _ => mismatch(expected, actual),
        },
    }
}

fn match_payload_val(
    name: &str,
    expected: Option<&WastVal>,
    actual: Option<&Val>,
) -> anyhow::Result<()> {
    match (expected, actual) {
        (Some(e), Some(a)) => {
            match_val(e, a).with_context(|| format!("failed to match case `{name}`"))
        }
        (None, None) => Ok(()),
        (Some(_), None) => bail!("expected payload for case `{name}`"),
        (None, Some(_)) => bail!("unexpected payload for case `{name}`"),
    }
}

fn match_debug<T>(actual: &T, expected: &T) -> anyhow::Result<()>
where
    T: Eq + Debug + ?Sized,
{
    if actual == expected {
        Ok(())
    } else {
        bail!(
            "
             expected {expected:?}
             actual   {actual:?}"
        )
    }
}

fn mismatch(expected: &WastVal, actual: &Val) -> anyhow::Result<()> {
    let expected = match expected {
        WastVal::Bool(..) => "bool",
        WastVal::U8(..) => "u8",
        WastVal::S8(..) => "s8",
        WastVal::U16(..) => "u16",
        WastVal::S16(..) => "s16",
        WastVal::U32(..) => "u32",
        WastVal::S32(..) => "s32",
        WastVal::U64(..) => "u64",
        WastVal::S64(..) => "s64",
        WastVal::Float32(..) => "float32",
        WastVal::Float64(..) => "float64",
        WastVal::Char(..) => "char",
        WastVal::String(..) => "string",
        WastVal::List(..) => "list",
        WastVal::Record(..) => "record",
        WastVal::Tuple(..) => "tuple",
        WastVal::Enum(..) => "enum",
        WastVal::Variant(..) => "variant",
        WastVal::Option(..) => "option",
        WastVal::Result(..) => "result",
        WastVal::Flags(..) => "flags",
    };
    let actual = match actual {
        Val::Bool(..) => "bool",
        Val::U8(..) => "u8",
        Val::S8(..) => "s8",
        Val::U16(..) => "u16",
        Val::S16(..) => "s16",
        Val::U32(..) => "u32",
        Val::S32(..) => "s32",
        Val::U64(..) => "u64",
        Val::S64(..) => "s64",
        Val::Float32(..) => "float32",
        Val::Float64(..) => "float64",
        Val::Char(..) => "char",
        Val::String(..) => "string",
        Val::List(..) => "list",
        Val::Record(..) => "record",
        Val::Tuple(..) => "tuple",
        Val::Enum(..) => "enum",
        Val::Variant(..) => "variant",
        Val::Option(..) => "option",
        Val::Result(..) => "result",
        Val::Flags(..) => "flags",
        Val::Resource(..) => "resource",
    };
    bail!("expected `{expected}` got `{actual}`")
}
