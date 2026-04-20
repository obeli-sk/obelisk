pub use indexmap;
use indexmap::{IndexMap, IndexSet};
use serde::{Deserialize, Serialize};
use serde_with::{DeserializeFromStr, SerializeDisplay};
use std::fmt::{self, Debug, Display};
use std::str::FromStr;

// TODO: Consider replacing IndexMap with ordermap - https://github.com/indexmap-rs/indexmap/issues/153#issuecomment-2189804150
#[derive(Clone, Eq, SerializeDisplay, DeserializeFromStr, schemars::JsonSchema)]
#[schemars(with = "String")]
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
    Record(IndexMap<TypeKey, TypeWrapper>), // TODO: use ordermap, ordering of keys matter!
    Variant(IndexMap<TypeKey, Option<TypeWrapper>>), // TODO: use ordermap, ordering of keys matter!
    List(Box<TypeWrapper>),
    Tuple(Box<[TypeWrapper]>),
    Enum(IndexSet<TypeKey>),
    Option(Box<TypeWrapper>),
    Result {
        ok: Option<Box<TypeWrapper>>,
        err: Option<Box<TypeWrapper>>,
    },
    Flags(IndexSet<TypeKey>),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Hash, schemars::JsonSchema)]
#[schemars(with = "String")]
pub struct TypeKey(Box<str>);
impl TypeKey {
    pub fn new_kebab(s: impl Into<Box<str>>) -> Self {
        let s = s.into();
        assert!(!s.contains('_'));
        TypeKey(s)
    }
    #[must_use]
    pub fn from_snake(s: &str) -> Self {
        Self(s.replace('_', "-").into())
    }
    #[must_use]
    pub fn as_kebab_str(&self) -> &str {
        &self.0
    }
    #[must_use]
    pub fn to_snake_string(&self) -> String {
        self.0.replace('-', "_")
    }
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
        }
    }
}

impl Debug for TypeWrapper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }
}

impl Display for TypeWrapper {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Bool => write!(f, "bool"),
            Self::S8 => write!(f, "s8"),
            Self::U8 => write!(f, "u8"),
            Self::S16 => write!(f, "s16"),
            Self::U16 => write!(f, "u16"),
            Self::S32 => write!(f, "s32"),
            Self::U32 => write!(f, "u32"),
            Self::S64 => write!(f, "s64"),
            Self::U64 => write!(f, "u64"),
            Self::F32 => write!(f, "f32"),
            Self::F64 => write!(f, "f64"),
            Self::Char => write!(f, "char"),
            Self::String => write!(f, "string"),
            Self::Own => write!(f, "own"),
            Self::Borrow => write!(f, "borrow"),
            Self::List(inner) => write!(f, "list<{inner}>"),
            Self::Option(inner) => write!(f, "option<{inner}>"),
            Self::Tuple(items) => {
                write!(f, "tuple<")?;
                for (i, item) in items.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{item}")?;
                }
                write!(f, ">")
            }
            Self::Result { ok, err } => match (ok, err) {
                (None, None) => write!(f, "result"),
                (Some(ok), None) => write!(f, "result<{ok}>"),
                (None, Some(err)) => write!(f, "result<_, {err}>"),
                (Some(ok), Some(err)) => write!(f, "result<{ok}, {err}>"),
            },
            Self::Record(fields) => {
                write!(f, "record {{")?;
                for (i, (key, ty)) in fields.iter().enumerate() {
                    if i > 0 {
                        write!(f, ",")?;
                    }
                    write!(f, " {}: {ty}", key.as_kebab_str())?;
                }
                if !fields.is_empty() {
                    write!(f, " ")?;
                }
                write!(f, "}}")
            }
            Self::Variant(cases) => {
                write!(f, "variant {{")?;
                for (i, (key, payload)) in cases.iter().enumerate() {
                    if i > 0 {
                        write!(f, ",")?;
                    }
                    write!(f, " {}", key.as_kebab_str())?;
                    if let Some(ty) = payload {
                        write!(f, "({ty})")?;
                    }
                }
                if !cases.is_empty() {
                    write!(f, " ")?;
                }
                write!(f, "}}")
            }
            Self::Enum(cases) => {
                write!(f, "enum {{")?;
                for (i, key) in cases.iter().enumerate() {
                    if i > 0 {
                        write!(f, ",")?;
                    }
                    write!(f, " {}", key.as_kebab_str())?;
                }
                if !cases.is_empty() {
                    write!(f, " ")?;
                }
                write!(f, "}}")
            }
            Self::Flags(flags) => {
                write!(f, "flags {{")?;
                for (i, key) in flags.iter().enumerate() {
                    if i > 0 {
                        write!(f, ",")?;
                    }
                    write!(f, " {}", key.as_kebab_str())?;
                }
                if !flags.is_empty() {
                    write!(f, " ")?;
                }
                write!(f, "}}")
            }
        }
    }
}

impl FromStr for TypeWrapper {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        parse_wit_type(s)
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
                    .map(|field| {
                        Self::try_from(field.ty).map(|ty| (TypeKey(Box::from(field.name)), ty))
                    })
                    .collect::<Result<_, _>>()?;
                Ok(Self::Record(map))
            }
            Type::Variant(variant) => {
                let map = variant
                    .cases()
                    .map(|case| {
                        let key = TypeKey::new_kebab(case.name);
                        if let Some(ty) = case.ty {
                            Self::try_from(ty).map(|ty| (key, Some(ty)))
                        } else {
                            Ok((key, None))
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
            Type::Enum(names) => Ok(Self::Enum(names.names().map(TypeKey::new_kebab).collect())),
            Type::Borrow(_) => Ok(Self::Borrow),
            Type::Own(_) => Ok(Self::Own),
            Type::Flags(flags) => Ok(Self::Flags(flags.names().map(TypeKey::new_kebab).collect())),
            Type::Future(_) => Err(TypeConversionError::UnsupportedType("future")),
            Type::Stream(_) => Err(TypeConversionError::UnsupportedType("stream")),
            Type::ErrorContext => Err(TypeConversionError::UnsupportedType("error-context")),
            Type::Map(_) => Err(TypeConversionError::UnsupportedType("map")),
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
                                    .map(|ty| (TypeKey(Box::from(field.name.clone())), ty))
                            })
                            .collect::<Result<_, _>>()?;
                        Ok(TypeWrapper::Record(map))
                    }
                    TypeDefKind::Flags(flags) => Ok(TypeWrapper::Flags(
                        flags
                            .flags
                            .iter()
                            .map(|f| TypeKey::new_kebab(f.name.clone()))
                            .collect(),
                    )),
                    TypeDefKind::Enum(en) => Ok(TypeWrapper::Enum(
                        en.cases
                            .iter()
                            .map(|case| TypeKey::new_kebab(case.name.clone()))
                            .collect(),
                    )),
                    TypeDefKind::Variant(variant) => {
                        let map = variant
                            .cases
                            .iter()
                            .map(|case| {
                                let key = TypeKey::new_kebab(case.name.clone());
                                if let Some(ty) = &case.ty {
                                    TypeWrapper::from_wit_parser_type(resolve, ty)
                                        .map(|ty| (key, Some(ty)))
                                } else {
                                    Ok((key, None))
                                }
                            })
                            .collect::<Result<_, _>>()?;
                        Ok(TypeWrapper::Variant(map))
                    }
                    TypeDefKind::List(inner) => Ok(TypeWrapper::List(Box::new(
                        TypeWrapper::from_wit_parser_type(resolve, inner)?,
                    ))),
                    TypeDefKind::Map(_, _) => Err(TypeConversionError::UnsupportedType("Map")),
                    TypeDefKind::FixedLengthList(_inner, _size) => {
                        Err(TypeConversionError::UnsupportedType("FixedSizeList"))
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

/// Parse a WIT type syntax string into a `TypeWrapper`.
///
/// Supports primitives (`bool`, `u8`..`u64`, `s8`..`s64`, `f32`, `f64`, `char`, `string`),
/// `list<T>`, `option<T>`, `tuple<T1, T2, ...>`, and `result` variants.
pub fn parse_wit_type(s: &str) -> Result<TypeWrapper, String> {
    let s = s.trim();
    let (ty, rest) = parse_type(s)?;
    let rest = rest.trim();
    if !rest.is_empty() {
        return Err(format!("unexpected trailing characters: '{rest}'"));
    }
    Ok(ty)
}

fn parse_type(s: &str) -> Result<(TypeWrapper, &str), String> {
    let s = s.trim();
    if s.is_empty() {
        return Err("unexpected end of input".to_string());
    }

    let ident_end = s
        .find(|c: char| !c.is_ascii_alphanumeric() && c != '-' && c != '_')
        .unwrap_or(s.len());
    let ident = &s[..ident_end];
    let rest = &s[ident_end..];

    match ident {
        "bool" => Ok((TypeWrapper::Bool, rest)),
        "s8" => Ok((TypeWrapper::S8, rest)),
        "u8" => Ok((TypeWrapper::U8, rest)),
        "s16" => Ok((TypeWrapper::S16, rest)),
        "u16" => Ok((TypeWrapper::U16, rest)),
        "s32" => Ok((TypeWrapper::S32, rest)),
        "u32" => Ok((TypeWrapper::U32, rest)),
        "s64" => Ok((TypeWrapper::S64, rest)),
        "u64" => Ok((TypeWrapper::U64, rest)),
        "f32" => Ok((TypeWrapper::F32, rest)),
        "f64" => Ok((TypeWrapper::F64, rest)),
        "char" => Ok((TypeWrapper::Char, rest)),
        "string" => Ok((TypeWrapper::String, rest)),

        "list" => {
            let rest = expect_char(rest.trim_start(), '<')?;
            let (inner, rest) = parse_type(rest)?;
            let rest = expect_char(rest.trim_start(), '>')?;
            Ok((TypeWrapper::List(Box::new(inner)), rest))
        }

        "option" => {
            let rest = expect_char(rest.trim_start(), '<')?;
            let (inner, rest) = parse_type(rest)?;
            let rest = expect_char(rest.trim_start(), '>')?;
            Ok((TypeWrapper::Option(Box::new(inner)), rest))
        }

        "tuple" => {
            let rest = expect_char(rest.trim_start(), '<')?;
            let (items, rest) = parse_comma_separated_types(rest, '>')?;
            Ok((TypeWrapper::Tuple(items.into_boxed_slice()), rest))
        }

        "result" => {
            let rest_trimmed = rest.trim_start();
            if !rest_trimmed.starts_with('<') {
                return Ok((
                    TypeWrapper::Result {
                        ok: None,
                        err: None,
                    },
                    rest,
                ));
            }
            let rest = expect_char(rest_trimmed, '<')?;
            let rest_trimmed = rest.trim_start();

            let (ok, rest) = if let Some(stripped) = rest_trimmed.strip_prefix('_') {
                (None, stripped)
            } else {
                let (ty, rest) = parse_type(rest)?;
                (Some(Box::new(ty)), rest)
            };

            let rest_trimmed = rest.trim_start();

            if let Some(rest) = rest_trimmed.strip_prefix(',') {
                let (err_ty, rest) = parse_type(rest)?;
                let rest = expect_char(rest.trim_start(), '>')?;
                Ok((
                    TypeWrapper::Result {
                        ok,
                        err: Some(Box::new(err_ty)),
                    },
                    rest,
                ))
            } else {
                let rest = expect_char(rest_trimmed, '>')?;
                Ok((TypeWrapper::Result { ok, err: None }, rest))
            }
        }

        "variant" => {
            let rest = expect_char(rest.trim_start(), '{')?;
            let (cases, rest) = parse_variant_cases(rest)?;
            Ok((TypeWrapper::Variant(cases), rest))
        }

        "enum" => {
            let rest = expect_char(rest.trim_start(), '{')?;
            let (cases, rest) = parse_identifier_list(rest)?;
            Ok((TypeWrapper::Enum(cases), rest))
        }

        "record" => {
            let rest = expect_char(rest.trim_start(), '{')?;
            let (fields, rest) = parse_record_fields(rest)?;
            Ok((TypeWrapper::Record(fields), rest))
        }

        "flags" => {
            let rest = expect_char(rest.trim_start(), '{')?;
            let (flags, rest) = parse_identifier_list(rest)?;
            Ok((TypeWrapper::Flags(flags), rest))
        }

        _ => Err(format!("unknown type: '{ident}'")),
    }
}

fn expect_char(s: &str, expected: char) -> Result<&str, String> {
    let s = s.trim_start();
    if s.starts_with(expected) {
        Ok(&s[expected.len_utf8()..])
    } else {
        let found = s
            .chars()
            .next()
            .map_or("end of input".to_string(), |c| format!("'{c}'"));
        Err(format!("expected '{expected}', found {found}"))
    }
}

fn parse_comma_separated_types(s: &str, closing: char) -> Result<(Vec<TypeWrapper>, &str), String> {
    let mut items = Vec::new();
    let mut rest = s.trim_start();

    if rest.starts_with(closing) {
        return Ok((items, &rest[closing.len_utf8()..]));
    }

    loop {
        let (ty, r) = parse_type(rest)?;
        items.push(ty);
        rest = r.trim_start();

        if rest.starts_with(',') {
            rest = rest[1..].trim_start();
        } else if rest.starts_with(closing) {
            rest = &rest[closing.len_utf8()..];
            break;
        } else {
            let found = rest
                .chars()
                .next()
                .map_or("end of input".to_string(), |c| format!("'{c}'"));
            return Err(format!("expected ',' or '{closing}', found {found}"));
        }
    }

    Ok((items, rest))
}

/// Parse a kebab-case identifier (letters, digits, hyphens).
fn parse_identifier(s: &str) -> Result<(&str, &str), String> {
    let s = s.trim_start();
    if s.is_empty() {
        return Err("unexpected end of input, expected identifier".to_string());
    }

    let ident_end = s
        .find(|c: char| !c.is_ascii_alphanumeric() && c != '-')
        .unwrap_or(s.len());

    if ident_end == 0 {
        let found = s.chars().next().unwrap();
        return Err(format!("expected identifier, found '{found}'"));
    }

    Ok((&s[..ident_end], &s[ident_end..]))
}

/// Parse variant cases: `{ case1, case2(type), case3 }`
fn parse_variant_cases(s: &str) -> Result<(IndexMap<TypeKey, Option<TypeWrapper>>, &str), String> {
    let mut cases = IndexMap::new();
    let mut rest = s.trim_start();

    if let Some(stripped) = rest.strip_prefix('}') {
        return Ok((cases, stripped));
    }

    loop {
        let (ident, r) = parse_identifier(rest)?;
        let key = TypeKey::new_kebab(ident);
        rest = r.trim_start();

        // Check for optional payload: case-name(type)
        let payload = if rest.starts_with('(') {
            rest = &rest[1..];
            let (ty, r) = parse_type(rest)?;
            rest = expect_char(r.trim_start(), ')')?;
            Some(ty)
        } else {
            None
        };

        cases.insert(key, payload);
        rest = rest.trim_start();

        if rest.starts_with(',') {
            rest = rest[1..].trim_start();
        } else if rest.starts_with('}') {
            rest = &rest[1..];
            break;
        } else {
            let found = rest
                .chars()
                .next()
                .map_or("end of input".to_string(), |c| format!("'{c}'"));
            return Err(format!("expected ',' or '}}', found {found}"));
        }
    }

    Ok((cases, rest))
}

/// Parse a comma-separated list of identifiers: `{ ident1, ident2 }`
fn parse_identifier_list(s: &str) -> Result<(IndexSet<TypeKey>, &str), String> {
    let mut items = IndexSet::new();
    let mut rest = s.trim_start();

    if let Some(stripped) = rest.strip_prefix('}') {
        return Ok((items, stripped));
    }

    loop {
        let (ident, r) = parse_identifier(rest)?;
        items.insert(TypeKey::new_kebab(ident));
        rest = r.trim_start();

        if rest.starts_with(',') {
            rest = rest[1..].trim_start();
        } else if rest.starts_with('}') {
            rest = &rest[1..];
            break;
        } else {
            let found = rest
                .chars()
                .next()
                .map_or("end of input".to_string(), |c| format!("'{c}'"));
            return Err(format!("expected ',' or '}}', found {found}"));
        }
    }

    Ok((items, rest))
}

/// Parse record fields: `{ field1: type1, field2: type2 }`
fn parse_record_fields(s: &str) -> Result<(IndexMap<TypeKey, TypeWrapper>, &str), String> {
    let mut fields = IndexMap::new();
    let mut rest = s.trim_start();

    if let Some(stripped) = rest.strip_prefix('}') {
        return Ok((fields, stripped));
    }

    loop {
        let (ident, r) = parse_identifier(rest)?;
        let key = TypeKey::new_kebab(ident);
        rest = expect_char(r.trim_start(), ':')?;
        let (ty, r) = parse_type(rest)?;
        fields.insert(key, ty);
        rest = r.trim_start();

        if rest.starts_with(',') {
            rest = rest[1..].trim_start();
        } else if rest.starts_with('}') {
            rest = &rest[1..];
            break;
        } else {
            let found = rest
                .chars()
                .next()
                .map_or("end of input".to_string(), |c| format!("'{c}'"));
            return Err(format!("expected ',' or '}}', found {found}"));
        }
    }

    Ok((fields, rest))
}

#[cfg(test)]
mod tests {
    use super::TypeWrapper;
    use crate::type_wrapper::TypeKey;
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
    fn serde_roundtrip_wit_format() {
        let ty = TypeWrapper::Record(indexmap! {
            TypeKey(Box::from("logins")) => TypeWrapper::String,
            TypeKey(Box::from("cursor")) => TypeWrapper::String,
        });
        let json = serde_json::to_string(&ty).unwrap();
        assert_eq!(json, r#""record { logins: string, cursor: string }""#);
        let deser: TypeWrapper = serde_json::from_str(&json).unwrap();
        assert_eq!(ty, deser);
    }

    #[test]
    fn deser_should_preserve_its_attribute_order() {
        let json = r#""record { logins: string, cursor: string }""#;
        let deser: TypeWrapper = serde_json::from_str(json).unwrap();
        let fields = assert_matches!(deser, TypeWrapper::Record(fields) => fields);
        let expected = indexmap! {
            TypeKey(Box::from("logins")) => TypeWrapper::String,
            TypeKey(Box::from("cursor")) => TypeWrapper::String,
        };
        assert_eq!(expected, fields);
        assert_eq!(
            vec!["logins", "cursor"],
            fields.keys().map(TypeKey::as_kebab_str).collect_vec()
        );
    }

    mod tests_parse_wit_type {
        use crate::type_wrapper::{TypeKey, TypeWrapper, parse_wit_type};
        use indexmap::{IndexMap, IndexSet};

        #[test]
        fn primitives() {
            assert_eq!(parse_wit_type("bool").unwrap(), TypeWrapper::Bool);
            assert_eq!(parse_wit_type("u32").unwrap(), TypeWrapper::U32);
            assert_eq!(parse_wit_type("string").unwrap(), TypeWrapper::String);
            assert_eq!(parse_wit_type("f64").unwrap(), TypeWrapper::F64);
        }

        #[test]
        fn list() {
            assert_eq!(
                parse_wit_type("list<string>").unwrap(),
                TypeWrapper::List(Box::new(TypeWrapper::String))
            );
            assert_eq!(
                parse_wit_type("list<list<u8>>").unwrap(),
                TypeWrapper::List(Box::new(TypeWrapper::List(Box::new(TypeWrapper::U8))))
            );
        }

        #[test]
        fn option() {
            assert_eq!(
                parse_wit_type("option<u32>").unwrap(),
                TypeWrapper::Option(Box::new(TypeWrapper::U32))
            );
        }

        #[test]
        fn tuple() {
            assert_eq!(
                parse_wit_type("tuple<u32, string>").unwrap(),
                TypeWrapper::Tuple(vec![TypeWrapper::U32, TypeWrapper::String].into_boxed_slice())
            );
        }

        #[test]
        fn result_variants() {
            assert_eq!(
                parse_wit_type("result<string, string>").unwrap(),
                TypeWrapper::Result {
                    ok: Some(Box::new(TypeWrapper::String)),
                    err: Some(Box::new(TypeWrapper::String)),
                }
            );
            assert_eq!(
                parse_wit_type("result<string>").unwrap(),
                TypeWrapper::Result {
                    ok: Some(Box::new(TypeWrapper::String)),
                    err: None,
                }
            );
            assert_eq!(
                parse_wit_type("result").unwrap(),
                TypeWrapper::Result {
                    ok: None,
                    err: None,
                }
            );
            assert_eq!(
                parse_wit_type("result<_, string>").unwrap(),
                TypeWrapper::Result {
                    ok: None,
                    err: Some(Box::new(TypeWrapper::String)),
                }
            );
        }

        #[test]
        fn trailing_chars() {
            assert!(parse_wit_type("u32 extra").is_err());
        }

        #[test]
        fn unknown_type() {
            assert!(parse_wit_type("foobar").is_err());
        }

        #[test]
        fn variant_empty() {
            assert_eq!(
                parse_wit_type("variant {}").unwrap(),
                TypeWrapper::Variant(IndexMap::new())
            );
        }

        #[test]
        fn variant_simple() {
            use indexmap::indexmap;
            assert_eq!(
                parse_wit_type("variant { first, second }").unwrap(),
                TypeWrapper::Variant(indexmap! {
                    TypeKey::new_kebab("first") => None,
                    TypeKey::new_kebab("second") => None,
                })
            );
        }

        #[test]
        fn variant_with_payloads() {
            use indexmap::indexmap;
            assert_eq!(
                parse_wit_type("variant { first, second(string), third(u32) }").unwrap(),
                TypeWrapper::Variant(indexmap! {
                    TypeKey::new_kebab("first") => None,
                    TypeKey::new_kebab("second") => Some(TypeWrapper::String),
                    TypeKey::new_kebab("third") => Some(TypeWrapper::U32),
                })
            );
        }

        #[test]
        fn variant_nested_payload() {
            use indexmap::indexmap;
            assert_eq!(
                parse_wit_type("variant { ok(list<string>), err(option<u32>) }").unwrap(),
                TypeWrapper::Variant(indexmap! {
                    TypeKey::new_kebab("ok") => Some(TypeWrapper::List(Box::new(TypeWrapper::String))),
                    TypeKey::new_kebab("err") => Some(TypeWrapper::Option(Box::new(TypeWrapper::U32))),
                })
            );
        }

        #[test]
        fn enum_empty() {
            assert_eq!(
                parse_wit_type("enum {}").unwrap(),
                TypeWrapper::Enum(IndexSet::new())
            );
        }

        #[test]
        fn enum_simple() {
            use indexmap::indexset;
            assert_eq!(
                parse_wit_type("enum { red, green, blue }").unwrap(),
                TypeWrapper::Enum(indexset! {
                    TypeKey::new_kebab("red"),
                    TypeKey::new_kebab("green"),
                    TypeKey::new_kebab("blue"),
                })
            );
        }

        #[test]
        fn record_empty() {
            assert_eq!(
                parse_wit_type("record {}").unwrap(),
                TypeWrapper::Record(IndexMap::new())
            );
        }

        #[test]
        fn record_simple() {
            use indexmap::indexmap;
            assert_eq!(
                parse_wit_type("record { name: string, age: u32 }").unwrap(),
                TypeWrapper::Record(indexmap! {
                    TypeKey::new_kebab("name") => TypeWrapper::String,
                    TypeKey::new_kebab("age") => TypeWrapper::U32,
                })
            );
        }

        #[test]
        fn record_nested() {
            use indexmap::indexmap;
            assert_eq!(
                parse_wit_type("record { items: list<string>, count: option<u64> }").unwrap(),
                TypeWrapper::Record(indexmap! {
                    TypeKey::new_kebab("items") => TypeWrapper::List(Box::new(TypeWrapper::String)),
                    TypeKey::new_kebab("count") => TypeWrapper::Option(Box::new(TypeWrapper::U64)),
                })
            );
        }

        #[test]
        fn flags_empty() {
            assert_eq!(
                parse_wit_type("flags {}").unwrap(),
                TypeWrapper::Flags(IndexSet::new())
            );
        }

        #[test]
        fn flags_simple() {
            use indexmap::indexset;
            assert_eq!(
                parse_wit_type("flags { read, write, execute }").unwrap(),
                TypeWrapper::Flags(indexset! {
                    TypeKey::new_kebab("read"),
                    TypeKey::new_kebab("write"),
                    TypeKey::new_kebab("execute"),
                })
            );
        }

        #[test]
        fn kebab_case_identifiers() {
            use indexmap::indexmap;
            assert_eq!(
                parse_wit_type("record { first-name: string, last-name: string }").unwrap(),
                TypeWrapper::Record(indexmap! {
                    TypeKey::new_kebab("first-name") => TypeWrapper::String,
                    TypeKey::new_kebab("last-name") => TypeWrapper::String,
                })
            );
        }
    }

    mod tests_display {
        use super::super::*;
        use indexmap::{indexmap, indexset};

        #[test]
        fn primitives() {
            assert_eq!(TypeWrapper::Bool.to_string(), "bool");
            assert_eq!(TypeWrapper::U32.to_string(), "u32");
            assert_eq!(TypeWrapper::String.to_string(), "string");
            assert_eq!(TypeWrapper::F64.to_string(), "f64");
        }

        #[test]
        fn list() {
            assert_eq!(
                TypeWrapper::List(Box::new(TypeWrapper::String)).to_string(),
                "list<string>"
            );
        }

        #[test]
        fn option() {
            assert_eq!(
                TypeWrapper::Option(Box::new(TypeWrapper::U32)).to_string(),
                "option<u32>"
            );
        }

        #[test]
        fn tuple() {
            assert_eq!(
                TypeWrapper::Tuple(vec![TypeWrapper::U32, TypeWrapper::String].into_boxed_slice())
                    .to_string(),
                "tuple<u32, string>"
            );
        }

        #[test]
        fn result_variants() {
            assert_eq!(
                TypeWrapper::Result {
                    ok: Some(Box::new(TypeWrapper::String)),
                    err: Some(Box::new(TypeWrapper::U32)),
                }
                .to_string(),
                "result<string, u32>"
            );
            assert_eq!(
                TypeWrapper::Result {
                    ok: Some(Box::new(TypeWrapper::String)),
                    err: None,
                }
                .to_string(),
                "result<string>"
            );
            assert_eq!(
                TypeWrapper::Result {
                    ok: None,
                    err: None,
                }
                .to_string(),
                "result"
            );
            assert_eq!(
                TypeWrapper::Result {
                    ok: None,
                    err: Some(Box::new(TypeWrapper::String)),
                }
                .to_string(),
                "result<_, string>"
            );
        }

        #[test]
        fn variant() {
            let v = TypeWrapper::Variant(indexmap! {
                TypeKey::new_kebab("first") => None,
                TypeKey::new_kebab("second") => Some(TypeWrapper::String),
            });
            assert_eq!(v.to_string(), "variant { first, second(string) }");
        }

        #[test]
        fn enum_display() {
            let e = TypeWrapper::Enum(indexset! {
                TypeKey::new_kebab("red"),
                TypeKey::new_kebab("green"),
            });
            assert_eq!(e.to_string(), "enum { red, green }");
        }

        #[test]
        fn record() {
            let r = TypeWrapper::Record(indexmap! {
                TypeKey::new_kebab("name") => TypeWrapper::String,
                TypeKey::new_kebab("age") => TypeWrapper::U32,
            });
            assert_eq!(r.to_string(), "record { name: string, age: u32 }");
        }

        #[test]
        fn flags() {
            let f = TypeWrapper::Flags(indexset! {
                TypeKey::new_kebab("read"),
                TypeKey::new_kebab("write"),
            });
            assert_eq!(f.to_string(), "flags { read, write }");
        }

        #[test]
        fn empty_containers() {
            assert_eq!(TypeWrapper::Variant(indexmap! {}).to_string(), "variant {}");
            assert_eq!(TypeWrapper::Enum(indexset! {}).to_string(), "enum {}");
            assert_eq!(TypeWrapper::Record(indexmap! {}).to_string(), "record {}");
            assert_eq!(TypeWrapper::Flags(indexset! {}).to_string(), "flags {}");
        }
    }

    mod tests_roundtrip {
        use super::super::*;

        fn roundtrip(input: &str) {
            let parsed: TypeWrapper = input.parse().unwrap();
            let displayed = parsed.to_string();
            let reparsed: TypeWrapper = displayed.parse().unwrap();
            assert_eq!(parsed, reparsed, "roundtrip failed for: {input}");
        }

        #[test]
        fn primitives() {
            for s in [
                "bool", "u8", "u16", "u32", "u64", "s8", "s16", "s32", "s64", "f32", "f64", "char",
                "string",
            ] {
                roundtrip(s);
            }
        }

        #[test]
        fn generic_types() {
            roundtrip("list<string>");
            roundtrip("list<list<u8>>");
            roundtrip("option<u32>");
            roundtrip("tuple<u32, string, bool>");
            roundtrip("result");
            roundtrip("result<string>");
            roundtrip("result<string, u32>");
            roundtrip("result<_, string>");
        }

        #[test]
        fn anonymous_types() {
            roundtrip("variant { first, second(string) }");
            roundtrip("enum { red, green, blue }");
            roundtrip("record { name: string, age: u32 }");
            roundtrip("flags { read, write, execute }");
        }

        #[test]
        fn nested_anonymous() {
            roundtrip("record { items: list<variant { ok(string), err }>, count: u32 }");
            roundtrip(
                "variant { success(record { data: string }), failure(enum { timeout, error }) }",
            );
        }
    }

    mod tests_from_str {
        use super::super::*;

        #[test]
        fn from_str_works() {
            let ty: TypeWrapper = "list<string>".parse().unwrap();
            assert_eq!(ty, TypeWrapper::List(Box::new(TypeWrapper::String)));
        }

        #[test]
        fn from_str_error() {
            let result: Result<TypeWrapper, _> = "unknown_type".parse();
            assert!(result.is_err());
        }
    }
}
