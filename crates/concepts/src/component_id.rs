use crate::StrVariant;
use ::serde::{Deserialize, Serialize};
use std::{
    fmt::{Debug, Display, Write as _},
    hash::Hash,
    marker::PhantomData,
    str::FromStr,
};

#[derive(
    Debug,
    Clone,
    Copy,
    strum::Display,
    PartialEq,
    Eq,
    strum::EnumString,
    Hash,
    serde_with::SerializeDisplay,
    serde_with::DeserializeFromStr,
)]
#[strum(serialize_all = "snake_case")]
pub enum ComponentType {
    ActivityWasm,
    ActivityStub,
    ActivityExternal,
    Workflow,
    WebhookEndpoint,
}
impl ComponentType {
    #[must_use]
    pub fn is_activity(&self) -> bool {
        matches!(
            self,
            ComponentType::ActivityWasm
                | ComponentType::ActivityStub
                | ComponentType::ActivityExternal
        )
    }
}

#[derive(
    derive_more::Debug, Clone, PartialEq, Eq, Hash, derive_more::Display, Serialize, Deserialize,
)]
#[display("{component_type}:{name}")]
#[debug("{}", self)]
#[non_exhaustive] // force using the constructor as much as possible due to validation
pub struct ComponentId {
    pub component_type: ComponentType,
    pub name: StrVariant,
    pub input_digest: InputContentDigest,
}
impl ComponentId {
    pub fn new(
        component_type: ComponentType,
        name: StrVariant,
        input_digest: InputContentDigest,
    ) -> Result<Self, InvalidNameError<Self>> {
        Ok(Self {
            component_type,
            name: check_name(name, "_")?,
            input_digest,
        })
    }

    #[must_use]
    pub const fn dummy_activity() -> Self {
        Self {
            component_type: ComponentType::ActivityWasm,
            name: StrVariant::empty(),
            input_digest: InputContentDigest(CONTENT_DIGEST_DUMMY),
        }
    }

    #[cfg(any(test, feature = "test"))]
    #[must_use]
    pub const fn dummy_workflow() -> ComponentId {
        ComponentId {
            component_type: ComponentType::Workflow,
            name: StrVariant::empty(),
            input_digest: InputContentDigest(CONTENT_DIGEST_DUMMY),
        }
    }
}

pub fn check_name<T>(
    name: StrVariant,
    special: &'static str,
) -> Result<StrVariant, InvalidNameError<T>> {
    if let Some(invalid) = name
        .as_ref()
        .chars()
        .find(|c| !c.is_ascii_alphanumeric() && !special.contains(*c))
    {
        Err(InvalidNameError::<T> {
            invalid,
            name: name.as_ref().to_string(),
            special,
            phantom_data: PhantomData,
        })
    } else {
        Ok(name)
    }
}
#[derive(Debug, thiserror::Error)]
#[error(
    "name of {} `{name}` contains invalid character `{invalid}`, must only contain alphanumeric characters and following characters {special}",
    std::any::type_name::<T>().rsplit("::").next().unwrap()
)]
pub struct InvalidNameError<T> {
    invalid: char,
    name: String,
    special: &'static str,
    phantom_data: PhantomData<T>,
}

#[derive(
    Debug,
    Clone,
    derive_more::Display,
    derive_more::FromStr,
    derive_more::Deref,
    PartialEq,
    Eq,
    Hash,
    serde_with::SerializeDisplay,
    serde_with::DeserializeFromStr,
)]
pub struct InputContentDigest(pub ContentDigest);

#[derive(
    Debug,
    Clone,
    derive_more::Display,
    derive_more::FromStr,
    derive_more::Deref,
    PartialEq,
    Eq,
    Hash,
    serde_with::SerializeDisplay,
    serde_with::DeserializeFromStr,
)]
pub struct ContentDigest(pub Digest);
pub const CONTENT_DIGEST_DUMMY: ContentDigest = ContentDigest(Digest([0; 32]));

#[derive(
    Clone,
    PartialEq,
    Eq,
    Hash,
    serde_with::SerializeDisplay,
    serde_with::DeserializeFromStr,
    derive_more::Deref,
)]
pub struct Digest(pub [u8; 32]); // FIXME: Remove pub, use slice
impl Digest {
    #[must_use]
    fn digest_base16_without_prefix(&self) -> String {
        let mut out = String::with_capacity(self.0.len() * 2);
        for &b in &self.0 {
            write!(&mut out, "{b:02x}").expect("writing to string");
        }
        out
    }

    #[must_use]
    pub fn with_infix(&self, infix: &str) -> String {
        format!("{HASH_TYPE}{infix}{}", self.digest_base16_without_prefix())
    }

    fn parse_without_prefix(hash_base16: &str) -> Result<Digest, DigestParseErrror> {
        if hash_base16.len() != 64 {
            return Err(DigestParseErrror::SuffixHexLength(hash_base16.len()));
        }
        let mut digest = [0u8; 32];
        for i in 0..32 {
            let chunk = &hash_base16[i * 2..i * 2 + 2];
            digest[i] = u8::from_str_radix(chunk, 16).map_err(|_| DigestParseErrror::InvalidHex)?;
        }
        Ok(Digest(digest))
    }
}

impl TryFrom<&[u8]> for Digest {
    type Error = DigestParseErrror;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        if let Ok(value) = value.try_into() {
            Ok(Digest(value))
        } else {
            Err(DigestParseErrror::BinLength(value.len()))
        }
    }
}

const HASH_TYPE: &str = "sha256";
const HASH_TYPE_WITH_DELIMITER: &str = const_format::formatcp!("{}:", HASH_TYPE);
impl Display for Digest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{HASH_TYPE_WITH_DELIMITER}")?;
        for b in self.0 {
            write!(f, "{b:02x}")?;
        }
        Ok(())
    }
}
impl Debug for Digest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self, f)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum DigestParseErrror {
    #[error("cannot parse Digest - invalid prefix")]
    InvalidPrefix,
    #[error("cannot parse Digest - invalid suffix length, expected 64 hex digits, got {0}")]
    SuffixHexLength(usize),
    #[error("cannot parse Digest - suffix must be hex encoded")]
    InvalidHex,
    #[error("cannot parse Digest - expected 32 bytes, got {0}")]
    BinLength(usize),
}

impl FromStr for Digest {
    type Err = DigestParseErrror;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let Some(hash_base16) = input.strip_prefix(HASH_TYPE_WITH_DELIMITER) else {
            return Err(DigestParseErrror::InvalidPrefix);
        };
        Digest::parse_without_prefix(hash_base16)
    }
}
