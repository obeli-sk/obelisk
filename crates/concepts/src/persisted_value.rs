use crate::{SupportedFunctionReturnValue, component_id::Digest};
use serde::Serialize;
use sha2::{Digest as _, Sha256};
use std::io;

pub const DEFAULT_MAX_PERSISTED_VALUE_SIZE_BYTES: u64 = 1024 * 1024;
pub const PERSISTED_EVENT_OVERHEAD_BYTES: u64 = 64 * 1024;

#[must_use]
pub fn compact_json_sha256<T: Serialize + ?Sized>(value: &T) -> Digest {
    struct HashWriter(Sha256);

    impl io::Write for HashWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0.update(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    let mut writer = HashWriter(Sha256::new());
    serde_json::to_writer(&mut writer, value)
        .unwrap_or_else(|err| panic!("persisted value serialization failed: {err}"));
    Digest(writer.0.finalize().into())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodedSizeAndDigest {
    pub encoded_size: Result<u64, EncodedSizeExceeded>,
    pub sha256: Digest,
}

// backcompat: 0.41 created events without a limit remain unlimited during replay.
#[must_use]
pub const fn legacy_unlimited_persisted_value_size() -> u64 {
    u64::MAX
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EncodedSizeLimit(u64);

impl EncodedSizeLimit {
    pub const LEGACY_UNLIMITED: Self = Self(u64::MAX);

    #[must_use]
    pub const fn new(limit: u64) -> Option<Self> {
        if limit == 0 { None } else { Some(Self(limit)) }
    }

    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    pub fn validate<T: Serialize + ?Sized>(self, value: &T) -> Result<u64, EncodedSizeExceeded> {
        let mut writer = LimitWriter::new(self.0);
        match serde_json::to_writer(&mut writer, value) {
            Ok(()) => Ok(writer.written),
            Err(_err) if writer.exceeded => Err(EncodedSizeExceeded {
                limit: self.0,
                encoded_size_at_least: self.0.saturating_add(1),
            }),
            Err(err) => panic!("persisted value serialization failed: {err}"),
        }
    }

    pub fn validate_and_hash<T: Serialize + ?Sized>(self, value: &T) -> EncodedSizeAndDigest {
        struct Writer {
            limit: u64,
            written: u64,
            exceeded: bool,
            hasher: Sha256,
        }

        impl io::Write for Writer {
            fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
                self.hasher.update(buf);
                if !self.exceeded {
                    let next = self.written.saturating_add(buf.len() as u64);
                    if next > self.limit {
                        self.written = self.limit.saturating_add(1);
                        self.exceeded = true;
                    } else {
                        self.written = next;
                    }
                }
                Ok(buf.len())
            }

            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }

        let mut writer = Writer {
            limit: self.0,
            written: 0,
            exceeded: false,
            hasher: Sha256::new(),
        };
        serde_json::to_writer(&mut writer, value)
            .unwrap_or_else(|err| panic!("persisted value serialization failed: {err}"));
        EncodedSizeAndDigest {
            encoded_size: if writer.exceeded {
                Err(EncodedSizeExceeded {
                    limit: self.0,
                    encoded_size_at_least: writer.written,
                })
            } else {
                Ok(writer.written)
            },
            sha256: Digest(writer.hasher.finalize().into()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("encoded value exceeds the {limit}-byte persisted value limit")]
pub struct EncodedSizeExceeded {
    pub limit: u64,
    pub encoded_size_at_least: u64,
}

#[must_use]
pub fn enforce_return_value_limit(
    value: SupportedFunctionReturnValue,
    limit: u64,
) -> SupportedFunctionReturnValue {
    let encoded_size_limit =
        EncodedSizeLimit::new(limit).unwrap_or(EncodedSizeLimit::LEGACY_UNLIMITED);
    match encoded_size_limit.validate(&value) {
        Ok(_) => value,
        Err(_) => SupportedFunctionReturnValue::value_too_large(limit),
    }
}

struct LimitWriter {
    limit: u64,
    written: u64,
    exceeded: bool,
}

impl LimitWriter {
    const fn new(limit: u64) -> Self {
        Self {
            limit,
            written: 0,
            exceeded: false,
        }
    }
}

impl io::Write for LimitWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let next = self.written.saturating_add(buf.len() as u64);
        if next > self.limit {
            self.exceeded = true;
            return Err(io::Error::other("persisted value size limit exceeded"));
        }
        self.written = next;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counts_compact_json_encoding_and_stops_at_limit() {
        let escaped = "\0".repeat(2);
        assert_eq!(
            EncodedSizeLimit::new(14).unwrap().validate(&escaped),
            Ok(14)
        );
        assert_eq!(
            EncodedSizeLimit::new(13).unwrap().validate(&escaped),
            Err(EncodedSizeExceeded {
                limit: 13,
                encoded_size_at_least: 14
            })
        );
    }

    #[test]
    fn hashes_compact_json_encoding() {
        let value = serde_json::json!(["a", 1]);
        let expected = Sha256::digest(br#"["a",1]"#);
        assert_eq!(compact_json_sha256(&value), Digest(expected.into()));
    }

    #[test]
    fn validates_and_hashes_complete_oversized_value() {
        let first = serde_json::json!(["abcdef"]);
        let second = serde_json::json!(["abcdeg"]);
        let first_result = EncodedSizeLimit::new(4).unwrap().validate_and_hash(&first);
        let second_result = EncodedSizeLimit::new(4).unwrap().validate_and_hash(&second);
        assert_eq!(
            first_result.encoded_size,
            Err(EncodedSizeExceeded {
                limit: 4,
                encoded_size_at_least: 5,
            })
        );
        assert_ne!(first_result.sha256, second_result.sha256);
    }

    #[test]
    fn replaces_oversized_return_value_with_compact_failure() {
        let value = SupportedFunctionReturnValue::Ok(Some(crate::WastValWithType {
            value: val_json::wast_val::WastVal::String("secret-result".to_owned()),
            r#type: val_json::type_wrapper::TypeWrapper::String,
        }));

        let result = enforce_return_value_limit(value, 20);

        assert_eq!(result, SupportedFunctionReturnValue::value_too_large(20));
        assert!(
            !serde_json::to_string(&result)
                .unwrap()
                .contains("secret-result")
        );
    }
}
