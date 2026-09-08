use serde::Serialize;
use std::io;

pub const DEFAULT_MAX_PERSISTED_VALUE_SIZE_BYTES: u64 = 1024 * 1024;
pub const PERSISTED_EVENT_OVERHEAD_BYTES: u64 = 64 * 1024;

// backcompat: 0.41 created events without a limit remain unlimited during replay.
pub const fn legacy_unlimited_persisted_value_size() -> u64 {
    u64::MAX
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EncodedSizeLimit(u64);

impl EncodedSizeLimit {
    pub const LEGACY_UNLIMITED: Self = Self(u64::MAX);

    pub const fn new(limit: u64) -> Option<Self> {
        if limit == 0 { None } else { Some(Self(limit)) }
    }

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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("encoded value exceeds the {limit}-byte persisted value limit")]
pub struct EncodedSizeExceeded {
    pub limit: u64,
    pub encoded_size_at_least: u64,
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
}
