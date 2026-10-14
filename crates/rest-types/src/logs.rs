use base64::{Engine as _, prelude::BASE64_STANDARD};
use chrono::{DateTime, Utc};
use concepts::{
    prefixed_ulid::RunId,
    storage::{LogEntry, LogEntryRow, LogLevel, LogStreamType},
};
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use utoipa::ToSchema;

/// Log entry row with cursor
#[derive(Serialize, Deserialize, ToSchema)]
pub struct LogEntryRowSer {
    /// Opaque cursor position
    pub cursor: String,
    /// Run ID that produced this log
    #[schema(value_type = String)]
    pub run_id: RunId,
    /// Execution ID that produced this log
    pub execution_id: String,
    /// Log entry details
    #[serde(flatten)]
    pub info: LogEntrySer,
}

/// Log entry content
#[derive(Serialize, Deserialize, ToSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum LogEntrySer {
    /// Structured log message
    Log {
        created_at: DateTime<Utc>,
        level: LogLevelSer,
        message: String,
    },
    /// Raw stream output (stdout/stderr)
    Stream {
        created_at: DateTime<Utc>,
        /// Base64 encoded payload
        payload: String,
        stream_type: LogStreamTypeSer,
    },
}

impl From<LogEntryRow> for LogEntryRowSer {
    fn from(row: LogEntryRow) -> Self {
        Self {
            cursor: BASE64_STANDARD
                .encode(serde_json::to_vec(&row.cursor).expect("log cursor must serialize")),
            run_id: row.run_id,
            execution_id: row.execution_id.to_string(),
            info: match row.log_entry {
                LogEntry::Log {
                    created_at,
                    level,
                    message,
                } => LogEntrySer::Log {
                    created_at,
                    level: level.into(),
                    message,
                },
                LogEntry::Stream {
                    created_at,
                    payload,
                    stream_type,
                } => LogEntrySer::Stream {
                    created_at,
                    payload: BASE64_STANDARD.encode(payload),
                    stream_type: stream_type.into(),
                },
            },
        }
    }
}

#[derive(Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum LogLevelSer {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

impl Display for LogLevelSer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.pad(match self {
            Self::Trace => "TRACE",
            Self::Debug => "DEBUG",
            Self::Info => "INFO",
            Self::Warn => "WARN",
            Self::Error => "ERROR",
        })
    }
}

impl From<LogLevel> for LogLevelSer {
    fn from(value: LogLevel) -> Self {
        match value {
            LogLevel::Trace => Self::Trace,
            LogLevel::Debug => Self::Debug,
            LogLevel::Info => Self::Info,
            LogLevel::Warn => Self::Warn,
            LogLevel::Error => Self::Error,
        }
    }
}

#[derive(Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum LogStreamTypeSer {
    Stdout,
    Stderr,
}

impl Display for LogStreamTypeSer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.pad(match self {
            Self::Stdout => "STDOUT",
            Self::Stderr => "STDERR",
        })
    }
}

impl From<LogStreamType> for LogStreamTypeSer {
    fn from(value: LogStreamType) -> Self {
        match value {
            LogStreamType::StdOut => Self::Stdout,
            LogStreamType::StdErr => Self::Stderr,
        }
    }
}
