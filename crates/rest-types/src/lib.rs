//! Request and response bodies of the obelisk REST API.

pub mod admin;
pub mod backtrace;
pub mod components;
pub mod deployment;
pub mod executions;
pub mod functions;
pub mod logs;
pub mod replay;

use serde::{Deserialize, Serialize};

/// JSON body of plain-message responses.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum MessageBody {
    Ok { ok: String },
    Err { err: String },
}
