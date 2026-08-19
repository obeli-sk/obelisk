//! Core naming/id types and the deployment env-var config, shared via `concepts`.
//!
//! The deployment manifest data model lives in the obelisk binary (`config::toml`).

pub mod component_id;
pub mod env_var;
pub mod naming;
#[cfg(feature = "postgres")]
mod postgres_ext;
#[cfg(feature = "rusqlite")]
mod rusqlite_ext;

pub use component_id::{ComponentId, ComponentType, ContentDigest, InvalidNameError, check_name};
pub use naming::{FunctionFqn, StrVariant};
