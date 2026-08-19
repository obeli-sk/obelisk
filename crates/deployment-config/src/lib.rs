//! Deployment configuration schema, transient resolution types, and core naming types.
//!
//! This crate holds the manifest data model used for TOML parsing and DB storage, plus
//! transient resolved forms used by the local server and `obelisk deployment verify`.
//! Behavior that needs the server runtime (OCI fetching, executor configuration, env var
//! resolution) lives in the obelisk binary.

pub mod component_id;
pub mod config;
pub mod env_var;
pub mod naming;
#[cfg(feature = "postgres")]
mod postgres_ext;
#[cfg(feature = "rusqlite")]
mod rusqlite_ext;

pub use component_id::{ComponentId, ComponentType, ContentDigest, InvalidNameError, check_name};
pub use naming::{FunctionFqn, StrVariant};
