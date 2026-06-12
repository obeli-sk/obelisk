//! Shared deployment configuration schema and core naming types.
//!
//! This crate holds the data model that is shared between the obelisk server
//! (TOML parsing, canonicalization, DB storage) and the webui (parsing
//! `config_json` received over gRPC). It must keep compiling for
//! `wasm32-unknown-unknown`, so it contains plain data types only; all
//! behavior that needs the server runtime (OCI fetching, executor
//! configuration, env var resolution) lives in the obelisk binary.

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
