//! TOML configuration for the obelisk binary, split by concern:
//! - [`authored`]: the user-authored deployment manifest and its validation.
//! - [`resolve`]: resolving + fetching/verifying a manifest into runtime-ready configs.
//! - [`server`]: server/runtime config (`obelisk.toml`), orthogonal to the manifest.
//! - [`common`]: serde data-shape primitives shared across the stages.
//!
//! This module is only module wiring: it declares the submodules and re-exports them so
//! `crate::config::toml::<Type>` paths stay stable.

mod authored;
mod common;
mod resolve;
mod server;
pub(crate) use authored::*;
pub(crate) use common::*;
pub(crate) use resolve::*;
pub(crate) use server::*;

#[cfg(test)]
mod tests;
