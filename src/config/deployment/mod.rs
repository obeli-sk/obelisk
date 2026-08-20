//! The deployment config model and its pipeline, one stage per module:
//!
//! 1. **authored** ([`authored`]) — `DeploymentToml`, parsed from the hand-written
//!    `deployment.toml` and validated for unique names / safe paths.
//! 2. **prepared** ([`prepared`]) — `PreparedDeploymentManifest` / `DeploymentManifest`:
//!    the authored TOML enriched with content digests and `component_files`. This is what
//!    the submit RPC receives, validates, and stores.
//! 3. **resolved** ([`resolved`]) — `DeploymentResolved`: every deployment-owned reference
//!    fetched from the CAS (and OCI), used by `deployment verify`.
//! 4. **verified** ([`resolved`]) — the `*ConfigVerified` forms: components fetched and
//!    digests verified, ready for the runtime.
//!
//! The runtime materialization (`DeploymentRunnable`) lives in the server command. Server
//! runtime config (`obelisk.toml`) is orthogonal and lives in [`super::server`]. [`common`]
//! holds the serde data-shape primitives shared across the stages.

mod authored;
mod common;
mod prepared;
mod resolved;
pub(crate) use authored::*;
pub(crate) use common::*;
pub(crate) use prepared::*;
pub(crate) use resolved::*;

#[cfg(test)]
mod tests;
