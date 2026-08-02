//! Config verify pre-pass: aggregate config findings across a whole `server verify`
//! run instead of bailing on the first, and apply the auto-fixable subset under `--fix`.
//!
//! The pre-pass drives the existing `resolve_*` validators and collects their findings;
//! it never re-implements validation, so the resolvers stay the single source of truth.
//! This module owns the "unregistered secret" category and the server-config `--fix`
//! applier; the outbound-HTTP coverage/replacement collectors live next to the pre-pass
//! in `server.rs`. See `meta/designs/config-verify-prepass.md`.

use super::RuntimeConfigAvailability;
use crate::config::secret_registry::SecretRegistry;
use crate::config::toml::{AllowedHostToml, ConfigWarnings};
use anyhow::{Context, bail};
use std::collections::BTreeSet;
use std::path::Path;
use toml_edit::{DocumentMut, Item, Table, value};

/// Append every secret name referenced by `entries` that the operator registry does not
/// know onto `unregistered` (deduplicated). An unregistered name can never be injected,
/// so the operator must register it or drop the reference.
pub(super) fn collect_unregistered_secrets(
    entries: &[AllowedHostToml],
    secret_registry: &SecretRegistry,
    unregistered: &mut BTreeSet<String>,
) {
    for entry in entries {
        for secret in &entry.secrets {
            if secret_registry.secret_lookup(secret).is_none() {
                unregistered.insert(secret.clone());
            }
        }
    }
}

/// Render a paste-able `[secrets]` block scaffolding each name as `X = { env = "X" }`.
/// `env` defaults to the logical name; the operator adjusts it when the source differs.
pub(super) fn secret_scaffold_snippet(names: &BTreeSet<String>) -> String {
    use std::fmt::Write as _;
    let mut snippet = String::from("[secrets]\n");
    for name in names {
        let _ = writeln!(snippet, "{name} = {{ env = \"{name}\" }}");
    }
    snippet
}

/// Emit a single finding covering every unregistered secret `source_desc` references,
/// with a paste-able `[secrets]` snippet. Fatal under strict availability; downgraded to
/// a warning when unavailable runtime config is allowed (activation re-checks strictly).
pub(super) fn report_unregistered_secrets(
    unregistered: &BTreeSet<String>,
    source_desc: &str,
    availability: RuntimeConfigAvailability,
    warnings: &ConfigWarnings,
) -> Result<(), anyhow::Error> {
    if unregistered.is_empty() {
        return Ok(());
    }
    let list = unregistered
        .iter()
        .cloned()
        .collect::<Vec<_>>()
        .join("`, `");
    let snippet = secret_scaffold_snippet(unregistered);
    let message = format!(
        "{source_desc} reference(s) secret(s) `{list}` that are not registered in the server \
         `[secrets]` table.\n\
         Add them to server.toml (adjust each `env` to its source variable), or remove the \
         references. Run again with `--fix` to scaffold them:\n\n\
         {snippet}"
    );
    if availability.allows_unavailable() {
        warnings.insert(format!(
            "{message}\nSkipping these load-time secret checks because unavailable runtime \
             configuration is allowed; activation will enforce them strictly."
        ));
        Ok(())
    } else {
        bail!("{message}");
    }
}

/// Append a `[secrets]` scaffold entry `X = { env = "X" }` for each name.
pub(super) async fn fix_server_secret_scaffolds(
    server_config_path: &Path,
    names: &BTreeSet<String>,
) -> Result<(), anyhow::Error> {
    if names.is_empty() {
        return Ok(());
    }
    let source = tokio::fs::read_to_string(server_config_path)
        .await
        .with_context(|| format!("cannot read server config {server_config_path:?}"))?;
    let mut doc = source
        .parse::<DocumentMut>()
        .context("cannot parse server config as TOML")?;
    let table = doc
        .as_table_mut()
        .entry("secrets")
        .or_insert_with(|| Item::Table(Table::new()))
        .as_table_mut()
        .context("`secrets` in server config is not a table")?;
    for name in names {
        if table.contains_key(name) {
            bail!("secret {name} must not be present");
        }
        let mut inline = toml_edit::InlineTable::new();
        inline.insert("env", name.as_str().into());
        table.insert(name, value(inline));
    }
    tokio::fs::write(server_config_path, doc.to_string())
        .await
        .with_context(|| format!("cannot write fixed server config {server_config_path:?}"))?;

    Ok(())
}
