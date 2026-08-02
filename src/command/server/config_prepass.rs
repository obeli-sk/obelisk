//! Config verify pre-pass: aggregate config findings across a whole server startup or
//! `server verify` run instead of bailing on the first, and apply the auto-fixable subset
//! under `--fix`.
//!
//! The pre-pass owns the continue/bail/fix decision for the outbound-HTTP allowlist:
//! unregistered secret names, secret replacements the operator allowlist does not authorize,
//! and destinations no allowlist entry covers. The resolvers (`resolve_allowed_hosts`,
//! `resolve_named_secrets`) no longer make that decision; they drop what they cannot resolve,
//! and `http_request_policy` enforces the same rules per request (failing closed). Running the
//! pre-pass here lets the operator fix every finding in one edit.

use super::RuntimeConfigAvailability;
use crate::config::secret_registry::SecretRegistry;
use crate::config::toml::{
    AllowedHostToml, ConfigName, DeploymentResolved, MethodsInput, ReplaceIn, resolve_allowed_hosts,
};
use anyhow::{Context, bail};
use std::collections::BTreeSet;
use std::path::Path;
use toml_edit::{DocumentMut, Item, Table, value};
use tracing::warn;
use wasm_workers::http_request_policy::{GlobalHttpConfig, ReplacementLocation};

/// Run the outbound-HTTP allowlist pre-pass over the server's own config and, when present,
/// the deployment being started/verified. Bails on the first fatal category collected so far
/// under strict availability; downgrades the fatal categories to warnings when unavailable
/// runtime configuration is allowed (activation re-checks strictly).
pub(super) fn preflight(
    server_verified: &super::ServerVerified,
    deployment: Option<&DeploymentResolved>,
    availability: RuntimeConfigAvailability,
) -> Result<(), anyhow::Error> {
    let secret_registry = &*server_verified.secret_registry;
    let global_http_config = &server_verified.global_http_config;

    // Unregistered secrets: the server's own `[[outbound_http.allowed_host]]` entries are always
    // fatal (the server cannot run with an invalid allowlist); the deployment's follow availability.
    let mut server_unregistered = BTreeSet::new();
    collect_unregistered_secrets(
        &server_verified.server_outbound_allowed_hosts,
        secret_registry,
        &mut server_unregistered,
    );
    report_unregistered_secrets(
        &server_unregistered,
        "server.toml `[[outbound_http.allowed_host]]` entries",
        RuntimeConfigAvailability::Strict,
    )?;

    let Some(deployment) = deployment else {
        return Ok(());
    };

    let ignore_missing_env_vars = availability.allows_unavailable();
    let server_replacements = global_secret_replacements(global_http_config);
    let mut missing_replacements = Vec::new();
    let mut uncovered_hosts = Vec::new();
    let mut unregistered = BTreeSet::new();
    let mut check = |section: &'static str, name: &ConfigName, hosts: &[AllowedHostToml]| {
        collect_outbound_http_secret_replacements(
            section,
            name,
            hosts,
            &server_replacements,
            secret_registry,
            &mut missing_replacements,
        );
        collect_uncovered_outbound_http_hosts(
            section,
            name,
            hosts,
            global_http_config,
            secret_registry,
            ignore_missing_env_vars,
            &mut uncovered_hosts,
        );
        collect_unregistered_secrets(hosts, secret_registry, &mut unregistered);
    };
    for activity in &deployment.activities_wasm {
        check(
            "activity_wasm",
            &activity.common.name,
            &activity.allowed_hosts,
        );
    }
    for activity in &deployment.activities_js {
        check("activity_js", &activity.name, &activity.allowed_hosts);
    }
    for webhook in &deployment.webhooks_wasm {
        check(
            "webhook_endpoint_wasm",
            &webhook.common.name,
            &webhook.allowed_hosts,
        );
    }
    for webhook in &deployment.webhooks_js {
        check("webhook_endpoint_js", &webhook.name, &webhook.allowed_hosts);
    }
    // Exec activities reference secrets directly (exposed on stdin), not via `allowed_host`.
    for exec in &deployment.activities_exec {
        for name in &exec.secrets {
            if secret_registry.secret_lookup(name).is_none() {
                unregistered.insert(name.clone());
            }
        }
    }

    report_unregistered_secrets(
        &unregistered,
        "the deployment's secret references",
        availability,
    )?;
    report_missing_outbound_http_secret_replacements(&missing_replacements, availability)?;
    report_uncovered_outbound_http_hosts(&uncovered_hosts);
    Ok(())
}

/// Every component's outbound HTTP `allowed_hosts`, over the same component kinds the
/// deployment pre-pass checks. Used by `--fix` to gather referenced secret names.
pub(super) fn deployment_allowed_host_lists(
    deployment: &DeploymentResolved,
) -> Vec<&[AllowedHostToml]> {
    let mut lists: Vec<&[AllowedHostToml]> = Vec::new();
    lists.extend(deployment.activities_wasm.iter().map(|c| &*c.allowed_hosts));
    lists.extend(deployment.activities_js.iter().map(|c| &*c.allowed_hosts));
    lists.extend(deployment.webhooks_wasm.iter().map(|c| &*c.allowed_hosts));
    lists.extend(deployment.webhooks_js.iter().map(|c| &*c.allowed_hosts));
    lists
}

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
        warn!(
            "{message}\nSkipping these load-time secret checks because unavailable runtime \
             configuration is allowed; activation will enforce them strictly."
        );
        Ok(())
    } else {
        bail!("{message}");
    }
}

/// The `(secret name, replacement target)` pairs the operator global allowlist authorizes.
pub(super) fn global_secret_replacements(
    global_http_config: &GlobalHttpConfig,
) -> hashbrown::HashSet<(&str, ReplacementLocation)> {
    global_http_config
        .entries()
        .iter()
        .flat_map(|entry| {
            entry.secret_env_mappings.iter().flat_map(|(name, _)| {
                entry
                    .replace_in
                    .iter()
                    .copied()
                    .map(move |target| (name.as_str(), target))
            })
        })
        .collect()
}

fn replacement_target(replace_in: ReplaceIn) -> ReplacementLocation {
    match replace_in {
        ReplaceIn::Headers => ReplacementLocation::Headers,
        ReplaceIn::Body => ReplacementLocation::Body,
        ReplaceIn::Params => ReplacementLocation::Params,
    }
}

fn replacement_target_name(replace_in: ReplaceIn) -> &'static str {
    match replace_in {
        ReplaceIn::Headers => "headers",
        ReplaceIn::Body => "body",
        ReplaceIn::Params => "params",
    }
}

fn allowed_host_snippet(
    section: &str,
    entry: &AllowedHostToml,
    secret: &str,
    replace_in: ReplaceIn,
) -> String {
    let mut entry = entry.clone();
    entry.secrets = vec![secret.to_string()];
    entry.replace_in = vec![replace_in];
    let body = toml::to_string(&entry).expect("AllowedHostToml must serialize to TOML");
    format!("[[{section}.allowed_host]]\n{body}")
}

/// A secret replacement a component requests that the operator global allowlist
/// does not authorize. Collected across the whole deployment so every missing
/// allowance is reported at once, letting the operator add them all in one pass.
pub(super) struct MissingSecretReplacement {
    component_section: &'static str,
    component_name: ConfigName,
    entry: AllowedHostToml,
    secret: String,
    replace_in: ReplaceIn,
}

/// Append every secret replacement `entries` requests that `server_replacements`
/// does not authorize onto `missing`. Registered-only secrets are checked (an
/// unregistered secret name cannot be injected, so it is skipped here).
pub(super) fn collect_outbound_http_secret_replacements(
    component_section: &'static str,
    component_name: &ConfigName,
    entries: &[AllowedHostToml],
    server_replacements: &hashbrown::HashSet<(&str, ReplacementLocation)>,
    secret_registry: &SecretRegistry,
    missing: &mut Vec<MissingSecretReplacement>,
) {
    for entry in entries {
        if entry.methods.is_none()
            || matches!(&entry.methods, Some(MethodsInput::List(methods)) if methods.is_empty())
        {
            continue;
        }
        for secret in &entry.secrets {
            if secret_registry.secret_lookup(secret).is_none() {
                continue;
            }
            for replace_in in &entry.replace_in {
                let requested_replacement = (secret.as_str(), replacement_target(*replace_in));
                if server_replacements.contains(&requested_replacement) {
                    continue;
                }
                missing.push(MissingSecretReplacement {
                    component_section,
                    component_name: component_name.clone(),
                    entry: entry.clone(),
                    secret: secret.clone(),
                    replace_in: *replace_in,
                });
            }
        }
    }
}

/// Emit a single report covering every collected missing secret replacement, so
/// the operator can add all the required allowlist entries to server.toml in one edit.
/// Bails under strict availability; downgrades to a warning when unavailable
/// runtime config is allowed (activation re-checks strictly).
pub(super) fn report_missing_outbound_http_secret_replacements(
    missing: &[MissingSecretReplacement],
    availability: RuntimeConfigAvailability,
) -> Result<(), anyhow::Error> {
    if missing.is_empty() {
        return Ok(());
    }
    use std::fmt::Write as _;
    let mut details = String::new();
    let mut server_snippets: Vec<String> = Vec::new();
    let mut deployment_snippets: Vec<String> = Vec::new();
    for m in missing {
        let _ = writeln!(
            details,
            "  - component `{name}` (`[[{section}.allowed_host]]`) requests replacing secret \
             `{secret}` in `{target}`",
            name = m.component_name,
            section = m.component_section,
            secret = m.secret,
            target = replacement_target_name(m.replace_in),
        );
        // Multiple components may request the same allowlist entry; list each unique
        // snippet once so the operator does not paste duplicates.
        let server_snippet =
            allowed_host_snippet("outbound_http", &m.entry, &m.secret, m.replace_in);
        if !server_snippets.contains(&server_snippet) {
            server_snippets.push(server_snippet);
        }
        let deployment_snippet =
            allowed_host_snippet(m.component_section, &m.entry, &m.secret, m.replace_in);
        if !deployment_snippets.contains(&deployment_snippet) {
            deployment_snippets.push(deployment_snippet);
        }
    }
    let message = format!(
        "the deployment requests {count} outbound HTTP secret replacement(s) that no server.toml \
         `[[outbound_http.allowed_host]]` entry authorizes:\n\
         {details}\n\
         After review, add these allowlist entries to server.toml:\n\n\
         {server}\n\
         The corresponding deployment.toml entries are:\n\n\
         {deployment}",
        count = missing.len(),
        server = server_snippets.join("\n"),
        deployment = deployment_snippets.join("\n"),
    );
    if availability.allows_unavailable() {
        warn!(
            "{message}\nSkipping these load-time secret replacement checks because unavailable \
             runtime configuration is allowed; activation will enforce them strictly."
        );
        Ok(())
    } else {
        bail!("{message}");
    }
}

/// A component destination that no operator global allowlist entry covers, collected
/// across the deployment so every gap is reported at load time rather than one-by-one.
pub(super) struct UncoveredOutboundHost {
    component_section: &'static str,
    component_name: ConfigName,
    pub(super) entry: AllowedHostToml,
}

/// Render a destination-only `[[<section>.allowed_host]]` snippet, dropping secret fields.
pub(super) fn host_allowlist_snippet(section: &str, entry: &AllowedHostToml) -> String {
    #[derive(serde::Serialize)]
    struct DestinationEntry {
        pattern: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        methods: Option<MethodsInput>,
        #[serde(skip_serializing_if = "Option::is_none")]
        request_url_regex: Option<String>,
    }
    let body = toml::to_string(&DestinationEntry {
        pattern: entry.pattern.clone(),
        methods: entry.methods.clone(),
        request_url_regex: entry.request_url_regex.clone(),
    })
    .expect("destination allowlist entry must serialize to TOML");
    format!("[[{section}.allowed_host]]\n{body}")
}

/// Append every destination in `entries` that no global allowlist entry covers onto
/// `uncovered`. Allow-nothing entries need no allowlist entry; resolution failures are
/// left for component preparation to report authoritatively.
pub(super) fn collect_uncovered_outbound_http_hosts(
    component_section: &'static str,
    component_name: &ConfigName,
    entries: &[AllowedHostToml],
    global_http_config: &GlobalHttpConfig,
    secret_registry: &SecretRegistry,
    ignore_missing_env_vars: bool,
    uncovered: &mut Vec<UncoveredOutboundHost>,
) {
    for entry in entries {
        if entry.methods.is_none()
            || matches!(&entry.methods, Some(MethodsInput::List(methods)) if methods.is_empty())
        {
            continue;
        }
        // Resolve one entry at a time to keep the original TOML for the snippet.
        let Ok(resolved) = resolve_allowed_hosts(
            vec![entry.clone()],
            ignore_missing_env_vars,
            secret_registry,
        ) else {
            continue;
        };
        let Some(resolved) = resolved.first() else {
            continue;
        };
        if !global_http_config
            .entries()
            .iter()
            .any(|allowed| allowed.covers(resolved))
        {
            uncovered.push(UncoveredOutboundHost {
                component_section,
                component_name: component_name.clone(),
                entry: entry.clone(),
            });
        }
    }
}

/// Warn once, listing the allowlist entries to add. A warning, not an error: coverage
/// is conservative and a component may declare destinations it never calls.
pub(super) fn report_uncovered_outbound_http_hosts(uncovered: &[UncoveredOutboundHost]) {
    if uncovered.is_empty() {
        return;
    }
    use std::fmt::Write as _;
    let mut details = String::new();
    let mut snippets: Vec<String> = Vec::new();
    for host in uncovered {
        let _ = writeln!(
            details,
            "  - component `{name}` (`[[{section}.allowed_host]]`) allows `{pattern}`",
            name = host.component_name,
            section = host.component_section,
            pattern = host.entry.pattern,
        );
        let snippet = host_allowlist_snippet("outbound_http", &host.entry);
        if !snippets.contains(&snippet) {
            snippets.push(snippet);
        }
    }
    warn!(
        "{count} outbound HTTP destination(s) the deployment allows are not covered by any \
         server.toml `[[outbound_http.allowed_host]]` allowlist entry; requests to them will be \
         denied at runtime:\n\
         {details}\n\
         After review, add these allowlist entries to server.toml:\n\n\
         {snippets}",
        count = uncovered.len(),
        snippets = snippets.join("\n"),
    );
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
