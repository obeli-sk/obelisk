//! Config verify pre-pass: aggregate outbound-HTTP allowlist findings across a server startup
//! or `server verify` run instead of bailing on the first, and apply the auto-fixable subset
//! under `--fix`. Owns the continue/bail/fix decision; the verified config carries secret names
//! only, resolved lazily per run through a component-scoped `RestrictedSecretRegistry`.

use super::RuntimeConfigAvailability;
use crate::config::secret_registry::SecretRegistry;
use crate::config::deployment::{
    AllowedHostToml, ConfigName, DeploymentResolved, MethodsInput, ReplaceIn,
    allowed_host_fingerprint, resolve_allowed_hosts,
};
use anyhow::{Context, bail};
use serde::Deserialize;
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use toml_edit::{DocumentMut, Item, Table, value};
use tracing::warn;
use wasm_workers::http_request_policy::{GlobalHttpConfig, ReplacementLocation};

/// Pairs each per-entry `allowed_host` advisory with the source `path:line` of its
/// `[[*.allowed_host]]` block, deduplicating identical findings across entries.
#[derive(Default)]
struct LocatedWarnings {
    /// advisory message -> set of `path:line` locations
    by_message: BTreeMap<String, BTreeSet<String>>,
    /// `allowed_host` TOML fingerprint -> source locations parsed out of the indexed files
    locations: BTreeMap<String, BTreeSet<(PathBuf, usize)>>,
}

impl LocatedWarnings {
    /// Record the source line of every `[[*.allowed_host]]` block in `path`, keyed by entry
    /// fingerprint. Best-effort: unreadable or unparsable files leave the index empty.
    fn index_file(&mut self, path: &Path) {
        #[derive(Deserialize)]
        struct AllowedHostBlock {
            allowed_host: Vec<AllowedHostToml>,
        }
        let Ok(source) = std::fs::read_to_string(path) else {
            return;
        };
        let lines = source.lines().collect::<Vec<_>>();
        for (start, line) in lines.iter().enumerate() {
            let header = line.split('#').next().unwrap_or_default().trim();
            if !(header.starts_with("[[") && header.ends_with(".allowed_host]]")) {
                continue;
            }
            let end = lines[start + 1..]
                .iter()
                .position(|line| {
                    line.split('#')
                        .next()
                        .unwrap_or_default()
                        .trim_start()
                        .starts_with('[')
                })
                .map_or(lines.len(), |offset| start + 1 + offset);
            let mut block = String::from("[[allowed_host]]\n");
            for line in &lines[start + 1..end] {
                block.push_str(line);
                block.push('\n');
            }
            let Ok(block) = toml::from_str::<AllowedHostBlock>(&block) else {
                continue;
            };
            let Some(entry) = block.allowed_host.first() else {
                continue;
            };
            self.locations
                .entry(allowed_host_fingerprint(entry))
                .or_default()
                .insert((path.to_path_buf(), start + 1));
        }
    }

    /// Resolve `entries` for their advisories and record each against its source locations.
    fn lint(
        &mut self,
        entries: &[AllowedHostToml],
        ignore_missing_env_vars: bool,
        secret_registry: &SecretRegistry,
    ) {
        let Ok((_hosts, advisories)) =
            resolve_allowed_hosts(entries.to_vec(), ignore_missing_env_vars, secret_registry)
        else {
            return;
        };
        for advisory in advisories {
            let located = self
                .locations
                .get(&advisory.fingerprint)
                .into_iter()
                .flatten()
                .map(|(path, line)| format!("{}:{line}", path.display()))
                .collect::<Vec<_>>();
            self.by_message
                .entry(advisory.message)
                .or_default()
                .extend(located);
        }
    }

    fn emit(self) {
        if self.by_message.is_empty() {
            return;
        }
        let lines = self
            .by_message
            .into_iter()
            .map(|(message, locations)| {
                if locations.is_empty() {
                    message
                } else {
                    format!(
                        "{message} ({})",
                        locations.into_iter().collect::<Vec<_>>().join(", ")
                    )
                }
            })
            .collect::<Vec<_>>();
        warn!("Configuration warnings:\n- {}", lines.join("\n- "));
    }
}

/// Run the outbound-HTTP allowlist pre-pass over the server config and, when present, the
/// deployment. Fatal categories bail under strict availability, warn when unavailable runtime
/// config is allowed (activation re-checks strictly).
pub(super) fn preflight(
    server_verified: &super::ServerVerified,
    deployment: Option<&DeploymentResolved>,
    availability: RuntimeConfigAvailability,
) -> Result<(), anyhow::Error> {
    let secret_registry = &*server_verified.secret_registry;
    let global_http_config = &server_verified.global_http_config;
    let ignore_missing_env_vars = availability == RuntimeConfigAvailability::AllowUnavailable;

    // Index source files so advisories can be located, then lint the server's own allowlist.
    let mut warnings = LocatedWarnings::default();
    if let Some(path) = &server_verified.source_path {
        warnings.index_file(path);
    }
    if let Some(deployment) = deployment
        && let Some(path) = &deployment.source_path
    {
        warnings.index_file(path);
    }
    warnings.lint(
        &server_verified.server_outbound_allowed_hosts,
        false,
        secret_registry,
    );

    // The server's own unregistered secrets are always fatal; the deployment's follow availability.
    let mut server_unregistered = BTreeSet::new();
    collect_unregistered_allowed_host_secrets(
        &server_verified.server_outbound_allowed_hosts,
        secret_registry,
        &mut server_unregistered,
    );

    let server_replacements = global_secret_replacements(global_http_config);
    let mut missing_replacements = Vec::new();
    let mut uncovered_hosts = Vec::new();
    let mut unregistered = BTreeSet::new();
    if let Some(deployment) = deployment {
        let mut check = |section: &'static str, name: &ConfigName, hosts: &[AllowedHostToml]| {
            warnings.lint(hosts, ignore_missing_env_vars, secret_registry);
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
            collect_unregistered_allowed_host_secrets(hosts, secret_registry, &mut unregistered);
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
    }

    // Emit informational warnings before any fatal report, so every finding is seen.
    warnings.emit();
    report_uncovered_outbound_http_hosts(&uncovered_hosts);

    report_unregistered_secrets(
        &server_unregistered,
        "server.toml `[[outbound_http.allowed_host]]` entries",
        RuntimeConfigAvailability::Strict,
    )?;
    report_unregistered_secrets(
        &unregistered,
        "the deployment's secret references",
        availability,
    )?;
    report_missing_outbound_http_secret_replacements(&missing_replacements, availability)?;
    Ok(())
}

/// Every component's outbound HTTP `allowed_hosts`; used by `collect_deployment_unregistered_secrets`.
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

/// Append every unregistered secret a deployment references onto `unregistered`: both
/// `allowed_host` entries and exec-activity `secrets`. Keeps `--fix` in sync with `preflight`.
pub(super) fn collect_deployment_unregistered_secrets(
    deployment: &DeploymentResolved,
    secret_registry: &SecretRegistry,
    unregistered: &mut BTreeSet<String>,
) {
    for hosts in deployment_allowed_host_lists(deployment) {
        collect_unregistered_allowed_host_secrets(hosts, secret_registry, unregistered);
    }
    for exec in &deployment.activities_exec {
        for name in &exec.secrets {
            if secret_registry.secret_lookup(name).is_none() {
                unregistered.insert(name.clone());
            }
        }
    }
}

/// Append every secret name in `allowed_host` `entries` the operator registry does not know onto
/// `unregistered`. See `collect_deployment_unregistered_secrets` for the deployment-wide collector.
pub(super) fn collect_unregistered_allowed_host_secrets(
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
pub(super) fn secret_scaffold_snippet(names: &BTreeSet<String>) -> String {
    use std::fmt::Write as _;
    let mut snippet = String::from("[secrets]\n");
    for name in names {
        let _ = writeln!(snippet, "{name} = {{ env = \"{name}\" }}");
    }
    snippet
}

/// Emit a single finding for every unregistered secret `source_desc` references, with a
/// `[secrets]` snippet. Fatal under strict availability, otherwise a warning.
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
    if availability == RuntimeConfigAvailability::AllowUnavailable {
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
            entry.secret_names.iter().flat_map(|name| {
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

/// A secret replacement a component requests that the operator global allowlist does not
/// authorize. Collected across the deployment so every missing allowance reports at once.
pub(super) struct MissingSecretReplacement {
    component_section: &'static str,
    component_name: ConfigName,
    entry: AllowedHostToml,
    secret: String,
    replace_in: ReplaceIn,
}

/// Append every secret replacement `entries` request that `server_replacements` does not
/// authorize onto `missing`. Unregistered secrets are skipped (they can never be injected).
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

/// Emit a single report of every missing secret replacement so the operator can add all the
/// allowlist entries in one edit. Fatal under strict availability, otherwise a warning.
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
        // Multiple components may request the same entry; list each unique snippet once.
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
    if availability == RuntimeConfigAvailability::AllowUnavailable {
        warn!(
            "{message}\nSkipping these load-time secret replacement checks because unavailable \
             runtime configuration is allowed; activation will enforce them strictly."
        );
        Ok(())
    } else {
        bail!("{message}");
    }
}

/// A component destination no operator global allowlist entry covers, collected across the deployment.
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

/// Append every destination in `entries` no global allowlist entry covers onto `uncovered`.
/// Allow-nothing entries are skipped; resolution failures are left for component preparation.
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
        let Ok((resolved, _advisories)) = resolve_allowed_hosts(
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

/// Warn once, listing the allowlist entries to add: coverage is conservative, so this is not fatal.
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

#[cfg(test)]
mod tests {
    use super::*;

    /// A resolver advisory is joined to its `[[*.allowed_host]]` block and annotated with `path:line`.
    #[test]
    fn advisory_is_located_to_source_line() {
        // `methods` omitted triggers the "no methods" advisory; the entry still resolves.
        let entry = AllowedHostToml {
            pattern: "http://localhost:5005".to_string(),
            methods: None,
            request_url_regex: None,
            secrets: Vec::new(),
            replace_in: Vec::new(),
        };
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("server.toml");
        // Write the block from the same entry so its fingerprint matches on both sides.
        std::fs::write(
            &path,
            format!(
                "# policy\n[[outbound_http.allowed_host]]\n{}",
                toml::to_string(&entry).unwrap()
            ),
        )
        .unwrap();

        let mut warnings = LocatedWarnings::default();
        warnings.index_file(&path);
        warnings.lint(&[entry], false, &SecretRegistry::empty());

        let (message, locations) = warnings.by_message.iter().next().expect("one advisory");
        assert!(message.contains("has no `methods`"), "{message}");
        let location = locations.iter().next().expect("one location");
        assert!(location.ends_with("server.toml:2"), "{location}");
    }
}
