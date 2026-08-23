use crate::args::{self, DeploymentSource};
use crate::client::{ClientStartup, send_bytes, send_json};
use crate::config::deployment::sanitize_deployment_relative_path;
use crate::config::deployment::{
    PreparedDeploymentManifest, prepare_deployment_manifest_from_disk,
};
use crate::server::web_api_server::deployment::{
    DeploymentRecordSer, DeploymentStateSer, DeploymentStatusSer, DeploymentSubmitPayload,
    DeploymentSubmitResponse, DeploymentSwitchPayload, GcOrphanFilesResponseSer,
    SubmitPackageErrorBody,
};
use anyhow::{Context as _, bail};
use concepts::prefixed_ulid::DeploymentId;
use http::header::ACCEPT;
use serde::Deserialize;
use std::path::PathBuf;

impl args::Deployment {
    pub(crate) async fn run(self, client_startup: ClientStartup) -> Result<(), anyhow::Error> {
        match self {
            args::Deployment::Submit {
                file,
                empty,
                allow_unavailable_runtime_config,
                description,
                deployment_id,
                api_url,
            } => {
                let prepared = prepare_manifest_from_file_or_empty(file, empty).await?;
                let id = upload_and_submit_manifest(
                    &client_startup,
                    &api_url,
                    prepared,
                    allow_unavailable_runtime_config,
                    description,
                    deployment_id,
                )
                .await?;
                println!("{id}");
                Ok(())
            }

            args::Deployment::Enqueue {
                source,
                empty,
                allow_unavailable_runtime_config,
                description,
                deployment_id,
                api_url,
            } => {
                let id = submit_deployment(
                    &client_startup,
                    &api_url,
                    source,
                    empty,
                    allow_unavailable_runtime_config,
                    description,
                    deployment_id,
                )
                .await?;
                switch_deployment(
                    &client_startup,
                    &api_url,
                    id,
                    allow_unavailable_runtime_config,
                    SwitchCommand::Enqueue,
                )
                .await
            }

            args::Deployment::Apply {
                source,
                empty,
                description,
                deployment_id,
                api_url,
            } => {
                let id = submit_deployment(
                    &client_startup,
                    &api_url,
                    source,
                    empty,
                    false,
                    description,
                    deployment_id,
                )
                .await?;
                switch_deployment(&client_startup, &api_url, id, false, SwitchCommand::Apply).await
            }

            args::Deployment::List { api_url } => {
                let client = client_startup.web_api_client()?;
                let deployments: Vec<DeploymentStateSer> = send_json(
                    client
                        .get(format!("{api_url}/v1/deployments"))
                        .header(ACCEPT, "application/json"),
                )
                .await?;

                if deployments.is_empty() {
                    println!("No deployments found.");
                    return Ok(());
                }

                println!(
                    "{:<32}  {:<12}  {:<19}  {:<19}  DESCRIPTION",
                    "ID", "STATUS", "CREATED_AT", "LAST_ACTIVE_AT"
                );
                for dep in deployments {
                    let id = dep.deployment_id;
                    let status = format_status(&dep.status);
                    let created = dep.created_at.format("%Y-%m-%d %H:%M:%S");
                    let last_active = dep
                        .last_active_at
                        .map(|t| t.format("%Y-%m-%d %H:%M:%S").to_string())
                        .unwrap_or_default();
                    println!(
                        "{id:<32}  {status:<12}  {created:<19}  {last_active:<19}  {}",
                        dep.description.unwrap_or_default()
                    );
                }
                Ok(())
            }

            args::Deployment::Gc { api_url } => {
                let client = client_startup.web_api_client()?;
                let resp: GcOrphanFilesResponseSer = send_json(
                    client
                        .delete(format!("{api_url}/v1/files/orphans"))
                        .header(ACCEPT, "application/json"),
                )
                .await?;
                println!("Deleted {} orphan file blob(s).", resp.deleted_count);
                Ok(())
            }

            args::Deployment::Active { api_url, json } => {
                let client = client_startup.web_api_client()?;
                let id: DeploymentId = send_json(
                    client
                        .get(format!("{api_url}/v1/deployment-id"))
                        .header(ACCEPT, "application/json"),
                )
                .await?;
                if json {
                    println!("\"{id}\"");
                } else {
                    println!("{id}");
                }
                Ok(())
            }

            args::Deployment::Show {
                id,
                file,
                json,
                api_url,
            } => {
                let client = client_startup.web_api_client()?;
                let dep: DeploymentRecordSer = send_json(
                    client
                        .get(format!("{api_url}/v1/deployments/{id}"))
                        .header(ACCEPT, "application/json")
                        .query(&[("include_generated_metadata", "true")]),
                )
                .await?;
                let deployment_toml = dep.deployment_toml;

                if let Some(file) = file {
                    // Normalize the requested path the same way `get` writes it, so a
                    // `./scripts/x` or `scripts//x` still matches the stored ref path.
                    let rel = sanitize_deployment_relative_path(&file)
                        .with_context(|| format!("invalid source path `{file}`"))?;
                    let file_ref = dep.files.iter().find(|f| f.path == rel).with_context(|| {
                        format!("deployment {id} has no deployment-owned source file `{rel}`")
                    })?;
                    let bytes = fetch_file(&client, &api_url, &file_ref.digest).await?;
                    print!("{}", String::from_utf8_lossy(&bytes));
                    return Ok(());
                }

                if json {
                    // The manifest is the source of truth; render it as JSON for tooling.
                    let value: toml::Value = toml::from_str(&deployment_toml)
                        .context("cannot parse stored deployment manifest")?;
                    println!("{}", serde_json::to_string_pretty(&value)?);
                    return Ok(());
                }

                print!("{deployment_toml}");
                Ok(())
            }

            args::Deployment::Get {
                id,
                output,
                force,
                include_generated_metadata,
                api_url,
            } => {
                let client = client_startup.web_api_client()?;
                let dep: DeploymentRecordSer = send_json(
                    client
                        .get(format!("{api_url}/v1/deployments/{id}"))
                        .header(ACCEPT, "application/json")
                        .query(&[(
                            "include_generated_metadata",
                            include_generated_metadata.to_string(),
                        )]),
                )
                .await?;
                let deployment_toml = dep.deployment_toml;

                let output_dir = output.unwrap_or_else(|| PathBuf::from("."));
                tokio::fs::create_dir_all(&output_dir)
                    .await
                    .with_context(|| format!("cannot create output directory {output_dir:?}"))?;

                let toml_path = output_dir.join("deployment.toml");
                write_new_file(&toml_path, deployment_toml.as_bytes(), force).await?;
                let file_count = dep.files.len();
                for file_ref in &dep.files {
                    // Defensively re-validate the relative path so a malformed stored
                    // manifest can never write outside the output directory.
                    let rel =
                        sanitize_deployment_relative_path(&file_ref.path).with_context(|| {
                            format!("refusing to write unsafe source path `{}`", file_ref.path)
                        })?;
                    let path = output_dir.join(&rel);
                    if let Some(parent) = path.parent() {
                        tokio::fs::create_dir_all(parent).await.with_context(|| {
                            format!("cannot create source directory {parent:?}")
                        })?;
                    }
                    let bytes = fetch_file(&client, &api_url, &file_ref.digest).await?;
                    write_new_file(&path, &bytes, force).await?;
                }
                println!(
                    "Wrote {} ({file_count} source file{}) for deployment {id}",
                    toml_path.display(),
                    if file_count == 1 { "" } else { "s" }
                );
                Ok(())
            }

            // `deployment verify` is dispatched through the local verification path in `main`.
            args::Deployment::Verify(_) => unreachable!("handled in main before ClientStartup"),
        }
    }
}

/// Write `contents` to `path`. Refuses to overwrite an existing file unless `force`.
async fn write_new_file(
    path: &std::path::Path,
    contents: &[u8],
    force: bool,
) -> anyhow::Result<()> {
    use tokio::io::AsyncWriteExt as _;
    let mut file = tokio::fs::OpenOptions::new()
        .write(true)
        .create(true) // allow creating new files
        .truncate(true) // truncate when overwriting is permitted
        .create_new(!force) // when set, only new-file creation is allowed (no overwrite)
        .open(path)
        .await
        .with_context(|| {
            format!(
                "cannot open {path:?} for writing{}",
                if force { "" } else { ", try using `--force`" }
            )
        })?;
    file.write_all(contents)
        .await
        .with_context(|| format!("cannot write {path:?}"))?;
    Ok(())
}

/// If the source is a file, submit it and return the new ID. If it's an ID, return it directly.
/// If `empty`, submit an empty deployment and return the new ID.
async fn submit_deployment(
    client_startup: &ClientStartup,
    api_url: &str,
    source: Option<DeploymentSource>,
    empty: bool,
    allow_unavailable_runtime_config: bool,
    description: Option<String>,
    deployment_id: Option<DeploymentId>,
) -> anyhow::Result<DeploymentId> {
    assert_ne!(source.is_some(), empty);
    let prepared = match source {
        Some(DeploymentSource::Id(id)) => {
            if description.is_some() {
                bail!("--description cannot be used with an existing deployment ID");
            }
            if deployment_id.is_some() {
                bail!("--deployment-id cannot be used with an existing deployment ID source");
            }
            return Ok(id);
        }
        Some(DeploymentSource::File(path)) => prepare_deployment_manifest_from_disk(&path).await?,
        None => prepare_manifest_from_file_or_empty(None, empty).await?,
    };
    let id = upload_and_submit_manifest(
        client_startup,
        api_url,
        prepared,
        allow_unavailable_runtime_config,
        description,
        deployment_id,
    )
    .await?;
    println!("Submitted as {id}");
    Ok(id)
}

/// Submit the enriched manifest as a CAS-efficient package: first without file
/// blobs, and if the server reports missing files, retry the same submit with only
/// those blobs attached. The requested TOML is identical on both attempts.
async fn upload_and_submit_manifest(
    client_startup: &ClientStartup,
    api_url: &str,
    prepared: PreparedDeploymentManifest,
    allow_unavailable_runtime_config: bool,
    description: Option<String>,
    deployment_id: Option<DeploymentId>,
) -> anyhow::Result<DeploymentId> {
    let client = client_startup.web_api_client()?;
    // Preflight: no blobs, so digests already in the CAS are not re-uploaded.
    let missing = match submit_attempt(
        &client,
        api_url,
        &prepared,
        allow_unavailable_runtime_config,
        description.as_deref(),
        deployment_id,
        &[],
    )
    .await?
    {
        SubmitAttempt::Stored(id) => return Ok(id),
        SubmitAttempt::Missing(digests) => digests,
    };

    // Retry with only the blobs the server is missing.
    let files: Vec<_> = prepared
        .files
        .iter()
        .filter(|file| missing.contains(&file.digest.to_string()))
        .collect();
    match submit_attempt(
        &client,
        api_url,
        &prepared,
        allow_unavailable_runtime_config,
        description.as_deref(),
        deployment_id,
        &files,
    )
    .await?
    {
        SubmitAttempt::Stored(id) => Ok(id),
        SubmitAttempt::Missing(digests) => bail!(
            "server is still missing {} file blob(s) after upload: {}",
            digests.len(),
            digests.join(", ")
        ),
    }
}

enum SubmitAttempt {
    Stored(DeploymentId),
    /// Digests the server is missing; the deployment was not stored.
    Missing(Vec<String>),
}

/// One submit request. A conflict carrying only `missing_files` becomes `Missing`.
async fn submit_attempt(
    client: &reqwest::Client,
    api_url: &str,
    prepared: &PreparedDeploymentManifest,
    allow_unavailable_runtime_config: bool,
    description: Option<&str>,
    deployment_id: Option<DeploymentId>,
    files: &[&crate::config::deployment::DeploymentManifestFile],
) -> anyhow::Result<SubmitAttempt> {
    let url = format!("{api_url}/v1/deployments");
    let request = if files.is_empty() {
        client
            .post(url)
            .header(ACCEPT, "application/json")
            .json(&DeploymentSubmitPayload {
                deployment_toml: prepared.deployment_toml.clone(),
                description: description.map(str::to_string),
                allow_unavailable_runtime_config,
                deployment_id: deployment_id.map(|id| id.to_string()),
            })
    } else {
        let mut form = reqwest::multipart::Form::new()
            .text("deployment_toml", prepared.deployment_toml.clone())
            .text(
                "allow_unavailable_runtime_config",
                allow_unavailable_runtime_config.to_string(),
            );
        if let Some(description) = description {
            form = form.text("description", description.to_string());
        }
        if let Some(deployment_id) = deployment_id {
            form = form.text("deployment_id", deployment_id.to_string());
        }
        for file in files {
            form = form.part(
                file.digest.to_string(),
                reqwest::multipart::Part::bytes(file.bytes.clone()).file_name(file.path.clone()),
            );
        }
        client
            .post(url)
            .header(ACCEPT, "application/json")
            .multipart(form)
    };
    let response = request.send().await?;
    let status = response.status();
    if status.is_success() {
        let response: DeploymentSubmitResponse = response.json().await?;
        return Ok(SubmitAttempt::Stored(response.deployment_id.parse()?));
    }
    if status == reqwest::StatusCode::CONFLICT {
        let detail: SubmitPackageErrorBody = response.json().await?;
        let only_missing = detail.unexpected_files.is_empty()
            && detail.digest_mismatches.is_empty()
            && detail.oversized_files.is_empty()
            && detail.missing_digest_fields.is_empty()
            && !detail.missing_files.is_empty();
        if only_missing {
            return Ok(SubmitAttempt::Missing(
                detail
                    .missing_files
                    .iter()
                    .filter_map(|issue| issue.digest.clone())
                    .collect(),
            ));
        }
        bail!(
            "deployment submit rejected: {}",
            format_submit_detail(&detail)
        );
    }
    let body = response.text().await.unwrap_or_default();
    bail!("server returned {status}: {body}")
}

fn format_submit_detail(detail: &SubmitPackageErrorBody) -> String {
    let mut lines = Vec::new();
    for issue in &detail.missing_digest_fields {
        lines.push(format!("missing content_digest at {}", issue.field_path));
    }
    for issue in &detail.missing_files {
        lines.push(format!(
            "missing file at {} ({})",
            issue.field_path, issue.message
        ));
    }
    for issue in &detail.unexpected_files {
        lines.push(format!("unexpected file {}", issue.field_path));
    }
    for mismatch in &detail.digest_mismatches {
        lines.push(format!(
            "digest mismatch for {}: supplied {}, actual {}",
            mismatch.file.field_path, mismatch.supplied_digest, mismatch.actual_digest
        ));
    }
    for issue in &detail.oversized_files {
        lines.push(format!("oversized file {}", issue.field_path));
    }
    lines.join("; ")
}

/// Fetch a deployment file blob from the server's content-addressed store.
async fn fetch_file(
    client: &reqwest::Client,
    api_url: &str,
    digest: &str,
) -> anyhow::Result<bytes::Bytes> {
    send_bytes(client.get(format!("{api_url}/v1/files/{digest}")))
        .await
        .with_context(|| format!("cannot fetch deployment file `{digest}`"))
}

async fn switch_deployment(
    client_startup: &ClientStartup,
    api_url: &str,
    id: DeploymentId,
    allow_unavailable_runtime_config: bool,
    command: SwitchCommand,
) -> anyhow::Result<()> {
    #[derive(Deserialize)]
    struct SwitchResponse {
        ok: String,
    }
    let client = client_startup.web_api_client()?;
    let response: SwitchResponse = send_json(
        client
            .put(format!("{api_url}/v1/deployments/{id}/switch"))
            .header(ACCEPT, "application/json")
            .json(&DeploymentSwitchPayload {
                allow_unavailable_runtime_config,
                apply: command == SwitchCommand::Apply,
            }),
    )
    .await?;

    match (command, response.ok.as_str()) {
        (SwitchCommand::Apply, "switched") => {
            println!("Applied successfully.");
        }
        (SwitchCommand::Apply, "restart_required") => {
            bail!("Could not apply immediately; deployment enqueued. Restart the server to apply.");
        }
        (SwitchCommand::Enqueue, "switched") => {
            println!("Deployment already active; it will remain active after restart.");
        }
        (SwitchCommand::Enqueue, "restart_required") => {
            println!("Deployment enqueued. Restart the server to apply.");
        }
        (_, outcome) => bail!("unexpected outcome from server: {outcome}"),
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SwitchCommand {
    Apply,
    Enqueue,
}

async fn prepare_manifest_from_file_or_empty(
    file: Option<std::path::PathBuf>,
    empty: bool,
) -> anyhow::Result<PreparedDeploymentManifest> {
    assert_ne!(file.is_some(), empty);
    if let Some(path) = file {
        prepare_deployment_manifest_from_disk(&path).await
    } else {
        Ok(PreparedDeploymentManifest::empty())
    }
}

fn format_status(status: &DeploymentStatusSer) -> &'static str {
    match status {
        DeploymentStatusSer::Inactive => "Inactive",
        DeploymentStatusSer::Enqueued => "Enqueued",
        DeploymentStatusSer::Active => "Active",
    }
}
