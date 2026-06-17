use crate::args::{self, DeploymentSource};
use crate::config::manifest::{
    PreparedDeploymentManifest, prepare_deployment_manifest_from_disk,
};
use crate::config::toml::sanitize_deployment_relative_path;
use crate::get_deployment_repository_client;
use anyhow::{Context as _, bail};
use chrono::DateTime;
use concepts::prefixed_ulid::DeploymentId;
use grpc::grpc_gen;
use grpc::grpc_gen::switch_deployment_response::Outcome;
use grpc::injector::TracingInjector;
use grpc::to_channel;
use std::path::PathBuf;
use tonic::transport::Channel;

impl args::Deployment {
    pub(crate) async fn run(self) -> Result<(), anyhow::Error> {
        match self {
            args::Deployment::Submit {
                file,
                empty,
                verify,
                description,
                deployment_id,
                api_url,
            } => {
                let prepared = prepare_manifest_from_file_or_empty(file, empty).await?;
                let channel = to_channel(&api_url).await?;
                let mut client = get_deployment_repository_client(channel).await?;
                let id = upload_and_submit_manifest(
                    &mut client,
                    prepared,
                    verify,
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
                verify,
                description,
                deployment_id,
                api_url,
            } => {
                let channel = to_channel(&api_url).await?;
                let mut client = get_deployment_repository_client(channel).await?;
                let id = submit_deployment(
                    &mut client,
                    source,
                    empty,
                    false, // will be verified in switch
                    description,
                    deployment_id,
                )
                .await?;
                switch_deployment(
                    &mut client,
                    id,
                    verify,
                    false, // hot
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
                let channel = to_channel(&api_url).await?;
                let mut client = get_deployment_repository_client(channel).await?;
                let id = submit_deployment(
                    &mut client,
                    source,
                    empty,
                    false, // will be verified in switch
                    description,
                    deployment_id,
                )
                .await?;
                switch_deployment(
                    &mut client,
                    id,
                    true, // does not matter, will be verified in any case
                    true, // hot
                )
                .await
            }

            args::Deployment::List { api_url } => {
                let channel = to_channel(&api_url).await?;
                let mut client = get_deployment_repository_client(channel).await?;
                let resp = client
                    .list_deployments(grpc_gen::ListDeploymentsRequest {
                        pagination: None,
                        include_deployment_toml: false,
                        include_derived: false,
                    })
                    .await?
                    .into_inner();

                if resp.deployments.is_empty() {
                    println!("No deployments found.");
                    return Ok(());
                }

                println!(
                    "{:<32}  {:<12}  {:<19}  {:<19}  DESCRIPTION",
                    "ID", "STATUS", "CREATED_AT", "LAST_ACTIVE_AT"
                );
                for summary in resp.deployments {
                    let dep = summary.deployment.context("missing deployment")?;
                    let id = dep
                        .deployment_id
                        .as_ref()
                        .map(|d| d.id.as_str())
                        .unwrap_or_default()
                        .to_string();
                    let status = format_status(dep.status());
                    let created: DateTime<_> = dep.created_at.expect("created_at is sent").into();
                    let created = created.format("%Y-%m-%d %H:%M:%S");
                    let last_active = dep
                        .last_active_at
                        .map(|t| {
                            let dt: DateTime<_> = t.into();
                            dt.format("%Y-%m-%d %H:%M:%S").to_string()
                        })
                        .unwrap_or_default();
                    println!(
                        "{id:<32}  {status:<12}  {created:<19}  {last_active:<19}  {}",
                        dep.description.unwrap_or_default()
                    );
                }
                Ok(())
            }

            args::Deployment::Active { api_url, json } => {
                let channel = to_channel(&api_url).await?;
                let mut client = get_deployment_repository_client(channel).await?;
                let resp = client
                    .get_current_deployment_id(grpc_gen::GetCurrentDeploymentIdRequest {})
                    .await?
                    .into_inner();
                let id = resp.deployment_id.context("missing deployment_id")?.id;
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
                let channel = to_channel(&api_url).await?;
                let mut client = get_deployment_repository_client(channel).await?;
                let resp = client
                    .get_deployment(grpc_gen::GetDeploymentRequest {
                        deployment_id: Some(grpc_gen::DeploymentId { id: id.to_string() }),
                    })
                    .await?
                    .into_inner();
                let dep = resp.deployment.context("deployment not found")?;
                let deployment_toml = dep
                    .deployment_toml
                    .context("deployment_toml not available")?;

                if let Some(file) = file {
                    // Normalize the requested path the same way `get` writes it, so a
                    // `./scripts/x` or `scripts//x` still matches the stored ref path.
                    let rel = sanitize_deployment_relative_path(&file)
                        .with_context(|| format!("invalid source path `{file}`"))?;
                    let file_ref = dep.files.iter().find(|f| f.path == rel).with_context(|| {
                        format!("deployment {id} has no deployment-owned source file `{rel}`")
                    })?;
                    let bytes = fetch_file(&mut client, &file_ref.digest).await?;
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
                api_url,
            } => {
                let channel = to_channel(&api_url).await?;
                let mut client = get_deployment_repository_client(channel).await?;
                let resp = client
                    .get_deployment(grpc_gen::GetDeploymentRequest {
                        deployment_id: Some(grpc_gen::DeploymentId { id: id.to_string() }),
                    })
                    .await?
                    .into_inner();
                let dep = resp.deployment.context("deployment not found")?;
                let deployment_toml = dep
                    .deployment_toml
                    .context("deployment_toml not available")?;

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
                    let rel = sanitize_deployment_relative_path(&file_ref.path).with_context(
                        || format!("refusing to write unsafe source path `{}`", file_ref.path),
                    )?;
                    let path = output_dir.join(&rel);
                    if let Some(parent) = path.parent() {
                        tokio::fs::create_dir_all(parent).await.with_context(|| {
                            format!("cannot create source directory {parent:?}")
                        })?;
                    }
                    let bytes = fetch_file(&mut client, &file_ref.digest).await?;
                    write_new_file(&path, &bytes, force).await?;
                }
                println!(
                    "Wrote {} ({file_count} source file{}) for deployment {id}",
                    toml_path.display(),
                    if file_count == 1 { "" } else { "s" }
                );
                Ok(())
            }
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

type DeploymentClient = grpc::grpc_gen::deployment_repository_client::DeploymentRepositoryClient<
    tonic::service::interceptor::InterceptedService<Channel, TracingInjector>,
>;

/// If the source is a file, submit it and return the new ID. If it's an ID, return it directly.
/// If `empty`, submit an empty deployment and return the new ID.
async fn submit_deployment(
    client: &mut DeploymentClient,
    source: Option<DeploymentSource>,
    empty: bool,
    verify: bool,
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
    let id =
        upload_and_submit_manifest(client, prepared, verify, description, deployment_id).await?;
    println!("Submitted as {id}");
    Ok(id)
}

/// Upload the manifest's referenced file blobs to the content-addressed store, then submit the
/// verbatim manifest. The server is content-addressed and idempotent, so re-uploading a blob it
/// already has is a no-op.
async fn upload_and_submit_manifest(
    client: &mut DeploymentClient,
    prepared: PreparedDeploymentManifest,
    verify: bool,
    description: Option<String>,
    deployment_id: Option<DeploymentId>,
) -> anyhow::Result<DeploymentId> {
    for file in &prepared.files {
        client
            .upload_file(grpc_gen::UploadFileRequest {
                deployment_id: deployment_id.map(grpc_gen::DeploymentId::from),
                content: file.bytes.clone(),
            })
            .await
            .with_context(|| format!("cannot upload deployment file `{}`", file.path))?;
    }
    let resp = client
        .submit_deployment(grpc_gen::SubmitDeploymentRequest {
            deployment_toml: prepared.deployment_toml,
            created_by: Some("cli".to_string()),
            verify,
            description,
            deployment_id: deployment_id.map(grpc_gen::DeploymentId::from),
        })
        .await?
        .into_inner();
    if !resp.missing_digests.is_empty() {
        bail!(
            "server is still missing {} file blob(s) after upload: {}",
            resp.missing_digests.len(),
            resp.missing_digests.join(", ")
        );
    }
    DeploymentId::try_from(resp.deployment_id.context("missing deployment_id")?).map_err(Into::into)
}

/// Fetch a deployment file blob from the server's content-addressed store.
async fn fetch_file(client: &mut DeploymentClient, digest: &str) -> anyhow::Result<Vec<u8>> {
    let resp = client
        .get_file(grpc_gen::GetFileRequest {
            digest: digest.to_string(),
        })
        .await
        .with_context(|| format!("cannot fetch deployment file `{digest}`"))?
        .into_inner();
    Ok(resp.content)
}

async fn switch_deployment(
    client: &mut DeploymentClient,
    id: DeploymentId,
    verify: bool,
    hot: bool,
) -> anyhow::Result<()> {
    let resp = client
        .switch_deployment(grpc_gen::SwitchDeploymentRequest {
            deployment_id: Some(grpc_gen::DeploymentId::from(id)),
            verify,
            hot_redeploy: hot,
        })
        .await?
        .into_inner();

    match resp.outcome() {
        Outcome::SwitchOutcomeSwitched => {
            println!("Applied successfully.");
        }
        Outcome::SwitchOutcomeRestartRequired => {
            if hot {
                bail!(
                    "Could not apply immediately; deployment enqueued. Restart the server to apply."
                );
            }
            println!("Deployment enqueued. Restart the server to apply.");
        }
        Outcome::SwitchOutcomeUnspecified => {
            bail!("Unexpected outcome from server.");
        }
    }
    Ok(())
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

fn format_status(status: grpc_gen::DeploymentStatus) -> &'static str {
    match status {
        grpc_gen::DeploymentStatus::Inactive => "Inactive",
        grpc_gen::DeploymentStatus::Enqueued => "Enqueued",
        grpc_gen::DeploymentStatus::Active => "Active",
        grpc_gen::DeploymentStatus::Unspecified => "Unknown",
    }
}
