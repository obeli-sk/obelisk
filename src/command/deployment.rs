use crate::args::{self, DeploymentSource};
use crate::config::config_holder::load_deployment_canonical;
use crate::config::toml::DeploymentTomlValidated;
use crate::get_deployment_repository_client;
use anyhow::{Context as _, bail};
use chrono::DateTime;
use concepts::prefixed_ulid::DeploymentId;
use grpc::grpc_gen;
use grpc::grpc_gen::switch_deployment_response::Outcome;
use grpc::injector::TracingInjector;
use grpc::to_channel;
use tonic::transport::Channel;

impl args::Deployment {
    pub(crate) async fn run(self) -> Result<(), anyhow::Error> {
        match self {
            args::Deployment::Submit {
                file,
                empty,
                verify,
                api_url,
            } => {
                let config_json = load_config_json_from_file_or_empty(file, empty).await?;
                let channel = to_channel(&api_url).await?;
                let mut client = get_deployment_repository_client(channel).await?;
                let resp = client
                    .submit_deployment(grpc_gen::SubmitDeploymentRequest {
                        config_json,
                        created_by: Some("cli".to_string()),
                        verify,
                    })
                    .await?
                    .into_inner();
                let id = resp.deployment_id.context("missing deployment_id")?.id;
                println!("{id}");
                Ok(())
            }

            args::Deployment::Enqueue {
                source,
                empty,
                verify,
                api_url,
            } => {
                let channel = to_channel(&api_url).await?;
                let mut client = get_deployment_repository_client(channel).await?;
                let id = submit_deployment(
                    &mut client,
                    source,
                    empty,
                    false, // will be verified in switch
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
                api_url,
            } => {
                let channel = to_channel(&api_url).await?;
                let mut client = get_deployment_repository_client(channel).await?;
                let id = submit_deployment(
                    &mut client,
                    source,
                    empty,
                    false, // will be verified in switch
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
                        include_config_json: false,
                    })
                    .await?
                    .into_inner();

                if resp.deployments.is_empty() {
                    println!("No deployments found.");
                    return Ok(());
                }

                println!(
                    "{:<32}  {:<12}  {:<19}  {:<19}",
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
                    println!("{id:<32}  {status:<12}  {created:<19}  {last_active:<19}");
                }
                Ok(())
            }

            args::Deployment::Show { id, api_url } => {
                let channel = to_channel(&api_url).await?;
                let mut client = get_deployment_repository_client(channel).await?;
                let resp = client
                    .get_deployment(grpc_gen::GetDeploymentRequest {
                        deployment_id: Some(grpc_gen::DeploymentId { id }),
                    })
                    .await?
                    .into_inner();
                let dep = resp.deployment.context("deployment not found")?;
                let config_json = dep.config_json.context("config_json not available")?;
                let value: serde_json::Value = serde_json::from_str(&config_json)?;
                println!("{}", serde_json::to_string_pretty(&value)?);
                Ok(())
            }
        }
    }
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
) -> anyhow::Result<DeploymentId> {
    assert_ne!(source.is_some(), empty);
    let config_json = match source {
        Some(DeploymentSource::Id(id)) => {
            return Ok(id);
        }
        Some(DeploymentSource::File(path)) => load_config_json(path).await?,
        None => load_config_json_from_file_or_empty(None, empty).await?,
    };
    let resp = client
        .submit_deployment(grpc_gen::SubmitDeploymentRequest {
            config_json,
            created_by: Some("cli".to_string()),
            verify,
        })
        .await?
        .into_inner();
    let id = DeploymentId::try_from(resp.deployment_id.context("missing deployment_id")?)?;
    println!("Submitted as {id}");
    Ok(id)
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

async fn load_config_json_from_file_or_empty(
    file: Option<std::path::PathBuf>,
    empty: bool,
) -> anyhow::Result<String> {
    assert_ne!(file.is_some(), empty);
    if let Some(path) = file {
        load_config_json(path).await
    } else {
        let deployment = DeploymentTomlValidated::default().canonicalize().await?;
        Ok(crate::config::toml::compute_config_json(&deployment))
    }
}

async fn load_config_json(path: std::path::PathBuf) -> anyhow::Result<String> {
    let deployment = load_deployment_canonical(&path).await?;
    Ok(crate::config::toml::compute_config_json(&deployment))
}

fn format_status(status: grpc_gen::DeploymentStatus) -> &'static str {
    match status {
        grpc_gen::DeploymentStatus::Inactive => "Inactive",
        grpc_gen::DeploymentStatus::Enqueued => "Enqueued",
        grpc_gen::DeploymentStatus::Active => "Active",
        grpc_gen::DeploymentStatus::Unspecified => "Unknown",
    }
}
