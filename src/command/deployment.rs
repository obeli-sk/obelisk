use crate::args;
use crate::config::config_holder::{ConfigFileOption, ConfigHolder, ConfigSource};
use crate::get_deployment_repository_client;
use crate::project_dirs;
use anyhow::{Context as _, bail};
use chrono::{DateTime, Utc};
use directories::BaseDirs;
use grpc::grpc_gen;
use grpc::grpc_gen::switch_deployment_response::Outcome;
use grpc::to_channel;
use prost_wkt_types::Timestamp;

impl args::Deployment {
    pub(crate) async fn run(self) -> Result<(), anyhow::Error> {
        match self {
            args::Deployment::Submit { config, api_url } => {
                let config_json = load_config_json(config).await?;
                let channel = to_channel(&api_url).await?;
                let mut client = get_deployment_repository_client(channel).await?;
                let resp = client
                    .submit_deployment(grpc_gen::SubmitDeploymentRequest {
                        config_json,
                        created_by: Some("cli".to_string()),
                    })
                    .await?
                    .into_inner();
                let id = resp.deployment_id.context("missing deployment_id")?.id;
                println!("{id}");
                Ok(())
            }

            args::Deployment::Switch {
                id,
                config,
                hot,
                verify,
                api_url,
            } => {
                let channel = to_channel(&api_url).await?;
                let mut client = get_deployment_repository_client(channel).await?;
                let deployment_id = if let Some(id) = id {
                    id
                } else {
                    let config_json = load_config_json(config).await?;
                    let resp = client
                        .submit_deployment(grpc_gen::SubmitDeploymentRequest {
                            config_json,
                            created_by: Some("cli".to_string()),
                        })
                        .await?
                        .into_inner();
                    let id = resp.deployment_id.context("missing deployment_id")?.id;
                    println!("Submitted as Candidate: {id}");
                    id
                };

                let resp = client
                    .switch_deployment(grpc_gen::SwitchDeploymentRequest {
                        deployment_id: Some(grpc_gen::DeploymentId { id: deployment_id }),
                        verify,
                        hot_redeploy: hot,
                    })
                    .await?
                    .into_inner();

                match resp.outcome() {
                    Outcome::SwitchOutcomeSwitched => {
                        println!("Hot-redeployed successfully.");
                    }
                    Outcome::SwitchOutcomeRestartRequired => {
                        if hot {
                            bail!("Could not hot-redeploy; deployment queued. Restart the server to apply.");
                        }
                        println!("Deployment activated. Restart the server to apply.");
                    }
                    Outcome::SwitchOutcomeUnspecified => {
                        bail!("Unexpected outcome from server.");
                    }
                }
                Ok(())
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
                    "{:<26}  {:<12}  {:<20}  {:<20}  {}",
                    "ID", "STATUS", "CREATED_AT", "UPDATED_AT", "CONFIG_HASH"
                );
                for summary in resp.deployments {
                    let dep = summary.deployment.context("missing deployment")?;
                    let id = dep.deployment_id.as_ref().map(|d| d.id.as_str()).unwrap_or_default().to_string();
                    let status = format_status(dep.status());
                    let created = format_timestamp(dep.created_at);
                    let updated = format_timestamp(dep.updated_at);
                    println!(
                        "{:<26}  {:<12}  {:<20}  {:<20}  {}",
                        id, status, created, updated, dep.config_hash
                    );
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

async fn load_config_json(config: Option<ConfigSource>) -> anyhow::Result<String> {
    let holder = ConfigHolder::new(
        project_dirs(),
        BaseDirs::new(),
        ConfigFileOption::MustExist(config.unwrap_or_default()),
    )?;
    let config_toml = holder.load_config().await?;
    Ok(crate::config::toml::compute_config_json(&config_toml.deployment))
}

fn format_status(status: grpc_gen::DeploymentStatus) -> &'static str {
    match status {
        grpc_gen::DeploymentStatus::Candidate => "Candidate",
        grpc_gen::DeploymentStatus::Active => "Active",
        grpc_gen::DeploymentStatus::Superseded => "Superseded",
        _ => "Unknown",
    }
}

fn format_timestamp(ts: Option<Timestamp>) -> String {
    let Some(ts) = ts else {
        return "N/A".to_string();
    };
    DateTime::from_timestamp(ts.seconds, ts.nanos as u32)
        .map(|dt: DateTime<Utc>| dt.format("%Y-%m-%dT%H:%M:%SZ").to_string())
        .unwrap_or_else(|| "Invalid".to_string())
}
