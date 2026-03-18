use crate::args;
use crate::config::config_holder::load_deployment_toml;
use crate::get_deployment_repository_client;
use crate::project_dirs;
use anyhow::{Context as _, bail};
use chrono::DateTime;
use directories::BaseDirs;
use grpc::grpc_gen;
use grpc::grpc_gen::switch_deployment_response::Outcome;
use grpc::to_channel;

impl args::Deployment {
    pub(crate) async fn run(self) -> Result<(), anyhow::Error> {
        match self {
            args::Deployment::Submit {
                deployment,
                verify,
                api_url,
            } => {
                let config_json = load_config_json(deployment).await?;
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

            args::Deployment::Switch {
                id,
                hot,
                verify,
                api_url,
            } => {
                let channel = to_channel(&api_url).await?;
                let mut client = get_deployment_repository_client(channel).await?;

                let resp = client
                    .switch_deployment(grpc_gen::SwitchDeploymentRequest {
                        deployment_id: Some(grpc_gen::DeploymentId { id }),
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
                            bail!(
                                "Could not hot-redeploy; deployment queued. Restart the server to apply."
                            );
                        }
                        println!("Deployment queued. Restart the server to apply.");
                    }
                    Outcome::SwitchOutcomeUnspecified => {
                        bail!("Unexpected outcome from server.");
                    }
                }
                Ok(())
            }

            args::Deployment::SubmitSwitch {
                deployment,
                hot,
                verify,
                api_url,
            } => {
                let config_json = load_config_json(deployment).await?;
                let channel = to_channel(&api_url).await?;
                let mut client = get_deployment_repository_client(channel).await?;
                let resp = client
                    .submit_deployment(grpc_gen::SubmitDeploymentRequest {
                        config_json,
                        created_by: Some("cli".to_string()),
                        verify: false,
                    })
                    .await?
                    .into_inner();
                let id = resp.deployment_id.context("missing deployment_id")?.id;
                println!("Submitted as {id}");

                let resp = client
                    .switch_deployment(grpc_gen::SwitchDeploymentRequest {
                        deployment_id: Some(grpc_gen::DeploymentId { id }),
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
                            bail!(
                                "Could not hot-redeploy; deployment queued. Restart the server to apply."
                            );
                        }
                        println!("Deployment queued. Restart the server to apply.");
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

async fn load_config_json(deployment_path: std::path::PathBuf) -> anyhow::Result<String> {
    let (deployment_toml, deployment_dir) = load_deployment_toml(deployment_path).await?;
    let project_dirs = project_dirs();
    let base_dirs = BaseDirs::new();
    let path_prefixes = crate::config::config_holder::PathPrefixes {
        server_config_dir: None,
        deployment_dir: Some(deployment_dir),
        project_dirs,
        base_dirs,
    };
    let deployment =
        crate::config::toml::resolve_local_refs_to_canonical(&deployment_toml, &path_prefixes)
            .await?;
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
