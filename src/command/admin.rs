use crate::{
    args,
    client::{ClientStartup, send_json},
    server::web_api_server::admin::{
        CleanupRequest, CleanupResponse, DeleteDeploymentResponse, DeleteResponse,
        RetainDeploymentsRequest, RetainSystemEventsRequest, RetainSystemEventsResponse,
        StorageStatusResponse, SystemEventsResponse,
    },
};
use http::header::ACCEPT;
use serde::Serialize;

#[derive(Serialize)]
struct ExecutionDeleteResult {
    execution_id: concepts::ExecutionId,
    #[serde(flatten)]
    response: DeleteResponse,
}

#[derive(Serialize)]
struct DeploymentDeleteResult {
    deployment_id: concepts::prefixed_ulid::DeploymentId,
    #[serde(flatten)]
    response: DeleteDeploymentResponse,
}

impl args::Admin {
    pub(crate) async fn run(self, client_startup: ClientStartup) -> anyhow::Result<()> {
        let client = client_startup.web_api_client()?;
        match self {
            Self::Events(args::AdminEvents::Get {
                event_id,
                json,
                api_url,
            }) => {
                let event: crate::server::web_api_server::admin::SystemEventResponse = send_json(
                    client
                        .get(format!("{api_url}/v1/admin/system-events/{event_id}"))
                        .header(ACCEPT, "application/json"),
                )
                .await?;
                if json {
                    println!("{}", serde_json::to_string_pretty(&event)?);
                } else {
                    println!(
                        "{} {} {}: {}",
                        event.created_at.to_rfc3339(),
                        event.level,
                        event.code,
                        event.message
                    );
                }
                Ok(())
            }
            Self::Executions(args::AdminExecutions::Delete {
                execution_ids,
                force,
                json,
                api_url,
            }) => {
                if force {
                    eprintln!(
                        "WARNING: forcibly deleting non-terminal execution trees whose roots do not belong to the active deployment"
                    );
                }
                let mut results = Vec::with_capacity(execution_ids.len());
                for execution_id in execution_ids {
                    eprintln!("Deleting execution tree {execution_id}");
                    let response: DeleteResponse = send_json(
                        client
                            .delete(format!("{api_url}/v1/admin/executions/{execution_id}"))
                            .query(&[("force_non_terminal", force)])
                            .header(ACCEPT, "application/json"),
                    )
                    .await?;
                    if !json {
                        println!(
                            "{execution_id}: {}",
                            if response.deleted {
                                "deleted"
                            } else {
                                "already absent"
                            }
                        );
                    }
                    results.push(ExecutionDeleteResult {
                        execution_id,
                        response,
                    });
                }
                if json {
                    println!("{}", serde_json::to_string_pretty(&results)?);
                }
                Ok(())
            }
            Self::Executions(args::AdminExecutions::Retain {
                count,
                max_age,
                batch_size,
                force,
                dry_run,
                json,
                api_url,
            }) => {
                if force {
                    eprintln!(
                        "WARNING: forcibly deleting non-terminal execution trees whose roots do not belong to the active deployment"
                    );
                }
                if !dry_run {
                    eprintln!("Deleting execution trees outside the retention policy");
                }
                let response: CleanupResponse = send_json(
                    client
                        .post(format!("{api_url}/v1/admin/executions/retain"))
                        .header(ACCEPT, "application/json")
                        .json(&CleanupRequest {
                            retain_count: count,
                            max_age_seconds: max_age.map(|age| age.as_secs()),
                            batch_size,
                            force_non_terminal: force,
                            dry_run,
                        }),
                )
                .await?;
                let action = if dry_run { "would delete" } else { "deleted" };
                let message = format!(
                    "{} execution tree(s) {action}; {} blocked; has_more={}",
                    response.deleted_execution_trees,
                    response.blocked_non_terminal,
                    response.has_more
                );
                print_result(json, &response, &message)
            }
            Self::Deployments(args::AdminDeployments::Delete {
                deployment_ids,
                delete_executions,
                force,
                json,
                api_url,
            }) => {
                if force {
                    eprintln!(
                        "WARNING: forcibly deleting non-terminal execution trees whose roots do not belong to the active deployment"
                    );
                }
                let mut results = Vec::with_capacity(deployment_ids.len());
                for deployment_id in deployment_ids {
                    eprintln!("Deleting deployment {deployment_id}");
                    let response: DeleteDeploymentResponse = send_json(
                        client
                            .delete(format!("{api_url}/v1/admin/deployments/{deployment_id}"))
                            .query(&[
                                ("delete_executions", delete_executions),
                                ("force_non_terminal", force),
                            ])
                            .header(ACCEPT, "application/json"),
                    )
                    .await?;
                    if !json {
                        println!(
                            "{deployment_id}: {} ({} execution tree(s) deleted)",
                            if response.deleted {
                                "deleted"
                            } else {
                                "already absent"
                            },
                            response.deleted_execution_trees
                        );
                    }
                    results.push(DeploymentDeleteResult {
                        deployment_id,
                        response,
                    });
                }
                if json {
                    println!("{}", serde_json::to_string_pretty(&results)?);
                }
                Ok(())
            }
            Self::Deployments(args::AdminDeployments::Retain {
                count,
                max_age,
                delete_executions,
                force,
                batch_size,
                dry_run,
                json,
                api_url,
            }) => {
                if force {
                    eprintln!(
                        "WARNING: forcibly deleting non-terminal execution trees whose roots do not belong to the active deployment"
                    );
                }
                if !dry_run {
                    eprintln!("Deleting inactive deployments outside the retention policy");
                }
                let response: CleanupResponse = send_json(
                    client
                        .post(format!("{api_url}/v1/admin/deployments/retain"))
                        .header(ACCEPT, "application/json")
                        .json(&RetainDeploymentsRequest {
                            retain_count: count,
                            max_age_seconds: max_age.map(|age| age.as_secs()),
                            batch_size,
                            delete_executions,
                            force_non_terminal: force,
                            dry_run,
                        }),
                )
                .await?;
                let action = if dry_run { "would delete" } else { "deleted" };
                let message = format!(
                    "{} deployment(s) {action}; {} referenced and {} non-terminal blocked; has_more={}",
                    response.deleted_deployments,
                    response.blocked_by_execution_reference,
                    response.blocked_non_terminal,
                    response.has_more
                );
                print_result(json, &response, &message)
            }
            Self::Events(args::AdminEvents::List {
                server_run_id,
                level,
                code,
                limit,
                json,
                api_url,
            }) => {
                let response: SystemEventsResponse = send_json(
                    client
                        .get(format!("{api_url}/v1/admin/system-events"))
                        .query(&[
                            ("server_run_id", server_run_id.map(|id| id.to_string())),
                            ("level", level),
                            ("code", code),
                        ])
                        .query(&[("limit", limit)])
                        .header(ACCEPT, "application/json"),
                )
                .await?;
                if json {
                    println!("{}", serde_json::to_string_pretty(&response)?);
                } else {
                    for event in response.events {
                        println!(
                            "{} {} {}: {}",
                            event.created_at.to_rfc3339(),
                            event.level,
                            event.code,
                            event.message
                        );
                    }
                }
                Ok(())
            }
            Self::Events(args::AdminEvents::Retain {
                max_age,
                batch_size,
                json,
                api_url,
            }) => {
                let response: RetainSystemEventsResponse = send_json(
                    client
                        .post(format!("{api_url}/v1/admin/system-events/retain"))
                        .header(ACCEPT, "application/json")
                        .json(&RetainSystemEventsRequest {
                            max_age_seconds: max_age.as_secs(),
                            batch_size,
                        }),
                )
                .await?;
                let message = format!(
                    "{} system event(s) deleted; has_more={}",
                    response.deleted, response.has_more
                );
                print_result(json, &response, &message)
            }
            Self::Storage(args::AdminStorage::Show { json, api_url }) => {
                let response: StorageStatusResponse = send_json(
                    client
                        .get(format!("{api_url}/v1/admin/storage"))
                        .header(ACCEPT, "application/json"),
                )
                .await?;
                let message = format!(
                    "database: {} bytes; executions: {}; deployments: {}; system events: {}",
                    response
                        .database_bytes
                        .map_or_else(|| "unknown".into(), |n| n.to_string()),
                    response.execution_count,
                    response.deployment_count,
                    response.system_event_count
                );
                print_result(json, &response, &message)
            }
        }
    }
}

fn print_result<T: serde::Serialize>(json: bool, value: &T, message: &str) -> anyhow::Result<()> {
    if json {
        println!("{}", serde_json::to_string_pretty(value)?);
    } else {
        println!("{message}");
    }
    Ok(())
}
