use crate::{
    args,
    client::{ClientStartup, send_json},
    server::web_api_server::admin::{
        CleanupRequest, CleanupResponse, DeleteDeploymentResponse, DeleteResponse, GcCasRequest,
        GcCasResponse, RetainDeploymentsRequest,
    },
};
use http::header::ACCEPT;

impl args::Admin {
    pub(crate) async fn run(self, client_startup: ClientStartup) -> anyhow::Result<()> {
        let client = client_startup.web_api_client()?;
        match self {
            Self::Execution(args::AdminExecution::Delete {
                execution_id,
                json,
                api_url,
            }) => {
                eprintln!("Deleting execution tree {execution_id}");
                let response: DeleteResponse = send_json(
                    client
                        .delete(format!("{api_url}/v1/admin/executions/{execution_id}"))
                        .header(ACCEPT, "application/json"),
                )
                .await?;
                print_result(
                    json,
                    &response,
                    if response.deleted {
                        "Execution tree deleted."
                    } else {
                        "Execution tree was already absent."
                    },
                )
            }
            Self::Executions(args::AdminExecutions::Retain {
                count,
                batch_size,
                dry_run,
                json,
                api_url,
            }) => {
                if !dry_run {
                    eprintln!("Deleting completed execution trees older than the newest {count}");
                }
                let response: CleanupResponse = send_json(
                    client
                        .post(format!("{api_url}/v1/admin/executions/retain"))
                        .header(ACCEPT, "application/json")
                        .json(&CleanupRequest {
                            retain_count: count,
                            batch_size,
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
            Self::Deployment(args::AdminDeployment::Delete {
                deployment_id,
                delete_executions,
                json,
                api_url,
            }) => {
                eprintln!("Deleting deployment {deployment_id}");
                let response: DeleteDeploymentResponse = send_json(
                    client
                        .delete(format!("{api_url}/v1/admin/deployments/{deployment_id}"))
                        .query(&[("delete_executions", delete_executions)])
                        .header(ACCEPT, "application/json"),
                )
                .await?;
                print_result(
                    json,
                    &response,
                    if response.deleted {
                        "Deployment deleted."
                    } else {
                        "Deployment was already absent."
                    },
                )
            }
            Self::Deployments(args::AdminDeployments::Retain {
                count,
                delete_executions,
                batch_size,
                dry_run,
                json,
                api_url,
            }) => {
                if !dry_run {
                    eprintln!("Deleting inactive deployments older than the newest {count}");
                }
                let response: CleanupResponse = send_json(
                    client
                        .post(format!("{api_url}/v1/admin/deployments/retain"))
                        .header(ACCEPT, "application/json")
                        .json(&RetainDeploymentsRequest {
                            retain_count: count,
                            batch_size,
                            delete_executions,
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
            Self::CasGc {
                dry_run,
                json,
                api_url,
            } => {
                if !dry_run {
                    eprintln!("Deleting unreferenced CAS blobs");
                }
                let response: GcCasResponse = send_json(
                    client
                        .post(format!("{api_url}/v1/admin/cas/gc"))
                        .header(ACCEPT, "application/json")
                        .json(&GcCasRequest { dry_run }),
                )
                .await?;
                let message = format!(
                    "{} orphan blob(s), {} deleted ({} bytes).",
                    response.orphan_blobs, response.deleted_blobs, response.deleted_bytes
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
