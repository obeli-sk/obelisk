use crate::args;
use crate::client::{ClientStartup, send_bytes, send_json};
use crate::command::deployment::{pull_deployment, write_new_file};
use anyhow::Context as _;
use concepts::prefixed_ulid::DeploymentId;
use http::header::ACCEPT;
use rest_types::server::ServerInfoResponse;
use std::path::PathBuf;

impl args::App {
    pub(crate) async fn run(self, client_startup: ClientStartup) -> anyhow::Result<()> {
        match self {
            args::App::Pull(args::AppPull {
                output,
                force,
                include_generated_metadata,
            }) => {
                let api_url = client_startup.api_url();
                let client = client_startup.web_api_client()?;
                let app_toml = send_bytes(
                    client
                        .get(format!("{api_url}/v1/app-toml"))
                        .header(ACCEPT, "application/toml"),
                )
                .await?;
                let info: ServerInfoResponse = send_json(
                    client
                        .get(format!("{api_url}/v1/info"))
                        .header(ACCEPT, "application/json"),
                )
                .await?;
                let deployment_id: DeploymentId = info
                    .deployment_id
                    .parse()
                    .with_context(|| format!("invalid deployment ID `{}`", info.deployment_id))?;

                let output_dir = output.unwrap_or_else(|| PathBuf::from("."));
                tokio::fs::create_dir_all(&output_dir)
                    .await
                    .with_context(|| format!("cannot create output directory {output_dir:?}"))?;
                let app_path = output_dir.join("app.toml");
                write_new_file(&app_path, &app_toml, force).await?;
                eprintln!("Wrote {} for app {}", app_path.display(), info.app_name);
                pull_deployment(
                    &client_startup,
                    deployment_id,
                    &output_dir,
                    force,
                    include_generated_metadata,
                )
                .await
            }
        }
    }
}
