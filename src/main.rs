#![recursion_limit = "512"]

mod api;
mod args;
mod client;
mod command;
mod config;
mod env_vars;
mod init;
mod oci;
mod server;
mod wit_printer;

use args::{Args, Server, Subcommand};
use clap::Parser;
use config::config_holder::ConfigHolder;
use config::secret_registry::SecretRegistry;
use config::toml::ServerConfigToml;
use directories::{BaseDirs, ProjectDirs};
use std::sync::Arc;
use tracing::error;

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

/// `main` is intentionally a plain `fn`, not `#[tokio::main]`: the secret registry is
/// built (and env-backed secret sources wiped from the process environment) while the
/// process is still single-threaded, before the tokio runtime is constructed. See
/// `SecretRegistry::resolve_and_wipe` and `meta/designs/secret-registry.md`.
fn main() -> Result<(), anyhow::Error> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("default tls provider must be installed");
    let args = Args::parse();
    let command = args.command;

    // Parse the complete server config once, then resolve and wipe its secret
    // sources before starting the runtime.
    let server_startup = prepare_server_startup(&command)?;
    client::init_api_token(args.api_token);

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("cannot build tokio runtime");
    runtime.block_on(run_command(command, server_startup))
}

struct ServerStartup {
    config_holder: ConfigHolder,
    config: ServerConfigToml,
    secret_registry: Arc<SecretRegistry>,
}

fn prepare_server_startup(command: &Subcommand) -> anyhow::Result<Option<ServerStartup>> {
    let server_config = match command {
        Subcommand::Server(
            Server::Run { server_config, .. } | Server::Verify { server_config, .. },
        ) => server_config.clone(),
        _ => return Ok(None),
    };
    let mut config_holder = ConfigHolder::new(project_dirs(), BaseDirs::new(), server_config)?;
    let config = config_holder.load_config_sync()?;
    let secret_registry = Arc::new(SecretRegistry::resolve_and_wipe(config.secrets.clone())?);
    config_holder.set_secret_registry(secret_registry.clone());
    Ok(Some(ServerStartup {
        config_holder,
        config,
        secret_registry,
    }))
}

async fn run_command(
    command: Subcommand,
    server_startup: Option<ServerStartup>,
) -> Result<(), anyhow::Error> {
    match command {
        Subcommand::Server(server) => {
            let ServerStartup {
                config_holder,
                config,
                secret_registry,
            } = server_startup.expect("server startup must be prepared");
            server
                .run(config_holder, config, secret_registry)
                .await
                .inspect_err(|err| error!("Server error: {err:#?}"))
        }
        Subcommand::Component(component) => component.run().await,
        Subcommand::Execution(execution) => execution.run().await,
        Subcommand::Deployment(deployment) => deployment.run().await,
        Subcommand::Generate(generate) => generate.run().await,
    }
}

pub(crate) fn project_dirs() -> Option<ProjectDirs> {
    ProjectDirs::from("", "obelisk", "obelisk")
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd)]
enum FunctionMetadataVerbosity {
    ExportsOnly,
    ExportsAndImports,
}
