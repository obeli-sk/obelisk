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

use args::{Args, ComponentArgs, DeploymentArgs, ExecutionArgs, Server, Subcommand};
use clap::Parser;
use client::ClientStartup;
use config::config_holder::ConfigHolder;
use config::secret_registry::{SecretRegistry, SecretsToml};
use config::toml::ServerConfigToml;
use directories::{BaseDirs, ProjectDirs};
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

fn main() -> Result<(), anyhow::Error> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("default tls provider must be installed");
    let args = Args::parse();
    let command = args.command;

    type CommandFuture = Pin<Box<dyn Future<Output = Result<(), anyhow::Error>>>>;
    let future: CommandFuture = match command {
        Subcommand::Server(server) => {
            let (Server::Run { server_config, .. } | Server::Verify { server_config, .. }) =
                &server;
            let ServerStartup {
                config_holder,
                config,
                secret_registry,
            } = prepare_server_startup(server_config.clone())?;
            Box::pin(server.run(config_holder, config, secret_registry))
        }
        Subcommand::Component(ComponentArgs { command, token }) => {
            // Resolve the client token before the wipe in `resolve_and_wipe`.
            let client_startup = ClientStartup::new(token.api_token);
            let secret_registry = Arc::new(SecretRegistry::resolve_and_wipe(SecretsToml::new())?);
            Box::pin(command.run(client_startup, secret_registry))
        }
        Subcommand::Execution(ExecutionArgs { command, token }) => {
            Box::pin(command.run(ClientStartup::new(token.api_token)))
        }
        Subcommand::Deployment(DeploymentArgs { command, token }) => {
            Box::pin(command.run(ClientStartup::new(token.api_token)))
        }
        Subcommand::Generate(generate) => {
            let secret_registry = Arc::new(SecretRegistry::resolve_and_wipe(SecretsToml::new())?);
            Box::pin(generate.run(secret_registry))
        }
    };

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("cannot build tokio runtime");
    runtime.block_on(future)
}

struct ServerStartup {
    config_holder: ConfigHolder,
    config: ServerConfigToml,
    secret_registry: Arc<SecretRegistry>,
}

/// Parse the complete server config once, then resolve and wipe its secret sources
/// before the runtime starts.
fn prepare_server_startup(server_config: Option<PathBuf>) -> anyhow::Result<ServerStartup> {
    let config_holder = ConfigHolder::new(project_dirs(), BaseDirs::new(), server_config)?;
    let config = config_holder.load_config_sync()?;
    let secret_registry = Arc::new(SecretRegistry::resolve_and_wipe(config.secrets.clone())?);
    Ok(ServerStartup {
        config_holder,
        config,
        secret_registry,
    })
}

pub(crate) fn project_dirs() -> Option<ProjectDirs> {
    ProjectDirs::from("", "obelisk", "obelisk")
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd)]
enum FunctionMetadataVerbosity {
    ExportsOnly,
    ExportsAndImports,
}
