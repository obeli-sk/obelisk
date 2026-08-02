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

use crate::config::secret_registry::EnvVarCleanupStrategy;
use args::{
    Args, ComponentArgs, Deployment, DeploymentArgs, DeploymentVerifyArgs, ExecutionArgs, Server,
    Subcommand, VerifyArgs,
};
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
            let (server_config, env_var_cleanup) = match &server {
                Server::Run { server_config, .. } => (server_config, EnvVarCleanupStrategy::Wipe), // only wipe on `server run`, as `* verify --fix` reloads the secret registry.
                Server::Verify(VerifyArgs { server_config, .. }) => {
                    (server_config, EnvVarCleanupStrategy::Noop)
                }
            };
            let ServerStartup {
                config_holder,
                config,
                secret_registry,
            } = prepare_server_startup(server_config.clone(), env_var_cleanup)?;
            Box::pin(server.run(config_holder, config, secret_registry))
        }
        Subcommand::Component(ComponentArgs { command, token }) => {
            // Resolve the client token before the wipe in `resolve_and_wipe`.
            let client_startup = ClientStartup::new(token.api_token);
            let secret_registry = Arc::new(SecretRegistry::resolve(
                SecretsToml::new(),
                EnvVarCleanupStrategy::Noop,
            )?);
            Box::pin(command.run(client_startup, secret_registry))
        }
        Subcommand::Execution(ExecutionArgs { command, token }) => {
            Box::pin(command.run(ClientStartup::new(token.api_token)))
        }
        // `deployment verify` uses server configuration and compilation machinery locally, but
        // never opens the database or uses its content-addressed store.
        Subcommand::Deployment(DeploymentArgs {
            command: Deployment::Verify(args),
            token: _,
        }) => {
            let DeploymentVerifyArgs {
                clean_cache,
                clean_codegen_cache,
                server_config,
                deployment,
                allow_unavailable_runtime_config,
                suppress_type_checking_errors,
                fix,
            } = args;
            let ServerStartup {
                config_holder,
                config,
                secret_registry,
            } = prepare_server_startup(server_config.clone(), EnvVarCleanupStrategy::Noop)?;
            let server = Server::Verify(VerifyArgs {
                clean_cache,
                clean_codegen_cache,
                server_config,
                deployment: Some(deployment),
                allow_unavailable_runtime_config,
                suppress_type_checking_errors,
                skip_db: true,
                fix,
            });
            Box::pin(server.run(config_holder, config, secret_registry))
        }
        Subcommand::Deployment(DeploymentArgs { command, token }) => {
            Box::pin(command.run(ClientStartup::new(token.api_token)))
        }
        Subcommand::Generate(generate) => {
            let secret_registry = Arc::new(SecretRegistry::resolve(
                SecretsToml::new(),
                EnvVarCleanupStrategy::Noop,
            )?);
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
fn prepare_server_startup(
    server_config: Option<PathBuf>,
    env_var_cleanup: EnvVarCleanupStrategy,
) -> anyhow::Result<ServerStartup> {
    let config_holder = ConfigHolder::new(project_dirs(), BaseDirs::new(), server_config)?;
    let config = config_holder.load_config()?;
    let secret_registry = Arc::new(SecretRegistry::resolve(
        config.secrets.clone(),
        env_var_cleanup,
    )?);
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
