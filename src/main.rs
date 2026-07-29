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
use config::secret_registry::SecretRegistry;
use directories::ProjectDirs;
use std::path::Path;
use std::sync::Arc;
use tracing::error;

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

/// `main` is intentionally a plain `fn`, not `#[tokio::main]`: the secret registry is
/// built (and env-backed secret sources wiped from the process environment) while the
/// process is still single-threaded, before the tokio runtime is constructed. See
/// `SecretRegistry::load_and_wipe` and `meta/designs/secret-registry.md`.
fn main() -> Result<(), anyhow::Error> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("default tls provider must be installed");
    let args = Args::parse();
    client::init_api_token(args.api_token);
    let command = args.command;

    // Resolve secrets and wipe their source env vars before starting the runtime.
    let secret_registry = Arc::new(SecretRegistry::load_and_wipe(server_config_for_secrets(
        &command,
    ))?);

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("cannot build tokio runtime");
    runtime.block_on(run_command(command, secret_registry))
}

/// The server config path whose `[secrets]` table seeds the registry. Only the server
/// subcommands consult secrets; every other subcommand runs with an empty registry.
fn server_config_for_secrets(command: &Subcommand) -> Option<&Path> {
    match command {
        Subcommand::Server(
            Server::Run { server_config, .. } | Server::Verify { server_config, .. },
        ) => server_config.as_deref(),
        _ => None,
    }
}

async fn run_command(
    command: Subcommand,
    secret_registry: Arc<SecretRegistry>,
) -> Result<(), anyhow::Error> {
    match command {
        Subcommand::Server(server) => server
            .run(secret_registry)
            .await
            .inspect_err(|err| error!("Server error: {err:#?}")),
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
