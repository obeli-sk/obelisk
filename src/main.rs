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

use args::{Args, Subcommand};
use clap::Parser;
use directories::ProjectDirs;
use tracing::error;

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("default tls provider must be installed");
    let args = Args::parse();
    client::init_api_token(args.api_token);
    match args.command {
        Subcommand::Server(server) => server
            .run()
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
