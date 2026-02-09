mod args;
mod command;
mod config;
mod env_vars;
mod github;
mod init;
mod oci;
mod server;
mod wit_printer;

use args::{Args, Subcommand};
use clap::Parser;
use directories::ProjectDirs;
use grpc::{grpc_gen, injector::TracingInjector};
use tonic::{codec::CompressionEncoding, transport::Channel};
use tracing::error;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("default tls provider must be installed");
    match Args::parse().command {
        Subcommand::Server(server) => server
            .run()
            .await
            .inspect_err(|err| error!("Server error: {err:#?}")),
        Subcommand::Component(component) => component.run().await,
        Subcommand::Execution(execution) => execution.run().await,
        Subcommand::Generate(generate) => generate.run().await,
    }
}

pub(crate) fn project_dirs() -> Option<ProjectDirs> {
    ProjectDirs::from("", "obelisk", "obelisk")
}

type ExecutionRepositoryClient = grpc_gen::execution_repository_client::ExecutionRepositoryClient<
    tonic::service::interceptor::InterceptedService<Channel, TracingInjector>,
>;

async fn get_execution_repository_client(
    channel: Channel,
) -> Result<ExecutionRepositoryClient, anyhow::Error> {
    Ok(
        grpc_gen::execution_repository_client::ExecutionRepositoryClient::with_interceptor(
            channel,
            TracingInjector,
        )
        .send_compressed(CompressionEncoding::Zstd)
        .accept_compressed(CompressionEncoding::Zstd)
        .accept_compressed(CompressionEncoding::Gzip),
    )
}
type FunctionRepositoryClient = grpc_gen::function_repository_client::FunctionRepositoryClient<
    tonic::service::interceptor::InterceptedService<Channel, TracingInjector>,
>;
async fn get_fn_repository_client(
    channel: Channel,
) -> Result<FunctionRepositoryClient, anyhow::Error> {
    Ok(
        grpc_gen::function_repository_client::FunctionRepositoryClient::with_interceptor(
            channel,
            TracingInjector,
        )
        .send_compressed(CompressionEncoding::Zstd)
        .accept_compressed(CompressionEncoding::Zstd)
        .accept_compressed(CompressionEncoding::Gzip),
    )
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd)]
enum FunctionMetadataVerbosity {
    ExportsOnly,
    ExportsAndImports,
}
