#![recursion_limit = "512"]

mod args;
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
use grpc::{grpc_gen, injector::TracingInjector};
use tonic::{codec::CompressionEncoding, transport::Channel};
use tracing::error;

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

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
        Subcommand::Deployment(deployment) => deployment.run().await,
        Subcommand::Generate(generate) => generate.run().await,
    }
}

pub(crate) fn project_dirs() -> Option<ProjectDirs> {
    ProjectDirs::from("", "obelisk", "obelisk")
}

/// Maximum encoded gRPC message size for the deployment repository service. Submit
/// requests inline deployment-owned file blobs and `GetFile` returns them, so the
/// default 4 MiB tonic limit is far too small. This caps both the server's decoding
/// and the client's encoding/decoding for that service.
pub(crate) const MAX_GRPC_MESSAGE_SIZE: usize = 512 * 1024 * 1024;

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
type DeploymentRepositoryClient =
    grpc_gen::deployment_repository_client::DeploymentRepositoryClient<
        tonic::service::interceptor::InterceptedService<Channel, TracingInjector>,
    >;
async fn get_deployment_repository_client(
    channel: Channel,
) -> Result<DeploymentRepositoryClient, anyhow::Error> {
    Ok(
        grpc_gen::deployment_repository_client::DeploymentRepositoryClient::with_interceptor(
            channel,
            TracingInjector,
        )
        .send_compressed(CompressionEncoding::Zstd)
        .accept_compressed(CompressionEncoding::Zstd)
        .accept_compressed(CompressionEncoding::Gzip)
        .max_encoding_message_size(MAX_GRPC_MESSAGE_SIZE)
        .max_decoding_message_size(MAX_GRPC_MESSAGE_SIZE),
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
