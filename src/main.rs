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
use secrecy::{ExposeSecret as _, SecretString};
use tonic::{codec::CompressionEncoding, transport::Channel};
use tracing::error;

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("default tls provider must be installed");
    let args = Args::parse();
    API_TOKEN
        .set(resolve_api_token(args.api_token))
        .expect("API_TOKEN is only set here");
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

/// Token this CLI invocation presents to the server, resolved once from
/// `--api-token` > `OBELISK_API_TOKEN` > `OBELISK__API__TOKEN`.
static API_TOKEN: std::sync::OnceLock<Option<SecretString>> = std::sync::OnceLock::new();

fn resolve_api_token(flag: Option<SecretString>) -> Option<SecretString> {
    flag.or_else(|| {
        std::env::var("OBELISK_API_TOKEN")
            .ok()
            .map(SecretString::from)
    })
    .or_else(|| {
        std::env::var("OBELISK__API__TOKEN")
            .ok()
            .map(SecretString::from)
    })
    .filter(|token| !token.expose_secret().is_empty())
}

pub(crate) fn api_token() -> Option<&'static SecretString> {
    API_TOKEN.get_or_init(|| None).as_ref()
}

/// Client interceptor for all gRPC calls: injects tracing metadata and, if a
/// token is configured, the `authorization` header.
#[derive(Clone)]
pub(crate) struct ClientInterceptor {
    authorization: Option<tonic::metadata::MetadataValue<tonic::metadata::Ascii>>,
}

impl ClientInterceptor {
    fn new() -> Result<Self, anyhow::Error> {
        let authorization = if let Some(token) = api_token() {
            let mut authorization: tonic::metadata::MetadataValue<tonic::metadata::Ascii> =
                format!("Bearer {}", token.expose_secret())
                    .parse()
                    .map_err(|_| anyhow::anyhow!("API token contains invalid header characters"))?;
            authorization.set_sensitive(true);
            Some(authorization)
        } else {
            None
        };
        Ok(Self { authorization })
    }
}

impl tonic::service::Interceptor for ClientInterceptor {
    fn call(&mut self, request: tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status> {
        let mut request = tonic::service::Interceptor::call(&mut TracingInjector, request)?;
        if let Some(authorization) = &self.authorization {
            request
                .metadata_mut()
                .insert("authorization", authorization.clone());
        }
        Ok(request)
    }
}

/// HTTP client for web-API calls, presenting the API token as a default header.
pub(crate) fn web_api_client() -> Result<reqwest::Client, anyhow::Error> {
    Ok(web_api_client_builder()?.build()?)
}

pub(crate) fn web_api_client_builder() -> Result<reqwest::ClientBuilder, anyhow::Error> {
    let mut headers = reqwest::header::HeaderMap::new();
    if let Some(token) = api_token() {
        let mut value =
            reqwest::header::HeaderValue::from_str(&format!("Bearer {}", token.expose_secret()))
                .map_err(|_| anyhow::anyhow!("API token contains invalid header characters"))?;
        value.set_sensitive(true);
        headers.insert(reqwest::header::AUTHORIZATION, value);
    }
    Ok(reqwest::Client::builder().default_headers(headers))
}

/// Maximum encoded gRPC message size for the deployment repository service. Submit
/// requests inline deployment-owned file blobs and `GetFile` returns them, so the
/// default 4 MiB tonic limit is far too small. This caps both the server's decoding
/// and the client's encoding/decoding for that service.
pub(crate) const MAX_GRPC_MESSAGE_SIZE: usize = 512 * 1024 * 1024;

type ExecutionRepositoryClient = grpc_gen::execution_repository_client::ExecutionRepositoryClient<
    tonic::service::interceptor::InterceptedService<Channel, ClientInterceptor>,
>;

async fn get_execution_repository_client(
    channel: Channel,
) -> Result<ExecutionRepositoryClient, anyhow::Error> {
    Ok(
        grpc_gen::execution_repository_client::ExecutionRepositoryClient::with_interceptor(
            channel,
            ClientInterceptor::new()?,
        )
        .send_compressed(CompressionEncoding::Zstd)
        .accept_compressed(CompressionEncoding::Zstd)
        .accept_compressed(CompressionEncoding::Gzip),
    )
}
type DeploymentRepositoryClient =
    grpc_gen::deployment_repository_client::DeploymentRepositoryClient<
        tonic::service::interceptor::InterceptedService<Channel, ClientInterceptor>,
    >;
async fn get_deployment_repository_client(
    channel: Channel,
) -> Result<DeploymentRepositoryClient, anyhow::Error> {
    Ok(
        grpc_gen::deployment_repository_client::DeploymentRepositoryClient::with_interceptor(
            channel,
            ClientInterceptor::new()?,
        )
        .send_compressed(CompressionEncoding::Zstd)
        .accept_compressed(CompressionEncoding::Zstd)
        .accept_compressed(CompressionEncoding::Gzip)
        .max_encoding_message_size(MAX_GRPC_MESSAGE_SIZE)
        .max_decoding_message_size(MAX_GRPC_MESSAGE_SIZE),
    )
}

type FunctionRepositoryClient = grpc_gen::function_repository_client::FunctionRepositoryClient<
    tonic::service::interceptor::InterceptedService<Channel, ClientInterceptor>,
>;
async fn get_fn_repository_client(
    channel: Channel,
) -> Result<FunctionRepositoryClient, anyhow::Error> {
    Ok(
        grpc_gen::function_repository_client::FunctionRepositoryClient::with_interceptor(
            channel,
            ClientInterceptor::new()?,
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
