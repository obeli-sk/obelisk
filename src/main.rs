mod args;
mod command;
mod config;
mod env_vars;
mod grpc_util;
mod init;
mod oci;

use anyhow::{bail, Context};
use args::{Args, Client, ClientSubcommand, Server, Subcommand};
use clap::Parser;
use command::grpc::{
    function_repository_client::FunctionRepositoryClient, scheduler_client::SchedulerClient,
};
use config::config_holder::ConfigHolder;
use directories::ProjectDirs;
use grpc_util::to_channel;
use tonic::{codec::CompressionEncoding, transport::Channel};

pub type StdError = Box<dyn std::error::Error + Send + Sync + 'static>;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    match Args::parse().command {
        Subcommand::Server(Server::Run {
            clean_db,
            clean_cache,
            clean_codegen_cache,
        }) => {
            let config_holder = ConfigHolder::new(ProjectDirs::from("com", "obelisk", "obelisk"));
            let config = config_holder.load_config().await?;

            Box::pin(command::server::run(
                config,
                clean_db,
                clean_cache,
                clean_codegen_cache,
                config_holder,
            ))
            .await
        }
        Subcommand::Client(Client { api_url, command }) => {
            match command {
                ClientSubcommand::Component(args::Component::Inspect { path, verbosity }) => {
                    command::component::inspect(path, FunctionMetadataVerbosity::from(verbosity))
                }
                ClientSubcommand::Component(args::Component::List { verbosity }) => {
                    let client = get_fn_repository_client(api_url).await?;
                    command::component::find_components(
                        client,
                        None,
                        None,
                        FunctionMetadataVerbosity::from(verbosity),
                    )
                    .await
                }
                ClientSubcommand::Component(args::Component::Push { path, image_name }) => {
                    oci::push(&path, &image_name).await
                }
                ClientSubcommand::Execution(args::Execution::Submit {
                    ffqn,
                    params,
                    follow,
                    verbosity,
                    correlation_id,
                }) => {
                    // TODO interactive search for ffqn showing param types and result, file name
                    // enter parameters one by one
                    let client = get_scheduler_client(api_url).await?;
                    let params =
                        serde_json::from_str(&params).context("params should be a json array")?;
                    let serde_json::Value::Array(params) = params else {
                        bail!("params should be a JSON array");
                    };
                    command::execution::submit(
                        client,
                        ffqn,
                        params,
                        follow,
                        verbosity.into(),
                        correlation_id,
                    )
                    .await
                }
                ClientSubcommand::Execution(args::Execution::Get {
                    execution_id,
                    verbosity,
                    follow,
                }) => {
                    let client = get_scheduler_client(api_url).await?;
                    command::execution::get(client, execution_id, follow, verbosity.into()).await
                }
            }
        }
    }
}

async fn get_scheduler_client(url: String) -> Result<SchedulerClient<Channel>, anyhow::Error> {
    Ok(SchedulerClient::new(to_channel(url).await?)
        .send_compressed(CompressionEncoding::Zstd)
        .accept_compressed(CompressionEncoding::Zstd)
        .accept_compressed(CompressionEncoding::Gzip))
}

async fn get_fn_repository_client(
    url: String,
) -> Result<FunctionRepositoryClient<Channel>, anyhow::Error> {
    Ok(FunctionRepositoryClient::new(to_channel(url).await?)
        .send_compressed(CompressionEncoding::Zstd)
        .accept_compressed(CompressionEncoding::Zstd)
        .accept_compressed(CompressionEncoding::Gzip))
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd)]
enum FunctionMetadataVerbosity {
    ExportsOnly,
    ExportsAndImports,
}

impl From<u8> for FunctionMetadataVerbosity {
    fn from(verbosity: u8) -> Self {
        match verbosity {
            0 => FunctionMetadataVerbosity::ExportsOnly,
            _ => FunctionMetadataVerbosity::ExportsAndImports,
        }
    }
}
