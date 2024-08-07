mod args;
mod command;
mod config;
mod grpc_util;
mod init;
mod oci;

use anyhow::{bail, Context};
use args::{Args, Daemon, Subcommand};
use clap::Parser;
use command::grpc::{
    function_repository_client::FunctionRepositoryClient, scheduler_client::SchedulerClient,
};
use config::toml::ConfigHolder;
use directories::ProjectDirs;
use tonic::{codec::CompressionEncoding, transport::Channel};

pub type StdError = Box<dyn std::error::Error + Send + Sync + 'static>;

fn main() -> Result<(), anyhow::Error> {
    let _guard = init::init();
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(main_async())
}

#[allow(clippy::too_many_lines)]
async fn main_async() -> Result<(), anyhow::Error> {
    let config_holder = ConfigHolder::new(ProjectDirs::from("com", "obelisk", "obelisk"));
    let config = config_holder.load_config().await?;
    let grpc_addr = "127.0.0.1:50055";
    let grpc_url = format!("http://{grpc_addr}");

    match Args::parse().command {
        Subcommand::Executor(Daemon::Serve { clean }) => {
            command::daemon::run(config, clean, config_holder, grpc_addr.parse()?).await
        }
        Subcommand::Component(args::Component::Inspect { path, verbosity }) => {
            command::component::inspect(path, FunctionMetadataVerbosity::from(verbosity)).await
        }
        Subcommand::Component(args::Component::List { verbosity }) => {
            let client = get_fn_repository_client(grpc_url).await?;
            command::component::find_components(
                client,
                None,
                None,
                FunctionMetadataVerbosity::from(verbosity),
            )
            .await
        }
        Subcommand::Execution(args::Execution::Schedule {
            ffqn,
            params,
            follow,
            verbosity,
        }) => {
            // TODO interactive search for ffqn showing param types and result, file name
            // enter parameters one by one
            let client = get_scheduler_client(grpc_url).await?;
            let params = serde_json::from_str(&params).context("params should be a json array")?;
            let serde_json::Value::Array(params) = params else {
                bail!("params should be a JSON array");
            };
            command::execution::submit(client, ffqn, params, follow, verbosity.into()).await
        }
        Subcommand::Execution(args::Execution::Get {
            execution_id,
            verbosity,
            follow,
        }) => {
            let client = get_scheduler_client(grpc_url).await?;
            command::execution::get(client, execution_id, follow, verbosity.into()).await
        }
    }
}

async fn get_scheduler_client<D>(url: D) -> Result<SchedulerClient<Channel>, anyhow::Error>
where
    D: TryInto<tonic::transport::Endpoint>,
    D::Error: Into<StdError>,
{
    Ok(SchedulerClient::connect(url)
        .await
        .context("cannot create gRPC client")?
        .send_compressed(CompressionEncoding::Zstd)
        .accept_compressed(CompressionEncoding::Zstd)
        .accept_compressed(CompressionEncoding::Gzip))
}

async fn get_fn_repository_client<D>(
    url: D,
) -> Result<FunctionRepositoryClient<Channel>, anyhow::Error>
where
    D: TryInto<tonic::transport::Endpoint>,
    D::Error: Into<StdError>,
{
    Ok(FunctionRepositoryClient::connect(url)
        .await
        .context("cannot create gRPC client")?
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
