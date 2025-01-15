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
use command::server::{RunParams, VerifyParams};
use config::config_holder::ConfigHolder;
use directories::ProjectDirs;
use grpc_util::{injector::TracingInjector, to_channel};
use std::path::PathBuf;
use tonic::{codec::CompressionEncoding, transport::Channel};

pub type StdError = Box<dyn std::error::Error + Send + Sync + 'static>;

#[tokio::main]
#[expect(clippy::too_many_lines)]
async fn main() -> Result<(), anyhow::Error> {
    match Args::parse().command {
        Subcommand::Server(Server::Run {
            clean_db,
            clean_cache,
            clean_codegen_cache,
            config,
        }) => {
            Box::pin(command::server::run(
                project_dirs(),
                config,
                RunParams {
                    clean_db,
                    clean_cache,
                    clean_codegen_cache,
                },
            ))
            .await
        }
        Subcommand::Server(Server::GenerateConfig) => {
            let obelisk_toml = PathBuf::from("obelisk.toml");
            ConfigHolder::generate_default_config(&obelisk_toml).await?;
            println!("Generated {obelisk_toml:?}");
            Ok(())
        }
        Subcommand::Server(Server::Verify {
            clean_db,
            clean_cache,
            clean_codegen_cache,
            config,
            ignore_missing_env_vars,
        }) => {
            command::server::verify(
                project_dirs(),
                config,
                VerifyParams {
                    clean_db,
                    clean_cache,
                    clean_codegen_cache,
                    ignore_missing_env_vars,
                },
            )
            .await
        }
        Subcommand::Client(Client { api_url, command }) => {
            match command {
                ClientSubcommand::Component(args::Component::Inspect {
                    path,
                    verbosity,
                    extensions,
                    convert_core_module,
                }) => {
                    command::component::inspect(
                        path,
                        FunctionMetadataVerbosity::from(verbosity),
                        extensions,
                        convert_core_module,
                    )
                    .await
                }
                ClientSubcommand::Component(args::Component::List {
                    verbosity,
                    extensions,
                }) => {
                    let client = get_fn_repository_client(api_url).await?;
                    command::component::list_components(
                        client,
                        FunctionMetadataVerbosity::from(verbosity),
                        extensions,
                    )
                    .await
                }
                ClientSubcommand::Component(args::Component::Push {
                    path,
                    image_name,
                    convert_core_module,
                }) => oci::push(path, &image_name, convert_core_module).await,
                ClientSubcommand::Execution(args::Execution::Submit {
                    ffqn,
                    params,
                    follow,
                    verbosity,
                }) => {
                    // TODO interactive search for ffqn showing param types and result, file name
                    // enter parameters one by one
                    let client = get_execution_repository_client(api_url).await?;
                    let params =
                        serde_json::from_str(&params).context("params should be a json array")?;
                    let serde_json::Value::Array(params) = params else {
                        bail!("params should be a JSON array");
                    };
                    command::execution::submit(client, ffqn, params, follow, verbosity.into()).await
                }
                ClientSubcommand::Execution(args::Execution::Get {
                    execution_id,
                    verbosity,
                    follow,
                }) => {
                    let client = get_execution_repository_client(api_url).await?;
                    command::execution::get(client, execution_id, follow, verbosity.into()).await
                }
            }
        }
    }
}

fn project_dirs() -> Option<ProjectDirs> {
    ProjectDirs::from("com", "obelisk", "obelisk")
}

type ExecutionRepositoryClient =
    command::grpc::execution_repository_client::ExecutionRepositoryClient<
        tonic::service::interceptor::InterceptedService<Channel, TracingInjector>,
    >;

async fn get_execution_repository_client(
    url: String,
) -> Result<ExecutionRepositoryClient, anyhow::Error> {
    Ok(
        command::grpc::execution_repository_client::ExecutionRepositoryClient::with_interceptor(
            to_channel(url).await?,
            TracingInjector,
        )
        .send_compressed(CompressionEncoding::Zstd)
        .accept_compressed(CompressionEncoding::Zstd)
        .accept_compressed(CompressionEncoding::Gzip),
    )
}
type FunctionRepositoryClient = command::grpc::function_repository_client::FunctionRepositoryClient<
    tonic::service::interceptor::InterceptedService<Channel, TracingInjector>,
>;
async fn get_fn_repository_client(url: String) -> Result<FunctionRepositoryClient, anyhow::Error> {
    Ok(
        command::grpc::function_repository_client::FunctionRepositoryClient::with_interceptor(
            to_channel(url).await?,
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

impl From<u8> for FunctionMetadataVerbosity {
    fn from(verbosity: u8) -> Self {
        match verbosity {
            0 => FunctionMetadataVerbosity::ExportsOnly,
            _ => FunctionMetadataVerbosity::ExportsAndImports,
        }
    }
}
