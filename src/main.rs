mod args;
mod command;
mod config;
mod env_vars;
mod grpc_util;
mod init;
mod oci;

use anyhow::{Context, bail};
use args::{Args, Client, ClientSubcommand, Generate, Server, Subcommand};
use clap::Parser;
use command::{
    execution::{GetStatusOptions, SubmitOutputOpts},
    server::{RunParams, VerifyParams},
};
use config::config_holder::ConfigHolder;
use directories::{BaseDirs, ProjectDirs};
use grpc_util::{grpc_gen, injector::TracingInjector, to_channel};
use std::path::PathBuf;
use tonic::{codec::CompressionEncoding, transport::Channel};

pub type StdError = Box<dyn std::error::Error + Send + Sync + 'static>;

#[tokio::main]
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
                BaseDirs::new(),
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
            clean_cache,
            clean_codegen_cache,
            config,
            ignore_missing_env_vars,
        }) => {
            command::server::verify(
                project_dirs(),
                BaseDirs::new(),
                config,
                VerifyParams {
                    clean_cache,
                    clean_codegen_cache,
                    ignore_missing_env_vars,
                },
            )
            .await
        }
        Subcommand::Client(Client { api_url, command }) => match command {
            ClientSubcommand::Component(args::Component::Inspect {
                path,
                component_type,
                imports,
                extensions,
                convert_core_module,
            }) => {
                command::component::inspect(
                    path,
                    component_type,
                    if imports {
                        FunctionMetadataVerbosity::ExportsAndImports
                    } else {
                        FunctionMetadataVerbosity::ExportsOnly
                    },
                    extensions,
                    convert_core_module,
                )
                .await
            }
            ClientSubcommand::Component(args::Component::List {
                imports,
                extensions,
            }) => {
                let client = get_fn_repository_client(api_url).await?;
                command::component::list_components(
                    client,
                    if imports {
                        FunctionMetadataVerbosity::ExportsAndImports
                    } else {
                        FunctionMetadataVerbosity::ExportsOnly
                    },
                    extensions,
                )
                .await
            }
            ClientSubcommand::Component(args::Component::Push { path, image_name }) => {
                oci::push(path, &image_name).await
            }
            ClientSubcommand::Execution(args::Execution::Submit {
                ffqn,
                params,
                follow,
                json: json_output,
                no_reconnect,
            }) => {
                let client = get_execution_repository_client(api_url).await?;
                let params =
                    serde_json::from_str(&params).context("params should be a json array")?;
                let serde_json::Value::Array(params) = params else {
                    bail!("params should be a JSON array");
                };
                let opts = if json_output {
                    SubmitOutputOpts::Json
                } else {
                    SubmitOutputOpts::PlainFollow { no_reconnect }
                };
                command::execution::submit(client, ffqn, params, follow, opts).await
            }
            ClientSubcommand::Execution(args::Execution::Get {
                execution_id,
                follow,
                no_reconnect,
            }) => {
                let client = get_execution_repository_client(api_url).await?;
                let opts = GetStatusOptions {
                    follow,
                    no_reconnect,
                };
                command::execution::get_status(client, execution_id, opts).await
            }
            ClientSubcommand::Execution(args::Execution::GetJson {
                follow,
                execution_id,
            }) => {
                let client = get_execution_repository_client(api_url).await?;
                command::execution::get_status_json(client, execution_id, follow, false).await
            }
        },
        Subcommand::Generate(Generate::ConfigSchema { output }) => {
            command::generate::generate_toml_schema(output)
        }
    }
}

fn project_dirs() -> Option<ProjectDirs> {
    ProjectDirs::from("", "obelisk", "obelisk")
}

type ExecutionRepositoryClient = grpc_gen::execution_repository_client::ExecutionRepositoryClient<
    tonic::service::interceptor::InterceptedService<Channel, TracingInjector>,
>;

async fn get_execution_repository_client(
    url: String,
) -> Result<ExecutionRepositoryClient, anyhow::Error> {
    Ok(
        grpc_gen::execution_repository_client::ExecutionRepositoryClient::with_interceptor(
            to_channel(url).await?,
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
async fn get_fn_repository_client(url: String) -> Result<FunctionRepositoryClient, anyhow::Error> {
    Ok(
        grpc_gen::function_repository_client::FunctionRepositoryClient::with_interceptor(
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
