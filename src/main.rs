mod args;
mod command;
mod config;
mod init;
mod oci;

use anyhow::{bail, Context};
use args::{Args, Executor, Subcommand};
use clap::Parser;
use command::execution::ExecutionVerbosity;
use concepts::storage::ComponentToggle;
use config::toml::ConfigHolder;
use directories::ProjectDirs;

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
    let db_file = &config
        .get_sqlite_file(config_holder.project_dirs.as_ref())
        .await?;
    match Args::parse().command {
        Subcommand::Executor(Executor::Serve { clean }) => {
            command::server::run(config, db_file, clean, config_holder).await
        }
        Subcommand::Component(args::Component::Inspect {
            path,
            verbose,
        }) => {
            command::component::inspect(
                path,
                if verbose {
                    FunctionMetadataVerbosity::WithTypes
                } else {
                    FunctionMetadataVerbosity::FfqnOnly
                },
            )
            .await
        }
        Subcommand::Component(args::Component::List {
            disabled,
            verbosity,
        }) => {
            command::component::list(
                db_file,
                ComponentToggle::from(!disabled),
                match verbosity {
                    0 => None,
                    1 => Some(FunctionMetadataVerbosity::FfqnOnly),
                    _ => Some(FunctionMetadataVerbosity::WithTypes),
                },
            )
            .await
        }
        Subcommand::Component(args::Component::Get {
            config_id,
            verbosity,
        }) => {
            command::component::get(
                db_file,
                &config_id,
                match verbosity {
                    0 => None,
                    1 => Some(FunctionMetadataVerbosity::FfqnOnly),
                    _ => Some(FunctionMetadataVerbosity::WithTypes),
                },
            )
            .await
        }
        Subcommand::Execution(args::Execution::Schedule { ffqn, params }) => {
            // TODO interactive search for ffqn showing param types and result, file name
            // enter parameters one by one
            let params = serde_json::from_str(&params).context("params should be a json array")?;
            let params = match params {
                serde_json::Value::Array(vec) => vec,
                _ => bail!("params should be a JSON array"),
            };
            command::execution::submit(ffqn, params).await
        }
        Subcommand::Execution(args::Execution::Get {
            execution_id,
            verbosity,
        }) => {
            command::execution::get(
                db_file,
                execution_id,
                match verbosity {
                    0 => None,
                    1 => Some(ExecutionVerbosity::EventHistory),
                    _ => Some(ExecutionVerbosity::Full),
                },
            )
            .await
        }
    }
}

#[derive(Copy, Clone, PartialEq, Eq)]
enum FunctionMetadataVerbosity {
    FfqnOnly,
    WithTypes,
}
