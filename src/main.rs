#![recursion_limit = "512"]

mod api;
mod args;
mod client;
mod command;
mod config;
mod env_vars;
mod init;
mod javascript;
mod oci;
mod server;
mod wit_printer;

use crate::command::server::{
    PrepareDirsParams, RunParams, RuntimeConfigAvailability, ServerAuth, VerifyParams, run, verify,
};
use crate::config::secret_registry::{API_TOKEN, API_TOKEN_LEGACY, EnvVarCleanupStrategy};
use anyhow::ensure;
use args::{
    Args, ComponentArgs, Deployment, DeploymentArgs, DeploymentVerifyArgs, ExecutionArgs, Server,
    Subcommand, VerifyArgs,
};
use clap::Parser;
use client::ClientStartup;
use config::config_holder::ConfigHolder;
use config::secret_registry::{SecretRegistry, SecretsToml};
use config::server::ServerConfigToml;
use directories::{BaseDirs, ProjectDirs};
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

fn main() -> Result<(), anyhow::Error> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("default tls provider must be installed");
    let args = Args::parse();
    let command = args.command;

    type CommandFuture = Pin<Box<dyn Future<Output = Result<(), anyhow::Error>>>>;
    let future: CommandFuture = match command {
        Subcommand::Server(Server::Run {
            server_config,
            clean_sqlite_directory,
            clean_cache,
            clean_codegen_cache,
            deployment,
            empty: deployment_empty,
            description,
            suppress_type_checking_errors,
            no_auth,
            api_token,
        }) => {
            let ServerStartup {
                config_holder,
                config,
                legacy_api_token,
                secret_registry,
            } = prepare_server_startup(
                server_config.clone(),
                EnvVarCleanupStrategy::Wipe,
                RuntimeConfigAvailability::Strict,
            )?;
            let auth = if no_auth {
                assert!(api_token.is_none(), "{API_TOKEN} conflicts with --no-auth");
                // Remove ambiguity
                ensure!(
                    legacy_api_token.is_none(),
                    "unset {API_TOKEN_LEGACY} when using --no-auth"
                );
                ServerAuth::NoAuth
            } else {
                ensure!(!no_auth, "guarded by conflicts_with");
                let api_token = api_token.or(legacy_api_token);
                ServerAuth::Auth { api_token }
            };
            Box::pin(run(
                config_holder,
                config,
                deployment,
                deployment_empty,
                description,
                RunParams {
                    dir_params: PrepareDirsParams {
                        clean_cache,
                        clean_codegen_cache,
                    },
                    clean_sqlite_directory,
                    suppress_type_checking_errors,
                    auth,
                },
                secret_registry,
            ))
        }

        Subcommand::Server(Server::Verify(VerifyArgs {
            server_config,
            allow_unavailable_runtime_config,
            clean_cache,
            clean_codegen_cache,
            deployment,
            suppress_type_checking_errors,
            skip_db,
            fix,
        })) => {
            let runtime_config_availability = if allow_unavailable_runtime_config {
                RuntimeConfigAvailability::AllowUnavailable
            } else {
                RuntimeConfigAvailability::Strict
            };
            let ServerStartup {
                config_holder,
                config,
                legacy_api_token: _,
                secret_registry,
            } = prepare_server_startup(
                server_config.clone(),
                EnvVarCleanupStrategy::Noop,
                runtime_config_availability,
            )?;
            Box::pin(verify(
                config_holder,
                config,
                deployment,
                VerifyParams {
                    dir_params: PrepareDirsParams {
                        clean_cache,
                        clean_codegen_cache,
                    },
                    runtime_config_availability,
                    suppress_type_checking_errors,
                    suppress_linking_errors: false,
                },
                skip_db,
                fix,
                secret_registry,
            ))
        }

        // `deployment verify` uses server configuration and compilation machinery locally, but
        // never opens the database or uses its content-addressed store.
        Subcommand::Deployment(DeploymentArgs {
            command: Deployment::Verify(args),
            token: _,
        }) => {
            let DeploymentVerifyArgs {
                clean_cache,
                clean_codegen_cache,
                server_config,
                deployment,
                allow_unavailable_runtime_config,
                suppress_type_checking_errors,
                fix,
            } = args;
            let runtime_config_availability = if allow_unavailable_runtime_config {
                RuntimeConfigAvailability::AllowUnavailable
            } else {
                RuntimeConfigAvailability::Strict
            };
            let ServerStartup {
                config_holder,
                config,
                legacy_api_token: _,
                secret_registry,
            } = prepare_server_startup(
                server_config.clone(),
                EnvVarCleanupStrategy::Noop,
                runtime_config_availability,
            )?;
            Box::pin(verify(
                config_holder,
                config,
                Some(deployment),
                VerifyParams {
                    dir_params: PrepareDirsParams {
                        clean_cache,
                        clean_codegen_cache,
                    },
                    runtime_config_availability,
                    suppress_type_checking_errors,
                    suppress_linking_errors: false,
                },
                true, // `deployment verify` does not verify db.
                fix,
                secret_registry,
            ))
        }

        Subcommand::Deployment(DeploymentArgs { command, token }) => {
            Box::pin(command.run(ClientStartup::new(token.api_token)))
        }

        Subcommand::Generate(generate) => {
            let secret_registry = Arc::new(SecretRegistry::resolve(
                SecretsToml::new(),
                EnvVarCleanupStrategy::Noop,
                RuntimeConfigAvailability::AllowUnavailable,
                None,
            )?);
            Box::pin(generate.run(secret_registry))
        }

        Subcommand::Component(ComponentArgs { command, token }) => {
            let client_startup = ClientStartup::new(token.api_token);
            let secret_registry = Arc::new(SecretRegistry::resolve(
                SecretsToml::new(),
                EnvVarCleanupStrategy::Noop,
                RuntimeConfigAvailability::AllowUnavailable,
                None,
            )?);
            Box::pin(command.run(client_startup, secret_registry))
        }

        Subcommand::Execution(ExecutionArgs { command, token }) => {
            Box::pin(command.run(ClientStartup::new(token.api_token)))
        }
    };

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("cannot build tokio runtime");
    runtime.block_on(future)
}

struct ServerStartup {
    config_holder: ConfigHolder,
    config: ServerConfigToml,
    legacy_api_token: Option<secrecy::SecretString>,
    secret_registry: Arc<SecretRegistry>,
}

/// Parse the complete server config once, then resolve and wipe its secret sources
/// before the runtime starts.
fn prepare_server_startup(
    server_config: Option<PathBuf>,
    env_var_cleanup: EnvVarCleanupStrategy,
    runtime_config_availability: RuntimeConfigAvailability,
) -> anyhow::Result<ServerStartup> {
    if env_var_cleanup == EnvVarCleanupStrategy::Wipe {
        assert_eq!(
            RuntimeConfigAvailability::Strict,
            runtime_config_availability,
        );
    }
    let config_holder = ConfigHolder::new(project_dirs(), BaseDirs::new(), server_config)?;
    // backcompat: 0.41 exposed OBELISK__API__TOKEN as api.token; remove after 0.43.
    let legacy_env = std::env::var(API_TOKEN_LEGACY).ok();
    if legacy_env.is_some() {
        // SAFETY: server configuration is loaded before the runtime and its threads start.
        // Wipe it so that server config can be loaded.
        unsafe { std::env::remove_var(API_TOKEN_LEGACY) };
    }
    let config = config_holder.load_config()?;

    let legacy_api_token = legacy_env.filter(|token| !token.is_empty()).map(|token| {
        eprintln!(
            "warning: {API_TOKEN_LEGACY} is deprecated; use {API_TOKEN} or configure api.token_hashes in server.toml for server authentication"
        );
        secrecy::SecretString::from(token)
    });
    let secret_registry = Arc::new(SecretRegistry::resolve(
        config.secrets.clone(),
        env_var_cleanup,
        runtime_config_availability,
        legacy_api_token.as_ref(),
    )?);
    Ok(ServerStartup {
        config_holder,
        config,
        legacy_api_token,
        secret_registry,
    })
}

pub(crate) fn project_dirs() -> Option<ProjectDirs> {
    ProjectDirs::from("", "obelisk", "obelisk")
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd)]
enum FunctionMetadataVerbosity {
    ExportsOnly,
    ExportsAndImports,
}
