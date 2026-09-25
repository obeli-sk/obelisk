#![recursion_limit = "512"]

mod api;
mod args;
mod client;
mod command;
mod config;
mod init;
mod javascript;
mod oci;
mod server;
mod wit_printer;

use crate::command::server::{
    PrepareDirsParams, RunParams, RuntimeConfigAvailability, ServerAuth, VerifyParams, run, verify,
};
use crate::config::secret_registry::{API_TOKEN, API_TOKEN_LEGACY, EnvVarSecretsCleanup};
use anyhow::ensure;
use args::{
    AdminArgs, Args, ComponentArgs, Deployment, DeploymentArgs, DeploymentVerifyArgs,
    ExecutionArgs, Server, Subcommand, VerifyArgs,
};
use clap::Parser;
use client::ClientStartup;
use config::config_holder::ConfigHolder;
use config::env_var::StartupEnvVars;
use config::secret_registry::{PublicEnvToml, SecretRegistry, SecretsToml};
use config::server::{PlatformExecActivities, ServerConfigToml};
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
            app_config,
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
                js_runtime,
            } = prepare_server_startup(
                server_config.clone(),
                app_config,
                EnvVarSecretsCleanup::Wipe,
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
                    js_runtime,
                },
                secret_registry,
            ))
        }

        Subcommand::Server(Server::Verify(VerifyArgs {
            server_config,
            app_config,
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
                js_runtime,
            } = prepare_server_startup(
                server_config.clone(),
                app_config,
                EnvVarSecretsCleanup::Noop,
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
                    js_runtime,
                },
                skip_db,
                fix,
                secret_registry,
            ))
        }

        // `deployment verify` aliases `server verify --skip-db --deployment <PATH>`.
        Subcommand::Deployment(DeploymentArgs {
            command: Deployment::Verify(args),
            token: _,
        }) => {
            let DeploymentVerifyArgs {
                clean_cache,
                clean_codegen_cache,
                server_config,
                app_config,
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
                js_runtime,
            } = prepare_server_startup(
                server_config.clone(),
                app_config,
                EnvVarSecretsCleanup::Noop,
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
                    js_runtime,
                },
                true, // `deployment verify` does not verify db.
                fix,
                secret_registry,
            ))
        }

        Subcommand::Deployment(DeploymentArgs { command, token }) => {
            Box::pin(command.run(ClientStartup::new(token.api_token)))
        }

        Subcommand::Admin(AdminArgs { command, token }) => {
            Box::pin(command.run(ClientStartup::new(token.api_token)))
        }

        Subcommand::Generate(generate) => {
            let env_vars = StartupEnvVars::capture();
            let secret_registry = Arc::new(SecretRegistry::resolve(
                SecretsToml::new(),
                PublicEnvToml::default(),
                EnvVarSecretsCleanup::Noop,
                RuntimeConfigAvailability::AllowUnavailable,
                None,
                &env_vars,
            )?);
            Box::pin(generate.run(secret_registry))
        }

        Subcommand::Component(ComponentArgs { command, token }) => {
            let client_startup = ClientStartup::new(token.api_token);
            let env_vars = StartupEnvVars::capture();
            let secret_registry = Arc::new(SecretRegistry::resolve(
                SecretsToml::new(),
                PublicEnvToml::default(),
                EnvVarSecretsCleanup::Noop,
                RuntimeConfigAvailability::AllowUnavailable,
                None,
                &env_vars,
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

#[derive(Debug)]
struct ServerStartup {
    config_holder: ConfigHolder,
    config: ServerConfigToml,
    legacy_api_token: Option<secrecy::SecretString>,
    secret_registry: Arc<SecretRegistry>,
    js_runtime: crate::command::server::JsRuntimeMode,
}

/// Parse the complete server config once, then resolve and wipe its secret sources
/// before the runtime starts.
fn prepare_server_startup(
    server_config: Option<PathBuf>,
    app_config: Option<PathBuf>,
    env_var_cleanup: EnvVarSecretsCleanup,
    runtime_config_availability: RuntimeConfigAvailability,
) -> anyhow::Result<ServerStartup> {
    if env_var_cleanup == EnvVarSecretsCleanup::Wipe {
        assert_eq!(
            RuntimeConfigAvailability::Strict,
            runtime_config_availability,
        );
    }
    let mut config_holder = ConfigHolder::new(project_dirs(), BaseDirs::new(), server_config)?
        .with_app_source(app_config)?;
    // backcompat: 0.41 exposed OBELISK__API__TOKEN as api.token; remove after 0.43.
    let legacy_env = std::env::var(API_TOKEN_LEGACY).ok();
    if legacy_env.is_some() {
        // SAFETY: server configuration is loaded before the runtime and its threads start.
        // Wipe it so that server config can be loaded.
        unsafe { std::env::remove_var(API_TOKEN_LEGACY) };
    }
    let mut config = config_holder.load_config()?;
    let mut app = config_holder.load_app_config()?;
    config_holder.path_prefixes.app_name = app.effective_name()?;
    let app_config_digest = app.digest()?;
    tracing::info!(app_name = %config_holder.path_prefixes.app_name, %app_config_digest, "Loaded app policy");
    let env_vars = StartupEnvVars::capture();
    let js_runtime = if env_vars
        .lookup("OBELISK_UNSTABLE_V8")
        .and_then(|value| value.parse::<bool>().ok())
        .unwrap_or_default()
    {
        crate::command::server::JsRuntimeMode::V8
    } else {
        crate::command::server::JsRuntimeMode::BoaWasm
    };
    config.resolve_env_vars(&config_holder.path_prefixes, &env_vars)?;
    app.resolve_env_vars(&env_vars)?;
    match &config.platform_exec_activities {
        PlatformExecActivities::Disabled => {
            anyhow::ensure!(
                app.allowed_exec_activities.is_empty(),
                "app.toml allows exec activities, but server.toml has exec activities off"
            );
        }
        PlatformExecActivities::All => eprintln!(
            "warning: server.toml enables unrestricted platform exec activity allowance; app.toml still controls deployments"
        ),
        PlatformExecActivities::Allowlist(platform) => {
            for (name, digests) in &app.allowed_exec_activities {
                let platform_digests = platform.get(name);
                anyhow::ensure!(
                    digests
                        .iter()
                        .all(|digest| platform_digests
                            .is_some_and(|allowed| allowed.contains(digest))),
                    "app.toml `[allowed_exec_activities].{name}` is not covered by server.toml `[allowed_exec_activities]`"
                );
            }
        }
    }
    config.secrets = app.secrets;
    config.public_env = app.public_env;
    config.allowed_exec_activities = app.allowed_exec_activities;
    config.outbound_http = app.outbound_http;
    config.source_path.clone_from(&config_holder.app_source);
    config
        .app_name
        .clone_from(&config_holder.path_prefixes.app_name);
    config.app_config_digest = Some(app_config_digest);

    let legacy_api_token = legacy_env.filter(|token| !token.is_empty()).map(|token| {
        eprintln!(
            "warning: {API_TOKEN_LEGACY} is deprecated; use {API_TOKEN} or configure api.token_hashes in server.toml for server authentication"
        );
        secrecy::SecretString::from(token)
    });
    let secret_registry = Arc::new(SecretRegistry::resolve(
        config.secrets.clone(),
        config.public_env.clone(),
        env_var_cleanup,
        runtime_config_availability,
        legacy_api_token.as_ref(),
        &env_vars,
    )?);
    Ok(ServerStartup {
        config_holder,
        config,
        legacy_api_token,
        secret_registry,
        js_runtime,
    })
}

pub(crate) fn project_dirs() -> Option<ProjectDirs> {
    ProjectDirs::from("", "obelisk", "obelisk")
}

#[cfg(test)]
mod app_policy_tests {
    use super::*;

    #[test]
    fn platform_exec_allowlist_must_cover_app_allowlist() {
        let dir = tempfile::tempdir().unwrap();
        let server = dir.path().join("server.toml");
        let app = dir.path().join("app.toml");
        let digest = "sha256:abababababababababababababababababababababababababababababababab";
        std::fs::write(
            &app,
            format!("app_name='foo'\n[allowed_exec_activities]\nworker = '{digest}'\n"),
        )
        .unwrap();
        std::fs::write(&server, "").unwrap();
        let load = || {
            prepare_server_startup(
                Some(server.clone()),
                Some(app.clone()),
                EnvVarSecretsCleanup::Noop,
                RuntimeConfigAvailability::AllowUnavailable,
            )
        };
        const ERR_ACTIVITIES_ARE_OFF: &str =
            "app.toml allows exec activities, but server.toml has exec activities off";
        assert_eq!(ERR_ACTIVITIES_ARE_OFF, load().unwrap_err().to_string());
        std::fs::write(&server, "[allowed_exec_activities]\nother = 'sha256:abababababababababababababababababababababababababababababababab'\n").unwrap();
        assert!(load().err().unwrap().to_string().contains("not covered"));
        std::fs::write(
            &server,
            format!("[allowed_exec_activities]\nworker = '{digest}'\n"),
        )
        .unwrap();
        assert!(load().is_ok());
        std::fs::write(&server, "allowed_exec_activities = false\n").unwrap();
        assert_eq!(ERR_ACTIVITIES_ARE_OFF, load().unwrap_err().to_string());
        std::fs::write(&server, "allowed_exec_activities = '*'\n").unwrap();
        assert!(load().is_ok());
    }
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd)]
enum FunctionMetadataVerbosity {
    ExportsOnly,
    ExportsAndImports,
}
