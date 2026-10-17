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
    ActivityVmRuntimeMode, JsRuntimeMode, PrepareDirsParams, RunParams, RuntimeConfigAvailability,
    ServerAuth, VerifyParams, run, verify,
};
use crate::config::secret_registry::{API_TOKEN, EnvVarSecretsCleanup};
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
use tracing::{info, warn};

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

fn main() -> Result<(), anyhow::Error> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("default tls provider must be installed");
    let args = Args::parse();
    let future = command_future(args.command, None)?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("cannot build tokio runtime");
    runtime.block_on(future)
}

type CommandFuture = Pin<Box<dyn Future<Output = Result<(), anyhow::Error>>>>;

fn command_future(
    command: Subcommand,
    client_override: Option<ClientStartup>,
) -> anyhow::Result<CommandFuture> {
    let future: CommandFuture = match command {
        Subcommand::Cloud(command) => Box::pin(command.run()),
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
                secret_registry,
                js_runtime,
                activity_vm_runtime,
            } = prepare_server_startup(
                server_config.clone(),
                app_config,
                EnvVarSecretsCleanup::Wipe,
                RuntimeConfigAvailability::Strict,
            )?;
            let auth = if no_auth {
                assert!(api_token.is_none(), "{API_TOKEN} conflicts with --no-auth");
                ServerAuth::NoAuth
            } else {
                ensure!(!no_auth, "guarded by conflicts_with");
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
                    activity_vm_runtime,
                    #[cfg(test)]
                    test_hooks: command::server::RuntimeTestHooks::default(),
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
            fixes,
        })) => {
            let app_config = config::app::resolve_app_config(app_config)?;
            anyhow::ensure!(
                !(fixes.app_declarations()
                    || fixes.app_http_permissions()
                    || fixes.app_exec_permissions()
                    || fixes.app_secret_exposure())
                    || app_config.is_some(),
                "app repairs require --app-config or ./app.toml"
            );
            let runtime_config_availability = if allow_unavailable_runtime_config {
                RuntimeConfigAvailability::AllowMissingValues
            } else {
                RuntimeConfigAvailability::Strict
            };
            let ServerStartup {
                config_holder,
                config,
                secret_registry,
                js_runtime,
                activity_vm_runtime,
            } = prepare_server_startup_with_exec_policy(
                server_config.clone(),
                app_config,
                EnvVarSecretsCleanup::Noop,
                if fixes.any() {
                    RuntimeConfigAvailability::AllowMissingValues
                } else {
                    runtime_config_availability
                },
                !fixes.any(),
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
                    activity_vm_runtime,
                },
                skip_db,
                fixes,
                secret_registry,
            ))
        }

        Subcommand::Deployment(DeploymentArgs {
            command: Deployment::Verify(args),
            token: _,
        }) => {
            let DeploymentVerifyArgs {
                api_url: _,
                clean_cache,
                clean_codegen_cache,
                deployment,
                suppress_type_checking_errors,
            } = args;
            let env_vars = StartupEnvVars::capture();
            Box::pin(command::server::verify_deployment(
                deployment,
                VerifyParams {
                    dir_params: PrepareDirsParams {
                        clean_cache,
                        clean_codegen_cache,
                    },
                    runtime_config_availability: RuntimeConfigAvailability::AllowUnavailable,
                    suppress_type_checking_errors,
                    suppress_linking_errors: false,
                    js_runtime: parse_js_runtime_from_env(&env_vars)?,
                    activity_vm_runtime: parse_activity_vm_runtime_from_env(&env_vars)?,
                },
            ))
        }

        Subcommand::Deployment(DeploymentArgs { command, token }) => Box::pin(command.run(
            client_override.unwrap_or_else(|| ClientStartup::new(token.api_url, token.api_token)),
        )),

        Subcommand::Admin(AdminArgs { command, token }) => Box::pin(command.run(
            client_override.unwrap_or_else(|| ClientStartup::new(token.api_url, token.api_token)),
        )),

        Subcommand::Generate(generate) => {
            let env_vars = StartupEnvVars::capture();
            let secret_registry = Arc::new(SecretRegistry::resolve(
                SecretsToml::new(),
                PublicEnvToml::default(),
                EnvVarSecretsCleanup::Noop,
                RuntimeConfigAvailability::AllowUnavailable,
                &env_vars,
            )?);
            Box::pin(generate.run(secret_registry))
        }

        Subcommand::Component(ComponentArgs { command, token }) => {
            let client_startup = client_override
                .unwrap_or_else(|| ClientStartup::new(token.api_url, token.api_token));
            let env_vars = StartupEnvVars::capture();
            let secret_registry = Arc::new(SecretRegistry::resolve(
                SecretsToml::new(),
                PublicEnvToml::default(),
                EnvVarSecretsCleanup::Noop,
                RuntimeConfigAvailability::AllowUnavailable,
                &env_vars,
            )?);
            Box::pin(command.run(client_startup, secret_registry))
        }

        Subcommand::Execution(ExecutionArgs { command, token }) => Box::pin(command.run(
            client_override.unwrap_or_else(|| ClientStartup::new(token.api_url, token.api_token)),
        )),
    };

    Ok(future)
}

#[derive(Debug)]
struct ServerStartup {
    config_holder: ConfigHolder,
    config: ServerConfigToml,
    secret_registry: Arc<SecretRegistry>,
    js_runtime: crate::command::server::JsRuntimeMode,
    activity_vm_runtime: crate::command::server::ActivityVmRuntimeMode,
}

/// Parse the complete server config once, then resolve and wipe its secret sources
/// before the runtime starts.
fn prepare_server_startup(
    server_config: Option<PathBuf>,
    app_config: Option<PathBuf>,
    env_var_cleanup: EnvVarSecretsCleanup,
    runtime_config_availability: RuntimeConfigAvailability,
) -> anyhow::Result<ServerStartup> {
    prepare_server_startup_with_exec_policy(
        server_config,
        app_config,
        env_var_cleanup,
        runtime_config_availability,
        true,
    )
}

fn prepare_server_startup_with_exec_policy(
    server_config: Option<PathBuf>,
    app_config: Option<PathBuf>,
    env_var_cleanup: EnvVarSecretsCleanup,
    runtime_config_availability: RuntimeConfigAvailability,
    check_exec_policy: bool,
) -> anyhow::Result<ServerStartup> {
    if env_var_cleanup == EnvVarSecretsCleanup::Wipe {
        assert_eq!(
            RuntimeConfigAvailability::Strict,
            runtime_config_availability,
        );
    }
    let mut config_holder = ConfigHolder::new(project_dirs(), BaseDirs::new(), server_config)?
        .with_app_source(config::app::resolve_app_config(app_config)?)?;
    let mut config = config_holder.load_config()?;
    let mut app = config_holder.load_app_config()?;
    config_holder.path_prefixes.app_name = app.effective_name()?;
    let app_policy = app.policy();
    let app_config_digest = app_policy.digest();
    info!(app_name = %config_holder.path_prefixes.app_name, %app_config_digest, "Loaded app policy");
    let env_vars = StartupEnvVars::capture();
    let js_runtime = parse_js_runtime_from_env(&env_vars)?;
    let activity_vm_runtime = parse_activity_vm_runtime_from_env(&env_vars)?;
    config.resolve_env_vars(&config_holder.path_prefixes, &env_vars)?;
    app.resolve_env_vars(&env_vars)?;
    if check_exec_policy {
        match &config.platform_exec_activities {
            PlatformExecActivities::Disabled => {
                anyhow::ensure!(
                    app.allowed_exec_activities.is_empty(),
                    "app.toml allows exec activities, but server.toml has exec activities off"
                );
            }
            PlatformExecActivities::All => warn!(
                "server.toml enables unrestricted platform exec activity allowance; app.toml still controls deployments"
            ),
            PlatformExecActivities::Allowlist(platform) => {
                for (name, digests) in &app.allowed_exec_activities {
                    let platform_digests = platform.get(name);
                    anyhow::ensure!(
                        digests.iter().all(|digest| platform_digests
                            .is_some_and(|allowed| allowed.contains(digest))),
                        "app.toml `[allowed_exec_activities].{name}` is not covered by server.toml `[allowed_exec_activities]`"
                    );
                }
            }
        }
    }
    config.secrets = app.secrets;
    config.public_env = app.public_env;
    config.allowed_exec_activities = app.allowed_exec_activities;
    config.outbound_http = app.outbound_http;
    config.app_source_path.clone_from(&config_holder.app_source);
    config
        .app_name
        .clone_from(&config_holder.path_prefixes.app_name);
    config.app_policy = app_policy;

    let secret_registry = Arc::new(SecretRegistry::resolve(
        config.secrets.clone(),
        config.public_env.clone(),
        env_var_cleanup,
        runtime_config_availability,
        &env_vars,
    )?);
    Ok(ServerStartup {
        config_holder,
        config,
        secret_registry,
        js_runtime,
        activity_vm_runtime,
    })
}

fn parse_js_runtime_from_env(env_vars: &StartupEnvVars) -> anyhow::Result<JsRuntimeMode> {
    let value = env_vars.lookup("OBELISK_JS_RUNTIME");
    match value.as_deref() {
        None | Some("boa-wasm") => Ok(JsRuntimeMode::BoaWasm),
        Some("v8") => Ok(JsRuntimeMode::V8),
        Some(other) => {
            anyhow::bail!(
                "invalid OBELISK_JS_RUNTIME value `{other:?}`; expected `v8` or `boa-wasm`"
            )
        }
    }
}

pub(crate) fn parse_activity_vm_runtime_from_env(
    env_vars: &StartupEnvVars,
) -> anyhow::Result<ActivityVmRuntimeMode> {
    let value = env_vars.lookup("OBELISK_UNSTABLE_ACTIVITY_VM");
    match value.as_deref() {
        None => Ok(ActivityVmRuntimeMode::Disabled),
        Some("bochs-wasm") => Ok(ActivityVmRuntimeMode::BochsWasm),
        Some("qemu-tcg") => Ok(ActivityVmRuntimeMode::QemuTcg),
        Some("qemu-kvm") => Ok(ActivityVmRuntimeMode::QemuKvm),
        Some("firecracker") => Ok(ActivityVmRuntimeMode::Firecracker),
        Some(other) => anyhow::bail!(
            "invalid OBELISK_UNSTABLE_ACTIVITY_VM value `{other:?}`; expected `bochs-wasm`, `qemu-tcg`, `qemu-kvm`, or `firecracker`"
        ),
    }
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
            format!("app_name='example'\n[allowed_exec_activities]\nworker = '{digest}'\n"),
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

    #[test]
    fn startup_keeps_server_and_app_source_paths_separate() {
        let dir = tempfile::tempdir().unwrap();
        let server = dir.path().join("server.toml");
        let app = dir.path().join("app.toml");
        std::fs::write(&server, "").unwrap();
        std::fs::write(&app, "app_name='example'\n").unwrap();
        let startup = prepare_server_startup(
            Some(server.clone()),
            Some(app.clone()),
            EnvVarSecretsCleanup::Noop,
            RuntimeConfigAvailability::AllowUnavailable,
        )
        .unwrap();
        // The API token-hash reload watches `source_path`.
        assert_eq!(Some(server), startup.config.source_path);
        assert_eq!(Some(app), startup.config.app_source_path);
    }
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd)]
enum FunctionMetadataVerbosity {
    ExportsOnly,
    ExportsAndImports,
}
