//! Processed deployment manifest: resolving a validated manifest against the CAS and
//! fetching/verifying each component into its runtime-ready `*ConfigVerified` form. Here
//! content digests and `component_files` are mandatory. Data shapes shared with the
//! authored side live in [`super::common`].

use super::{
    ActivityExternalComponentConfigToml, ActivityExternalFileConfigToml,
    ActivityStubComponentConfigToml, ActivityStubFileConfigToml, ActivityWasmComponentConfigToml,
    AllowedHostToml, AuthoredFunctionInterfaceToml, BlockingStrategyConfigToml,
    ComponentBacktraceConfig, ComponentCommon, ComponentLocationToml, ComponentStdOutputToml,
    ConfigName, CronComponentConfigToml, DeploymentTomlValidated, DurationConfig, ExecConfigToml,
    FunctionInterfaceToml, InflightSemaphore, InlineFunctionInterfaceToml, JsParamToml,
    LockingStrategy, LogLevelToml, MethodsInput, ReplaceIn, ScriptLocationPathOrOci, WebhookRoute,
    WebhookRouteDetail, sanitize_deployment_relative_path, strip_deployment_dir_prefix,
};
use crate::command::server::{FrameFilesToSource, FrameSource};
use crate::config::env_var::{
    EnvVarConfig, EnvVarError, EnvVarsMissing, interpolate_env_vars_plaintext,
};
use crate::config::file_provider::{
    parse_js_graph_from_cas, parse_wit_files_from_cas, read_package_blob, verify_content_digest,
};
use crate::config::secret_registry::{RestrictedSecretRegistry, SecretRegistry, SecretViolation};
use crate::config::{content_digest_to_exec_file, wasm_cache_metadata_dir};
use crate::oci;
use anyhow::{Context, anyhow, bail, ensure};
use concepts::cas::Cas;
use concepts::component_id::{ComponentDigest, ContentDigest, Digest};
use concepts::{
    ComponentId, ComponentRetryConfig, ComponentType, FunctionFqn, ReturnType, StrVariant,
    prefixed_ulid::ExecutorId, storage::LogLevel,
};
use hashbrown::HashMap;
use regex::Regex;
use sha2::{Digest as _, Sha256};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, instrument, warn};
use utils::wasm_tools::WasmComponent;
use wasm_workers::activity::activity_exec_worker::ExecSecrets;
use wasm_workers::cron::cron_worker::CronOrOnce;
use wasm_workers::http_hooks::ConfigSectionHint;
use wasm_workers::http_request_policy::HostPatternError;
use wasm_workers::{
    activity::activity_worker::ActivityConfig,
    envvar::EnvVar,
    http_request_policy::{
        AllowedHostConfig, GlobalHttpConfig, HostPattern, MethodsPattern, ReplacementLocation,
        SecretResolver,
    },
    std_output_stream::StdOutputConfig,
    workflow::workflow_worker::{
        DEFAULT_NON_BLOCKING_EVENT_BATCHING, JoinNextBlockingStrategy, WorkflowConfig,
        WorkflowConfigMode,
    },
};

// backcompat: accept component digest overrides until 0.42.
fn warn_deprecated_component_digest_override(
    component_name: &str,
    component_digest: Option<&ComponentDigest>,
) {
    if let Some(component_digest) = component_digest {
        warn!(
            component_name,
            %component_digest,
            "`component_digest` override is deprecated and will be removed in 0.42"
        );
    }
}

// Components

#[derive(Debug, Clone, Hash)]
pub(crate) struct ComponentCommonVerified {
    pub(crate) name: ConfigName,
    pub(crate) location: ComponentLocationToml,
}

pub(crate) trait ComponentLocationFetchExt {
    async fn fetch(
        &self,
        wasm_cache_dir: &Path,
        metadata_dir: &Path,
    ) -> Result<(ContentDigest, PathBuf), anyhow::Error>;
}

impl ComponentLocationFetchExt for ComponentLocationToml {
    /// Fetch wasm file and calculate its content digest.
    ///
    /// Read wasm file either from local fs, or pull from an OCI registry and cache it.
    async fn fetch(
        &self,
        wasm_cache_dir: &Path,
        metadata_dir: &Path,
    ) -> Result<(ContentDigest, PathBuf), anyhow::Error> {
        use utils::sha256sum::calculate_sha256_file;

        debug!("Fetching {self:?}");
        let stopwatch = std::time::Instant::now();

        let (actual_digest, path) = match &self {
            ComponentLocationToml::Path(wasm_path) => {
                let wasm_path = PathBuf::from(wasm_path);
                if !wasm_path.exists() {
                    bail!("file does not exist: {wasm_path:?}");
                }
                let actual_digest = calculate_sha256_file(&wasm_path)
                    .await
                    .with_context(|| format!("cannot compute hash of file `{wasm_path:?}`"))?;
                (actual_digest, wasm_path)
            }
            ComponentLocationToml::Oci(image) => {
                let image = oci_client::Reference::from_str(image)
                    .map_err(|e| anyhow!("invalid OCI reference `{image}`: {e}"))?;
                let (digest, path, _, _) =
                    oci::pull_to_cache_dir(&image, wasm_cache_dir, metadata_dir)
                        .await
                        .context("try cleaning the cache directory with `--clean-cache`")?;
                (digest, path)
            }
        };
        let stopwatch = stopwatch.elapsed();
        debug!("Fetching done in {stopwatch:?}");
        Ok((actual_digest, path))
    }
}

pub(crate) trait ComponentCommonFetchExt {
    async fn fetch(
        self,
        wasm_cache_dir: &Path,
        metadata_dir: &Path,
    ) -> Result<(ComponentCommonVerified, ContentDigest, PathBuf), anyhow::Error>;
}

impl ComponentCommonFetchExt for ComponentCommon {
    async fn fetch(
        self,
        wasm_cache_dir: &Path,
        metadata_dir: &Path,
    ) -> Result<(ComponentCommonVerified, ContentDigest, PathBuf), anyhow::Error> {
        let (content_digest, wasm_path) = self.location.fetch(wasm_cache_dir, metadata_dir).await?;

        let verified = ComponentCommonVerified {
            name: self.name,
            location: self.location,
        };
        Ok((verified, content_digest, wasm_path))
    }
}

pub(crate) fn verify_fetched_content_digest(
    actual: &ContentDigest,
    expected: Option<&ContentDigest>,
    what: &str,
) -> anyhow::Result<()> {
    ensure!(
        expected.is_none_or(|expected| expected == actual),
        "content digest mismatch for {what}: expected {}, got {actual}",
        expected.expect("checked above")
    );
    Ok(())
}

#[derive(Debug)]
pub(crate) struct ActivityStubExtConfigVerified {
    pub(crate) wasm_path: PathBuf,
    pub(crate) component_id: ComponentId,
}

#[derive(Debug)]
pub(crate) struct ActivityStubExtInlineConfigVerified {
    pub(crate) component_id: ComponentId,
    pub(crate) ffqn: FunctionFqn,
    pub(crate) params: Vec<concepts::ParameterType>,
    pub(crate) return_type: concepts::ReturnTypeExtendable,
    pub(crate) user_wasm_component: Option<WasmComponent>,
}

struct VerifiedFunctionInterface {
    params: Vec<concepts::ParameterType>,
    return_type: concepts::ReturnTypeExtendable,
    authored_component: Option<WasmComponent>,
}

fn verify_function_interface(
    interface: FunctionInterfaceResolved,
    ffqn: &FunctionFqn,
    component_type: ComponentType,
    default_params: Vec<concepts::ParameterType>,
    default_return_type: &str,
) -> anyhow::Result<VerifiedFunctionInterface> {
    match interface {
        FunctionInterfaceResolved::Inline(InlineFunctionInterfaceResolved {
            params,
            return_type,
        }) => {
            let params = match params {
                None => default_params,
                Some(params) => params
                    .iter()
                    .map(|param| {
                        let type_wrapper = val_json::type_wrapper::parse_wit_type(&param.wit_type)
                            .map_err(|err| {
                                anyhow!("invalid param type `{}`: {err}", param.wit_type)
                            })?;
                        Ok(concepts::ParameterType {
                            type_wrapper,
                            name: StrVariant::from(param.name.clone()),
                            wit_type: StrVariant::from(param.wit_type.clone()),
                        })
                    })
                    .collect::<anyhow::Result<_>>()?,
            };
            let return_type_str = return_type.as_deref().unwrap_or(default_return_type);
            let type_wrapper = val_json::type_wrapper::parse_wit_type(return_type_str)
                .map_err(|err| anyhow!("invalid return_type `{return_type_str}`: {err}"))?;
            let return_type = concepts::ReturnType::detect(
                type_wrapper,
                StrVariant::from(return_type_str.to_string()),
            );
            let ReturnType::Extendable(return_type) = return_type else {
                bail!(
                    "return_type must be `result`, `result<T>`, `result<T, string>`, or \
                     `result<T, variant {{ execution-failed, ... }}>`, got `{return_type_str}`"
                )
            };
            Ok(VerifiedFunctionInterface {
                params,
                return_type,
                authored_component: None,
            })
        }
        FunctionInterfaceResolved::Authored { wit } => {
            let root = wit.root;
            let component = WasmComponent::new_from_wit_resolve_for_ffqn(
                wit.resolve,
                wit.main_pkg_id,
                component_type,
                ffqn,
            )
            .with_context(|| format!("cannot verify authored WIT directory `{root}`"))?;
            let exports = component.exported_functions(false);
            ensure!(
                exports.len() == 1 && exports[0].ffqn == *ffqn,
                "authored WIT must expose exactly selected function `{ffqn}`"
            );
            let metadata = &exports[0];
            let ReturnType::Extendable(return_type) = &metadata.return_type else {
                bail!("authored WIT function `{ffqn}` must return an extendable result")
            };
            Ok(VerifiedFunctionInterface {
                params: metadata.parameter_types.0.clone(),
                return_type: return_type.clone(),
                authored_component: Some(component),
            })
        }
    }
}

#[derive(Debug)]
pub(crate) enum ActivityStubConfigVerified {
    File(ActivityStubExtConfigVerified),
    Inline(Box<ActivityStubExtInlineConfigVerified>),
}

pub(crate) trait ActivityStubComponentConfigResolvedExt {
    async fn fetch_and_verify(
        self,
        wasm_cache_dir: Arc<Path>,
        metadata_dir: Arc<Path>,
    ) -> Result<ActivityStubConfigVerified, anyhow::Error>;
}

impl ActivityStubComponentConfigResolvedExt for ActivityStubComponentConfigResolved {
    #[instrument(skip_all, fields(component_name = self.name_str(), component_id))]
    async fn fetch_and_verify(
        self,
        wasm_cache_dir: Arc<Path>,
        metadata_dir: Arc<Path>,
    ) -> Result<ActivityStubConfigVerified, anyhow::Error> {
        match self {
            Self::File(file) => {
                let expected_content_digest = file.content_digest;
                let (common, content_digest, wasm_path) =
                    file.common.fetch(&wasm_cache_dir, &metadata_dir).await?;
                verify_fetched_content_digest(
                    &content_digest,
                    expected_content_digest.as_ref(),
                    &common.location.to_string(),
                )?;
                let component_id = ComponentId::new(
                    ComponentType::ActivityStub,
                    StrVariant::from(common.name),
                    ComponentDigest(content_digest.0),
                )?;
                Ok(ActivityStubConfigVerified::File(
                    ActivityStubExtConfigVerified {
                        wasm_path,
                        component_id,
                    },
                ))
            }
            Self::Inline(inline) => {
                let ffqn = inline.ffqn;
                let default_params = vec![concepts::ParameterType {
                    type_wrapper: val_json::type_wrapper::TypeWrapper::List(Box::new(
                        val_json::type_wrapper::TypeWrapper::String,
                    )),
                    name: StrVariant::Static("params"),
                    wit_type: StrVariant::Static("list<string>"),
                }];
                let verified = verify_function_interface(
                    inline.interface,
                    &ffqn,
                    ComponentType::ActivityStub,
                    default_params,
                    "result<string, string>",
                )?;
                let parsed_params = verified.params;
                let return_type = verified.return_type;

                // Compute component digest: SHA256 of prefix + ffqn + params + return_type
                let mut hasher = Sha256::new();
                hasher.update(b"activity_stub_inline:");
                hasher.update(ffqn.to_string().as_bytes());
                for p in &parsed_params {
                    hasher.update(p.wit_type.as_ref().as_bytes());
                }
                hasher.update(return_type.wit_type.as_bytes());
                if let Some(component) = &verified.authored_component {
                    hasher.update(component.wit().as_bytes());
                }
                let hash: [u8; 32] = hasher.finalize().into();
                let component_digest = ComponentDigest(Digest(hash));

                let component_id = ComponentId::new(
                    ComponentType::ActivityStub,
                    StrVariant::from(inline.name),
                    component_digest,
                )?;

                Ok(ActivityStubConfigVerified::Inline(Box::new(
                    ActivityStubExtInlineConfigVerified {
                        component_id,
                        ffqn,
                        params: parsed_params,
                        return_type,
                        user_wasm_component: verified.authored_component,
                    },
                )))
            }
        }
    }
}

#[derive(Debug)]
pub(crate) enum ActivityExternalConfigVerified {
    File(ActivityStubExtConfigVerified),
    Inline(Box<ActivityStubExtInlineConfigVerified>),
}

pub(crate) trait ActivityExternalComponentConfigResolvedExt {
    async fn fetch_and_verify(
        self,
        wasm_cache_dir: Arc<Path>,
        metadata_dir: Arc<Path>,
    ) -> Result<ActivityExternalConfigVerified, anyhow::Error>;
}

impl ActivityExternalComponentConfigResolvedExt for ActivityExternalComponentConfigResolved {
    #[instrument(skip_all, fields(component_name = self.name_str(), component_id))]
    async fn fetch_and_verify(
        self,
        wasm_cache_dir: Arc<Path>,
        metadata_dir: Arc<Path>,
    ) -> Result<ActivityExternalConfigVerified, anyhow::Error> {
        match self {
            Self::File(file) => {
                let component_digest_override = file.component_digest;
                let expected_content_digest = file.content_digest;
                let (common, content_digest, wasm_path) =
                    file.common.fetch(&wasm_cache_dir, &metadata_dir).await?;
                warn_deprecated_component_digest_override(
                    common.name.as_str(),
                    component_digest_override.as_ref(),
                );
                verify_fetched_content_digest(
                    &content_digest,
                    expected_content_digest.as_ref(),
                    &common.location.to_string(),
                )?;
                let component_digest =
                    component_digest_override.unwrap_or(ComponentDigest(content_digest.0));
                let component_id = ComponentId::new(
                    ComponentType::Activity,
                    StrVariant::from(common.name),
                    component_digest,
                )?;
                Ok(ActivityExternalConfigVerified::File(
                    ActivityStubExtConfigVerified {
                        wasm_path,
                        component_id,
                    },
                ))
            }
            Self::Inline(inline) => {
                let ffqn = inline.ffqn;
                let default_params = vec![concepts::ParameterType {
                    type_wrapper: val_json::type_wrapper::TypeWrapper::List(Box::new(
                        val_json::type_wrapper::TypeWrapper::String,
                    )),
                    name: StrVariant::Static("params"),
                    wit_type: StrVariant::Static("list<string>"),
                }];
                let verified = verify_function_interface(
                    inline.interface,
                    &ffqn,
                    ComponentType::Activity,
                    default_params,
                    "result<string, string>",
                )?;
                let parsed_params = verified.params;
                let return_type = verified.return_type;

                // Compute component digest: SHA256 of prefix + ffqn + params + return_type
                let mut hasher = Sha256::new();
                hasher.update(b"activity_external_inline:");
                hasher.update(ffqn.to_string().as_bytes());
                for p in &parsed_params {
                    hasher.update(p.wit_type.as_ref().as_bytes());
                }
                hasher.update(return_type.wit_type.as_bytes());
                if let Some(component) = &verified.authored_component {
                    hasher.update(component.wit().as_bytes());
                }
                let hash: [u8; 32] = hasher.finalize().into();
                let component_digest = ComponentDigest(Digest(hash));

                let component_id = ComponentId::new(
                    ComponentType::Activity,
                    StrVariant::from(inline.name),
                    component_digest,
                )?;

                Ok(ActivityExternalConfigVerified::Inline(Box::new(
                    ActivityStubExtInlineConfigVerified {
                        component_id,
                        ffqn,
                        params: parsed_params,
                        return_type,
                        user_wasm_component: verified.authored_component,
                    },
                )))
            }
        }
    }
}

#[derive(Debug)]
pub(crate) struct ActivityWasmConfigVerified {
    pub(crate) wasm_path: PathBuf,
    pub(crate) activity_config: ActivityConfig,
    pub(crate) exec_config: executor::executor::ExecConfig,
    pub(crate) logs_store_min_level: Option<LogLevel>,
}

impl ActivityWasmConfigVerified {
    pub fn component_id(&self) -> &ComponentId {
        &self.activity_config.component_id
    }
}

pub(crate) trait ActivityWasmComponentConfigTomlExt {
    #[expect(clippy::too_many_arguments)]
    async fn fetch_and_verify(
        self,
        wasm_cache_dir: Arc<Path>,
        metadata_dir: Arc<Path>,
        ignore_missing_env_vars: bool,
        secret_registry: &Arc<SecretRegistry>,
        global_http_config: GlobalHttpConfig,
        global_executor_instance_limiter: Option<Arc<tokio::sync::Semaphore>>,
        fuel: Option<u64>,
    ) -> Result<ActivityWasmConfigVerified, anyhow::Error>;
}

impl ActivityWasmComponentConfigTomlExt for ActivityWasmComponentConfigToml {
    #[instrument(skip_all, fields(component_name = self.common.name.as_str()))]
    async fn fetch_and_verify(
        self,
        wasm_cache_dir: Arc<Path>,
        metadata_dir: Arc<Path>,
        ignore_missing_env_vars: bool,
        secret_registry: &Arc<SecretRegistry>,
        global_http_config: GlobalHttpConfig,
        global_executor_instance_limiter: Option<Arc<tokio::sync::Semaphore>>,
        fuel: Option<u64>,
    ) -> Result<ActivityWasmConfigVerified, anyhow::Error> {
        let expected_content_digest = self.content_digest;
        let (common, content_digest, wasm_path) =
            self.common.fetch(&wasm_cache_dir, &metadata_dir).await?;
        verify_fetched_content_digest(
            &content_digest,
            expected_content_digest.as_ref(),
            &common.location.to_string(),
        )?;
        warn_deprecated_component_digest_override(
            common.name.as_str(),
            self.component_digest.as_ref(),
        );

        let env_vars =
            resolve_env_vars_plaintext(self.env_vars, ignore_missing_env_vars, secret_registry)?;
        let (allowed_hosts, _advisories) =
            resolve_allowed_hosts(self.allowed_hosts, ignore_missing_env_vars, secret_registry)?;

        // Validate no collision between env_vars and secret env names
        validate_no_env_collision(&env_vars, &allowed_hosts)?;

        let component_digest = self
            .component_digest
            .unwrap_or(ComponentDigest(content_digest.0));
        let component_id = ComponentId::new(
            ComponentType::Activity,
            StrVariant::from(common.name),
            component_digest,
        )?;
        let secrets = restricted_secret_registry(secret_registry, &allowed_hosts, None);
        let activity_config = ActivityConfig {
            component_id: component_id.clone(),
            forward_stdout: self.forward_stdout.into_std_output_config(),
            forward_stderr: self.forward_stderr.into_std_output_config(),
            env_vars,
            fuel,
            allowed_hosts,
            global_http_config,
            secrets,
            config_section_hint: ConfigSectionHint::ActivityWasm,
        };
        let retry_config = ComponentRetryConfig {
            max_retries: Some(self.max_retries),
            retry_exp_backoff: self.retry_exp_backoff.into(),
        };
        Ok(ActivityWasmConfigVerified {
            wasm_path,
            activity_config,
            exec_config: self.exec.into_exec_exec_config(
                component_id,
                global_executor_instance_limiter,
                retry_config,
            )?,
            logs_store_min_level: self.logs_store_min_level.into_log_level(),
        })
    }
}

#[derive(Debug)]
pub(crate) struct ActivityJsConfigVerified {
    pub(crate) wasm_path: Arc<Path>, // same for all JS activities
    pub(crate) js_entry_path: String,
    pub(crate) js_files: BTreeMap<String, String>,
    pub(crate) ffqn: FunctionFqn,
    pub(crate) params: Vec<concepts::ParameterType>,
    pub(crate) return_type: concepts::ReturnTypeExtendable,
    pub(crate) user_wasm_component: Option<WasmComponent>,
    pub(crate) activity_config: ActivityConfig,
    pub(crate) exec_config: executor::executor::ExecConfig,
    pub(crate) logs_store_min_level: Option<LogLevel>,
}

impl ActivityJsConfigVerified {
    pub fn component_id(&self) -> &ComponentId {
        &self.activity_config.component_id
    }

    pub(crate) fn as_frame_sources(&self) -> FrameFilesToSource {
        self.js_files
            .clone()
            .into_iter()
            .map(|(name, content)| (name, FrameSource::Content(content)))
            .collect()
    }
}

#[derive(Debug)]
pub(crate) struct ResolvedExecProgram {
    /// Path to the immutable cached script file the worker executes directly.
    pub(crate) program: PathBuf,
    /// Content digest of the script text (component identity and allowlist line).
    pub(crate) content_digest: ContentDigest,
}

async fn write_inline_exec_file_to_cache_dir(
    exec_path: &Path,
    exec_cache_dir: &Path,
    content: &[u8],
) -> anyhow::Result<()> {
    if let Ok(existing) = tokio::fs::read(exec_path).await
        && existing == content
    {
        return Ok(());
    }
    let tmp = tempfile::NamedTempFile::new_in(exec_cache_dir)?;
    tokio::fs::write(tmp.path(), content).await?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        tokio::fs::set_permissions(tmp.path(), std::fs::Permissions::from_mode(0o755)).await?;
    }
    tmp.persist(exec_path)?;
    Ok(())
}

pub(crate) trait ActivityExecComponentConfigResolvedExt {
    async fn resolve(
        &self,
        wasm_cache_dir: &std::path::Path,
    ) -> anyhow::Result<ResolvedExecProgram>;
    fn fetch_and_verify(
        self,
        resolved_program: ResolvedExecProgram,
        ignore_missing_env_vars: bool,
        secret_registry: &Arc<SecretRegistry>,
        global_executor_instance_limiter: Option<Arc<tokio::sync::Semaphore>>,
    ) -> Result<ActivityExecConfigVerified, anyhow::Error>;
}

impl ActivityExecComponentConfigResolvedExt for ActivityExecComponentConfigResolved {
    /// Resolve the program to a form the worker can execute.
    async fn resolve(
        &self,
        wasm_cache_dir: &std::path::Path,
    ) -> anyhow::Result<ResolvedExecProgram> {
        let exec_cache_dir = wasm_cache_dir.join("exec");
        match &self.location {
            ScriptLocationResolved::Content { content, .. } => {
                let hash: [u8; 32] = Sha256::digest(content.as_bytes()).into();
                let content_digest = ContentDigest(Digest(hash));
                if let Some(expected) = self.content_digest.as_ref() {
                    ensure!(
                        *expected == content_digest,
                        "content digest mismatch for inline exec content: expected {expected}, got {content_digest}"
                    );
                }
                tokio::fs::create_dir_all(&exec_cache_dir).await?;
                let exec_path = content_digest_to_exec_file(&exec_cache_dir, &content_digest);
                write_inline_exec_file_to_cache_dir(
                    &exec_path,
                    &exec_cache_dir,
                    content.as_bytes(),
                )
                .await?;
                Ok(ResolvedExecProgram {
                    program: exec_path,
                    content_digest,
                })
            }
            ScriptLocationResolved::Graph { .. } => {
                bail!("activity_exec does not support module graphs")
            }
            ScriptLocationResolved::Oci { image } => {
                tokio::fs::create_dir_all(&exec_cache_dir).await?;
                let metadata_dir = wasm_cache_metadata_dir(wasm_cache_dir);
                let result =
                    crate::oci::pull_exec_to_cache(image, &exec_cache_dir, &metadata_dir).await?;
                if let Some(expected) = self.content_digest.as_ref() {
                    let actual = utils::sha256sum::calculate_sha256_file(&result.exec_path).await?;
                    ensure!(
                        *expected == actual,
                        "content digest mismatch for OCI exec `{image}`: expected {expected}, got {actual}"
                    );
                }
                Ok(ResolvedExecProgram {
                    program: result.exec_path,
                    content_digest: result.content_digest,
                })
            }
        }
    }

    #[instrument(skip_all, fields(component_name = self.name.as_str()))]
    fn fetch_and_verify(
        self,
        resolved_program: ResolvedExecProgram,
        ignore_missing_env_vars: bool,
        secret_registry: &Arc<SecretRegistry>,
        global_executor_instance_limiter: Option<Arc<tokio::sync::Semaphore>>,
    ) -> Result<ActivityExecConfigVerified, anyhow::Error> {
        let verified = verify_function_interface(
            self.interface,
            &self.ffqn,
            ComponentType::Activity,
            Vec::new(),
            "result",
        )?;
        let parsed_params = verified.params;
        let return_type = verified.return_type;
        warn_deprecated_component_digest_override(
            self.name.as_str(),
            self.component_digest.as_ref(),
        );
        let component_digest = self.component_digest.unwrap_or_else(|| {
            let mut hasher = Sha256::new();
            hasher.update(b"activity_exec:");
            hasher.update(resolved_program.content_digest.0.0);
            hasher.update(self.ffqn.to_string().as_bytes());
            for p in &parsed_params {
                hasher.update(p.wit_type.as_ref().as_bytes());
            }
            hasher.update(return_type.wit_type.as_bytes());
            if let Some(component) = &verified.authored_component {
                hasher.update(component.wit().as_bytes());
            }
            let hash: [u8; 32] = hasher.finalize().into();
            ComponentDigest(Digest(hash))
        });
        let component_id = ComponentId::new(
            ComponentType::Activity,
            StrVariant::from(self.name),
            component_digest,
        )?;
        let env_vars =
            resolve_env_vars_plaintext(self.env_vars, ignore_missing_env_vars, secret_registry)?;
        // Carry only the declared names plus a component-scoped resolver; values are
        // fetched by name when the child's stdin is assembled, never baked here.
        let resolved_secrets = if self.secrets.is_empty() {
            None
        } else {
            let resolver =
                restricted_secret_registry(secret_registry, &[], self.secrets.iter().cloned());
            Some(ExecSecrets {
                names: self.secrets,
                resolver,
            })
        };
        let retry_config = ComponentRetryConfig {
            max_retries: Some(self.max_retries),
            retry_exp_backoff: self.retry_exp_backoff.into(),
        };
        Ok(ActivityExecConfigVerified {
            program: resolved_program.program,
            ffqn: self.ffqn,
            params: parsed_params,
            return_type,
            user_wasm_component: verified.authored_component,
            env_vars,
            max_output_bytes: self.max_output_bytes,
            forward_stdout: self.forward_stdout.into_std_output_config(),
            forward_stderr: self.forward_stderr.into_std_output_config(),
            secrets: resolved_secrets,
            params_via_stdin: self.params_via_stdin,
            component_id: component_id.clone(),
            exec_config: self.exec.into_exec_exec_config(
                component_id,
                global_executor_instance_limiter,
                retry_config,
            )?,
            logs_store_min_level: self.logs_store_min_level.into_log_level(),
        })
    }
}

#[derive(Debug)]
pub(crate) struct ActivityExecConfigVerified {
    pub(crate) program: PathBuf,
    pub(crate) ffqn: FunctionFqn,
    pub(crate) params: Vec<concepts::ParameterType>,
    pub(crate) return_type: concepts::ReturnTypeExtendable,
    pub(crate) user_wasm_component: Option<WasmComponent>,
    pub(crate) env_vars: Arc<[EnvVar]>,
    pub(crate) max_output_bytes: u64,
    pub(crate) forward_stdout: Option<StdOutputConfig>,
    pub(crate) forward_stderr: Option<StdOutputConfig>,
    pub(crate) secrets: Option<wasm_workers::activity::activity_exec_worker::ExecSecrets>,
    pub(crate) params_via_stdin: bool,
    pub(crate) component_id: ComponentId,
    pub(crate) exec_config: executor::executor::ExecConfig,
    pub(crate) logs_store_min_level: Option<LogLevel>,
}

impl ActivityExecConfigVerified {
    pub fn component_id(&self) -> &ComponentId {
        &self.component_id
    }
}

#[derive(Debug)]
pub(crate) struct WorkflowJsConfigVerified {
    pub(crate) wasm_path: Arc<Path>, // same for all JS workflows
    pub(crate) js_entry_path: String,
    pub(crate) js_files: BTreeMap<String, String>,
    pub(crate) ffqn: FunctionFqn,
    pub(crate) params: Vec<concepts::ParameterType>,
    pub(crate) return_type: concepts::ReturnTypeExtendable,
    pub(crate) user_wasm_component: Option<WasmComponent>,
    pub(crate) workflow_config: WorkflowConfig,
    pub(crate) exec_config: executor::executor::ExecConfig,
    pub(crate) logs_store_min_level: Option<LogLevel>,
    pub(crate) lock_extension_leeway: Duration,
}

impl WorkflowJsConfigVerified {
    pub fn component_id(&self) -> &ComponentId {
        &self.workflow_config.component_id
    }

    pub(crate) fn frame_sources(js_files: BTreeMap<String, String>) -> FrameFilesToSource {
        js_files
            .into_iter()
            .map(|(name, content)| (name, FrameSource::Content(content)))
            .collect()
    }
}

pub(crate) struct JsContent {
    pub(crate) entry_path: String,
    pub(crate) files: BTreeMap<String, String>,
}

pub(crate) fn hash_js_graph(
    hasher: &mut Sha256,
    entry_path: &str,
    files: &BTreeMap<String, String>,
) {
    if files.len() == 1
        && let Some((only_path, only_source)) = files.iter().next()
        && only_path == entry_path
    {
        hasher.update(only_source.as_bytes());
        return;
    }
    hasher.update(b"v1\0");
    for (path, source) in files {
        hasher.update(path.as_bytes());
        hasher.update(b"\0");
        hasher.update(source.as_bytes());
        hasher.update(b"\0");
    }
    hasher.update(entry_path.as_bytes());
}

pub(crate) trait JsLocationResolvedExt {
    async fn get_content(
        &self,
        wasm_cache_dir: &Path,
        expected_digest: Option<&ContentDigest>,
    ) -> anyhow::Result<JsContent>;
}

impl JsLocationResolvedExt for ScriptLocationResolved {
    /// Return the JS source content and file name.
    /// For `Content`, returns them directly (validating digest if provided).
    /// For `Oci`, pulls from the registry (or cache) under `wasm_cache_dir/js/`.
    async fn get_content(
        &self,
        wasm_cache_dir: &Path,
        expected_digest: Option<&ContentDigest>,
    ) -> anyhow::Result<JsContent> {
        match self {
            ScriptLocationResolved::Content { content, file_name } => {
                if let Some(expected) = expected_digest {
                    let hash: [u8; 32] = Sha256::digest(content.as_bytes()).into();
                    let actual = ContentDigest(Digest(hash));
                    ensure!(
                        *expected == actual,
                        "content digest mismatch for inline JS `{file_name}`: expected {expected}, got {actual}"
                    );
                }
                // Preserve the historical basename used by single-file stack frames and
                // backtrace source lookup. True module graphs use deployment-relative keys.
                let entry_path = std::path::Path::new(file_name)
                    .file_name()
                    .and_then(|name| name.to_str())
                    .unwrap_or(file_name)
                    .to_string();
                let files = BTreeMap::from([(entry_path.clone(), content.clone())]);
                Ok(JsContent { entry_path, files })
            }
            ScriptLocationResolved::Graph { entry_path, files } => Ok(JsContent {
                entry_path: entry_path.clone(),
                files: files.iter().cloned().collect(),
            }),
            ScriptLocationResolved::Oci { image } => {
                let js_cache_dir = wasm_cache_dir.join("js");
                tokio::fs::create_dir_all(&js_cache_dir)
                    .await
                    .with_context(|| {
                        format!("cannot create JS cache directory {js_cache_dir:?}")
                    })?;
                let metadata_dir = wasm_cache_metadata_dir(wasm_cache_dir);
                tokio::fs::create_dir_all(&metadata_dir)
                    .await
                    .with_context(|| {
                        format!("cannot create metadata directory {metadata_dir:?}")
                    })?;
                let crate::oci::JsCacheResult { js_path, .. } =
                    crate::oci::pull_js_to_cache(image, &js_cache_dir, &metadata_dir)
                        .await
                        .with_context(|| format!("cannot pull JS from OCI: {image}"))?;
                if let Some(expected) = expected_digest {
                    let hash = utils::sha256sum::calculate_sha256_file(&js_path).await?;
                    ensure!(
                        *expected == hash,
                        "content digest mismatch for OCI JS `{image}`: expected {expected}, got {hash}"
                    );
                }
                let file_name = js_path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or("cached.js")
                    .to_string();
                let source = tokio::fs::read_to_string(&js_path)
                    .await
                    .with_context(|| format!("cannot read cached JS file {js_path:?}"))?;
                Ok(JsContent {
                    entry_path: file_name.clone(),
                    files: BTreeMap::from([(file_name, source)]),
                })
            }
        }
    }
}

#[derive(Debug)]
pub(crate) struct WorkflowConfigVerified {
    pub(crate) wasm_path: PathBuf,
    pub(crate) workflow_config: WorkflowConfig,
    pub(crate) exec_config: executor::executor::ExecConfig,
    pub(crate) frame_files_to_sources: FrameFilesToSource,
    pub(crate) logs_store_min_level: Option<LogLevel>,
    pub(crate) lock_extension_leeway: Duration,
}

impl WorkflowConfigVerified {
    pub fn component_id(&self) -> &ComponentId {
        &self.workflow_config.component_id
    }
}

// Resolved component config types live in the `model` submodule.

pub(crate) trait ActivityJsComponentConfigResolvedExt {
    #[expect(clippy::too_many_arguments)]
    async fn fetch_and_verify(
        self,
        wasm_path: Arc<Path>,
        wasm_cache_dir: Arc<Path>,
        ignore_missing_env_vars: bool,
        secret_registry: &Arc<SecretRegistry>,
        global_http_config: GlobalHttpConfig,
        global_executor_instance_limiter: Option<Arc<tokio::sync::Semaphore>>,
        fuel: Option<u64>,
    ) -> Result<ActivityJsConfigVerified, anyhow::Error>;
}

impl ActivityJsComponentConfigResolvedExt for ActivityJsComponentConfigResolved {
    #[instrument(skip_all, fields(component_name = self.name.as_str()))]
    async fn fetch_and_verify(
        self,
        wasm_path: Arc<Path>,
        wasm_cache_dir: Arc<Path>,
        ignore_missing_env_vars: bool,
        secret_registry: &Arc<SecretRegistry>,
        global_http_config: GlobalHttpConfig,
        global_executor_instance_limiter: Option<Arc<tokio::sync::Semaphore>>,
        fuel: Option<u64>,
    ) -> Result<ActivityJsConfigVerified, anyhow::Error> {
        let verified = verify_function_interface(
            self.interface,
            &self.ffqn,
            ComponentType::Activity,
            Vec::new(),
            "result",
        )?;
        let parsed_params = verified.params;
        let return_type = verified.return_type;
        let JsContent {
            entry_path: js_entry_path,
            files: js_files,
        } = self
            .location
            .get_content(&wasm_cache_dir, self.content_digest.as_ref())
            .await?;
        warn_deprecated_component_digest_override(
            self.name.as_str(),
            self.component_digest.as_ref(),
        );
        let component_digest = self.component_digest.unwrap_or_else(|| {
            let mut hasher = Sha256::new();
            hasher.update(b"activity_js:");
            hash_js_graph(&mut hasher, &js_entry_path, &js_files);
            hasher.update(self.ffqn.to_string().as_bytes());
            for p in &parsed_params {
                hasher.update(p.wit_type.as_ref().as_bytes());
            }
            hasher.update(return_type.wit_type.as_bytes());
            if let Some(component) = &verified.authored_component {
                hasher.update(component.wit().as_bytes());
            }
            let hash: [u8; 32] = hasher.finalize().into();
            ComponentDigest(Digest(hash))
        });
        let component_id = ComponentId::new(
            ComponentType::Activity,
            StrVariant::from(self.name),
            component_digest,
        )?;
        let env_vars =
            resolve_env_vars_plaintext(self.env_vars, ignore_missing_env_vars, secret_registry)?;
        let (allowed_hosts, _advisories) =
            resolve_allowed_hosts(self.allowed_hosts, ignore_missing_env_vars, secret_registry)?;
        validate_no_env_collision(&env_vars, &allowed_hosts)?;
        let secrets = restricted_secret_registry(secret_registry, &allowed_hosts, None);
        let activity_config = ActivityConfig {
            component_id: component_id.clone(),
            forward_stdout: self.forward_stdout.into_std_output_config(),
            forward_stderr: self.forward_stderr.into_std_output_config(),
            env_vars,
            fuel,
            allowed_hosts,
            global_http_config,
            secrets,
            config_section_hint: ConfigSectionHint::ActivityJs,
        };
        let retry_config = ComponentRetryConfig {
            max_retries: Some(self.max_retries),
            retry_exp_backoff: self.retry_exp_backoff.into(),
        };
        Ok(ActivityJsConfigVerified {
            wasm_path,
            js_entry_path,
            js_files,
            ffqn: self.ffqn,
            params: parsed_params,
            return_type,
            user_wasm_component: verified.authored_component,
            activity_config,
            exec_config: self.exec.into_exec_exec_config(
                component_id,
                global_executor_instance_limiter,
                retry_config,
            )?,
            logs_store_min_level: self.logs_store_min_level.into_log_level(),
        })
    }
}

pub(crate) trait WorkflowWasmComponentConfigResolvedExt {
    #[expect(clippy::too_many_arguments)]
    async fn fetch_and_verify(
        self,
        wasm_cache_dir: Arc<Path>,
        metadata_dir: Arc<Path>,
        global_executor_instance_limiter: Option<Arc<tokio::sync::Semaphore>>,
        fuel: Option<u64>,
        subscription_interruption: Option<Duration>,
        max_events_per_run: usize,
        response_refresh_interval: usize,
    ) -> Result<WorkflowConfigVerified, anyhow::Error>;
}

impl WorkflowWasmComponentConfigResolvedExt for WorkflowWasmComponentConfigResolved {
    #[instrument(skip_all, fields(component_name = self.common.name.as_str()))]
    async fn fetch_and_verify(
        self,
        wasm_cache_dir: Arc<Path>,
        metadata_dir: Arc<Path>,
        global_executor_instance_limiter: Option<Arc<tokio::sync::Semaphore>>,
        fuel: Option<u64>,
        subscription_interruption: Option<Duration>,
        max_events_per_run: usize,
        response_refresh_interval: usize,
    ) -> Result<WorkflowConfigVerified, anyhow::Error> {
        let retry_exp_backoff = Duration::from(self.retry_exp_backoff);
        if retry_exp_backoff == Duration::ZERO {
            bail!(
                "invalid `retry_exp_backoff` setting for workflow `{}` - duration must not be zero",
                self.common.name
            );
        }
        let expected_content_digest = self.content_digest;
        let (common, content_digest, wasm_path) =
            self.common.fetch(&wasm_cache_dir, &metadata_dir).await?;
        verify_fetched_content_digest(
            &content_digest,
            expected_content_digest.as_ref(),
            &common.location.to_string(),
        )?;
        let wasm_path = WasmComponent::convert_core_module_to_component(
            &wasm_path,
            &content_digest,
            &wasm_cache_dir,
        )
        .await?
        .unwrap_or(wasm_path);
        warn_deprecated_component_digest_override(
            common.name.as_str(),
            self.component_digest.as_ref(),
        );
        let component_digest = self
            .component_digest
            .unwrap_or(ComponentDigest(content_digest.0));
        let component_id = ComponentId::new(
            ComponentType::Workflow,
            StrVariant::from(common.name),
            component_digest,
        )?;
        let workflow_config = WorkflowConfig {
            component_id: component_id.clone(),
            stub_wasi: self.stub_wasi,
            fuel,
            mode: WorkflowConfigMode::Real {
                join_next_blocking_strategy: self
                    .blocking_strategy
                    .into_blocking_strategy(subscription_interruption),
                lock_extension: self.lock_extension.then_some(self.exec.lock_expiry.into()),
                max_events_per_run,
                response_refresh_interval,
            },
        };
        let frame_files_to_sources: FrameFilesToSource = self
            .backtrace
            .into_frame_files()
            .into_iter()
            .map(|(name, digest)| (name, FrameSource::Digest(digest)))
            .collect();
        let retry_config = ComponentRetryConfig {
            max_retries: None,
            retry_exp_backoff,
        };
        Ok(WorkflowConfigVerified {
            wasm_path,
            workflow_config,
            exec_config: self.exec.into_exec_exec_config(
                component_id,
                global_executor_instance_limiter,
                retry_config,
            )?,
            frame_files_to_sources,
            logs_store_min_level: self.logs_store_min_level.into_log_level(),
            lock_extension_leeway: self.lock_extension_leeway.into(),
        })
    }
}

pub(crate) trait WorkflowJsComponentConfigResolvedExt {
    #[expect(clippy::too_many_arguments)]
    async fn fetch_and_verify(
        self,
        wasm_path: Arc<Path>,
        wasm_cache_dir: Arc<Path>,
        global_executor_instance_limiter: Option<Arc<tokio::sync::Semaphore>>,
        fuel: Option<u64>,
        subscription_interruption: Option<Duration>,
        max_events_per_run: usize,
        response_refresh_interval: usize,
    ) -> Result<WorkflowJsConfigVerified, anyhow::Error>;
}

impl WorkflowJsComponentConfigResolvedExt for WorkflowJsComponentConfigResolved {
    #[instrument(skip_all, fields(component_name = self.name.as_str()))]
    async fn fetch_and_verify(
        self,
        wasm_path: Arc<Path>,
        wasm_cache_dir: Arc<Path>,
        global_executor_instance_limiter: Option<Arc<tokio::sync::Semaphore>>,
        fuel: Option<u64>,
        subscription_interruption: Option<Duration>,
        max_events_per_run: usize,
        response_refresh_interval: usize,
    ) -> Result<WorkflowJsConfigVerified, anyhow::Error> {
        let verified = verify_function_interface(
            self.interface,
            &self.ffqn,
            ComponentType::Workflow,
            Vec::new(),
            "result",
        )?;
        let parsed_params = verified.params;
        let return_type = verified.return_type;
        let JsContent {
            entry_path: js_entry_path,
            files: js_files,
        } = self
            .location
            .get_content(&wasm_cache_dir, self.content_digest.as_ref())
            .await?;
        warn_deprecated_component_digest_override(
            self.name.as_str(),
            self.component_digest.as_ref(),
        );
        let component_digest = self.component_digest.unwrap_or_else(|| {
            let mut hasher = Sha256::new();
            hasher.update(b"workflow_js:");
            hash_js_graph(&mut hasher, &js_entry_path, &js_files);
            hasher.update(self.ffqn.to_string().as_bytes());
            for p in &parsed_params {
                hasher.update(p.wit_type.as_ref().as_bytes());
            }
            hasher.update(return_type.wit_type.as_bytes());
            if let Some(component) = &verified.authored_component {
                hasher.update(component.wit().as_bytes());
            }
            let hash: [u8; 32] = hasher.finalize().into();
            ComponentDigest(Digest(hash))
        });
        let component_id = ComponentId::new(
            ComponentType::Workflow,
            StrVariant::from(self.name),
            component_digest,
        )?;
        let workflow_config = WorkflowConfig {
            component_id: component_id.clone(),
            stub_wasi: false,
            fuel,
            mode: WorkflowConfigMode::Real {
                join_next_blocking_strategy: self
                    .blocking_strategy
                    .into_blocking_strategy(subscription_interruption),
                lock_extension: self.lock_extension.then_some(self.exec.lock_expiry.into()),
                max_events_per_run,
                response_refresh_interval,
            },
        };
        let retry_config = ComponentRetryConfig {
            max_retries: None,
            retry_exp_backoff: self.retry_exp_backoff.into(),
        };
        Ok(WorkflowJsConfigVerified {
            wasm_path,
            js_entry_path,
            js_files,
            ffqn: self.ffqn,
            params: parsed_params,
            return_type,
            user_wasm_component: verified.authored_component,
            workflow_config,
            exec_config: self.exec.into_exec_exec_config(
                component_id,
                global_executor_instance_limiter,
                retry_config,
            )?,
            logs_store_min_level: self.logs_store_min_level.into_log_level(),
            lock_extension_leeway: self.lock_extension_leeway.into(),
        })
    }
}

/// Resolve a `DeploymentToml` to `DeploymentResolved` by reading all local JS and backtrace
/// source files.
pub(crate) async fn resolve_local_refs(
    deployment: DeploymentTomlValidated,
    cas: &dyn Cas,
) -> anyhow::Result<DeploymentResolved> {
    let mut activities_js = Vec::with_capacity(deployment.activities_js.len());
    for (mut a, name) in deployment.activities_js {
        let interface =
            resolve_function_interface(a.interface, &mut a.component_files, cas).await?;
        activities_js.push(ActivityJsComponentConfigResolved {
            location: resolve_script_toml(
                ScriptToml::JavaScript {
                    location: a.location,
                    content: a.content,
                    component_files: a.component_files,
                },
                format!("{name}.js"),
                cas,
                a.content_digest.as_ref(),
            )
            .await?,
            name,
            content_digest: a.content_digest,
            component_digest: a.component_digest,
            ffqn: a.ffqn,
            interface,
            exec: a.exec,
            max_retries: a.max_retries,
            retry_exp_backoff: a.retry_exp_backoff,
            forward_stdout: a.forward_stdout,
            forward_stderr: a.forward_stderr,
            logs_store_min_level: a.logs_store_min_level,
            env_vars: a.env_vars,
            allowed_hosts: a.allowed_hosts,
        });
    }

    let mut workflows_wasm = Vec::with_capacity(deployment.workflows_wasm.len());
    for w in deployment.workflows_wasm {
        workflows_wasm.push(WorkflowWasmComponentConfigResolved {
            common: w.common,
            content_digest: w.content_digest,
            component_digest: w.component_digest,
            exec: w.exec,
            retry_exp_backoff: w.retry_exp_backoff,
            blocking_strategy: w.blocking_strategy,
            backtrace: resolve_backtrace(&w.backtrace, &w.component_files)?,
            stub_wasi: w.stub_wasi,
            lock_extension: w.lock_extension,
            lock_extension_leeway: w.lock_extension_leeway,
            logs_store_min_level: w.logs_store_min_level,
        });
    }

    let mut workflows_js = Vec::with_capacity(deployment.workflows_js.len());
    for (mut w, name) in deployment.workflows_js {
        let interface =
            resolve_function_interface(w.interface, &mut w.component_files, cas).await?;
        workflows_js.push(WorkflowJsComponentConfigResolved {
            location: resolve_script_toml(
                ScriptToml::JavaScript {
                    location: w.location,
                    content: w.content,
                    component_files: w.component_files,
                },
                format!("{name}.js"),
                cas,
                w.content_digest.as_ref(),
            )
            .await?,
            name,
            content_digest: w.content_digest,
            component_digest: w.component_digest,
            ffqn: w.ffqn,
            interface,
            exec: w.exec,
            retry_exp_backoff: w.retry_exp_backoff,
            blocking_strategy: w.blocking_strategy,
            lock_extension: w.lock_extension,
            lock_extension_leeway: w.lock_extension_leeway,
            logs_store_min_level: w.logs_store_min_level,
        });
    }

    let mut webhooks_wasm = Vec::with_capacity(deployment.webhooks_wasm.len());
    for w in deployment.webhooks_wasm {
        webhooks_wasm.push(WebhookWasmComponentConfigResolved {
            common: w.common,
            content_digest: w.content_digest,
            http_server: w.http_server,
            routes: w.routes,
            forward_stdout: w.forward_stdout,
            forward_stderr: w.forward_stderr,
            env_vars: w.env_vars,
            backtrace: resolve_backtrace(&w.backtrace, &w.component_files)?,
            backtrace_persist: w.backtrace_persist,
            logs_store_min_level: w.logs_store_min_level,
            allowed_hosts: w.allowed_hosts,
            is_webui: false,
        });
    }

    let mut webhooks_js = Vec::with_capacity(deployment.webhooks_js.len());
    for w in deployment.webhooks_js {
        webhooks_js.push(WebhookJsComponentConfigResolved {
            location: resolve_script_toml(
                ScriptToml::JavaScript {
                    location: w.location,
                    content: w.content,
                    component_files: w.component_files,
                },
                format!("{}.js", w.name),
                cas,
                w.content_digest.as_ref(),
            )
            .await?,
            name: w.name,
            content_digest: w.content_digest,
            http_server: w.http_server,
            routes: w.routes,
            forward_stdout: w.forward_stdout,
            forward_stderr: w.forward_stderr,
            logs_store_min_level: w.logs_store_min_level,
            env_vars: w.env_vars,
            backtrace_persist: w.backtrace_persist,
            allowed_hosts: w.allowed_hosts,
        });
    }

    let mut activities_exec = Vec::with_capacity(deployment.activities_exec.len());
    for (mut a, name) in deployment.activities_exec {
        let interface =
            resolve_function_interface(a.interface, &mut a.component_files, cas).await?;
        ensure!(
            a.component_files.is_empty(),
            "activity_exec component_files contains files not selected by its WIT"
        );
        let location = resolve_script_toml(
            ScriptToml::Exec {
                location: a.location,
                content: a.content,
            },
            name.to_string(),
            cas,
            a.content_digest.as_ref(),
        )
        .await?;
        activities_exec.push(ActivityExecComponentConfigResolved {
            name,
            location,
            content_digest: a.content_digest,
            ffqn: a.ffqn,
            interface,
            component_digest: a.component_digest,
            exec: a.exec,
            max_retries: a.max_retries,
            retry_exp_backoff: a.retry_exp_backoff,
            forward_stdout: a.forward_stdout,
            forward_stderr: a.forward_stderr,
            logs_store_min_level: a.logs_store_min_level,
            env_vars: a.env_vars,
            max_output_bytes: a.max_output_bytes,
            secrets: a.secrets,
            params_via_stdin: a.params_via_stdin,
        });
    }

    // Build resolved stubs/externals with their names filled in.
    let mut activities_stub = Vec::with_capacity(deployment.activities_stub.len());
    for (c, name) in deployment.activities_stub {
        activities_stub.push(match c {
            ActivityStubComponentConfigToml::File(f) => {
                ActivityStubComponentConfigResolved::File(f)
            }
            ActivityStubComponentConfigToml::Inline(mut i) => {
                let interface =
                    resolve_function_interface(i.interface, &mut i.component_files, cas).await?;
                ensure!(
                    i.component_files.is_empty(),
                    "activity_stub component_files contains files not selected by its WIT"
                );
                ActivityStubComponentConfigResolved::Inline(ActivityStubExtInlineConfigResolved {
                    name,
                    ffqn: i.ffqn,
                    interface,
                })
            }
        });
    }
    let mut activities_external = Vec::with_capacity(deployment.activities_external.len());
    for (c, name) in deployment.activities_external {
        activities_external.push(match c {
            ActivityExternalComponentConfigToml::File(f) => {
                ActivityExternalComponentConfigResolved::File(f)
            }
            ActivityExternalComponentConfigToml::Inline(mut i) => {
                let interface =
                    resolve_function_interface(i.interface, &mut i.component_files, cas).await?;
                ensure!(
                    i.component_files.is_empty(),
                    "activity_external component_files contains files not selected by its WIT"
                );
                ActivityExternalComponentConfigResolved::Inline(
                    ActivityStubExtInlineConfigResolved {
                        name,
                        ffqn: i.ffqn,
                        interface,
                    },
                )
            }
        });
    }

    let resolved = DeploymentResolved {
        source_path: None,
        activities_wasm: deployment.activities_wasm,
        activities_stub,
        activities_external,
        activities_js,
        activities_exec,
        workflows_wasm,
        workflows_js,
        webhooks_wasm,
        webhooks_js,
        crons: deployment.crons,
    };
    validate_owned_source_file_names(&resolved)?;
    Ok(resolved)
}

async fn resolve_function_interface(
    interface: FunctionInterfaceToml,
    component_files: &mut BTreeMap<String, ContentDigest>,
    cas: &dyn Cas,
) -> anyhow::Result<FunctionInterfaceResolved> {
    let root = match interface {
        FunctionInterfaceToml::Authored(AuthoredFunctionInterfaceToml { wit }) => wit,
        FunctionInterfaceToml::Inline(InlineFunctionInterfaceToml {
            params,
            return_type,
        }) => {
            return Ok(FunctionInterfaceResolved::Inline(
                InlineFunctionInterfaceResolved {
                    params,
                    return_type,
                },
            ));
        }
    };
    let root = sanitize_deployment_relative_path(&root)?;
    // TODO: Cache parsed WIT packages by root during deployment resolution.
    let parsed = parse_wit_files_from_cas(cas, &root, component_files).await?;
    let prefix = format!("{root}/");
    for (path, _) in &parsed.files {
        ensure!(
            path.starts_with(&prefix),
            "parsed WIT file `{path}` is outside configured WIT directory `{root}`"
        );
        component_files.remove(path);
    }
    Ok(FunctionInterfaceResolved::Authored {
        wit: Box::new(WitSourceResolved {
            root,
            resolve: parsed.resolve,
            main_pkg_id: parsed.main_pkg_id,
        }),
    })
}

/// Reject deployments where two deployment-owned source files (inline/owned scripts and
/// recreated workflow/webhook backtrace sources) would be written to the same `file_name`
/// with differing content. Such a deployment hashes and runs fine, but could never be
/// retrieved with `deployment get`, which writes every owned source to disk at its
/// `file_name` and refuses to clobber. Identical re-uses of a name are allowed (they dedupe
/// to a single file). This surfaces the failure at submit time rather than on a later round-trip.
pub(crate) fn validate_owned_source_file_names(
    resolved: &DeploymentResolved,
) -> anyhow::Result<()> {
    // Compare by content digest: scripts carry inline content (hashed here), backtrace
    // sources already carry their CAS digest. Differing digests at the same `file_name` are
    // the collision `deployment get` cannot round-trip.
    fn digest_of(content: &str) -> ContentDigest {
        ContentDigest(Digest(Sha256::digest(content.as_bytes()).into()))
    }
    fn register<'a>(
        seen: &mut HashMap<&'a str, ContentDigest>,
        file_name: &'a str,
        digest: &ContentDigest,
    ) -> anyhow::Result<()> {
        if let Some(existing) = seen.insert(file_name, digest.clone()) {
            ensure!(
                existing == *digest,
                "two deployment-owned source files would be written to `{file_name}`; rename \
                 one of the colliding scripts or backtrace sources so the deployment can be \
                 retrieved with `deployment get`"
            );
        }
        Ok(())
    }

    let mut seen: HashMap<&str, ContentDigest> = HashMap::new();

    let script_locations = resolved
        .activities_js
        .iter()
        .map(|c| &c.location)
        .chain(resolved.activities_exec.iter().map(|c| &c.location))
        .chain(resolved.workflows_js.iter().map(|c| &c.location))
        .chain(resolved.webhooks_js.iter().map(|c| &c.location));
    for loc in script_locations {
        match loc {
            ScriptLocationResolved::Content { content, file_name } => {
                register(&mut seen, file_name, &digest_of(content))?;
            }
            ScriptLocationResolved::Graph { files, .. } => {
                for (file_name, content) in files {
                    register(&mut seen, file_name, &digest_of(content))?;
                }
            }
            ScriptLocationResolved::Oci { .. } => {}
        }
    }

    let backtraces = resolved
        .workflows_wasm
        .iter()
        .map(|c| &c.backtrace)
        .chain(resolved.webhooks_wasm.iter().map(|c| &c.backtrace));
    for bt in backtraces {
        for source in bt.frame_files_to_sources.values() {
            register(&mut seen, &source.file_name, &source.content_digest)?;
        }
    }
    Ok(())
}

pub(crate) enum ScriptToml {
    JavaScript {
        location: Option<ScriptLocationPathOrOci>,
        content: Option<String>,
        component_files: BTreeMap<String, ContentDigest>,
    },
    Exec {
        location: Option<ScriptLocationPathOrOci>,
        content: Option<String>,
    },
}

pub(crate) enum ModuleGraphResolution {
    JavaScript(BTreeMap<String, ContentDigest>),
    Disabled,
}

/// Resolve a script source (JS or exec) TOML location to its resolved form.
///
/// - inline `content` → `Content { content, file_name: default_file_name }` (owned).
/// - a **relative** `Path` (bare, or `${DEPLOYMENT_DIR}/…`) → read + inline as `Content`,
///   with `file_name` preserving the deployment-relative subpath (owned). `..` escapes error.
/// - an **absolute** `Path` → rejected.
/// - an `Oci` reference → `Oci { image }`.
///
/// When `content_digest` is set it is verified here against the relevant bytes (inline
/// content or the owned file). `Oci` digests are verified at runtime.
pub(crate) async fn resolve_script_toml(
    script: ScriptToml,
    default_file_name: String,
    cas: &dyn Cas,
    content_digest: Option<&ContentDigest>,
) -> anyhow::Result<ScriptLocationResolved> {
    let (location, content, module_graph) = match script {
        ScriptToml::JavaScript {
            location,
            content,
            component_files,
        } => (
            location,
            content,
            ModuleGraphResolution::JavaScript(component_files),
        ),
        ScriptToml::Exec { location, content } => {
            (location, content, ModuleGraphResolution::Disabled)
        }
    };
    match (location, content) {
        (None, Some(content)) => {
            if let ModuleGraphResolution::JavaScript(component_files) = &module_graph {
                ensure!(
                    component_files.is_empty(),
                    "inline scripts cannot set `component_files`"
                );
            }
            verify_content_digest(content.as_bytes(), content_digest, &default_file_name)?;
            Ok(ScriptLocationResolved::Content {
                content,
                file_name: default_file_name,
            })
        }
        (Some(ScriptLocationPathOrOci::Path(path)), None) => {
            if std::path::Path::new(&path).is_absolute() {
                bail!("absolute local paths are not allowed in deployment manifests: `{path}`")
            }
            let path = strip_deployment_dir_prefix(&path).unwrap_or(&path);
            let path = sanitize_deployment_relative_path(path)?;
            if let ModuleGraphResolution::JavaScript(component_files) = module_graph {
                // `component_files` is the manifest's declared closure (`path -> digest`).
                // Ensure the entry is in it (an entry-only manifest declares none), then walk
                // the import graph and require it be fully contained: an import missing from
                // the package is rejected here rather than trapping at runtime.
                let declared: std::collections::BTreeSet<String> =
                    component_files.keys().cloned().collect();
                let mut known = component_files;
                match (known.get(&path), content_digest) {
                    (Some(entry_digest), Some(expected)) => ensure!(
                        expected == entry_digest,
                        "content_digest for `{path}` does not match its component_files digest"
                    ),
                    (None, Some(cd)) => {
                        known.insert(path.clone(), cd.clone());
                    }
                    _ => {}
                }
                let files = parse_js_graph_from_cas(cas, &path, &known).await?;
                let entry_source = files
                    .iter()
                    .find_map(|(module_path, source)| (module_path == &path).then_some(source))
                    .context("parsed JS graph does not contain its entry")?;
                verify_content_digest(entry_source.as_bytes(), content_digest, &path)?;
                // Reject stray declared files: every `component_files` entry must be reachable
                // from the entry's imports, so the stored file set is exactly what runs and the
                // deployment -> digest mapping the orphan GC relies on carries no dead blobs.
                let reached: std::collections::BTreeSet<&str> = files
                    .iter()
                    .map(|(module_path, _)| module_path.as_str())
                    .collect();
                let stray: Vec<&str> = declared
                    .iter()
                    .map(String::as_str)
                    .filter(|declared_path| !reached.contains(declared_path))
                    .collect();
                ensure!(
                    stray.is_empty(),
                    "component_files for `{path}` declares {stray:?} that its module graph does not import"
                );
                if files.len() > 1 {
                    return Ok(ScriptLocationResolved::Graph {
                        entry_path: path,
                        files,
                    });
                }
            }
            let digest = content_digest.with_context(|| {
                // TODO: resolved deployment must have content_digest set, this can only happen on a corrupted deployment record.
                // This should be fixed by having a separate schema for DB deployment.
                format!("deployment-owned script `{path}` is missing a content digest")
            })?;
            let content = read_package_blob(cas, digest, &path).await?;
            let content = String::from_utf8(content)
                .with_context(|| format!("script file {path:?} is not valid UTF-8"))?;
            Ok(ScriptLocationResolved::Content {
                content,
                file_name: path,
            })
        }
        (Some(ScriptLocationPathOrOci::Oci(reference)), None) => {
            if let ModuleGraphResolution::JavaScript(component_files) = module_graph {
                ensure!(
                    component_files.is_empty(),
                    "OCI scripts cannot set `component_files`"
                );
            }
            Ok(ScriptLocationResolved::Oci { image: reference })
        }
        (None, None) | (Some(_), Some(_)) => {
            bail!("exactly one of `location` or `content` must be set for script components")
        }
    }
}

pub(crate) fn resolve_backtrace(
    backtrace: &ComponentBacktraceConfig,
    component_files: &BTreeMap<String, ContentDigest>,
) -> anyhow::Result<ComponentBacktraceConfigResolved> {
    let mut frame_files_to_sources = HashMap::new();
    for (key, path) in &backtrace.frame_files_to_sources {
        // Classify the source path like a script: a relative path (bare or
        // `${DEPLOYMENT_DIR}/…`) is deployment-relative and its subpath is mirrored on export.
        // The pre-resolve validation pass already rejected absolute paths.
        let file_name = if let Some(rest) = strip_deployment_dir_prefix(path) {
            sanitize_deployment_relative_path(rest)?
        } else if std::path::Path::new(path).is_absolute() {
            unreachable!("absolute backtrace source `{path}` must be rejected before resolution")
        } else {
            sanitize_deployment_relative_path(path)?
        };
        // The processed manifest carries every deployment-owned backtrace source's digest in
        // `component_files`; the bytes are in the CAS, so the digest is a complete reference.
        // The pre-resolve validation pass guarantees the entry is present.
        let content_digest = component_files
            .get(&file_name)
            .unwrap_or_else(|| {
                unreachable!(
                    "backtrace source `{file_name}` must have a digest in `component_files`"
                )
            })
            .clone();
        frame_files_to_sources.insert(
            key.clone(),
            BacktraceSourceResolved {
                content_digest,
                file_name,
            },
        );
    }
    Ok(ComponentBacktraceConfigResolved {
        frame_files_to_sources,
    })
}

// Webhook and cron verified configs + fetch/verify

#[derive(Debug)]
pub(crate) struct WebhookWasmComponentConfigVerified {
    pub(crate) component_id: ComponentId,
    pub(crate) wasm_path: PathBuf,
    pub(crate) routes: Vec<WebhookRouteVerified>,
    pub(crate) forward_stdout: Option<StdOutputConfig>,
    pub(crate) forward_stderr: Option<StdOutputConfig>,
    pub(crate) env_vars: Arc<[EnvVar]>,
    pub(crate) frame_files_to_sources: FrameFilesToSource,
    pub(crate) backtrace_persist: bool,
    pub(crate) subscription_interruption: Option<Duration>,
    pub(crate) logs_store_min_level: Option<LogLevel>,
    pub(crate) allowed_hosts: Arc<[AllowedHostConfig]>,
    /// Component-scoped resolver for the endpoint's declared secret names.
    pub(crate) secrets: Arc<dyn SecretResolver>,
    pub(crate) is_webui: bool,
    /// The TOML config section type for error messages
    pub(crate) config_section_hint: ConfigSectionHint,
}

#[derive(Debug)]
pub(crate) struct WebhookRouteVerified {
    pub(crate) methods: Vec<http::Method>,
    pub(crate) route: String,
}

impl TryFrom<WebhookRoute> for WebhookRouteVerified {
    type Error = anyhow::Error;

    fn try_from(value: WebhookRoute) -> Result<Self, Self::Error> {
        Ok(match value {
            WebhookRoute::String(route) => Self {
                methods: Vec::new(),
                route,
            },
            WebhookRoute::WebhookRouteDetail(WebhookRouteDetail { methods, route }) => {
                let methods = methods
                    .into_iter()
                    .map(|method| {
                        http::Method::from_bytes(method.as_bytes())
                            .with_context(|| format!("cannot parse route method `{method}`"))
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                Self { methods, route }
            }
        })
    }
}

#[derive(Debug)]
pub(crate) struct WebhookJsConfigVerified {
    pub(crate) wasm_path: Arc<Path>,
    pub(crate) component_id: ComponentId,
    pub(crate) js_entry_path: String,
    pub(crate) js_files: BTreeMap<String, String>,
    pub(crate) routes: Vec<WebhookRouteVerified>,
    pub(crate) forward_stdout: Option<StdOutputConfig>,
    pub(crate) forward_stderr: Option<StdOutputConfig>,
    pub(crate) env_vars: Arc<[EnvVar]>,
    pub(crate) backtrace_persist: bool,
    pub(crate) logs_store_min_level: Option<LogLevel>,
    pub(crate) allowed_hosts: Arc<[AllowedHostConfig]>,
    /// Component-scoped resolver for the endpoint's declared secret names.
    pub(crate) secrets: Arc<dyn SecretResolver>,
    /// The TOML config section type for error messages
    pub(crate) config_section_hint: ConfigSectionHint,
}

impl WebhookJsConfigVerified {
    pub(crate) fn as_frame_sources(&self) -> FrameFilesToSource {
        self.js_files
            .clone()
            .into_iter()
            .map(|(name, content)| (name, FrameSource::Content(content)))
            .collect()
    }
}

pub(crate) trait WebhookWasmComponentConfigResolvedExt {
    async fn fetch_and_verify(
        self,
        wasm_cache_dir: Arc<Path>,
        metadata_dir: Arc<Path>,
        ignore_missing_env_vars: bool,
        secret_registry: &Arc<SecretRegistry>,
        subscription_interruption: Option<Duration>,
    ) -> Result<(ConfigName, WebhookWasmComponentConfigVerified), anyhow::Error>;
}

impl WebhookWasmComponentConfigResolvedExt for WebhookWasmComponentConfigResolved {
    #[instrument(skip_all, fields(component_name = self.common.name.as_str()), err)]
    async fn fetch_and_verify(
        self,
        wasm_cache_dir: Arc<Path>,
        metadata_dir: Arc<Path>,
        ignore_missing_env_vars: bool,
        secret_registry: &Arc<SecretRegistry>,
        subscription_interruption: Option<Duration>,
    ) -> Result<(ConfigName, WebhookWasmComponentConfigVerified), anyhow::Error> {
        let expected_content_digest = self.content_digest;
        let (common, content_digest, wasm_path) =
            self.common.fetch(&wasm_cache_dir, &metadata_dir).await?;
        super::verify_fetched_content_digest(
            &content_digest,
            expected_content_digest.as_ref(),
            &common.location.to_string(),
        )?;
        let frame_files_to_sources: FrameFilesToSource = self
            .backtrace
            .into_frame_files()
            .into_iter()
            .map(|(name, digest)| (name, FrameSource::Digest(digest)))
            .collect();
        let component_id = ComponentId::new(
            ComponentType::WebhookEndpoint,
            StrVariant::from(common.name.clone()),
            ComponentDigest(content_digest.0),
        )?;
        let env_vars =
            resolve_env_vars_plaintext(self.env_vars, ignore_missing_env_vars, secret_registry)?;
        let (allowed_hosts, _advisories) =
            resolve_allowed_hosts(self.allowed_hosts, ignore_missing_env_vars, secret_registry)?;
        validate_no_env_collision(&env_vars, &allowed_hosts)?;
        let secrets = restricted_secret_registry(secret_registry, &allowed_hosts, None);
        Ok((
            common.name,
            WebhookWasmComponentConfigVerified {
                component_id,
                wasm_path,
                routes: self
                    .routes
                    .into_iter()
                    .map(WebhookRouteVerified::try_from)
                    .collect::<Result<Vec<_>, _>>()?,
                forward_stdout: self.forward_stdout.into_std_output_config(),
                forward_stderr: self.forward_stderr.into_std_output_config(),
                env_vars,
                frame_files_to_sources,
                backtrace_persist: self.backtrace_persist,
                subscription_interruption,
                logs_store_min_level: self.logs_store_min_level.into_log_level(),
                allowed_hosts,
                secrets,
                is_webui: self.is_webui,
                config_section_hint: ConfigSectionHint::WebhookEndpointWasm,
            },
        ))
    }
}

pub(crate) trait WebhookJsComponentConfigResolvedExt {
    async fn fetch_and_verify(
        self,
        wasm_path: Arc<Path>,
        wasm_cache_dir: Arc<Path>,
        ignore_missing_env_vars: bool,
        secret_registry: &Arc<SecretRegistry>,
    ) -> Result<(ConfigName, WebhookJsConfigVerified), anyhow::Error>;
}

impl WebhookJsComponentConfigResolvedExt for WebhookJsComponentConfigResolved {
    #[instrument(skip_all, fields(component_name = self.name.as_str()))]
    async fn fetch_and_verify(
        self,
        wasm_path: Arc<Path>,
        wasm_cache_dir: Arc<Path>,
        ignore_missing_env_vars: bool,
        secret_registry: &Arc<SecretRegistry>,
    ) -> Result<(ConfigName, WebhookJsConfigVerified), anyhow::Error> {
        let JsContent {
            entry_path: js_entry_path,
            files: js_files,
        } = self
            .location
            .get_content(&wasm_cache_dir, self.content_digest.as_ref())
            .await?;
        let mut hasher = Sha256::new();
        hasher.update(b"webhook_js:");
        hash_js_graph(&mut hasher, &js_entry_path, &js_files);
        let hash: [u8; 32] = hasher.finalize().into();
        let component_id = ComponentId::new(
            ComponentType::WebhookEndpoint,
            StrVariant::from(self.name.clone()),
            ComponentDigest(Digest(hash)),
        )?;
        let env_vars =
            resolve_env_vars_plaintext(self.env_vars, ignore_missing_env_vars, secret_registry)?;
        let (allowed_hosts, _advisories) =
            resolve_allowed_hosts(self.allowed_hosts, ignore_missing_env_vars, secret_registry)?;
        validate_no_env_collision(&env_vars, &allowed_hosts)?;
        let secrets = restricted_secret_registry(secret_registry, &allowed_hosts, None);
        Ok((
            self.name,
            WebhookJsConfigVerified {
                wasm_path,
                component_id,
                js_entry_path,
                js_files,
                routes: self
                    .routes
                    .into_iter()
                    .map(WebhookRouteVerified::try_from)
                    .collect::<Result<Vec<_>, _>>()?,
                forward_stdout: self.forward_stdout.into_std_output_config(),
                forward_stderr: self.forward_stderr.into_std_output_config(),
                env_vars,
                backtrace_persist: self.backtrace_persist,
                logs_store_min_level: self.logs_store_min_level.into_log_level(),
                allowed_hosts,
                secrets,
                config_section_hint: ConfigSectionHint::WebhookEndpointJs,
            },
        ))
    }
}

#[derive(Debug)]
pub(crate) struct CronConfigVerified {
    pub(crate) component_id: ComponentId,
    pub(crate) target_ffqn: FunctionFqn,
    pub(crate) params_json: Vec<serde_json::Value>,
    pub(crate) cron_schedule: CronOrOnce,
    pub(crate) exec_config: executor::executor::ExecConfig,
}

pub(crate) trait CronComponentConfigTomlExt {
    fn verify(self) -> Result<CronConfigVerified, anyhow::Error>;
}

impl CronComponentConfigTomlExt for CronComponentConfigToml {
    fn verify(self) -> Result<CronConfigVerified, anyhow::Error> {
        let name = self.name.to_string();
        let cron_schedule = if self.schedule == "@once" {
            CronOrOnce::Once
        } else {
            CronOrOnce::Cron(Box::new(
                croner::Cron::new(&self.schedule)
                    .with_seconds_optional()
                    .parse()
                    .with_context(|| {
                        format!(
                            "invalid cron expression `{}` for schedule `{name}`",
                            self.schedule
                        )
                    })?,
            ))
        };
        // Validate params JSON
        let serde_json::Value::Array(params_json) =
            serde_json::from_str::<serde_json::Value>(&self.params).with_context(|| {
                format!(
                    "invalid JSON params for schedule `{name}`: `{}`",
                    self.params
                )
            })?
        else {
            bail!("invalid params for schedule `{name}` - expected JSON array")
        };
        // Compute component digest from schedule config
        let mut hasher = Sha256::new();
        sha2::Digest::update(&mut hasher, name.as_bytes());
        sha2::Digest::update(&mut hasher, self.ffqn.to_string().as_bytes());
        sha2::Digest::update(&mut hasher, self.params.as_bytes());
        sha2::Digest::update(&mut hasher, self.schedule.as_bytes());
        let hash: [u8; 32] = sha2::Digest::finalize(hasher).into();
        let component_digest = ComponentDigest(Digest(hash));
        let component_id = ComponentId::new(
            ComponentType::Cron,
            StrVariant::from(name),
            component_digest,
        )?;
        let exec_config = self.exec.into_exec_exec_config(
            component_id.clone(),
            None, // no global instance limiter for crons
            ComponentRetryConfig::CRON,
        )?;
        Ok(CronConfigVerified {
            component_id,
            target_ffqn: self.ffqn,
            params_json,
            cron_schedule,
            exec_config,
        })
    }
}

// Runtime conversion helpers: turn TOML config into executor/worker runtime types, resolve
// env vars and allowed hosts against the secret registry, and build component secret resolvers.

fn locking_strategy_into_executor(value: LockingStrategy) -> executor::executor::LockingStrategy {
    match value {
        LockingStrategy::ByFfqns => executor::executor::LockingStrategy::ByFfqns,
        LockingStrategy::ByComponentDigest => {
            executor::executor::LockingStrategy::ByComponentDigest
        }
        LockingStrategy::Auto => executor::executor::LockingStrategy::Auto,
    }
}

pub(crate) trait ExecConfigTomlExt {
    fn into_exec_exec_config(
        self,
        component_id: ComponentId,
        task_limiter_global: Option<Arc<tokio::sync::Semaphore>>,
        retry_config: ComponentRetryConfig,
    ) -> Result<executor::executor::ExecConfig, anyhow::Error>;
}

impl ExecConfigTomlExt for ExecConfigToml {
    fn into_exec_exec_config(
        self,
        component_id: ComponentId,
        task_limiter_global: Option<Arc<tokio::sync::Semaphore>>,
        retry_config: ComponentRetryConfig,
    ) -> Result<executor::executor::ExecConfig, anyhow::Error> {
        Ok(executor::executor::ExecConfig {
            lock_expiry: self.lock_expiry.into(),
            tick_sleep: self.tick_sleep.into(),
            batch_size: self.batch_size,
            locking_strategy: locking_strategy(self.locking_strategy, component_id.component_type)?,
            component_id,
            task_limiter_global,
            task_limiter_local: self.instance_limiter.as_semaphore(),
            executor_id: ExecutorId::generate(),
            retry_config,
        })
    }
}

fn locking_strategy(
    locking_strategy_override: Option<LockingStrategy>,
    component_type: ComponentType,
) -> Result<executor::executor::LockingStrategy, anyhow::Error> {
    if component_type == ComponentType::Cron {
        ensure!(
            locking_strategy_override.is_none(),
            "locking strategy cannot be overridden for cron"
        );
        // needed for seed execution deduplication.
        return Ok(executor::executor::LockingStrategy::ByComponentDigest);
    }
    // Auto is only valid for workflows
    if component_type != ComponentType::Workflow
        && locking_strategy_override == Some(LockingStrategy::Auto)
    {
        bail!("Locking strategy `auto` is only available for workflows");
    }
    Ok(locking_strategy_override.map(locking_strategy_into_executor).unwrap_or_else(||
    match component_type {
        ComponentType::Activity => executor::executor::LockingStrategy::ByFfqns,
        ComponentType::Workflow => executor::executor::LockingStrategy::Auto,
        other => unreachable!(
            "unexpected type {other}, only workflows, activities, and crons expose locking strategy"
        ),
    }))
}

pub(crate) trait LogLevelTomlExt {
    fn into_log_level(self) -> Option<LogLevel>;
}
impl LogLevelTomlExt for LogLevelToml {
    fn into_log_level(self) -> Option<LogLevel> {
        match self {
            LogLevelToml::Off => None,
            LogLevelToml::Trace => Some(LogLevel::Trace),
            LogLevelToml::Debug => Some(LogLevel::Debug),
            LogLevelToml::Info => Some(LogLevel::Info),
            LogLevelToml::Warn => Some(LogLevel::Warn),
            LogLevelToml::Error => Some(LogLevel::Error),
        }
    }
}

pub(crate) trait BlockingStrategyConfigTomlExt {
    fn into_blocking_strategy(
        self,
        subscription_interruption: Option<Duration>,
    ) -> JoinNextBlockingStrategy;
}
impl BlockingStrategyConfigTomlExt for BlockingStrategyConfigToml {
    fn into_blocking_strategy(
        self,
        subscription_interruption: Option<Duration>,
    ) -> JoinNextBlockingStrategy {
        use crate::config::deployment::common::{
            BlockingStrategyAwaitConfig, BlockingStrategyConfigCustomized,
            BlockingStrategyConfigSimple,
        };
        match self {
            BlockingStrategyConfigToml::Tagged(BlockingStrategyConfigCustomized::Await(
                BlockingStrategyAwaitConfig {
                    non_blocking_event_batching,
                },
            )) => JoinNextBlockingStrategy::Await {
                non_blocking_event_batching,
                subscription_interruption,
            },
            BlockingStrategyConfigToml::Simple(BlockingStrategyConfigSimple::Interrupt) => {
                JoinNextBlockingStrategy::Interrupt
            }
            BlockingStrategyConfigToml::Simple(BlockingStrategyConfigSimple::Await) => {
                JoinNextBlockingStrategy::Await {
                    non_blocking_event_batching: DEFAULT_NON_BLOCKING_EVENT_BATCHING,
                    subscription_interruption,
                }
            }
        }
    }
}

pub(crate) trait ComponentStdOutputTomlExt {
    fn into_std_output_config(self) -> Option<StdOutputConfig>;
}
impl ComponentStdOutputTomlExt for ComponentStdOutputToml {
    fn into_std_output_config(self) -> Option<StdOutputConfig> {
        match self {
            ComponentStdOutputToml::None => None,
            ComponentStdOutputToml::Stdout => Some(StdOutputConfig::Stdout),
            ComponentStdOutputToml::Stderr => Some(StdOutputConfig::Stderr),
            ComponentStdOutputToml::Db => Some(StdOutputConfig::Db),
        }
    }
}

pub(crate) trait InflightSemaphoreExt {
    fn as_semaphore(&self) -> Option<Arc<tokio::sync::Semaphore>>;
}
impl InflightSemaphoreExt for InflightSemaphore {
    fn as_semaphore(&self) -> Option<Arc<tokio::sync::Semaphore>> {
        match self {
            InflightSemaphore::Unlimited(_) => None,
            InflightSemaphore::Some(permits) => Some(Arc::new(tokio::sync::Semaphore::new(
                usize::try_from(*permits).expect("usize >= u32"),
            ))),
        }
    }
}

// TODO: Move to env_var module
pub(crate) fn resolve_env_vars_plaintext(
    env_vars: Vec<EnvVarConfig>,
    ignore_missing: bool,
    secret_registry: &SecretRegistry,
) -> Result<Arc<[EnvVar]>, EnvVarError> {
    // A registered secret can never be forwarded to a guest as a plaintext env var, even
    // with `ignore_missing`: `EnvVarError::Secret` is always fatal, only `Missing` is skipped.
    let empty_if_missing = |key: String, err: EnvVarError| match err {
        EnvVarError::Missing(_) if ignore_missing => Ok(EnvVar {
            key,
            val: String::new(),
        }),
        other => Err(other),
    };
    env_vars
        .into_iter()
        .map(|env_var| match env_var {
            EnvVarConfig::KeyValue { key, value } => {
                match interpolate_env_vars_plaintext(&value, secret_registry) {
                    Ok(val) => Ok(EnvVar { key, val }),
                    Err(err) => empty_if_missing(key, err),
                }
            }
            EnvVarConfig::Key(key) => match secret_registry.public_env_lookup(&key) {
                Ok(Some(val)) => Ok(EnvVar { key, val }),
                Ok(None) => empty_if_missing(key.clone(), EnvVarError::Missing(key)),
                Err(violation) => Err(EnvVarError::Secret(violation)),
            },
        })
        .collect::<Result<_, _>>()
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ResolveAllowedHostsError {
    #[error(transparent)]
    HostPattern(#[from] HostPatternError),
    #[error(transparent)]
    EnvVarsMissing(#[from] EnvVarsMissing),
    #[error(transparent)]
    SecretViolation(#[from] SecretViolation),
    #[error("cannot parse HTTP method `{0}`")]
    InvalidMethod(String),
    #[error("use `methods = \"*\"` to allow all methods, not `methods = [\"*\"]`")]
    InvalidMethodStar,
    #[error("cannot parse request_url_regex `{pattern}`: {err}")]
    InvalidRequestUrlRegex { pattern: String, err: regex::Error },
}

/// An `allowed_host` entry that is valid but likely misconfigured. Carries the entry's TOML
/// fingerprint so a located reporter (`config_prepass`) can map it back to a source line.
/// Resolution collects these instead of logging them, so a single deduplicated, source-located
/// report is emitted once by the pre-pass rather than scattered across the resolvers.
#[derive(Debug)]
pub(crate) struct AllowedHostAdvisory {
    pub(crate) fingerprint: String,
    pub(crate) message: String,
}

/// The TOML serialization of an entry, used as a stable key to join a resolver advisory to the
/// `[[*.allowed_host]]` block it came from in the source file.
pub(crate) fn allowed_host_fingerprint(entry: &AllowedHostToml) -> String {
    toml::to_string(entry).expect("allowed host must serialize")
}

pub(crate) fn resolve_allowed_hosts(
    entries: Vec<AllowedHostToml>,
    ignore_missing_env_vars: bool,
    secret_registry: &SecretRegistry,
) -> Result<(Arc<[AllowedHostConfig]>, Vec<AllowedHostAdvisory>), ResolveAllowedHostsError> {
    let mut advisories = Vec::new();
    let hosts = entries
        .into_iter()
        .filter_map(|entry| {
            let fingerprint = allowed_host_fingerprint(&entry);
            let mut advise = |message: String| {
                advisories.push(AllowedHostAdvisory {
                    fingerprint: fingerprint.clone(),
                    message,
                });
            };
            // Convert MethodsInput to MethodsPattern
            let methods = match entry.methods {
                None => {
                    // Omitted methods: nothing allowed, warn and skip
                    advise(format!(
                        "allowed_host `{}` has no `methods` field - no requests will be allowed; \
                         use `methods = \"*\"` to allow all methods",
                        entry.pattern
                    ));
                    return None;
                }
                Some(MethodsInput::Star(_)) => {
                    // `methods = "*"` - all methods allowed
                    MethodsPattern::AllMethods
                }
                Some(MethodsInput::List(list)) => {
                    if list.is_empty() {
                        // Empty list: nothing allowed, warn and skip
                        advise(format!(
                            "allowed_host `{}` has empty `methods = []` - no requests will be allowed",
                            entry.pattern
                        ));
                        return None;
                    }
                    // Parse specific methods
                    match list
                        .into_iter()
                        .map(|m| {
                            http::Method::from_bytes(m.as_bytes()).map_err(|_| {
                                if m == "*" {
                                    ResolveAllowedHostsError::InvalidMethodStar
                                } else {
                                    ResolveAllowedHostsError::InvalidMethod(m)
                                }
                            })
                        })
                        .collect::<Result<Vec<_>, _>>()
                    {
                        Ok(methods) => MethodsPattern::Specific(methods),
                        Err(e) => return Some(Err(e)),
                    }
                }
            };

            let pattern_str = match interpolate_env_vars_plaintext(&entry.pattern, secret_registry) {
                Ok(s) => s,
                Err(EnvVarError::Missing(var)) => {
                    if ignore_missing_env_vars {
                        advise(format!(
                            "allowed_host pattern `{}` references missing env var `{var}`, skipping",
                            entry.pattern
                        ));
                        return None;
                    }
                    return Some(Err(ResolveAllowedHostsError::EnvVarsMissing(
                        EnvVarsMissing(vec![var]),
                    )));
                }
                Err(EnvVarError::Secret(violation)) => return Some(Err(violation.into())),
            };
            let request_url_regex = match entry.request_url_regex {
                Some(pattern) => {
                    let pattern = match interpolate_env_vars_plaintext(&pattern, secret_registry) {
                        Ok(s) => s,
                        Err(EnvVarError::Missing(var)) => {
                            if ignore_missing_env_vars {
                                advise(format!(
                                    "allowed_host request_url_regex `{pattern}` references missing env var `{var}`, skipping"
                                ));
                                return None;
                            }
                            return Some(Err(ResolveAllowedHostsError::EnvVarsMissing(
                                EnvVarsMissing(vec![var]),
                            )));
                        }
                        Err(EnvVarError::Secret(violation)) => {
                            return Some(Err(violation.into()));
                        }
                    };
                    match Regex::new(&pattern) {
                        Ok(regex) => Some(regex),
                        Err(err) => {
                            return Some(Err(ResolveAllowedHostsError::InvalidRequestUrlRegex {
                                pattern,
                                err,
                            }));
                        }
                    }
                }
                None => None,
            };
            let pattern = match HostPattern::parse_with_methods(&pattern_str, methods) {
                Ok(p) => p,
                Err(e) => return Some(Err(e.into())),
            };

            let (secret_names, replace_in) = if entry.secrets.is_empty() {
                if !entry.replace_in.is_empty() {
                    advise(format!(
                        "allowed_host `{}` has `replace_in` but no `secrets` - nothing to inject",
                        entry.pattern
                    ));
                }
                (Vec::new(), hashbrown::HashSet::new())
            } else {
                if entry.replace_in.is_empty() {
                    advise(format!(
                        "allowed_host `{}` has empty `replace_in` - secrets will never be injected",
                        entry.pattern
                    ));
                }
                if pattern.scheme.allows_unencrypted() {
                    advise(format!(
                        "secrets allowed for potentially unencrypted host `{pattern}`"
                    ));
                }

                let replace_in = entry
                    .replace_in
                    .into_iter()
                    .map(|r| match r {
                        ReplaceIn::Headers => ReplacementLocation::Headers,
                        ReplaceIn::Body => ReplacementLocation::Body,
                        ReplaceIn::Params => ReplacementLocation::Params,
                    })
                    .collect();
                // Carry only the declared names: values are resolved lazily per
                // execution run via the component's `RestrictedSecretRegistry`, never
                // baked into this verified config. An unregistered name is handled by
                // `config_prepass::preflight` (continue/bail/fix) and fails closed at
                // runtime when the resolver cannot supply it.
                (entry.secrets, replace_in)
            };

            Some(Ok(AllowedHostConfig {
                pattern,
                request_url_regex,
                secret_names,
                replace_in,
            }))
        })
        .collect::<Result<Arc<[AllowedHostConfig]>, _>>()?;
    Ok((hosts, advisories))
}

/// Build a component-scoped [`SecretResolver`] over the operator registry, limited
/// to the union of secret names the component declared across its `allowed_host`
/// entries plus `extra_names` (exec-activity `secrets`). Values are fetched through
/// this at execution time, never baked into the verified config.
fn restricted_secret_registry(
    secret_registry: &Arc<SecretRegistry>,
    allowed_hosts: &[AllowedHostConfig],
    extra_names: impl IntoIterator<Item = String>,
) -> Arc<dyn SecretResolver> {
    let names = allowed_hosts
        .iter()
        .flat_map(|h| h.secret_names.iter().cloned())
        .chain(extra_names);
    Arc::new(RestrictedSecretRegistry::new(
        secret_registry.clone(),
        names,
    ))
}

fn validate_no_env_collision(
    env_vars: &[EnvVar],
    allowed_hosts: &[AllowedHostConfig],
) -> Result<(), anyhow::Error> {
    let env_var_keys: hashbrown::HashSet<_> = env_vars.iter().map(|e| e.key.as_str()).collect();
    for host in allowed_hosts {
        for key in &host.secret_names {
            ensure!(
                !env_var_keys.contains(key.as_str()),
                "secret env var `{key}` collides with an `env_vars` entry"
            );
        }
    }
    Ok(())
}

// Resolved component shapes: the intermediate form produced by resolving a validated manifest
// against the CAS (deployment-owned scripts/backtrace sources inlined as content), before the
// runtime `*ConfigVerified` fetch/verify step above. `DeploymentResolved` is the aggregate.

#[derive(Debug, Clone)]
pub struct ActivityStubExtInlineConfigResolved {
    pub name: ConfigName,
    pub ffqn: FunctionFqn,
    pub interface: FunctionInterfaceResolved,
}

/// Parsed authored WIT belonging to a deployment component.
#[derive(Debug, Clone)]
pub struct WitSourceResolved {
    pub root: String,
    pub resolve: wit_parser::Resolve,
    pub main_pkg_id: wit_parser::PackageId,
}

#[derive(Debug, Clone)]
pub struct InlineFunctionInterfaceResolved {
    pub params: Option<Vec<JsParamToml>>,
    pub return_type: Option<String>,
}

#[derive(Debug, Clone)]
pub enum FunctionInterfaceResolved {
    Authored { wit: Box<WitSourceResolved> },
    Inline(InlineFunctionInterfaceResolved),
}

#[derive(Debug, Clone)]
pub enum ActivityStubComponentConfigResolved {
    File(ActivityStubFileConfigToml),
    Inline(ActivityStubExtInlineConfigResolved),
}

impl ActivityStubComponentConfigResolved {
    #[must_use]
    pub fn name_str(&self) -> &str {
        match self {
            Self::File(f) => f.common.name.as_str(),
            Self::Inline(i) => i.name.as_str(),
        }
    }
}

#[derive(Debug, Clone)]
pub enum ActivityExternalComponentConfigResolved {
    File(ActivityExternalFileConfigToml),
    Inline(ActivityStubExtInlineConfigResolved),
}

impl ActivityExternalComponentConfigResolved {
    #[must_use]
    pub fn name_str(&self) -> &str {
        match self {
            Self::File(f) => f.common.name.as_str(),
            Self::Inline(i) => i.name.as_str(),
        }
    }
}

/// Resolved location of a script source (JS or exec) after file-provider resolution.
///
/// - `Content` is owned by the deployment (inline content, or a file that lived under
///   the deployment directory and was read from disk/CAS); `file_name` is the
///   deployment-relative path (which may include subfolders), used for source names and
///   backtraces.
/// - `Oci` is an external registry reference.
#[derive(Debug, Clone, Hash)]
pub enum ScriptLocationResolved {
    Content {
        content: String,
        file_name: String,
    },
    Graph {
        entry_path: String,
        files: Vec<(String, String)>,
    },
    /// OCI-sourced script. No `oci://` prefix.
    Oci {
        image: oci_client::Reference,
    },
}

/// Resolved backtrace source: a CAS reference (`content_digest`) to the source bytes,
/// which are uploaded to the CAS with the deployment files, plus the deployment-relative
/// `file_name` it was read from (used to detect owned-source name collisions).
#[derive(Debug, Clone)]
pub struct BacktraceSourceResolved {
    pub content_digest: ContentDigest,
    pub file_name: String,
}

#[derive(Debug, Default, Clone)]
pub struct ComponentBacktraceConfigResolved {
    pub frame_files_to_sources: HashMap<String, BacktraceSourceResolved>,
}

impl ComponentBacktraceConfigResolved {
    /// Map each frame-symbol key to the CAS digest of its source (dropping the recreate
    /// path), as needed to persist the runtime backtrace-source lookup.
    #[must_use]
    pub fn into_frame_files(self) -> HashMap<String, ContentDigest> {
        self.frame_files_to_sources
            .into_iter()
            .map(|(k, v)| (k, v.content_digest))
            .collect()
    }
}

/// Resolved form of `ActivityJsComponentConfigToml`.
#[derive(Debug, Clone)]
pub struct ActivityJsComponentConfigResolved {
    pub name: ConfigName,
    pub location: ScriptLocationResolved,
    pub content_digest: Option<ContentDigest>,
    pub component_digest: Option<ComponentDigest>,
    pub ffqn: FunctionFqn,
    pub interface: FunctionInterfaceResolved,
    pub exec: ExecConfigToml,
    pub max_retries: u32,
    pub retry_exp_backoff: DurationConfig,
    pub forward_stdout: ComponentStdOutputToml,
    pub forward_stderr: ComponentStdOutputToml,
    pub logs_store_min_level: LogLevelToml,
    pub env_vars: Vec<EnvVarConfig>,
    pub allowed_hosts: Vec<AllowedHostToml>,
}

/// Resolved form of `ActivityExecComponentConfigToml`.
#[derive(Debug, Clone)]
pub struct ActivityExecComponentConfigResolved {
    pub name: ConfigName,
    pub location: ScriptLocationResolved,
    pub content_digest: Option<ContentDigest>,
    pub ffqn: FunctionFqn,
    pub interface: FunctionInterfaceResolved,
    pub component_digest: Option<ComponentDigest>,
    pub exec: ExecConfigToml,
    pub max_retries: u32,
    pub retry_exp_backoff: DurationConfig,
    pub forward_stdout: ComponentStdOutputToml,
    pub forward_stderr: ComponentStdOutputToml,
    pub logs_store_min_level: LogLevelToml,
    pub env_vars: Vec<EnvVarConfig>,
    pub max_output_bytes: u64,
    /// Registered secret names (from the operator-owned `server.toml` `[secrets]`
    /// table) to expose to the script in the stdin JSON `secrets` object.
    pub secrets: Vec<String>,
    pub params_via_stdin: bool,
}

/// Resolved form of `WorkflowWasmComponentConfigToml`.
#[derive(Debug, Clone)]
pub struct WorkflowWasmComponentConfigResolved {
    pub common: ComponentCommon,
    pub content_digest: Option<ContentDigest>,
    pub component_digest: Option<ComponentDigest>,
    pub exec: ExecConfigToml,
    pub retry_exp_backoff: DurationConfig,
    pub blocking_strategy: BlockingStrategyConfigToml,
    pub backtrace: ComponentBacktraceConfigResolved,
    pub stub_wasi: bool,
    pub lock_extension: bool,
    pub lock_extension_leeway: DurationConfig,
    pub logs_store_min_level: LogLevelToml,
}

/// Resolved form of `WorkflowJsComponentConfigToml`.
#[derive(Debug, Clone)]
pub struct WorkflowJsComponentConfigResolved {
    pub name: ConfigName,
    pub location: ScriptLocationResolved,
    pub content_digest: Option<ContentDigest>,
    pub component_digest: Option<ComponentDigest>,
    pub ffqn: FunctionFqn,
    pub interface: FunctionInterfaceResolved,
    pub exec: ExecConfigToml,
    pub retry_exp_backoff: DurationConfig,
    pub blocking_strategy: BlockingStrategyConfigToml,
    pub logs_store_min_level: LogLevelToml,
    pub lock_extension: bool,
    pub lock_extension_leeway: DurationConfig,
}

/// Resolved form of `WebhookWasmComponentConfigToml`.
#[derive(Debug, Clone)]
pub struct WebhookWasmComponentConfigResolved {
    pub common: ComponentCommon,
    pub content_digest: Option<ContentDigest>,
    pub http_server: ConfigName,
    pub routes: Vec<WebhookRoute>,
    pub forward_stdout: ComponentStdOutputToml,
    pub forward_stderr: ComponentStdOutputToml,
    pub env_vars: Vec<EnvVarConfig>,
    pub backtrace: ComponentBacktraceConfigResolved,
    pub backtrace_persist: bool,
    pub logs_store_min_level: LogLevelToml,
    pub allowed_hosts: Vec<AllowedHostToml>,
    pub is_webui: bool,
}

/// Resolved form of `WebhookJsComponentConfigToml`.
#[derive(Debug, Clone)]
pub struct WebhookJsComponentConfigResolved {
    pub name: ConfigName,
    pub location: ScriptLocationResolved,
    pub content_digest: Option<ContentDigest>,
    pub http_server: ConfigName,
    pub routes: Vec<WebhookRoute>,
    pub forward_stdout: ComponentStdOutputToml,
    pub forward_stderr: ComponentStdOutputToml,
    pub logs_store_min_level: LogLevelToml,
    pub env_vars: Vec<EnvVarConfig>,
    pub backtrace_persist: bool,
    pub allowed_hosts: Vec<AllowedHostToml>,
}

/// Resolved deployment configuration after resolving deployment-owned text sources.
///
/// This is a transient runtime/verification shape used by the local server and
/// `obelisk deployment verify`. It is derived from the stored manifest plus a file provider,
/// never serialized, and rebuilt on each server from the stored manifest and the CAS.
/// Deployment-owned scripts and backtrace sources are inlined as content;
/// deployment-owned WASM locations remain relative path + content digest until
/// `DeploymentRunnable` materializes them from the CAS into a runnable cache path. OCI
/// references remain external references.
#[derive(Debug, Default, Clone)]
pub struct DeploymentResolved {
    /// Path of the deployment manifest this configuration was loaded from, when available.
    pub source_path: Option<std::path::PathBuf>,
    pub activities_wasm: Vec<ActivityWasmComponentConfigToml>,
    pub activities_stub: Vec<ActivityStubComponentConfigResolved>,
    pub activities_external: Vec<ActivityExternalComponentConfigResolved>,
    pub activities_js: Vec<ActivityJsComponentConfigResolved>,
    pub activities_exec: Vec<ActivityExecComponentConfigResolved>,
    pub workflows_wasm: Vec<WorkflowWasmComponentConfigResolved>,
    pub workflows_js: Vec<WorkflowJsComponentConfigResolved>,
    pub webhooks_wasm: Vec<WebhookWasmComponentConfigResolved>,
    pub webhooks_js: Vec<WebhookJsComponentConfigResolved>,
    pub crons: Vec<CronComponentConfigToml>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cron_verification_accepts_optional_seconds_field() {
        for schedule in ["*/10 * * * *", "*/10 * * * * *"] {
            let config: CronComponentConfigToml = toml::from_str(&format!(
                r#"
                name = "frequent"
                ffqn = "testing:integration/activity.run"
                schedule = "{schedule}"
                "#
            ))
            .unwrap();

            assert!(matches!(
                config.verify().unwrap().cron_schedule,
                CronOrOnce::Cron(_)
            ));
        }
    }
}
