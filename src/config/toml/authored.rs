//! User-authored deployment manifest: the `deployment.toml` shapes as written by hand,
//! plus name/path validation into `DeploymentTomlValidated`. Content digests and
//! `component_files` are optional here; the processing pass ([`super::resolve`]) fills
//! and verifies them.

use super::*;

#[derive(Deserialize, Serialize, JsonSchema, Default, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct DeploymentToml {
    #[serde(default, rename = "activity_wasm")]
    pub(crate) activities_wasm: Vec<ActivityWasmComponentConfigToml>,
    #[serde(default, rename = "activity_stub")]
    pub(crate) activities_stub: Vec<ActivityStubComponentConfigToml>,
    #[serde(default, rename = "activity_external")]
    pub(crate) activities_external: Vec<ActivityExternalComponentConfigToml>,
    #[serde(default, rename = "activity_js")]
    pub(crate) activities_js: Vec<ActivityJsComponentConfigToml>,
    #[serde(default, rename = "activity_exec")]
    pub(crate) activities_exec: Vec<ActivityExecComponentConfigToml>,
    #[serde(default, rename = "workflow_wasm")]
    pub(crate) workflows_wasm: Vec<WorkflowWasmComponentConfigToml>,
    #[serde(default, rename = "workflow_js")]
    pub(crate) workflows_js: Vec<WorkflowJsComponentConfigToml>,
    #[serde(default, rename = "webhook_endpoint_wasm")]
    pub(crate) webhooks_wasm: Vec<WebhookWasmComponentConfigToml>,
    #[serde(default, rename = "webhook_endpoint_js")]
    pub(crate) webhooks_js: Vec<WebhookJsComponentConfigToml>,
    #[serde(default, rename = "cron")]
    pub(crate) crons: Vec<CronComponentConfigToml>,
}

/// A `DeploymentToml` that has passed name-uniqueness validation.
///
/// Components that support auto-derived names (`activity_js`, `activity_exec`,
/// `workflow_js`) are stored as `(Config, ConfigName)` tuples with the resolved name
/// pulled out of the `Option`.
#[derive(Default)]
pub(crate) struct DeploymentTomlValidated {
    pub(crate) activities_exec: Vec<(ActivityExecComponentConfigToml, ConfigName)>,
    pub(crate) activities_external: Vec<(ActivityExternalComponentConfigToml, ConfigName)>,
    pub(crate) activities_js: Vec<(ActivityJsComponentConfigToml, ConfigName)>,
    pub(crate) activities_stub: Vec<(ActivityStubComponentConfigToml, ConfigName)>,
    pub(crate) activities_wasm: Vec<ActivityWasmComponentConfigToml>,

    pub(crate) workflows_js: Vec<(WorkflowJsComponentConfigToml, ConfigName)>,
    pub(crate) workflows_wasm: Vec<WorkflowWasmComponentConfigToml>,

    pub(crate) webhooks_js: Vec<WebhookJsComponentConfigToml>,
    pub(crate) webhooks_wasm: Vec<WebhookWasmComponentConfigToml>,

    pub(crate) crons: Vec<CronComponentConfigToml>,

    pub(crate) component_names_to_types: hashbrown::HashMap<String, crate::args::TomlComponentType>,
}

impl DeploymentTomlValidated {
    /// Resolve every deployment-owned reference by reading its blob from `cas` by digest.
    ///
    /// The manifest must be processed (every deployment-owned reference carries a digest in
    /// `content_digest` / `component_files`); resolution never touches the submitter's disk.
    pub(crate) async fn resolve(self, cas: &dyn Cas) -> Result<DeploymentResolved, anyhow::Error> {
        resolve_local_refs(self, cas).await
    }
}

impl DeploymentToml {
    // backcompat: Delete ${DEPLOYMENT_DIR} in 0.42
    /// Expand `${DEPLOYMENT_DIR}/` prefixes in WASM component paths,
    /// verify that every component name is unique, and return a `DeploymentTomlValidated`
    /// that also carries the name→type index and the deployment directory.
    pub(crate) fn validate(
        mut self,
        deployment_dir: &std::path::Path,
    ) -> Result<DeploymentTomlValidated, anyhow::Error> {
        self.expand_deployment_dir_prefix(deployment_dir)?;
        self.normalize_oci_locations()?;
        self.validate_wit_sources()?;

        // Build the name→type index and check for duplicates.
        let mut component_names_to_types = hashbrown::HashMap::new();
        // Add components with mandatory names
        let iter = self
            .activities_wasm
            .iter()
            .map(|c| (c.common.name.as_str(), TomlComponentType::ActivityWasm))
            .chain(
                self.workflows_wasm
                    .iter()
                    .map(|c| (c.common.name.as_str(), TomlComponentType::WorkflowWasm)),
            )
            .chain(self.webhooks_wasm.iter().map(|c| {
                (
                    c.common.name.as_str(),
                    TomlComponentType::WebhookEndpointWasm,
                )
            }))
            .chain(
                self.webhooks_js
                    .iter()
                    .map(|c| (c.name.as_str(), TomlComponentType::WebhookEndpointJs)),
            )
            .chain(
                self.crons
                    .iter()
                    .map(|c| (c.name.as_str(), TomlComponentType::Cron)),
            );

        for (name, component_type) in iter {
            if component_names_to_types
                .insert(name.to_string(), component_type)
                .is_some()
            {
                bail!("duplicate component name `{name}` in deployment");
            }
        }

        let activities_js = Self::resolve_names(self.activities_js);
        let activities_exec = Self::resolve_names(self.activities_exec);
        let activities_stub = Self::resolve_stub_names(self.activities_stub);
        let activities_external = Self::resolve_external_names(self.activities_external);
        let workflows_js = Self::resolve_names(self.workflows_js);

        // Add components with optional names (now resolved)

        for (_, name) in &activities_js {
            if component_names_to_types
                .insert(name.to_string(), TomlComponentType::ActivityJs)
                .is_some()
            {
                bail!("duplicate component name `{name}` in deployment");
            }
        }
        for (_, name) in &activities_exec {
            if component_names_to_types
                .insert(name.to_string(), TomlComponentType::ActivityExec)
                .is_some()
            {
                bail!("duplicate component name `{name}` in deployment");
            }
        }
        for (_, name) in &activities_stub {
            if component_names_to_types
                .insert(name.to_string(), TomlComponentType::ActivityStub)
                .is_some()
            {
                bail!("duplicate component name `{name}` in deployment");
            }
        }
        for (_, name) in &activities_external {
            if component_names_to_types
                .insert(name.to_string(), TomlComponentType::ActivityExternal)
                .is_some()
            {
                bail!("duplicate component name `{name}` in deployment");
            }
        }
        for (_, name) in &workflows_js {
            if component_names_to_types
                .insert(name.to_string(), TomlComponentType::WorkflowJs)
                .is_some()
            {
                bail!("duplicate component name `{name}` in deployment");
            }
        }
        Ok(DeploymentTomlValidated {
            activities_exec,
            activities_external,
            activities_js,
            activities_stub,
            activities_wasm: self.activities_wasm,

            workflows_js,
            workflows_wasm: self.workflows_wasm,

            webhooks_js: self.webhooks_js,
            webhooks_wasm: self.webhooks_wasm,

            crons: self.crons,

            component_names_to_types,
        })
    }

    fn validate_wit_sources(&self) -> anyhow::Result<()> {
        fn validate(section: &str, interface: &FunctionInterfaceToml) -> anyhow::Result<()> {
            if let FunctionInterfaceToml::Authored(AuthoredFunctionInterfaceToml { wit }) =
                interface
            {
                sanitize_deployment_relative_path(wit)
                    .with_context(|| format!("invalid `{section}.wit`"))?;
            }
            Ok(())
        }

        for config in &self.activities_js {
            validate("activity_js", &config.interface)?;
        }
        for config in &self.activities_exec {
            validate("activity_exec", &config.interface)?;
        }
        for config in &self.workflows_js {
            validate("workflow_js", &config.interface)?;
        }
        for config in &self.activities_stub {
            if let ActivityStubComponentConfigToml::Inline(inline) = config {
                validate("activity_stub", &inline.interface)?;
            }
        }
        for config in &self.activities_external {
            if let ActivityExternalComponentConfigToml::Inline(inline) = config {
                validate("activity_external", &inline.interface)?;
            }
        }
        Ok(())
    }

    // Resolve optional names from FFQN.
    fn resolve_names<T: HasOptionalNameAndFfqn>(configs: Vec<T>) -> Vec<(T, ConfigName)> {
        configs
            .into_iter()
            .map(|c| {
                let name = c
                    .config_name()
                    .cloned()
                    .unwrap_or_else(|| ConfigName::from_ffqn(c.ffqn()));
                (c, name)
            })
            .collect()
    }

    /// Resolve names for `ActivityStubComponentConfigToml` enum variants.
    /// File variants always have an explicit name; Inline variants may derive from FFQN.
    fn resolve_stub_names(
        configs: Vec<ActivityStubComponentConfigToml>,
    ) -> Vec<(ActivityStubComponentConfigToml, ConfigName)> {
        configs
            .into_iter()
            .map(|c| {
                let name = match &c {
                    ActivityStubComponentConfigToml::File(f) => f.common.name.clone(),
                    ActivityStubComponentConfigToml::Inline(i) => i
                        .name
                        .clone()
                        .unwrap_or_else(|| ConfigName::from_ffqn(&i.ffqn)),
                };
                (c, name)
            })
            .collect()
    }

    /// Resolve names for `ActivityExternalComponentConfigToml` enum variants.
    fn resolve_external_names(
        configs: Vec<ActivityExternalComponentConfigToml>,
    ) -> Vec<(ActivityExternalComponentConfigToml, ConfigName)> {
        configs
            .into_iter()
            .map(|c| {
                let name = match &c {
                    ActivityExternalComponentConfigToml::File(f) => f.common.name.clone(),
                    ActivityExternalComponentConfigToml::Inline(i) => i
                        .name
                        .clone()
                        .unwrap_or_else(|| ConfigName::from_ffqn(&i.ffqn)),
                };
                (c, name)
            })
            .collect()
    }

    /// Resolve a WASM component file path to an absolute path. A `${DEPLOYMENT_DIR}/<suffix>`
    /// path and a bare relative path are both anchored to the deployment directory and must
    /// stay within it (no `..` escape); authored absolute paths are rejected. This makes every
    /// path in a deployment.toml deployment-relative.
    pub(crate) fn expand_deployment_dir(
        s: &mut String,
        deployment_dir: &std::path::Path,
    ) -> anyhow::Result<()> {
        // A `${DEPLOYMENT_DIR}/x` path and a bare relative `x` are equivalent.
        let candidate = strip_deployment_dir_prefix(s).unwrap_or(s.as_str());
        if std::path::Path::new(candidate).is_absolute() {
            bail!("absolute local paths are not allowed in deployment manifests: `{s}`");
        }
        let rel = sanitize_deployment_relative_path(candidate)
            .with_context(|| format!("invalid deployment-relative path `{s}`"))?;
        *s = deployment_dir.join(rel).to_string_lossy().into_owned();
        Ok(())
    }

    /// Validate and normalize OCI references of WASM components so that the resolved
    /// form matches the previous `oci_client::Reference`-based serialization.
    fn normalize_oci_locations(&mut self) -> Result<(), anyhow::Error> {
        fn normalize(loc: &mut ComponentLocationToml) -> Result<(), anyhow::Error> {
            if let ComponentLocationToml::Oci(image) = loc {
                let reference = oci_client::Reference::from_str(image)
                    .map_err(|e| anyhow!("invalid OCI reference `{image}`: {e}"))?;
                *image = reference.to_string();
            }
            Ok(())
        }
        for c in &mut self.activities_wasm {
            normalize(&mut c.common.location)?;
        }
        for c in &mut self.activities_stub {
            if let ActivityStubComponentConfigToml::File(c) = c {
                normalize(&mut c.common.location)?;
            }
        }
        for c in &mut self.activities_external {
            if let ActivityExternalComponentConfigToml::File(c) = c {
                normalize(&mut c.common.location)?;
            }
        }
        for c in &mut self.workflows_wasm {
            normalize(&mut c.common.location)?;
        }
        for c in &mut self.webhooks_wasm {
            normalize(&mut c.common.location)?;
        }
        Ok(())
    }

    /// Expand `${DEPLOYMENT_DIR}` prefixes in WASM component paths (which are read lazily
    /// at runtime and therefore must be absolute in the resolved form), rejecting `..`
    /// escapes.
    fn expand_deployment_dir_prefix(
        &mut self,
        deployment_dir: &std::path::Path,
    ) -> anyhow::Result<()> {
        fn expand_loc(
            loc: &mut ComponentLocationToml,
            deployment_dir: &std::path::Path,
        ) -> anyhow::Result<()> {
            if let ComponentLocationToml::Path(p) = loc {
                DeploymentToml::expand_deployment_dir(p, deployment_dir)?;
            }
            Ok(())
        }
        for c in &mut self.activities_wasm {
            expand_loc(&mut c.common.location, deployment_dir)?;
        }
        for c in &mut self.activities_stub {
            if let ActivityStubComponentConfigToml::File(c) = c {
                expand_loc(&mut c.common.location, deployment_dir)?;
            }
        }
        for c in &mut self.activities_external {
            if let ActivityExternalComponentConfigToml::File(c) = c {
                expand_loc(&mut c.common.location, deployment_dir)?;
            }
        }
        // Script (JS/exec) locations and backtrace sources are NOT expanded here. Their
        // `${DEPLOYMENT_DIR}` prefix is handled when resolving deployment-owned refs
        // (`resolve_script_toml` / `resolve_backtrace`), so
        // deployment-owned files preserve deployment-relative names.
        for c in &mut self.workflows_wasm {
            expand_loc(&mut c.common.location, deployment_dir)?;
        }
        for c in &mut self.webhooks_wasm {
            expand_loc(&mut c.common.location, deployment_dir)?;
        }
        Ok(())
    }
}

/// Trait for config structs that have an optional `name` and a required `ffqn`.
trait HasOptionalNameAndFfqn {
    fn config_name(&self) -> Option<&ConfigName>;
    fn ffqn(&self) -> &FunctionFqn;
}

impl HasOptionalNameAndFfqn for ActivityJsComponentConfigToml {
    fn config_name(&self) -> Option<&ConfigName> {
        self.name.as_ref()
    }
    fn ffqn(&self) -> &FunctionFqn {
        &self.ffqn
    }
}

impl HasOptionalNameAndFfqn for ActivityExecComponentConfigToml {
    fn config_name(&self) -> Option<&ConfigName> {
        self.name.as_ref()
    }
    fn ffqn(&self) -> &FunctionFqn {
        &self.ffqn
    }
}

impl HasOptionalNameAndFfqn for WorkflowJsComponentConfigToml {
    fn config_name(&self) -> Option<&ConfigName> {
        self.name.as_ref()
    }
    fn ffqn(&self) -> &FunctionFqn {
        &self.ffqn
    }
}

impl HasOptionalNameAndFfqn for ActivityStubExtInlineConfigToml {
    fn config_name(&self) -> Option<&ConfigName> {
        self.name.as_ref()
    }
    fn ffqn(&self) -> &FunctionFqn {
        &self.ffqn
    }
}

/// Location of a JavaScript source file.
/// Supports local file paths and OCI registry references (`oci://...`).
/// On-disk format only; replaced by [`ScriptLocationResolved`] before hash computation.
#[derive(Debug, Clone, Hash, JsonSchema, SerializeDisplay, DeserializeFromStr)]
#[schemars(with = "String")]
pub(crate) enum ScriptLocationPathOrOci {
    Path(String),
    Oci(oci_client::Reference),
}

impl Display for ScriptLocationPathOrOci {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ScriptLocationPathOrOci::Path(p) => write!(f, "{p}"),
            ScriptLocationPathOrOci::Oci(r) => write!(f, "{OCI_SCHEMA_PREFIX}{r}"),
        }
    }
}

impl FromStr for ScriptLocationPathOrOci {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some(location) = s.strip_prefix(OCI_SCHEMA_PREFIX) {
            Ok(ScriptLocationPathOrOci::Oci(
                oci_client::Reference::from_str(location)
                    .map_err(|e| anyhow::anyhow!("invalid OCI reference: {e}"))?,
            ))
        } else {
            Ok(ScriptLocationPathOrOci::Path(s.to_string()))
        }
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
pub(crate) struct ActivityStubExtInlineConfigToml {
    /// Component name. Optional — defaults to `{ifc_name}.{function_name}` from `ffqn`.
    #[serde(default)]
    pub(crate) name: Option<ConfigName>,
    #[schemars(with = "String")]
    pub(crate) ffqn: FunctionFqn,
    /// Generated CAS references for the parser-selected WIT files.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    #[schemars(skip)]
    pub(crate) component_files: BTreeMap<String, ContentDigest>,
    #[serde(flatten)]
    pub(crate) interface: FunctionInterfaceToml,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct AuthoredFunctionInterfaceToml {
    /// Authored WIT directory containing the exported `ffqn`.
    pub(crate) wit: String,
}

#[derive(Debug, Default, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct InlineFunctionInterfaceToml {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) params: Option<Vec<JsParamToml>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) return_type: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(untagged)]
pub(crate) enum FunctionInterfaceToml {
    Authored(AuthoredFunctionInterfaceToml),
    Inline(InlineFunctionInterfaceToml),
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(untagged)]
pub(crate) enum ActivityStubComponentConfigToml {
    File(ActivityStubFileConfigToml),
    Inline(ActivityStubExtInlineConfigToml),
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(untagged)]
pub(crate) enum ActivityExternalComponentConfigToml {
    File(ActivityExternalFileConfigToml),
    Inline(ActivityStubExtInlineConfigToml),
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
pub(crate) struct ActivityJsComponentConfigToml {
    /// Component name. Optional when `ffqn` is specified — defaults to `{ifc_name}.{function_name}`.
    #[serde(default)]
    pub(crate) name: Option<ConfigName>,
    /// Location of the JavaScript source file.
    /// Supports local file paths and OCI registry references (`oci://...`).
    #[serde(default)]
    pub(crate) location: Option<ScriptLocationPathOrOci>,
    /// Inline JavaScript source embedded in the TOML.
    /// Exactly one of `location` or `content` must be set.
    #[serde(default)]
    pub(crate) content: Option<String>,
    /// Content digest of the JS source file.
    #[serde(default)]
    #[schemars(with = "Option<String>")]
    pub(crate) content_digest: Option<ContentDigest>,
    /// CAS references for the closed module graph, populated during deployment preparation.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    #[schemars(skip)]
    pub(crate) component_files: BTreeMap<String, ContentDigest>,
    /// Deprecated override of the auto-computed component digest used for locking.
    /// This option will be removed in 0.42.
    #[serde(default)]
    #[schemars(with = "Option<String>")]
    pub(crate) component_digest: Option<ComponentDigest>,
    #[schemars(with = "String")]
    pub(crate) ffqn: FunctionFqn,
    /// Custom parameters for the JS function.
    /// Each entry has a `name` and a WIT `type` (e.g. `string`, `u32`, `list<string>`).
    /// Defaults to no parameters.
    /// The synthesized return type must be `result<T, string>`.
    #[serde(flatten)]
    pub(crate) interface: FunctionInterfaceToml,
    #[serde(default)]
    pub(crate) exec: ExecConfigToml,
    #[serde(default = "default_max_retries")]
    pub(crate) max_retries: u32,
    #[serde(default = "default_retry_exp_backoff")]
    pub(crate) retry_exp_backoff: DurationConfig,
    #[serde(default)]
    pub(crate) forward_stdout: ComponentStdOutputToml,
    #[serde(default)]
    pub(crate) forward_stderr: ComponentStdOutputToml,
    #[serde(default)]
    pub(crate) logs_store_min_level: LogLevelToml,
    #[serde(default)]
    pub(crate) env_vars: Vec<EnvVarConfig>,
    /// Allowed outgoing HTTP hosts with optional method restrictions and secrets.
    #[serde(default, rename = "allowed_host")]
    pub(crate) allowed_hosts: Vec<AllowedHostToml>,
}

// --- activity_exec config ---

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
pub(crate) struct ActivityExecComponentConfigToml {
    /// Component name. Optional when `ffqn` is specified — defaults to `{ifc_name}.{function_name}`.
    #[serde(default)]
    pub(crate) name: Option<ConfigName>,
    /// Location of the exec script.
    /// Supports local file paths and OCI registry references (`oci://...`).
    #[serde(default)]
    pub(crate) location: Option<ScriptLocationPathOrOci>,
    /// Inline script content embedded in the TOML.
    /// Exactly one of `location` or `content` must be set.
    #[serde(default)]
    pub(crate) content: Option<String>,
    /// Content digest of the exec script.
    #[serde(default)]
    #[schemars(with = "Option<String>")]
    pub(crate) content_digest: Option<ContentDigest>,
    /// Generated CAS references for parser-selected WIT files.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    #[schemars(skip)]
    pub(crate) component_files: BTreeMap<String, ContentDigest>,
    #[schemars(with = "String")]
    pub(crate) ffqn: FunctionFqn,
    /// Custom parameters for the exec activity.
    /// Each entry has a `name` and a WIT `type` (e.g. `string`, `u32`, `list<string>`).
    #[serde(flatten)]
    pub(crate) interface: FunctionInterfaceToml,
    /// Deprecated override of the auto-computed component digest used for locking.
    /// This option will be removed in 0.42.
    #[serde(default)]
    #[schemars(with = "Option<String>")]
    pub(crate) component_digest: Option<ComponentDigest>,
    #[serde(default)]
    pub(crate) exec: ExecConfigToml,
    #[serde(default = "default_max_retries")]
    pub(crate) max_retries: u32,
    #[serde(default = "default_retry_exp_backoff")]
    pub(crate) retry_exp_backoff: DurationConfig,
    #[serde(default)]
    pub(crate) forward_stdout: ComponentStdOutputToml,
    #[serde(default)]
    pub(crate) forward_stderr: ComponentStdOutputToml,
    #[serde(default)]
    pub(crate) logs_store_min_level: LogLevelToml,
    #[serde(default)]
    pub(crate) env_vars: Vec<EnvVarConfig>,
    /// Maximum bytes collected from stdout to form the response.
    /// Exceeding the limit fails the execution.
    /// Not used when `return_type` is result (default), since the response carries no data.
    #[serde(default = "default_max_output_bytes")]
    pub(crate) max_output_bytes: u64,
    /// Registered secret names (from the operator-owned `server.toml` `[secrets]`
    /// table) to expose to the script in the stdin JSON `secrets` object.
    #[serde(default)]
    pub(crate) secrets: Vec<String>,
    /// Pass parameters to the program via the stdin JSON `params` array instead
    /// of argv. Use this for large payloads that would exceed the `execve` argument-size
    /// limit. Defaults to `false` (parameters passed as command-line arguments).
    #[serde(default)]
    pub(crate) params_via_stdin: bool,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
pub(crate) struct WorkflowJsComponentConfigToml {
    /// Component name. Optional when `ffqn` is specified — defaults to `{ifc_name}.{function_name}`.
    #[serde(default)]
    pub(crate) name: Option<ConfigName>,
    /// Location of the JavaScript source file.
    /// Supports local file paths and OCI registry references (`oci://...`).
    #[serde(default)]
    pub(crate) location: Option<ScriptLocationPathOrOci>,
    /// Inline JavaScript source embedded in the TOML.
    /// Exactly one of `location` or `content` must be set.
    #[serde(default)]
    pub(crate) content: Option<String>,
    /// Content digest of the JS source file.
    #[serde(default)]
    #[schemars(with = "Option<String>")]
    pub(crate) content_digest: Option<ContentDigest>,
    /// CAS references for the closed module graph, populated during deployment preparation.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    #[schemars(skip)]
    pub(crate) component_files: BTreeMap<String, ContentDigest>,
    /// Deprecated override of the auto-computed component digest used for locking.
    /// This option will be removed in 0.42.
    #[serde(default)]
    #[schemars(with = "Option<String>")]
    pub(crate) component_digest: Option<ComponentDigest>,
    #[schemars(with = "String")]
    pub(crate) ffqn: FunctionFqn,
    /// Custom parameters for the JS workflow function.
    /// Each entry has a `name` and a WIT `type` (e.g. `string`, `u32`, `list<string>`).
    /// Defaults to no parameters.
    /// The synthesized return type must be an extendable `result`.
    #[serde(flatten)]
    pub(crate) interface: FunctionInterfaceToml,
    #[serde(default)]
    pub(crate) exec: ExecConfigToml,
    #[serde(default = "default_retry_exp_backoff")]
    pub(crate) retry_exp_backoff: DurationConfig,
    #[serde(default)]
    pub(crate) blocking_strategy: BlockingStrategyConfigToml,
    #[serde(default = "default_lock_extension")]
    pub(crate) lock_extension: bool,
    /// Starts extending the lock shortly before it expires, at `expires_at` minus this leeway.
    #[serde(default = "default_lock_extension_leeway")]
    pub(crate) lock_extension_leeway: DurationConfig,
    #[serde(default)]
    pub(crate) logs_store_min_level: LogLevelToml,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct WorkflowWasmComponentConfigToml {
    #[serde(flatten)]
    pub(crate) common: ComponentCommon,
    /// Optional content digest of the WASM file.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(with = "Option<String>")]
    pub(crate) content_digest: Option<ContentDigest>,
    /// Generated CAS references for backtrace source files.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    #[schemars(skip)]
    pub(crate) component_files: BTreeMap<String, ContentDigest>,
    /// Deprecated override of the auto-computed component digest used for locking.
    /// This option will be removed in 0.42.
    #[serde(default)]
    #[schemars(with = "Option<String>")]
    pub(crate) component_digest: Option<ComponentDigest>,
    #[serde(default)]
    pub(crate) exec: ExecConfigToml,
    #[serde(default = "default_retry_exp_backoff")]
    pub(crate) retry_exp_backoff: DurationConfig,
    #[serde(default)]
    pub(crate) blocking_strategy: BlockingStrategyConfigToml,
    #[serde(default)]
    pub(crate) backtrace: ComponentBacktraceConfig,
    #[serde(default)]
    pub(crate) stub_wasi: bool,
    #[serde(default = "default_lock_extension")]
    pub(crate) lock_extension: bool,
    /// Starts extending the lock shortly before it expires, at `expires_at` minus this leeway.
    #[serde(default = "default_lock_extension_leeway")]
    pub(crate) lock_extension_leeway: DurationConfig,
    #[serde(default)]
    pub(crate) logs_store_min_level: LogLevelToml,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Default, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct ComponentBacktraceConfig {
    /// Maps a frame-symbol key to a backtrace source file path. On-disk format only;
    /// resolved to `ComponentBacktraceConfigResolved` before hash
    /// computation. A relative path is deployment-dir-relative (a leading
    /// `${DEPLOYMENT_DIR}/` is accepted for backcompat); absolute paths are rejected.
    /// The source's content digest lives in the component's `component_files`, so
    /// this is a plain path map in both authored and processed manifests.
    #[serde(rename = "sources")]
    #[schemars(with = "std::collections::HashMap<String, String>")]
    pub(crate) frame_files_to_sources: HashMap<String, String>,
}

// Authored webhook component configs

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct WebhookWasmComponentConfigToml {
    #[serde(flatten)]
    pub(crate) common: ComponentCommon,
    /// Optional content digest of the WASM file.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(with = "Option<String>")]
    pub(crate) content_digest: Option<ContentDigest>,
    /// Generated CAS references for backtrace source files.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    #[schemars(skip)]
    pub(crate) component_files: BTreeMap<String, ContentDigest>,
    #[serde(default = "default_external_server_name")]
    pub(crate) http_server: ConfigName,
    pub(crate) routes: Vec<WebhookRoute>,
    #[serde(default)]
    pub(crate) forward_stdout: ComponentStdOutputToml,
    #[serde(default)]
    pub(crate) forward_stderr: ComponentStdOutputToml,
    #[serde(default)]
    pub(crate) env_vars: Vec<EnvVarConfig>,
    #[serde(default)]
    pub(crate) backtrace: ComponentBacktraceConfig,
    /// Capture and persist backtraces for requests handled by this webhook.
    #[serde(default)]
    pub(crate) backtrace_persist: bool,
    #[serde(default)]
    pub(crate) logs_store_min_level: LogLevelToml,
    /// Allowed outgoing HTTP hosts with optional method restrictions and secrets.
    #[serde(default, rename = "allowed_host")]
    pub(crate) allowed_hosts: Vec<AllowedHostToml>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct WebhookJsComponentConfigToml {
    pub(crate) name: ConfigName,
    /// Location of the JavaScript source file.
    /// Supports local file paths and OCI registry references (`oci://...`).
    #[serde(default)]
    pub(crate) location: Option<ScriptLocationPathOrOci>,
    /// Inline JavaScript source embedded in the TOML.
    /// Exactly one of `location` or `content` must be set.
    #[serde(default)]
    pub(crate) content: Option<String>,
    /// Content digest of the JS source file.
    #[serde(default)]
    #[schemars(with = "Option<String>")]
    pub(crate) content_digest: Option<ContentDigest>,
    /// CAS references for the closed module graph, populated during deployment preparation.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    #[schemars(skip)]
    pub(crate) component_files: BTreeMap<String, ContentDigest>,
    /// The HTTP server to bind this webhook to.
    #[serde(default = "default_external_server_name")]
    pub(crate) http_server: ConfigName,
    /// Routes that this webhook responds to.
    pub(crate) routes: Vec<WebhookRoute>,
    #[serde(default)]
    pub(crate) forward_stdout: ComponentStdOutputToml,
    #[serde(default)]
    pub(crate) forward_stderr: ComponentStdOutputToml,
    #[serde(default)]
    pub(crate) logs_store_min_level: LogLevelToml,
    #[serde(default)]
    pub(crate) env_vars: Vec<EnvVarConfig>,
    /// Capture and persist backtraces for requests handled by this webhook.
    #[serde(default)]
    pub(crate) backtrace_persist: bool,
    /// Allowed outgoing HTTP hosts with optional method restrictions and secrets.
    #[serde(default, rename = "allowed_host")]
    pub(crate) allowed_hosts: Vec<AllowedHostToml>,
}
