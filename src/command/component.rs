use crate::FunctionMetadataVerbosity;
use crate::FunctionRepositoryClient;
use crate::args;
use crate::args::TomlComponentType;
use crate::config::config_holder::{ConfigHolder, OBELISK_HELP_DEPLOYMENT_TOML};
use crate::config::env_var::EnvVarConfig;
use crate::config::toml::ComponentLocationToml;
use crate::config::toml::ConfigName;
use crate::config::toml::DeploymentTomlValidated;
use crate::config::toml::DurationConfig;
use crate::config::toml::JsLocationToml;
use crate::config::toml::OCI_SCHEMA_PREFIX;
use crate::get_fn_repository_client;
use crate::oci;
use crate::oci::ComponentMetadataAnnotation;
use crate::oci::JsWitConfigAnnotation;
use crate::project_dirs;
use anyhow::Context;
use anyhow::bail;
use concepts::ComponentType;
use concepts::FunctionFqn;
use directories::BaseDirs;
use grpc::grpc_gen;
use grpc::to_channel;
use std::path::PathBuf;
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt as _;
use tracing::info;

impl args::Component {
    pub(crate) async fn run(self) -> Result<(), anyhow::Error> {
        match self {
            args::Component::List {
                api_url,
                imports,
                extensions,
            } => {
                let channel = to_channel(&api_url).await?;
                let client = get_fn_repository_client(channel).await?;
                list_components(
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
            args::Component::Push {
                component_name,
                deployment,
                oci,
            } => push_component(&component_name, &deployment, &oci).await,
            args::Component::Add {
                location,
                component_name,
                deployment,
                locked,
            } => add_component_from_oci(location, component_name, deployment, locked).await,
        }
    }
}

enum ComponentPushData {
    Wasm {
        component_type: TomlComponentType,
        path: PathBuf,
        env_vars: Vec<String>,
        allowed_hosts: Vec<crate::config::toml::AllowedHostToml>,
        lock_duration: Option<DurationConfig>,
    },
    Js {
        component_type: TomlComponentType,
        path: PathBuf,
        /// `ffqn/params/return_type` config. None for `webhook_endpoint_js`.
        js_config: Option<JsWitConfigAnnotation>,
        env_vars: Vec<String>,
        allowed_hosts: Vec<crate::config::toml::AllowedHostToml>,
        lock_duration: Option<DurationConfig>,
    },
}

/// Find a component by name in the deployment TOML and build metadata for push.
fn find_component_for_push(
    deployment: &DeploymentTomlValidated,
    name: &str,
) -> anyhow::Result<ComponentPushData> {
    let component_type = deployment
        .component_type_by_name
        .get(name)
        .copied()
        .with_context(|| format!("component '{name}' not found in deployment TOML"))?;

    match component_type {
        TomlComponentType::ActivityWasm => {
            let cfg = deployment
                .inner
                .activities_wasm
                .iter()
                .find(|c| c.common.name.to_string() == name)
                .expect("name is in map so it must be in the list");
            let ComponentLocationToml::Path(ref path) = cfg.common.location else {
                bail!("component '{name}' uses OCI, only local paths are supported for push");
            };
            Ok(ComponentPushData::Wasm {
                component_type,
                path: PathBuf::from(path),
                env_vars: cfg.env_vars.iter().map(env_var_key).collect(),
                allowed_hosts: cfg.allowed_hosts.clone(),
                lock_duration: Some(cfg.exec.lock_expiry),
            })
        }
        TomlComponentType::WebhookEndpointWasm => {
            let cfg = deployment
                .inner
                .webhooks
                .iter()
                .find(|c| c.common.name.to_string() == name)
                .expect("name is in map so it must be in the list");
            let ComponentLocationToml::Path(ref path) = cfg.common.location else {
                bail!("component '{name}' uses OCI, only local paths are supported for push");
            };
            Ok(ComponentPushData::Wasm {
                component_type,
                path: PathBuf::from(path),
                env_vars: cfg.env_vars.iter().map(env_var_key).collect(),
                allowed_hosts: cfg.allowed_hosts.clone(),
                lock_duration: None,
            })
        }
        TomlComponentType::WorkflowWasm => {
            let cfg = deployment
                .inner
                .workflows
                .iter()
                .find(|c| c.common.name.to_string() == name)
                .expect("name is in map so it must be in the list");
            let ComponentLocationToml::Path(ref path) = cfg.common.location else {
                bail!("component '{name}' uses OCI, only local paths are supported for push");
            };
            Ok(ComponentPushData::Wasm {
                component_type,
                path: PathBuf::from(path),
                env_vars: Vec::new(),
                allowed_hosts: vec![],
                lock_duration: None,
            })
        }
        TomlComponentType::ActivityJs => {
            let (cfg, _) = deployment
                .activities_js
                .iter()
                .find(|(_, n)| n.to_string() == name)
                .expect("name is in map so it must be in the list");
            let JsLocationToml::Path(ref path) = cfg.location else {
                bail!("component '{name}' uses OCI, only local paths are supported for push");
            };
            Ok(ComponentPushData::Js {
                component_type,
                path: PathBuf::from(path),
                js_config: Some(JsWitConfigAnnotation {
                    ffqn: cfg.ffqn.clone(),
                    params: cfg.params.clone(),
                    return_type: cfg.return_type.clone(),
                }),
                env_vars: cfg.env_vars.iter().map(env_var_key).collect(),
                allowed_hosts: cfg.allowed_hosts.clone(),
                lock_duration: Some(cfg.exec.lock_expiry),
            })
        }
        TomlComponentType::WorkflowJs => {
            let (cfg, _) = deployment
                .workflows_js
                .iter()
                .find(|(_, n)| n.to_string() == name)
                .expect("name is in map so it must be in the list");
            let JsLocationToml::Path(ref path) = cfg.location else {
                bail!("component '{name}' uses OCI, only local paths are supported for push");
            };
            Ok(ComponentPushData::Js {
                component_type,
                path: PathBuf::from(path),
                js_config: Some(JsWitConfigAnnotation {
                    ffqn: cfg.ffqn.clone(),
                    params: cfg.params.clone(),
                    return_type: cfg.return_type.clone(),
                }),
                env_vars: Vec::new(),
                allowed_hosts: vec![],
                lock_duration: Some(cfg.exec.lock_expiry),
            })
        }
        TomlComponentType::WebhookEndpointJs => {
            let cfg = deployment
                .inner
                .webhooks_js
                .iter()
                .find(|c| c.name.to_string() == name)
                .expect("name is in map so it must be in the list");
            let JsLocationToml::Path(ref path) = cfg.location else {
                bail!("component '{name}' uses OCI, only local paths are supported for push");
            };
            Ok(ComponentPushData::Js {
                component_type,
                path: PathBuf::from(path),
                js_config: None,
                env_vars: cfg.env_vars.iter().map(env_var_key).collect(),
                allowed_hosts: cfg.allowed_hosts.clone(),
                lock_duration: None,
            })
        }
        other @ (TomlComponentType::ActivityExec
        | TomlComponentType::ActivityExternal
        | TomlComponentType::ActivityStub
        | TomlComponentType::Cron) => {
            bail!("component type `{other}` does not support push")
        }
    }
}

fn env_var_key(ev: &EnvVarConfig) -> String {
    match ev {
        EnvVarConfig::Key(k) => k.clone(),
        EnvVarConfig::KeyValue { key, .. } => key.clone(),
    }
}

async fn push_component(
    component_name: &str,
    deployment_path: &std::path::Path,
    reference: &oci_client::Reference,
) -> anyhow::Result<()> {
    let validated =
        crate::config::config_holder::load_deployment_toml(deployment_path.to_path_buf()).await?;
    match find_component_for_push(&validated, component_name)? {
        ComponentPushData::Wasm {
            component_type,
            path,
            env_vars,
            allowed_hosts,
            lock_duration,
        } => {
            let metadata = ComponentMetadataAnnotation {
                component_type,
                env_vars,
                allowed_hosts,
                lock_duration,
            };
            oci::push(path, reference, &metadata).await
        }
        ComponentPushData::Js {
            component_type,
            path,
            js_config,
            env_vars,
            allowed_hosts,
            lock_duration,
        } => {
            let metadata = ComponentMetadataAnnotation {
                component_type,
                env_vars,
                allowed_hosts,
                lock_duration,
            };
            oci::push_js(path, reference, &metadata, js_config.as_ref()).await
        }
    }
}

async fn add_component_from_oci(
    oci_ref: oci_client::Reference,
    name: String,
    deployment_path: PathBuf,
    locked: bool,
) -> anyhow::Result<()> {
    // Validate name
    ConfigName::new(name.clone().into()).context("name is invalid")?;

    // Open/create deployment TOML
    let (mut file, contents, prefix) = if deployment_path.try_exists().unwrap_or_default() {
        let contents = tokio::fs::read_to_string(&deployment_path)
            .await
            .with_context(|| format!("cannot read {deployment_path:?}"))?;
        let file = OpenOptions::new()
            .create(false)
            .truncate(true)
            .write(true)
            .open(&deployment_path)
            .await
            .with_context(|| format!("cannot open {deployment_path:?}"))?;
        (file, contents, "")
    } else {
        (
            OpenOptions::new()
                .create_new(true)
                .write(true)
                .append(false)
                .open(&deployment_path)
                .await
                .with_context(|| format!("cannot create {deployment_path:?}"))?,
            String::new(),
            OBELISK_HELP_DEPLOYMENT_TOML,
        )
    };

    // Fetch metadata to determine component type (always needed).
    let (component_metadata_annotation, js_config_annotation) = oci::pull_metadata(&oci_ref)
        .await
        .context("failed to fetch OCI image metadata")?;
    let component_metadata_annotation: ComponentMetadataAnnotation = component_metadata_annotation.context(
        "cannot determine component type: OCI image was pushed without metadata (use a newer `obelisk component push`)",
    )?;
    let toml_component_type = component_metadata_annotation.component_type;

    // For locked images, also pull the blob and record the pinned manifest digest.
    let (oci_manifest_digest_if_locked, js_config_annotation) = if locked {
        let project_dirs = project_dirs();
        let base_dirs = BaseDirs::new();
        let config_holder = ConfigHolder::new(project_dirs, base_dirs, None)?;
        let config = config_holder.load_config().await?;
        let wasm_cache_dir = config
            .wasm_global_config
            .get_wasm_cache_directory(&config_holder.path_prefixes)
            .await?;
        let metadata_dir = wasm_cache_dir.join("metadata");
        tokio::fs::create_dir_all(&metadata_dir)
            .await
            .with_context(|| format!("cannot create metadata directory {metadata_dir:?}"))?;

        match toml_component_type {
            TomlComponentType::ActivityJs
            | TomlComponentType::WorkflowJs
            | TomlComponentType::WebhookEndpointJs => {
                let js_cache_dir = wasm_cache_dir.join("js");
                tokio::fs::create_dir_all(&js_cache_dir)
                    .await
                    .with_context(|| {
                        format!("cannot create JS cache directory {js_cache_dir:?}")
                    })?;
                let oci::JsCacheResult {
                    manifest_digest,
                    js_config: js_config2,
                    ..
                } = oci::pull_js_to_cache(&oci_ref, &js_cache_dir, &metadata_dir)
                    .await
                    .context("failed to pull JS OCI image")?;
                info!("Fetched JS OCI image, manifest_digest: {manifest_digest}");
                (Some(manifest_digest), js_config2.or(js_config_annotation))
            }
            TomlComponentType::ActivityWasm
            | TomlComponentType::WorkflowWasm
            | TomlComponentType::WebhookEndpointWasm => {
                let (_digest, _path, manifest_digest, _meta2) =
                    oci::pull_to_cache_dir(&oci_ref, &wasm_cache_dir, &metadata_dir)
                        .await
                        .context("failed to pull OCI image")?;
                info!("Fetched OCI image, manifest_digest: {manifest_digest}");
                (Some(manifest_digest), None)
            }
            TomlComponentType::ActivityExec
            | TomlComponentType::ActivityExternal
            | TomlComponentType::ActivityStub => {
                bail!("exec, external, and stub activity types cannot be pushed to an oci registry")
            }
            TomlComponentType::Cron => {
                bail!("cron type cannot be pushed to an oci registry")
            }
        }
    } else {
        (None, js_config_annotation)
    };

    let location_raw = if let Some(actual_digest) = oci_manifest_digest_if_locked {
        if let Some(requested_digest) = oci_ref.digest() {
            // Requested `oci_ref` is already pinned
            assert_eq!(requested_digest, actual_digest); // Registry must return the requested image based on the digest, disregarding tag.
            format!("{OCI_SCHEMA_PREFIX}{oci_ref}")
        } else {
            // Set digest from OCI image metadata.
            let oci_ref = oci_ref.clone_with_digest(actual_digest);
            format!("{OCI_SCHEMA_PREFIX}{oci_ref}")
        }
    } else {
        // Just output the requested reference.
        format!("{OCI_SCHEMA_PREFIX}{oci_ref}")
    };

    let contents = {
        use toml_edit::{ArrayOfTables, DocumentMut, Item, value};

        let mut doc = contents.parse::<DocumentMut>()?;
        let key = toml_component_type.to_string();

        // Ensure the entry exists
        if !doc.contains_key(&key) {
            doc.insert(&key, Item::ArrayOfTables(ArrayOfTables::new()));
        }

        let components = doc[&key]
            .as_array_of_tables_mut()
            .with_context(|| format!("expected {toml_component_type} to be an array of tables"))?;

        // Find existing table by name
        if let Some(table) = components.iter_mut().find(|t: &&mut toml_edit::Table| {
            t.get("name")
                .and_then(|item| item.as_str())
                .is_some_and(|s| s == name)
        }) {
            // Update existing
            table["location"] = value(location_raw);
            // Remove stale content_digest if present from a previous version
            table.remove("content_digest");
        } else {
            components.push(build_component_table(
                &name,
                &location_raw,
                &component_metadata_annotation,
                js_config_annotation.as_ref(),
            ));
        }
        format!("{prefix}{doc}")
    };
    file.write_all(contents.as_bytes()).await?;
    file.flush().await?;
    Ok(())
}

fn build_component_table(
    name: &str,
    location_raw: &str,
    metadata: &ComponentMetadataAnnotation,
    js_config: Option<&JsWitConfigAnnotation>,
) -> toml_edit::Table {
    use toml_edit::{Item, Table, value};

    let mut t = Table::new();
    t["name"] = value(name);
    t["location"] = value(location_raw);

    // JS-specific fields
    if let Some(js) = js_config {
        t["ffqn"] = value(js.ffqn.to_string());
        if !js.params.is_empty() {
            let mut arr = toml_edit::Array::new();
            for p in &js.params {
                let mut inline = toml_edit::InlineTable::new();
                inline.insert("name", p.name.clone().into());
                inline.insert("type", p.wit_type.clone().into());
                let mut v = toml_edit::Value::InlineTable(inline);
                v.decor_mut().set_prefix("\n  ");
                arr.push_formatted(v);
            }
            arr.set_trailing("\n");
            arr.set_trailing_comma(true);
            t["params"] = Item::Value(toml_edit::Value::Array(arr));
        }
        if let Some(ref rt) = js.return_type {
            t["return_type"] = value(rt.clone());
        }
    }

    if !metadata.env_vars.is_empty() {
        let mut arr = toml_edit::Array::new();
        for v in &metadata.env_vars {
            arr.push(v.clone());
        }
        t["env_vars"] = Item::Value(toml_edit::Value::Array(arr));
    }

    if !metadata.allowed_hosts.is_empty() {
        let mut host_array = toml_edit::ArrayOfTables::new();
        for host in &metadata.allowed_hosts {
            let mut host_table = Table::new();
            host_table["pattern"] = value(&host.pattern);
            if let Some(ref methods) = host.methods {
                host_table["methods"] = serialize_methods_input(methods);
            }
            if let Some(ref secrets) = host.secrets {
                let mut secrets_table = Table::new();
                if !secrets.env_vars.is_empty() {
                    let mut secret_env_array = toml_edit::Array::new();
                    for ev in &secrets.env_vars {
                        let ev_val = match ev {
                            EnvVarConfig::Key(k) => {
                                toml_edit::Value::String(toml_edit::Formatted::new(k.clone()))
                            }
                            EnvVarConfig::KeyValue { key, value: v } => {
                                let mut ev_inline = toml_edit::InlineTable::new();
                                ev_inline.insert("key", key.clone().into());
                                ev_inline.insert("value", v.clone().into());
                                toml_edit::Value::InlineTable(ev_inline)
                            }
                        };
                        secret_env_array.push(ev_val);
                    }
                    secrets_table["env_vars"] =
                        Item::Value(toml_edit::Value::Array(secret_env_array));
                }
                if !secrets.replace_in.is_empty() {
                    let mut replace_array = toml_edit::Array::new();
                    for r in &secrets.replace_in {
                        let name = match r {
                            crate::config::toml::ReplaceIn::Headers => "headers",
                            crate::config::toml::ReplaceIn::Body => "body",
                            crate::config::toml::ReplaceIn::Params => "params",
                        };
                        replace_array.push(name);
                    }
                    secrets_table["replace_in"] =
                        Item::Value(toml_edit::Value::Array(replace_array));
                }
                host_table["secrets"] = Item::Value(toml_edit::Value::InlineTable(
                    secrets_table.into_inline_table(),
                ));
            }
            host_array.push(host_table);
        }
        t.insert("allowed_host", Item::ArrayOfTables(host_array));
    }

    // Webhook requires a `routes` field; write an empty array as a placeholder
    if matches!(
        metadata.component_type,
        TomlComponentType::WebhookEndpointWasm | TomlComponentType::WebhookEndpointJs
    ) {
        t["routes"] = Item::Value(toml_edit::Value::Array(toml_edit::Array::new()));
    }

    // Write as a dotted key: exec.lock_expiry.<unit> = N
    if let Some(duration) = metadata.lock_duration {
        let (unit, n) = match duration {
            DurationConfig::Milliseconds(n) => ("milliseconds", n),
            DurationConfig::Seconds(n) => ("seconds", n),
            DurationConfig::Minutes(n) => ("minutes", n),
            DurationConfig::Hours(n) => ("hours", n),
        };
        let mut lock_expiry_tbl = Table::new();
        lock_expiry_tbl.set_dotted(true);
        lock_expiry_tbl.insert(unit, value(i64::try_from(n).unwrap_or(i64::MAX)));
        let mut exec_tbl = Table::new();
        exec_tbl.set_dotted(true);
        exec_tbl.insert("lock_expiry", Item::Table(lock_expiry_tbl));
        t.insert("exec", Item::Table(exec_tbl));
    }

    t
}

fn serialize_methods_input(methods: &crate::config::toml::MethodsInput) -> toml_edit::Item {
    use crate::config::toml::MethodsInput;
    match methods {
        MethodsInput::Star(_) => toml_edit::Item::Value("*".into()),
        MethodsInput::List(list) => {
            let mut arr = toml_edit::Array::new();
            for m in list {
                arr.push(m);
            }
            toml_edit::Item::Value(toml_edit::Value::Array(arr))
        }
    }
}

pub(crate) async fn list_components(
    mut client: FunctionRepositoryClient,
    verbosity: FunctionMetadataVerbosity,
    extensions: bool,
) -> anyhow::Result<()> {
    let components = client
        .list_components(tonic::Request::new(grpc_gen::ListComponentsRequest {
            function_name: None,
            component_digest: None,
            extensions,
        }))
        .await?
        .into_inner()
        .components;
    for component in components {
        let component_id = component.component_id.expect("`component_id` is sent");
        println!(
            "{name}\t{ty}\t{sha}",
            name = component_id.name,
            ty = grpc_gen::ComponentType::try_from(component_id.component_type)
                .map_err(|_| ())
                .and_then(|ty| ComponentType::try_from(ty).map_err(|_| ()))
                .map(|ct| ct.to_string())
                .unwrap_or_else(|()| "unknown type".to_string()),
            sha = component_id.digest.map(|d| d.digest).unwrap_or_default()
        );
        println!("Exports:");
        print_fn_details(component.exports)?;
        if verbosity > FunctionMetadataVerbosity::ExportsOnly {
            println!("Imports:");
            print_fn_details(component.imports)?;
        }
        println!();
    }
    Ok(())
}

fn print_fn_details(vec: Vec<grpc_gen::FunctionDetail>) -> Result<(), anyhow::Error> {
    for fn_detail in vec {
        let func = fn_detail.function_name.context("function must exist")?;
        let func = if let Ok(func) = FunctionFqn::try_from(func.clone())
            .with_context(|| format!("ffqn sent by the server must be valid - {func:?}"))
        {
            func.to_string()
        } else {
            // FIXME: here because of functions like: interface_name: "wasi:io/poll@0.2.3", function_name: "[method]pollable.block"
            format!("{} . {}", func.interface_name, func.function_name)
        };
        print!("\t{func} : func(");
        let mut params = fn_detail.params.into_iter().peekable();
        while let Some(param) = params.next() {
            print!("{}: ", param.name);
            print_wit_type(&param.r#type.context("field `params.type` must exist")?);
            if params.peek().is_some() {
                print!(", ");
            }
        }
        print!(")");
        if let Some(return_type) = fn_detail.return_type {
            print!(" -> ");
            print_wit_type(&return_type);
        }
        println!();
    }
    Ok(())
}

fn print_wit_type(wit_type: &grpc_gen::WitType) {
    print!("{}", wit_type.wit_type);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::toml::{AllowedHostSecretsToml, AllowedHostToml, MethodsInput, ReplaceIn};
    use crate::oci::ComponentMetadataAnnotation;

    fn make_metadata_activity() -> ComponentMetadataAnnotation {
        ComponentMetadataAnnotation {
            component_type: TomlComponentType::ActivityWasm,
            env_vars: vec!["API_KEY".to_string(), "BASE_URL".to_string()],
            allowed_hosts: vec![AllowedHostToml {
                pattern: "api.example.com".to_string(),
                methods: Some(MethodsInput::List(vec![
                    "GET".to_string(),
                    "POST".to_string(),
                ])),
                secrets: Some(AllowedHostSecretsToml {
                    env_vars: vec![EnvVarConfig::Key("API_KEY".to_string())],
                    replace_in: vec![ReplaceIn::Headers],
                }),
            }],
            lock_duration: Some(DurationConfig::Seconds(5)),
        }
    }

    #[test]
    fn build_and_parse_activity_wasm_toml() {
        let metadata = make_metadata_activity();
        let table = build_component_table(
            "my_activity",
            "oci://registry.example.com/repo/my-activity:latest",
            &metadata,
            None,
        );

        // Wrap in an [[activity_wasm]] array-of-tables document and parse
        let mut doc = toml_edit::DocumentMut::new();
        let mut aot = toml_edit::ArrayOfTables::new();
        aot.push(table);
        doc.insert("activity_wasm", toml_edit::Item::ArrayOfTables(aot));

        let toml_str = doc.to_string();
        assert!(
            toml_str.contains("exec.lock_expiry.seconds = 5"),
            "unexpected exec format:\n{toml_str}"
        );
        let parsed: crate::config::toml::DeploymentToml =
            toml::from_str(&toml_str).expect("generated TOML must parse");

        assert_eq!(parsed.activities_wasm.len(), 1);
        let act = &parsed.activities_wasm[0];
        assert_eq!(act.common.name.to_string(), "my_activity");
        assert_eq!(act.env_vars.len(), 2);
        assert_eq!(act.allowed_hosts.len(), 1);
        assert_eq!(act.allowed_hosts[0].pattern, "api.example.com");
        assert!(act.allowed_hosts[0].secrets.is_some());
        // exec.lock_expiry.seconds = 5
        assert!(matches!(act.exec.lock_expiry, DurationConfig::Seconds(5)));
    }

    #[test]
    fn build_and_parse_webhook_toml() {
        let metadata = ComponentMetadataAnnotation {
            component_type: TomlComponentType::WebhookEndpointWasm,
            env_vars: vec!["TOKEN".to_string()],
            allowed_hosts: vec![],
            lock_duration: None,
        };
        let table = build_component_table(
            "my_webhook",
            "oci://registry.example.com/repo/webhook:v1",
            &metadata,
            None,
        );

        let mut doc = toml_edit::DocumentMut::new();
        let mut aot = toml_edit::ArrayOfTables::new();
        aot.push(table);
        doc.insert("webhook_endpoint_wasm", toml_edit::Item::ArrayOfTables(aot));

        let toml_str = doc.to_string();
        let parsed: crate::config::toml::DeploymentToml =
            toml::from_str(&toml_str).expect("generated TOML must parse");

        assert_eq!(parsed.webhooks.len(), 1);
        let wh = &parsed.webhooks[0];
        assert_eq!(wh.common.name.to_string(), "my_webhook");
        assert_eq!(wh.env_vars.len(), 1);
        assert_eq!(wh.allowed_hosts.len(), 0);
    }

    #[test]
    fn build_and_parse_activity_js_toml() {
        use crate::config::toml::JsParamToml;
        use concepts::FunctionFqn;

        let metadata = ComponentMetadataAnnotation {
            component_type: TomlComponentType::ActivityJs,
            env_vars: vec!["API_KEY".to_string()],
            allowed_hosts: vec![],
            lock_duration: Some(DurationConfig::Seconds(10)),
        };
        let js_config = JsWitConfigAnnotation {
            ffqn: FunctionFqn::new_arc("my-pkg:my-iface/my-ifc".into(), "my-fn".into()),
            params: vec![JsParamToml {
                name: "input".to_string(),
                wit_type: "string".to_string(),
            }],
            return_type: Some("result<string>".to_string()),
        };
        let table = build_component_table(
            "my_js_activity",
            "oci://registry.example.com/repo/js-activity:v1",
            &metadata,
            Some(&js_config),
        );

        let mut doc = toml_edit::DocumentMut::new();
        let mut aot = toml_edit::ArrayOfTables::new();
        aot.push(table);
        doc.insert("activity_js", toml_edit::Item::ArrayOfTables(aot));

        let toml_str = doc.to_string();
        // Format assertions: params rendered as a multi-line array of inline
        // tables, and exec.lock_expiry as a dotted key.
        assert!(
            toml_str.contains("params = [\n  { name = \"input\", type = \"string\" },\n]"),
            "unexpected params format:\n{toml_str}"
        );
        assert!(
            toml_str.contains("exec.lock_expiry.seconds = 10"),
            "unexpected exec format:\n{toml_str}"
        );
        let parsed: crate::config::toml::DeploymentToml =
            toml::from_str(&toml_str).expect("generated TOML must parse");

        assert_eq!(parsed.activities_js.len(), 1);
        let act = &parsed.activities_js[0];
        assert_eq!(
            act.name.as_ref().expect("name set").to_string(),
            "my_js_activity"
        );
        assert_eq!(act.ffqn.to_string(), "my-pkg:my-iface/my-ifc.my-fn");
        assert_eq!(act.return_type.as_deref(), Some("result<string>"));
        assert_eq!(act.params.len(), 1);
        assert_eq!(act.params[0].name, "input");
        assert_eq!(act.params[0].wit_type, "string");
        assert_eq!(act.env_vars.len(), 1);
        assert!(matches!(act.exec.lock_expiry, DurationConfig::Seconds(10)));
    }
}
