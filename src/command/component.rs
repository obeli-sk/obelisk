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
use crate::config::toml::OCI_SCHEMA_PREFIX;
use crate::get_fn_repository_client;
use crate::oci;
use crate::oci::ComponentMetadata;
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
                name,
                deployment,
                locked,
            } => add_component_from_oci(location, name, deployment, locked).await,
        }
    }
}

struct ComponentPushData {
    component_type: TomlComponentType,
    wasm_path: PathBuf,
    env_vars: Vec<String>,
    allowed_hosts: Vec<crate::config::toml::AllowedHostToml>,
    lock_duration: Option<DurationConfig>,
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

    let deployment = &deployment.inner;
    match component_type {
        TomlComponentType::ActivityWasm => {
            let cfg = deployment
                .activities_wasm
                .iter()
                .find(|c| c.common.name.to_string() == name)
                .expect("name is in map so it must be in the list");
            let ComponentLocationToml::Path(ref path) = cfg.common.location else {
                bail!(
                    "component '{name}' uses OCI/GitHub location, only local paths are supported for push"
                );
            };
            Ok(ComponentPushData {
                component_type,
                wasm_path: PathBuf::from(path),
                env_vars: cfg.env_vars.iter().map(env_var_key).collect(),
                allowed_hosts: cfg.allowed_hosts.clone(),
                lock_duration: Some(cfg.exec.lock_expiry),
            })
        }
        TomlComponentType::WebhookEndpointWasm => {
            let cfg = deployment
                .webhooks
                .iter()
                .find(|c| c.common.name.to_string() == name)
                .expect("name is in map so it must be in the list");
            let ComponentLocationToml::Path(ref path) = cfg.common.location else {
                bail!(
                    "component '{name}' uses OCI/GitHub location, only local paths are supported for push"
                );
            };
            Ok(ComponentPushData {
                component_type,
                wasm_path: PathBuf::from(path),
                env_vars: cfg.env_vars.iter().map(env_var_key).collect(),
                allowed_hosts: cfg.allowed_hosts.clone(),
                lock_duration: None,
            })
        }
        TomlComponentType::WorkflowWasm => {
            let cfg = deployment
                .workflows
                .iter()
                .find(|c| c.common.name.to_string() == name)
                .expect("name is in map so it must be in the list");
            let ComponentLocationToml::Path(ref path) = cfg.common.location else {
                bail!(
                    "component '{name}' uses OCI/GitHub location, only local paths are supported for push"
                );
            };
            Ok(ComponentPushData {
                component_type,
                wasm_path: PathBuf::from(path),
                env_vars: Vec::new(),
                allowed_hosts: vec![],
                lock_duration: None,
            })
        }
        other => bail!("component type `{other}` does not support push"),
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
    let data = find_component_for_push(&validated, component_name)?;
    let metadata = ComponentMetadata {
        component_type: data.component_type,
        env_vars: data.env_vars,
        allowed_hosts: data.allowed_hosts,
        lock_duration: data.lock_duration,
    };
    oci::push(data.wasm_path, reference, &metadata).await
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

    // Pull image and extract metadata; download WASM blob only if locked
    let (content_digest, metadata) = if locked {
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
        let (digest, _path, comp_metadata) =
            oci::pull_to_cache_dir(&oci_ref, &wasm_cache_dir, &metadata_dir)
                .await
                .context("failed to pull OCI image")?;
        info!("Fetched OCI image, content_digest: {digest}");
        (Some(digest), comp_metadata)
    } else {
        let comp_metadata = oci::pull_metadata(&oci_ref)
            .await
            .context("failed to fetch OCI image metadata")?;
        (None, comp_metadata)
    };

    // Determine component type and config from metadata
    let metadata = metadata
        .context("cannot determine component type: OCI image was pushed without metadata (use a newer `obelisk component push`)")?;
    let component_type = metadata.component_type;

    let location_raw = format!("{OCI_SCHEMA_PREFIX}{}", oci_ref.whole());

    let contents = {
        use toml_edit::{ArrayOfTables, DocumentMut, Item, value};

        let mut doc = contents.parse::<DocumentMut>()?;
        let key = component_type.to_string();

        // Ensure the entry exists
        if !doc.contains_key(&key) {
            doc.insert(&key, Item::ArrayOfTables(ArrayOfTables::new()));
        }

        let components = doc[&key]
            .as_array_of_tables_mut()
            .with_context(|| format!("expected {component_type} to be an array of tables"))?;

        // Find existing table by name
        if let Some(table) = components.iter_mut().find(|t| {
            t.get("name")
                .and_then(|item| item.as_str())
                .is_some_and(|s| s == name)
        }) {
            // Update existing
            table["location"] = value(location_raw);
            if let Some(ref digest) = content_digest {
                table["content_digest"] = value(digest.to_string());
            } else {
                table.remove("content_digest");
            }
        } else {
            components.push(build_component_table(
                &name,
                &location_raw,
                content_digest.as_ref(),
                &metadata,
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
    content_digest: Option<&concepts::ContentDigest>,
    metadata: &ComponentMetadata,
) -> toml_edit::Table {
    use toml_edit::{Item, Table, value};

    let mut t = Table::new();
    t["name"] = value(name);
    t["location"] = value(location_raw);
    if let Some(digest) = content_digest {
        t["content_digest"] = value(digest.to_string());
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

    // Write exec.lock_expiry as an inline table: exec = {lock_expiry = {seconds = N}}
    if let Some(duration) = metadata.lock_duration {
        let mut lock_expiry_inline = toml_edit::InlineTable::new();
        let (unit, n) = match duration {
            DurationConfig::Milliseconds(n) => ("milliseconds", n),
            DurationConfig::Seconds(n) => ("seconds", n),
            DurationConfig::Minutes(n) => ("minutes", n),
            DurationConfig::Hours(n) => ("hours", n),
        };
        lock_expiry_inline.insert(
            unit,
            toml_edit::Value::Integer(toml_edit::Formatted::new(
                i64::try_from(n).unwrap_or(i64::MAX),
            )),
        );
        let mut exec_inline = toml_edit::InlineTable::new();
        exec_inline.insert(
            "lock_expiry",
            toml_edit::Value::InlineTable(lock_expiry_inline),
        );
        t["exec"] = Item::Value(toml_edit::Value::InlineTable(exec_inline));
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
    use crate::oci::ComponentMetadata;

    fn make_metadata_activity() -> ComponentMetadata {
        ComponentMetadata {
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
            None,
            &metadata,
        );

        // Wrap in an [[activity_wasm]] array-of-tables document and parse
        let mut doc = toml_edit::DocumentMut::new();
        let mut aot = toml_edit::ArrayOfTables::new();
        aot.push(table);
        doc.insert("activity_wasm", toml_edit::Item::ArrayOfTables(aot));

        let toml_str = doc.to_string();
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
        let metadata = ComponentMetadata {
            component_type: TomlComponentType::WebhookEndpointWasm,
            env_vars: vec!["TOKEN".to_string()],
            allowed_hosts: vec![],
            lock_duration: None,
        };
        let table = build_component_table(
            "my_webhook",
            "oci://registry.example.com/repo/webhook:v1",
            None,
            &metadata,
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
}
