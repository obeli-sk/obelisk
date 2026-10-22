use super::output::{print_json, print_value};
use crate::args::CloudToken;
use crate::args::{
    AdminArgs, ClientConnection, Cloud, CloudApp, CloudAppTarget, CloudApps, CloudArgs,
    CloudCommandId, CloudEndpoint, CloudEnv, CloudSecrets, ComponentArgs, DeploymentArgs,
    ExecutionArgs, Subcommand,
};
use crate::client::ClientStartup;
use crate::config::app::AppConfigToml;
use anyhow::{Context as _, bail, ensure};
use concepts::prefixed_ulid::ExecutionIdTopLevel;
use reqwest::{Method, Url, header};
use secrecy::ExposeSecret as _;
use serde_json::{Value, json};
use std::{fmt::Write as _, io::Read as _, path::Path, time::Duration};

mod deploy;
mod output;
use output::Summary;

fn human_output(value: &Value) -> String {
    fn render(out: &mut String, value: &Value, indent: usize) {
        match value {
            Value::Object(fields) if !fields.is_empty() => {
                for (key, value) in fields
                    .iter()
                    .filter(|(key, _)| key.as_str() != "apps" || !fields.contains_key("global"))
                {
                    if key == "global" && value.is_boolean() {
                        let scope = if value == true {
                            "account-wide".to_owned()
                        } else {
                            fields["apps"]
                                .as_array()
                                .map(|apps| {
                                    apps.iter()
                                        .filter_map(Value::as_str)
                                        .collect::<Vec<_>>()
                                        .join(", ")
                                })
                                .unwrap_or_default()
                        };
                        let _ = writeln!(out, "{:indent$}scope: {scope}", "");
                    } else {
                        let label = key.replace('_', " ");
                        let _ = write!(out, "{:indent$}{label}:", "");
                        if value.is_object() || value.is_array() {
                            out.push('\n');
                            render(out, value, indent + 2);
                        } else {
                            out.push(' ');
                            render(out, value, 0);
                        }
                    }
                }
            }
            Value::Array(items) if !items.is_empty() => {
                for (index, item) in items.iter().enumerate() {
                    if index > 0 && item.is_object() {
                        out.push('\n');
                    }
                    render(out, item, indent);
                }
            }
            value => {
                let text = match value {
                    Value::String(text) => text.clone(),
                    Value::Null | Value::Array(_) | Value::Object(_) => "(none)".to_owned(),
                    value => value.to_string(),
                };
                let _ = writeln!(out, "{:indent$}{text}", "");
            }
        }
    }
    let mut output = String::new();
    render(&mut output, value, 0);
    output
}

#[derive(Clone, Copy)]
enum Kind {
    Read,
    /// Error bodies are redacted; staged secret edits are not provisioner commands.
    Secret,
    Command(Option<ExecutionIdTopLevel>),
    SecretCommand(Option<ExecutionIdTopLevel>),
}
impl From<CloudCommandId> for Kind {
    fn from(command_id: CloudCommandId) -> Self {
        Kind::Command(command_id.id)
    }
}

struct Client {
    http: reqwest::Client,
    origin: Url,
    verbose: bool,
}
fn api_origin(value: &str) -> anyhow::Result<Url> {
    let origin = value.parse::<Url>()?;
    ensure!(
        matches!(origin.scheme(), "http" | "https") && origin.host_str().is_some(),
        "API URL must use HTTP or HTTPS"
    );
    ensure!(
        origin.username().is_empty()
            && origin.password().is_none()
            && origin.query().is_none()
            && origin.fragment().is_none(),
        "API URL must not contain credentials, query or fragment"
    );
    Ok(origin)
}
#[derive(Debug, thiserror::Error)]
#[error("App name is already taken; choose another name with --app or OBELISK_APP_NAME")]
struct AppNameTaken;

impl Client {
    fn new(args: &CloudArgs) -> anyhow::Result<Self> {
        let origin = api_origin(
            args.api_url
                .as_ref()
                .context("Set --api-url or OBELISK_CLOUD_API_URL")?,
        )?;
        let mut headers = header::HeaderMap::new();
        if let Some(token) = &args.api_token {
            let mut value =
                header::HeaderValue::from_str(&format!("Bearer {}", token.expose_secret()))
                    .map_err(|_| {
                        anyhow::anyhow!("Cloud API token contains invalid header characters")
                    })?;
            value.set_sensitive(true);
            headers.insert(header::AUTHORIZATION, value);
        }
        Ok(Self {
            http: reqwest::Client::builder()
                .user_agent(crate::client::USER_AGENT)
                .default_headers(headers)
                .redirect(reqwest::redirect::Policy::none())
                .timeout(Duration::from_secs(360))
                .build()?,
            origin,
            verbose: args.verbose,
        })
    }
    fn url(&self, path: &[&str]) -> anyhow::Result<Url> {
        let mut url = self.origin.clone();
        url.path_segments_mut()
            .map_err(|()| anyhow::anyhow!("Cloud API URL cannot be a base URL"))?
            .pop_if_empty()
            .extend(path);
        Ok(url)
    }
    async fn request(
        &self,
        method: Method,
        path: &[&str],
        body: Option<Value>,
        kind: Kind,
    ) -> anyhow::Result<Value> {
        let mut request = self.http.request(method, self.url(path)?);
        let command_id = if let Kind::Command(id) | Kind::SecretCommand(id) = kind {
            let id = id.unwrap_or_else(ExecutionIdTopLevel::generate);
            if self.verbose {
                eprintln!("Cloud command execution ID: {id}");
            }
            request = request.header("Idempotency-Key", id.to_string());
            Some(id)
        } else {
            None
        };
        if let Some(body) = body {
            request = request.json(&body);
        }
        let response = request
            .send()
            .await
            .map_err(reqwest::Error::without_url)
            .with_context(|| {
                command_id.map_or_else(
                    || "Cloud request failed".to_owned(),
                    |id| {
                        format!(
                            "Cloud request failed; retry the same command with --command-id {id}"
                        )
                    },
                )
            })?;
        self.response(response, kind, command_id).await
    }
    async fn lookup_app(&self, name: &str) -> anyhow::Result<Option<Value>> {
        let response = self
            .http
            .get(self.url(&["v1", "apps", name])?)
            .send()
            .await
            .map_err(reqwest::Error::without_url)?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            Ok(None)
        } else {
            self.response(response, Kind::Read, None).await.map(Some)
        }
    }
    async fn response(
        &self,
        response: reqwest::Response,
        kind: Kind,
        command_id: Option<ExecutionIdTopLevel>,
    ) -> anyhow::Result<Value> {
        let status = response.status();
        if !status.is_success() {
            if matches!(kind, Kind::Secret) {
                bail!("Cloud secret operation failed: HTTP {status}");
            }
            let body = response.text().await.unwrap_or_default();
            if status == reqwest::StatusCode::CONFLICT
                && serde_json::from_str::<Value>(&body)
                    .is_ok_and(|body| body["code"] == "app_name_taken")
            {
                return Err(AppNameTaken.into());
            }
            if matches!(kind, Kind::SecretCommand(_)) {
                bail!(
                    "Cloud app creation failed: HTTP {status}. Command ID: {}",
                    command_id.expect("Creation command ID")
                );
            }
            if !self.verbose {
                let message = output::error_message(&body);
                if let Some(id) = command_id {
                    bail!("{message} (HTTP {status}). Command ID: {id}");
                }
                bail!("{message} (HTTP {status})");
            }
            bail!("Cloud API returned {status}: {body}");
        }
        response.json().await.map_err(reqwest::Error::without_url).with_context(|| {
            command_id.map_or_else(|| "Cannot read cloud response".to_owned(), |id| format!("Cannot read cloud response; retry the same command with --command-id {id}"))
        })
    }
    async fn app(
        &self,
        name: &str,
        method: Method,
        suffix: &[&str],
        body: Option<Value>,
        kind: Kind,
    ) -> anyhow::Result<Value> {
        let mut path = vec!["v1", "apps", name];
        path.extend_from_slice(suffix);
        self.request(method, &path, body, kind).await
    }
}
async fn app_name(name: Option<&str>) -> anyhow::Result<String> {
    let name = match name {
        Some(name) => name.to_owned(),
        None => policy(Path::new("app.toml"))
            .await
            .context("Select an app with --app, OBELISK_APP_NAME or app_name in ./app.toml")?
            .app_name
            .context("Set app_name in ./app.toml, --app or OBELISK_APP_NAME")?,
    };
    validate_cloud_app_name(&name)?;
    Ok(name)
}
fn validate_cloud_app_name(name: &str) -> anyhow::Result<()> {
    crate::config::app::validate_app_name(name)?;
    ensure!(
        name.len() <= 20 && !name.contains("--"),
        "Cloud app names must contain 4-20 lowercase letters, digits or single interior hyphens"
    );
    Ok(())
}
fn config_name(name: &str) -> anyhow::Result<()> {
    ensure!(
        !name.is_empty()
            && name.len() <= 128
            && !name.as_bytes()[0].is_ascii_digit()
            && name.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'_'),
        "Invalid configuration name: {name}"
    );
    Ok(())
}
async fn policy(path: &Path) -> anyhow::Result<AppConfigToml> {
    let text = tokio::fs::read_to_string(path)
        .await
        .with_context(|| format!("Cannot read policy {}", path.display()))?;
    toml::from_str(&text).with_context(|| format!("Invalid app policy {}", path.display()))
}
async fn token_command(client: &Client, command: CloudToken) -> anyhow::Result<Value> {
    let mut path = vec!["v1", "tokens"];
    match &command {
        CloudToken::New {
            name,
            token_file,
            scope,
            expires_at,
            ..
        } => {
            use std::io::Write as _;
            let mut options = std::fs::OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt as _;
                options.mode(0o600);
            }
            let mut file = options
                .open(token_file)
                .context("Cannot create private token file; use a new path")?;
            let expires_at = expires_at
                .map(|date| u64::try_from(date.timestamp()))
                .transpose()
                .context("Expiry must be after the Unix epoch")?;
            let result = client.request(Method::POST, &path, Some(json!({"name":name,"global":scope.is_empty(),"apps":scope,"expires_at":expires_at})), Kind::Read).await;
            let mut value = match result {
                Ok(value) => value,
                Err(error) => {
                    drop(file);
                    let _ = std::fs::remove_file(token_file);
                    return Err(error).context("Token issuance failed; if the response was lost, list tokens and revoke the inaccessible token before creating a replacement");
                }
            };
            let token = value.as_object_mut().and_then(|value| value.remove("token"))
                .and_then(|value| value.as_str().map(str::to_owned))
                .context("Missing issued credential; list tokens and revoke the inaccessible token before creating a replacement")?;
            file.write_all(token.as_bytes())
                .and_then(|()| file.sync_all())
                .with_context(|| {
                    format!(
                        "Cannot save issued token {}; revoke it before creating a replacement",
                        value["token_id"]
                    )
                })?;
            value["token_file"] = json!(token_file);
            Ok(value)
        }
        CloudToken::List => client.request(Method::GET, &path, None, Kind::Read).await,
        CloudToken::Get { id, .. } | CloudToken::Revoke { id, .. } => {
            path.push(id);
            client
                .request(
                    if matches!(command, CloudToken::Revoke { .. }) {
                        Method::DELETE
                    } else {
                        Method::GET
                    },
                    &path,
                    None,
                    Kind::Read,
                )
                .await
        }
    }
}
impl CloudArgs {
    fn selected_app(&self) -> Option<&str> {
        match &self.command {
            Cloud::App(args) => args.app.as_deref(),
            Cloud::Apps(_) | Cloud::Token(_) => None,
        }
    }
    async fn endpoints(&self) -> anyhow::Result<Value> {
        let name = app_name(self.selected_app()).await?;
        let domain = self.apps_domain.to_ascii_lowercase();
        ensure!(
            domain.len() <= 253
                && domain.split('.').all(|label| {
                    !label.is_empty()
                        && label.len() <= 63
                        && !label.starts_with('-')
                        && !label.ends_with('-')
                        && label
                            .bytes()
                            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'-')
                }),
            "Invalid apps domain: expected a DNS hostname without scheme, port or path"
        );
        let hostname = format!("{name}.{domain}");
        ensure!(
            hostname.len() + "webui--".len() <= 253,
            "App hostname exceeds the DNS length limit"
        );
        Ok(json!({
            "api_url": format!("https://api--{hostname}"),
            "webhook_url": format!("https://{hostname}"),
            "webui_url": format!("https://webui--{hostname}"),
        }))
    }
    async fn direct_api_url(&self) -> anyhow::Result<String> {
        Ok(self.endpoints().await?["api_url"]
            .as_str()
            .unwrap()
            .to_owned())
    }
    async fn run_app_api(self, api_url: String) -> anyhow::Result<()> {
        let startup = ClientStartup::cloud(api_url.clone(), self.api_token, self.verbose);
        let token = ClientConnection {
            api_url,
            api_token: None,
        };
        let command = match self.command {
            Cloud::App(CloudAppTarget {
                command: CloudApp::Deployment(command),
                ..
            }) => Subcommand::Deployment(DeploymentArgs { command, token }),
            Cloud::App(CloudAppTarget {
                command: CloudApp::Execution(command),
                ..
            }) => Subcommand::Execution(ExecutionArgs { command, token }),
            Cloud::App(CloudAppTarget {
                command: CloudApp::Component(command),
                ..
            }) => Subcommand::Component(ComponentArgs { command, token }),
            Cloud::App(CloudAppTarget {
                command: CloudApp::Admin(command),
                ..
            }) => Subcommand::Admin(AdminArgs { command, token }),
            _ => unreachable!(),
        };
        crate::command_future(command, Some(startup))?.await
    }
    pub(crate) async fn run(mut self) -> anyhow::Result<()> {
        if matches!(
            &self.command,
            Cloud::App(CloudAppTarget {
                command: CloudApp::Deploy { .. },
                ..
            })
        ) {
            return self.deploy().await;
        }
        if let Cloud::App(CloudAppTarget {
            command: CloudApp::Endpoint { command },
            ..
        }) = &self.command
        {
            if matches!(command, Some(CloudEndpoint::Api)) {
                return print_value(&self.direct_api_url().await?, self.json);
            }
            let endpoints = self.endpoints().await?;
            if let Some(command) = command {
                let key = match command {
                    CloudEndpoint::Api => "api_url",
                    CloudEndpoint::Webhook => "webhook_url",
                    CloudEndpoint::Webui => "webui_url",
                };
                return print_value(
                    &endpoints[key].as_str().context("Missing endpoint URL")?,
                    self.json,
                );
            }
            return if self.json {
                print_json(&endpoints)
            } else {
                print!("{}", human_output(&endpoints));
                Ok(())
            };
        }
        if matches!(
            self.command,
            Cloud::App(CloudAppTarget {
                command: CloudApp::Deployment(_)
                    | CloudApp::Execution(_)
                    | CloudApp::Component(_)
                    | CloudApp::Admin(_),
                ..
            })
        ) {
            let api_url = self.direct_api_url().await?;
            return self.run_app_api(api_url).await;
        }
        if let Cloud::App(CloudAppTarget { app, command }) = &mut self.command
            && app.is_none()
        {
            let path = match command {
                CloudApp::Stage { app_config, .. } => Some(app_config.clone()),
                CloudApp::Fill { app_config, .. } | CloudApp::Apply { app_config, .. } => {
                    crate::config::app::resolve_app_config(app_config.clone())?
                }
                _ => None,
            };
            if let Some(path) = path {
                *app = policy(&path).await?.app_name;
            }
        }
        let json = self.json;
        let verbose = self.verbose;
        let summary = Summary::from(&self.command);
        let value = self.execute().await?;
        if json {
            print_json(&value)
        } else {
            print!(
                "{}",
                if verbose {
                    human_output(&value)
                } else {
                    summary.render(&value)
                }
            );
            Ok(())
        }
    }
    async fn execute(self) -> anyhow::Result<Value> {
        let client = Client::new(&self)?;
        match self.command {
            Cloud::Token(command) => token_command(&client, command).await,
            Cloud::Apps(CloudApps::List) => {
                client
                    .request(Method::GET, &["v1", "apps"], None, Kind::Read)
                    .await
            }
            Cloud::App(CloudAppTarget {
                app,
                command:
                    CloudApp::New {
                        name,
                        app_config,
                        fill,
                        fill_non_interactive,
                        command_id,
                    },
            }) => {
                let config = match crate::config::app::resolve_app_config(app_config)? {
                    Some(path) => Some(policy(&path).await?),
                    None => None,
                };
                ensure!(
                    !(fill || fill_non_interactive) || config.is_some(),
                    "`--fill` requires an app config"
                );
                let name = app_name(
                    name.as_deref()
                        .or(app.as_deref())
                        .or_else(|| config.as_ref().and_then(|c| c.app_name.as_deref())),
                )
                .await?;
                let mut body = json!({"app_name":name});
                if let Some(config) = config {
                    body["policy"] = serde_json::to_value(config.policy())?;
                    body["cloud"] = serde_json::to_value(config.cloud)?;
                    let mut ask = ask;
                    let values = initial_values(
                        &body["policy"],
                        Values::new(fill, fill_non_interactive, terminal().then_some(&mut ask))?,
                    )?;
                    body["env"] = values["env"].clone();
                    body["secrets"] = values["secrets"].clone();
                }
                client
                    .request(
                        Method::POST,
                        &["v1", "apps"],
                        Some(body),
                        Kind::SecretCommand(command_id.id),
                    )
                    .await
            }
            Cloud::App(CloudAppTarget { app, command }) => {
                let (config_path, explicit) = match &command {
                    CloudApp::Stage { app_config, .. } => (Some(app_config.clone()), true),
                    CloudApp::Fill { app_config, .. } | CloudApp::Apply { app_config, .. } => (
                        crate::config::app::resolve_app_config(app_config.clone())?,
                        app_config.is_some(),
                    ),
                    _ => (None, false),
                };
                let config = match &config_path {
                    Some(path) => Some(policy(path).await?),
                    None => None,
                };
                let name = &app_name(
                    app.as_deref()
                        .or_else(|| config.as_ref().and_then(|c| c.app_name.as_deref())),
                )
                .await?;
                // A discovered ./app.toml naming another app is not this app's configuration.
                let local = config.zip(config_path).filter(|(config, _)| {
                    explicit || config.app_name.as_deref().is_none_or(|n| n == name)
                });
                match command {
                    CloudApp::Deployment(_)
                    | CloudApp::Execution(_)
                    | CloudApp::Component(_)
                    | CloudApp::Admin(_) => unreachable!("API commands are dispatched in run"),
                    CloudApp::Get => client.app(name, Method::GET, &[], None, Kind::Read).await,
                    CloudApp::Status => {
                        client
                            .app(name, Method::GET, &["status"], None, Kind::Read)
                            .await
                    }
                    CloudApp::Stage { command_id, .. } => {
                        let (config, _) = local.unwrap();
                        client
                            .app(
                                name,
                                Method::PUT,
                                &["config"],
                                Some(json!({"policy":config.policy(), "cloud":config.cloud})),
                                command_id.into(),
                            )
                            .await
                    }
                    CloudApp::Apply {
                        force,
                        staged,
                        fill,
                        fill_non_interactive,
                        command_id,
                        ..
                    } => {
                        let current = client.app(name, Method::GET, &[], None, Kind::Read).await?;
                        let mut ask = ask;
                        let interactive = terminal() && !self.json && !fill_non_interactive;
                        let desired = staged_or_local(
                            &client,
                            name,
                            &current,
                            local.filter(|_| !staged),
                            interactive.then_some(&mut ask),
                        )
                        .await?;
                        fill_values(
                            &client,
                            name,
                            &desired["policy"],
                            Values::new(
                                fill,
                                fill_non_interactive,
                                interactive.then_some(&mut ask),
                            )?,
                        )
                        .await?;
                        let origin =
                            api_origin(current["api_url"].as_str().context("Missing api_url")?)?;
                        let startup = ClientStartup::cloud(
                            origin.as_str().trim_end_matches('/').to_owned(),
                            self.api_token.clone(),
                            self.verbose,
                        );
                        let deployment_id: concepts::prefixed_ulid::DeploymentId =
                            crate::client::send_json(
                                startup
                                    .web_api_client()?
                                    .get(format!("{}/v1/deployment-id", startup.api_url())),
                            )
                            .await?;
                        client
                            .app(
                                name,
                                Method::POST,
                                &["apply"],
                                Some(json!({"force":force,"config":desired,"deployment_id":deployment_id})),
                                command_id.into(),
                            )
                            .await
                    }
                    CloudApp::Reactivate { command_id } => {
                        client
                            .app(name, Method::POST, &["reactivate"], None, command_id.into())
                            .await
                    }
                    CloudApp::Deactivate { command_id } => {
                        client
                            .app(name, Method::POST, &["deactivate"], None, command_id.into())
                            .await
                    }
                    CloudApp::Delete { command_id } => {
                        client
                            .app(name, Method::DELETE, &[], None, command_id.into())
                            .await
                    }
                    CloudApp::Fill {
                        non_interactive,
                        staged,
                        ..
                    } => {
                        let current = client.app(name, Method::GET, &[], None, Kind::Read).await?;
                        let mut ask = ask;
                        let desired = staged_or_local(
                            &client,
                            name,
                            &current,
                            local.filter(|_| !staged),
                            (!non_interactive && terminal()).then_some(&mut ask),
                        )
                        .await?;
                        let values = if non_interactive {
                            Values::Env
                        } else {
                            ensure!(
                                terminal(),
                                "`fill` requires a terminal; use `fill --non-interactive` to read same-named environment variables"
                            );
                            Values::Prompt(&mut ask)
                        };
                        fill_values(&client, name, &desired["policy"], values).await
                    }
                    CloudApp::Env(CloudEnv::List) => {
                        client
                            .app(name, Method::GET, &["env"], None, Kind::Read)
                            .await
                    }
                    CloudApp::Env(CloudEnv::Set {
                        name: key,
                        value,
                        command_id,
                    }) => {
                        config_name(&key)?;
                        client
                            .app(
                                name,
                                Method::PUT,
                                &["env", &key],
                                Some(json!({"value":value})),
                                command_id.into(),
                            )
                            .await
                    }
                    CloudApp::Env(CloudEnv::Unset {
                        name: key,
                        command_id,
                    }) => {
                        config_name(&key)?;
                        client
                            .app(
                                name,
                                Method::DELETE,
                                &["env", &key],
                                None,
                                command_id.into(),
                            )
                            .await
                    }
                    CloudApp::Secrets(CloudSecrets::List) => {
                        client
                            .app(name, Method::GET, &["secrets"], None, Kind::Secret)
                            .await
                    }
                    CloudApp::Secrets(CloudSecrets::Set {
                        name: key,
                        from_env,
                        stdin,
                    }) => {
                        config_name(&key)?;
                        let value = if stdin {
                            let mut value = String::new();
                            std::io::stdin()
                                .read_to_string(&mut value)
                                .context("Cannot read secret from stdin")?;
                            value
                        } else {
                            std::env::var(from_env.context("Select --from-env or --stdin")?)
                                .map_err(|_| {
                                    anyhow::anyhow!(
                                        "Secret source environment variable is missing or not UTF-8"
                                    )
                                })?
                        };
                        client
                            .app(
                                name,
                                Method::PUT,
                                &["secrets", &key],
                                Some(json!({"value":value})),
                                Kind::Secret,
                            )
                            .await?;
                        Ok(json!({"staged_secret":key}))
                    }
                    CloudApp::Secrets(CloudSecrets::Unset { name: key }) => {
                        config_name(&key)?;
                        client
                            .app(name, Method::DELETE, &["secrets", &key], None, Kind::Secret)
                            .await?;
                        Ok(json!({"deleted_secret":key}))
                    }
                    CloudApp::Endpoint { .. } => unreachable!("endpoint is printed in run"),
                    CloudApp::New { .. } => unreachable!(),
                    CloudApp::Deploy { .. } => unreachable!("deploy has its own output"),
                }
            }
        }
    }
}

enum Question {
    Line(String),
    Value { text: String, secret: bool },
}

fn desired_config(config: &AppConfigToml) -> Value {
    json!({"policy":config.policy(), "cloud":config.cloud})
}

fn config_differs(current: &Value, desired: &Value) -> bool {
    current["policy"] != desired["policy"] || current["cloud"] != desired["cloud"]
}

/// Returns the staged configuration, unless `local` differs and the user chooses to stage it.
async fn staged_or_local(
    client: &Client,
    name: &str,
    current: &Value,
    local: Option<(AppConfigToml, std::path::PathBuf)>,
    prompt: Option<Prompt<'_>>,
) -> anyhow::Result<Value> {
    #[derive(Clone, Copy, PartialEq)]
    enum Use {
        Local,
        Staged,
        Cancel,
    }
    let staged = json!({"policy":current["policy"],"cloud":current["cloud"]});
    let Some((config, path)) = local else {
        return Ok(staged);
    };
    let desired = desired_config(&config);
    if !config_differs(current, &desired) {
        return Ok(staged);
    }
    let path = path.display();
    let differs = match (
        current["policy"] != desired["policy"],
        current["cloud"] != desired["cloud"],
    ) {
        (true, true) => "policy and cloud settings",
        (true, false) => "policy",
        _ => "cloud settings",
    };
    let text = format!("{path} differs from the staged configuration ({differs}).\n");
    let Some(prompt) = prompt else {
        bail!(
            "{text}Run `obelisk cloud app --app {name} stage -a {path}`, or pass `--staged` to use the staged configuration"
        );
    };
    let choices = [
        ("stage", Use::Local),
        ("use staged", Use::Staged),
        ("cancel", Use::Cancel),
    ];
    match choose(prompt, &text, &choices, Use::Local)? {
        Use::Local => {
            client
                .app(
                    name,
                    Method::PUT,
                    &["config"],
                    Some(desired.clone()),
                    Kind::Command(None),
                )
                .await?;
            Ok(desired)
        }
        Use::Staged => Ok(staged),
        Use::Cancel => bail!("Cancelled; nothing was staged"),
    }
}

fn terminal() -> bool {
    use std::io::IsTerminal as _;
    std::io::stdin().is_terminal() && std::io::stderr().is_terminal()
}

fn kind_label(kind: &str) -> &'static str {
    if kind == "secrets" { "secret" } else { "env" }
}

fn ask(question: &Question) -> anyhow::Result<String> {
    use std::io::Write as _;
    let (Question::Line(text) | Question::Value { text, .. }) = question;
    if matches!(question, Question::Value { secret: true, .. }) {
        std::io::stderr().flush()?;
        return rpassword::prompt_password(text)
            .context("Cannot read secret; configuration cancelled");
    }
    eprint!("{text}");
    std::io::stderr().flush()?;
    let mut value = String::new();
    ensure!(
        std::io::stdin().read_line(&mut value)? > 0,
        "Configuration cancelled"
    );
    Ok(value.trim_end_matches(['\r', '\n']).to_owned())
}

/// Asks until the answer is empty (the default) or a prefix of one of `choices`.
fn choose<T: Copy + PartialEq>(
    prompt: &mut dyn FnMut(&Question) -> anyhow::Result<String>,
    text: &str,
    choices: &[(&str, T)],
    default: T,
) -> anyhow::Result<T> {
    let menu = choices
        .iter()
        .map(|(word, choice)| {
            let (first, rest) = word.split_at(1);
            if *choice == default {
                format!("[{}]{rest}", first.to_ascii_uppercase())
            } else {
                format!("[{first}]{rest}")
            }
        })
        .collect::<Vec<_>>()
        .join("/");
    loop {
        let answer = prompt(&Question::Line(format!("{text}{menu}? ")))?
            .trim()
            .to_ascii_lowercase();
        if answer.is_empty() {
            break Ok(default);
        }
        if let Some((_, choice)) = choices.iter().find(|(word, _)| word.starts_with(&answer)) {
            break Ok(*choice);
        }
    }
}

fn confirm(
    prompt: &mut dyn FnMut(&Question) -> anyhow::Result<String>,
    text: &str,
    default: bool,
) -> anyhow::Result<bool> {
    choose(prompt, text, &[("yes", true), ("no", false)], default)
}

#[derive(Clone, Copy, PartialEq)]
enum Fill {
    Skip,
    Env,
    Type,
}

fn shown(value: &str) -> String {
    const MAX: usize = 40;
    if value.chars().count() > MAX {
        format!("\"{}…\"", value.chars().take(MAX - 1).collect::<String>())
    } else {
        format!("\"{value}\"")
    }
}

async fn fill_values(
    client: &Client,
    app: &str,
    policy: &Value,
    values: Values<'_>,
) -> anyhow::Result<Value> {
    let env = client
        .app(app, Method::GET, &["env"], None, Kind::Read)
        .await?;
    let secrets = client
        .app(app, Method::GET, &["secrets"], None, Kind::Secret)
        .await?;
    let env_values = env["env"]
        .as_object()
        .context("Invalid cloud environment response")?;
    let secret_values = secrets["secrets"]
        .as_array()
        .context("Invalid cloud secrets response")?;
    let (report, pending) = collect_values(
        &format!(
            "Run `obelisk cloud app --app {app} fill`, or retry with `--fill` or `--fill-non-interactive`"
        ),
        policy,
        env_values,
        secret_values,
        values,
    )?;
    let mut staged = Vec::new();
    for (kind, name, value, _) in pending {
        client
            .app(
                app,
                Method::PUT,
                &[kind, &name],
                Some(json!({"value":value})),
                if kind == "secrets" {
                    Kind::Secret
                } else {
                    Kind::Command(None)
                },
            )
            .await
            .with_context(|| {
                format!("Could not stage {kind} {name}; earlier values may already be staged")
            })?;
        staged.push(json!({"kind":kind,"name":name}));
    }
    let apply_required = !staged.is_empty();
    Ok(json!({"staged":staged,"declarations":report,"apply_required":apply_required}))
}

type PendingValue = (&'static str, String, String, &'static str);
type Prompt<'a> = &'a mut dyn FnMut(&Question) -> anyhow::Result<String>;

/// Where `fill` takes values from; `Check` only verifies that required values are set.
enum Values<'a> {
    Check,
    Env,
    Prompt(Prompt<'a>),
}
impl<'a> Values<'a> {
    /// Maps `--fill` and `--fill-non-interactive`; `prompt` is `None` without a terminal.
    fn new(fill: bool, non_interactive: bool, prompt: Option<Prompt<'a>>) -> anyhow::Result<Self> {
        Ok(if non_interactive {
            Values::Env
        } else if fill {
            Values::Prompt(prompt.context(
                "`--fill` requires a terminal; use `--fill-non-interactive` to read same-named environment variables",
            )?)
        } else {
            Values::Check
        })
    }
}

fn collect_values(
    fill_hint: &str,
    policy: &Value,
    env_values: &serde_json::Map<String, Value>,
    secret_values: &[Value],
    mut values: Values,
) -> anyhow::Result<(Vec<Value>, Vec<PendingValue>)> {
    // (kind, name, optional, set in cloud, cloud value if readable)
    let mut declarations = Vec::new();
    for declaration in policy["public_env"]
        .as_array()
        .context("Invalid policy public_env declarations")?
    {
        if declaration["type"] != "internal" {
            let name = declaration
                .as_str()
                .or_else(|| declaration["name"].as_str())
                .context("Invalid env declaration")?;
            let current = env_values
                .get(name)
                .map(|value| value.as_str().unwrap_or_default());
            declarations.push((
                "env",
                name,
                declaration["optional"] == true,
                current.is_some(),
                current,
            ));
        }
    }
    for (name, declaration) in policy["secrets"]
        .as_object()
        .context("Invalid policy secret declarations")?
    {
        if declaration["type"] != "internal" {
            declarations.push((
                "secrets",
                name.as_str(),
                declaration["optional"] == true,
                secret_values.iter().any(|s| s["name"] == *name),
                None,
            ));
        }
    }
    let mut pending = Vec::new();
    let mut report = Vec::new();
    let mut missing = Vec::new();
    for (kind, name, optional, exists, current) in declarations {
        config_name(name)?;
        let source = match std::env::var(name) {
            Ok(value) => Some(value),
            Err(std::env::VarError::NotPresent) => None,
            Err(_) => bail!("Environment variable {name} is not UTF-8"),
        };
        let label = kind_label(kind);
        let secret = kind == "secrets";
        let action = if exists && current.is_some() && current == source.as_deref() {
            "unchanged"
        } else if let Values::Prompt(prompt) = &mut values {
            let describe = |value: Option<&str>, present: &str| match value {
                None => "not set".to_owned(),
                Some(_) if secret => present.to_owned(),
                Some(value) => shown(value),
            };
            let status = format!(
                "{label} {name} ({}): cloud {}, environment {}\n  ",
                if optional { "optional" } else { "required" },
                if exists && secret {
                    "set".to_owned()
                } else {
                    describe(current, "set")
                },
                describe(source.as_deref(), "present"),
            );
            let mut choices = Vec::new();
            if exists || optional {
                choices.push(("skip", Fill::Skip));
            }
            if source.is_some() {
                choices.push(("env", Fill::Env));
            }
            choices.push(("type", Fill::Type));
            let choice = match choices.as_slice() {
                [(_, only)] => *only,
                _ => choose(prompt, &status, &choices, choices[0].1)?,
            };
            let value = match choice {
                Fill::Skip => None,
                Fill::Env => source.map(|value| (value, "from environment")),
                Fill::Type => Some((
                    prompt(&Question::Value {
                        text: if choices.len() == 1 {
                            format!("{status}Value: ")
                        } else {
                            "  Value: ".to_owned()
                        },
                        secret,
                    })?,
                    "typed",
                )),
            };
            if let Some((value, origin)) = value {
                pending.push((kind, name.to_owned(), value, origin));
                "staged"
            } else if exists {
                "kept"
            } else {
                "unset"
            }
        } else if let Values::Env = values
            && let Some(value) = source
        {
            pending.push((kind, name.to_owned(), value, "from environment"));
            "staged"
        } else if exists {
            "kept"
        } else if optional {
            "unset"
        } else {
            missing.push(format!("{label} {name}"));
            "missing"
        };
        report.push(json!({"kind":kind,"name":name,"optional":optional,"action":action}));
    }
    ensure!(
        missing.is_empty(),
        "Missing required configuration: {}. {}",
        missing.join(", "),
        if let Values::Env = values {
            "Set them as environment variables of the same name"
        } else {
            fill_hint
        }
    );
    if let Values::Prompt(prompt) = &mut values
        && !pending.is_empty()
    {
        let mut plan = "Will stage:\n".to_owned();
        for (kind, name, _, origin) in &pending {
            let _ = writeln!(plan, "  {} {name} {origin}", kind_label(kind));
        }
        let _ = write!(plan, "Stage {} values ", pending.len());
        ensure!(
            confirm(prompt, &plan, true)?,
            "Configuration cancelled; nothing was staged"
        );
    }
    Ok((report, pending))
}

fn initial_values(policy: &Value, values: Values) -> anyhow::Result<Value> {
    let (_, pending) = collect_values(
        "Retry with `--fill`, or `--fill-non-interactive` to read same-named environment variables",
        policy,
        &serde_json::Map::new(),
        &[],
        values,
    )?;
    let mut values = json!({"env":{},"secrets":{}});
    for (kind, name, value, _) in pending {
        values[kind][name] = json!(value);
    }
    Ok(values)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::args::{Args, Subcommand};
    use axum::{Router, body::Bytes, extract::State, http::HeaderMap, routing::any};
    use clap::Parser as _;
    use std::sync::{Arc, Mutex};

    #[derive(Default)]
    struct Requests {
        received: Vec<(String, Value)>,
        error: bool,
        redirect: bool,
        existing_config: bool,
        api_url: Option<String>,
        staged: Value,
    }
    #[tokio::test]
    async fn cloud_token_saves_server_secret_and_rejects_existing_file() {
        let (origin, state, task) = server().await;
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("token");
        let command = [
            "token",
            "new",
            "--name",
            "laptop",
            "--token-file",
            path.to_str().unwrap(),
        ];
        state.lock().unwrap().error = true;
        assert!(args(&origin, &command).execute().await.is_err());
        assert!(!path.exists());
        state.lock().unwrap().error = false;
        args(&origin, &command).execute().await.unwrap();
        let token = std::fs::read_to_string(&path).unwrap();
        assert_eq!(token, format!("obk_{}_{}", "a".repeat(32), "b".repeat(64)));
        assert!(args(&origin, &command).execute().await.is_err());
        assert_eq!(std::fs::read_to_string(&path).unwrap(), token);
        let state = state.lock().unwrap();
        assert_eq!(state.received.len(), 2);
        assert_eq!(state.received[0], state.received[1]);
        assert_eq!(state.received[0].0, "POST /prefix/v1/tokens");
        assert_eq!(
            state.received[0].1,
            json!({"name":"laptop","global":true,"apps":[],"expires_at":null})
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            assert_eq!(
                std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
        task.abort();
    }
    #[tokio::test]
    async fn cloud_token_scopes_are_explicit_app_grants() {
        let (origin, state, task) = server().await;
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("token");
        args(
            &origin,
            &[
                "token",
                "new",
                "--name",
                "ci",
                "--token-file",
                path.to_str().unwrap(),
                "--scope",
                "app-1",
                "--scope",
                "app-2",
            ],
        )
        .execute()
        .await
        .unwrap();
        let state = state.lock().unwrap();
        assert_eq!(state.received[0].0, "POST /prefix/v1/tokens");
        assert_eq!(state.received[0].1["global"], false);
        assert_eq!(state.received[0].1["apps"], json!(["app-1", "app-2"]));
        task.abort();
    }

    #[test]
    fn cloud_token_rejects_empty_scopes_and_unrelated_options() {
        let base = [
            "obelisk",
            "cloud",
            "token",
            "new",
            "--name",
            "ci",
            "--token-file",
            "ci.token",
        ];
        for extra in [
            vec!["--scope", ""],
            vec!["--user", "alice"],
            vec!["--global"],
            vec!["--apps", "demo"],
            vec!["--app", "demo"],
            vec!["--app-api-url", "https://example.com"],
        ] {
            assert!(Args::try_parse_from(base.iter().copied().chain(extra)).is_err());
        }
        assert!(Args::try_parse_from(["obelisk", "cloud", "token", "create"]).is_err());
        use clap::CommandFactory as _;
        let mut command = Args::command();
        let token = command
            .find_subcommand_mut("cloud")
            .unwrap()
            .find_subcommand_mut("token")
            .unwrap();
        assert!(!token.render_long_help().to_string().contains("--app"));
        assert!(
            token
                .find_subcommand_mut("new")
                .unwrap()
                .render_long_help()
                .to_string()
                .contains("including future apps")
        );
    }

    async fn handle(
        State(state): State<Arc<Mutex<Requests>>>,
        headers: HeaderMap,
        uri: axum::http::Uri,
        method: axum::http::Method,
        body: Bytes,
    ) -> axum::response::Response {
        use axum::response::IntoResponse as _;
        assert_eq!(headers["authorization"], "Bearer cloud-token");
        assert!(!headers.contains_key("x-obelisk-user"));
        let body: Value = if body.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&body).unwrap()
        };
        let mut state = state.lock().unwrap();
        state
            .received
            .push((format!("{method} {uri}"), body.clone()));
        if method != Method::GET
            && uri.path().starts_with("/prefix/v1/apps")
            && !uri.path().contains("/secrets/")
        {
            let id = headers["idempotency-key"].to_str().unwrap();
            assert!(
                id.parse::<concepts::prefixed_ulid::ExecutionIdTopLevel>()
                    .is_ok()
            );
        } else {
            assert!(!headers.contains_key("idempotency-key"));
        }
        if state.redirect {
            return (
                axum::http::StatusCode::TEMPORARY_REDIRECT,
                [("location", "/leaked")],
            )
                .into_response();
        }
        if state.error {
            return (axum::http::StatusCode::CONFLICT, "DO-NOT-ECHO-SECRET").into_response();
        }
        let result = match uri.path() {
            "/prefix/v1/apps/demo/config" if method == Method::PUT => {
                state.staged = body;
                json!({"ok":true})
            }
            "/prefix/v1/apps/demo" => {
                json!({
                    "api_url":state.api_url.clone().unwrap_or_else(||format!("http://{}/prefix/tenant",headers["host"].to_str().unwrap())),
                    "policy":state.staged["policy"],
                    "cloud":state.staged["cloud"],
                })
            }
            path if path.starts_with("/prefix/tenant/v1/executions/")
                && path.ends_with("/cancel") =>
            {
                json!({"ok":"cancelled"})
            }
            "/prefix/tenant/v1/deployment-id" => {
                json!(concepts::prefixed_ulid::DeploymentId::generate())
            }
            "/prefix/tenant/v1/components" => json!([]),
            "/prefix/tenant/v1/info" => json!(rest_types::server::ServerInfoResponse {
                app_env_prefix: String::new(),
                app_name: "demo".to_owned(),
                obelisk_version: "0.0.0".to_owned(),
                deployment_id: concepts::prefixed_ulid::DeploymentId::generate(),
                node_run_id: concepts::prefixed_ulid::NodeRunId::generate(),
                app_config_digest: "digest".to_owned(),
                started_at: chrono::Utc::now(),
            }),
            "/prefix/v1/tokens" if method == Method::POST => {
                json!({"token_id":"a".repeat(32),"token":format!("obk_{}_{}", "a".repeat(32), "b".repeat(64))})
            }
            "/prefix/v1/apps/demo/policy" if method == Method::GET => {
                json!({"public_env":["CLOUD_TEST_MISSING_REQUIRED"],"secrets":{}})
            }
            "/prefix/v1/apps/demo/env" if state.existing_config => {
                json!({"env":{"ALREADY_SET":"stored","HOME":std::env::var("HOME").unwrap()}})
            }
            "/prefix/v1/apps/demo/env" => json!({"env":{}}),
            "/prefix/v1/apps/demo/secrets" if state.existing_config => {
                json!({"secrets":[{"name":"ALREADY_SET"}]})
            }
            "/prefix/v1/apps/demo/secrets" => json!({"secrets":[]}),
            _ => json!({"ok":true}),
        };
        axum::Json(result).into_response()
    }
    async fn server() -> (String, Arc<Mutex<Requests>>, tokio::task::JoinHandle<()>) {
        test_utils::set_up();
        let state = Arc::new(Mutex::new(Requests::default()));
        let router = Router::new()
            .fallback(any(handle))
            .with_state(state.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}/prefix/", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        (origin, state, task)
    }
    pub(super) fn args(origin: &str, command: &[&str]) -> CloudArgs {
        let mut args = vec![
            "obelisk",
            "cloud",
            "--api-url",
            origin,
            "--api-token",
            "cloud-token",
        ];
        let position = command.iter().position(|arg| *arg == "app");
        if let Some(position) = position {
            args.extend_from_slice(&command[..=position]);
            args.extend_from_slice(&["--app", "demo"]);
            args.extend_from_slice(&command[position + 1..]);
        } else {
            args.extend_from_slice(command);
        }
        let Subcommand::Cloud(args) = Args::try_parse_from(args).unwrap().command else {
            panic!()
        };
        args
    }
    #[test]
    fn cloud_json_flag_reaches_tenant_commands() {
        for command in [
            vec!["--json", "app", "admin", "node-run-id"],
            vec!["app", "admin", "node-run-id", "--json"],
        ] {
            let args = args("https://cloud.example.com", &command);
            assert!(args.json);
            assert!(matches!(
                args.command,
                Cloud::App(CloudAppTarget {
                    command: CloudApp::Admin(crate::args::Admin::NodeRunId { json: true }),
                    ..
                })
            ));
        }
    }
    #[tokio::test]
    async fn cloud_endpoints_are_derived_without_provisioner_or_credentials() {
        let mut args = args("http://127.0.0.1:1", &["app", "endpoint"]);
        args.api_url = None;
        args.api_token = None;
        assert_eq!(
            args.endpoints().await.unwrap(),
            json!({
                "api_url":"https://api--demo.obelisk.works",
                "webhook_url":"https://demo.obelisk.works",
                "webui_url":"https://webui--demo.obelisk.works",
            })
        );
        args.apps_domain = "dev.obelisk.works".into();
        assert_eq!(
            args.direct_api_url().await.unwrap(),
            "https://api--demo.dev.obelisk.works"
        );
        args.run().await.unwrap();
        for domain in [
            "",
            "https://obelisk.works",
            "obelisk.works/path",
            "obelisk.works:443",
            "*.obelisk.works",
            "-invalid.example",
            "bad..example",
        ] {
            let mut args = super::tests::args("http://127.0.0.1:1", &["app", "endpoint"]);
            args.apps_domain = domain.to_owned();
            assert!(args.endpoints().await.is_err(), "{domain}");
        }
    }

    #[tokio::test]
    async fn cloud_api_commands_use_direct_api_and_cloud_token() {
        let (origin, state, task) = server().await;
        let (target, target_state, target_task) = server().await;
        let api_url = format!("{target}tenant");
        let execution_id = concepts::ExecutionId::generate().to_string();
        state.lock().unwrap().error = true;
        for command in [
            vec!["app", "deployment", "active"],
            vec!["app", "execution", "pause", &execution_id],
            vec!["app", "execution", "cancel", &execution_id],
            vec!["app", "component", "list", "--imports"],
            vec!["app", "admin", "node-run-id"],
        ] {
            args(&origin, &command)
                .run_app_api(api_url.clone())
                .await
                .unwrap();
        }
        assert!(state.lock().unwrap().received.is_empty());
        assert_eq!(
            target_state
                .lock()
                .unwrap()
                .received
                .iter()
                .map(|(route, _)| route.clone())
                .collect::<Vec<_>>(),
            vec![
                "GET /prefix/tenant/v1/deployment-id".to_owned(),
                format!("PUT /prefix/tenant/v1/executions/{execution_id}/pause"),
                format!("PUT /prefix/tenant/v1/executions/{execution_id}/cancel"),
                "GET /prefix/tenant/v1/components?exports=true&imports=true&extensions=false"
                    .to_owned(),
                "GET /prefix/tenant/v1/info".to_owned(),
            ]
        );
        task.abort();
        target_task.abort();
    }

    #[tokio::test]
    async fn cloud_api_commands_do_not_follow_redirects() {
        let (origin, state, task) = server().await;
        state.lock().unwrap().redirect = true;
        assert!(
            args("http://127.0.0.1:1", &["app", "deployment", "active"])
                .run_app_api(format!("{origin}tenant"))
                .await
                .unwrap_err()
                .to_string()
                .contains("307")
        );
        assert_eq!(state.lock().unwrap().received.len(), 1);
        task.abort();
    }
    #[tokio::test]
    async fn local_config_differing_from_staged_needs_a_choice() {
        let (origin, state, task) = server().await;
        let client = Client::new(&args(&origin, &["app", "get"])).unwrap();
        let config: AppConfigToml =
            toml::from_str("[public_env]\nREGION={optional=true}\n").unwrap();
        let staged = json!({"policy":{"public_env":[]},"cloud":{"memory_mb":512}});
        let local = || Some((config.clone(), std::path::PathBuf::from("app.toml")));
        let pick = async |answer: &str| {
            let mut prompt = |_: &Question| Ok(answer.to_owned());
            staged_or_local(&client, "demo", &staged, local(), Some(&mut prompt)).await
        };
        let error = staged_or_local(&client, "demo", &staged, local(), None)
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("(policy and cloud settings)"), "{error}");
        assert!(
            error.contains("stage -a app.toml`, or pass `--staged`"),
            "{error}"
        );
        assert_eq!(pick("u").await.unwrap(), staged);
        assert!(
            pick("c")
                .await
                .unwrap_err()
                .to_string()
                .contains("Cancelled")
        );
        assert!(state.lock().unwrap().received.is_empty());
        let desired = desired_config(&config);
        assert_eq!(pick("").await.unwrap(), desired);
        assert_eq!(
            state.lock().unwrap().received,
            [(
                "PUT /prefix/v1/apps/demo/config".to_owned(),
                desired.clone()
            )]
        );
        let mut unexpected = |_: &Question| -> anyhow::Result<String> { unreachable!() };
        assert_eq!(
            staged_or_local(&client, "demo", &desired, local(), Some(&mut unexpected))
                .await
                .unwrap(),
            desired
        );
        task.abort();
    }
    #[tokio::test]
    async fn cloud_commands_keep_policy_staged_and_force_explicit() {
        let (origin, state, task) = server().await;
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("app.toml");
        tokio::fs::write(
            &path,
            "app_name='from-file'\n[cloud]\nmemory_mb=1024\n[public_env]\nREGION={optional=true}\n[secrets]\nKEY={optional=true}\n[[outbound_http.allowed_host]]\npattern='${REGION}'\n",
        )
        .await
        .unwrap();
        args(
            &origin,
            &["app", "new", "explicit", "-a", path.to_str().unwrap()],
        )
        .execute()
        .await
        .unwrap();
        args(&origin, &["app", "stage", "-a", path.to_str().unwrap()])
            .execute()
            .await
            .unwrap();
        args(&origin, &["app", "apply"]).execute().await.unwrap();
        args(&origin, &["app", "apply", "--force"])
            .execute()
            .await
            .unwrap();
        args(&origin, &["app", "env", "set", "OBELISK_API_URL", "value"])
            .execute()
            .await
            .unwrap();
        let received = &state.lock().unwrap().received;
        assert_eq!(received[0].0, "POST /prefix/v1/apps");
        assert_eq!(received[0].1["app_name"], "explicit");
        assert_eq!(received[0].1["policy"]["format"], "obelisk-app-config-v1");
        assert_eq!(received[0].1["cloud"]["memory_mb"], 1024);
        assert_eq!(received[1].0, "PUT /prefix/v1/apps/demo/config");
        assert_eq!(received[1].1["cloud"]["memory_mb"], 1024);
        assert!(received[1].1.get("app_name").is_none());
        assert_eq!(
            received[1].1["policy"]["outbound_http"][0]["pattern"],
            "${REGION}"
        );
        let applies: Vec<_> = received
            .iter()
            .filter(|(path, _)| path.ends_with("/apply"))
            .collect();
        assert_eq!(applies.len(), 2);
        for (request, force) in applies.iter().zip([false, true]) {
            assert_eq!(request.1["force"], force);
            assert!(
                request.1["deployment_id"]
                    .as_str()
                    .unwrap()
                    .parse::<concepts::prefixed_ulid::DeploymentId>()
                    .is_ok()
            );
            assert_eq!(request.1["config"], received[1].1);
        }
        assert_eq!(
            received.last().unwrap().0,
            "PUT /prefix/v1/apps/demo/env/OBELISK_API_URL"
        );
        task.abort();
    }
    #[tokio::test]
    async fn cloud_secret_set_forwards_process_environment() {
        let (origin, state, task) = server().await;
        args(
            &origin,
            &[
                "app",
                "secrets",
                "set",
                "OBELISK_API_TOKEN",
                "--from-env",
                "PATH",
            ],
        )
        .execute()
        .await
        .unwrap();
        let path_value = std::env::var("PATH").unwrap();
        assert!(
            state
                .lock()
                .unwrap()
                .received
                .iter()
                .any(
                    |(route, body)| route == "PUT /prefix/v1/apps/demo/secrets/OBELISK_API_TOKEN"
                        && body["value"] == path_value
                )
        );
        task.abort();
    }
    #[tokio::test]
    async fn cloud_fill_asks_per_value_and_collects_before_writing() {
        let (origin, state, task) = server().await;
        state.lock().unwrap().existing_config = true;
        let client = Client::new(&args(&origin, &["app", "get"])).unwrap();
        let policy = json!({
            "public_env":["PATH", "ALREADY_SET", {"name":"HOME", "optional":true}, {"name":"OBELISK_CLOUD_TEST_EMPTY", "optional":true}, {"name":"SELF_API", "type":"internal", "source":"api-url"}, {"name":"BROWSER_UI", "type":"internal", "source":"webui-url"}],
            "secrets":{"OBELISK_CLOUD_TEST_PROMPT_SECRET":{},"ALREADY_SET":{},"HOME":{"optional":true},"OBELISK_API_TOKEN":{"type":"internal"}}
        });
        let path_value = std::env::var("PATH").unwrap();
        let home_value = std::env::var("HOME").unwrap();
        // (question, Some(secret) for value prompts, answer)
        let script = [
            (
                format!(
                    "env PATH (required): cloud not set, environment {}\n  [E]nv/[t]ype? ",
                    shown(&path_value)
                ),
                None,
                "",
            ),
            (
                "env ALREADY_SET (required): cloud \"stored\", environment not set\n  [S]kip/[t]ype? ".to_owned(),
                None,
                "invalid",
            ),
            (
                "env ALREADY_SET (required): cloud \"stored\", environment not set\n  [S]kip/[t]ype? ".to_owned(),
                None,
                "t",
            ),
            ("  Value: ".to_owned(), Some(false), "replaced"),
            (
                "env OBELISK_CLOUD_TEST_EMPTY (optional): cloud not set, environment not set\n  [S]kip/[t]ype? ".to_owned(),
                None,
                "type",
            ),
            ("  Value: ".to_owned(), Some(false), ""),
            (
                "secret OBELISK_CLOUD_TEST_PROMPT_SECRET (required): cloud not set, environment not set\n  Value: ".to_owned(),
                Some(true),
                "typed-secret",
            ),
            (
                "secret ALREADY_SET (required): cloud set, environment not set\n  [S]kip/[t]ype? ".to_owned(),
                None,
                "",
            ),
            (
                "secret HOME (optional): cloud not set, environment present\n  [S]kip/[e]nv/[t]ype? ".to_owned(),
                None,
                "e",
            ),
            (
                "Will stage:\n  env PATH from environment\n  env ALREADY_SET typed\n  env OBELISK_CLOUD_TEST_EMPTY typed\n  secret OBELISK_CLOUD_TEST_PROMPT_SECRET typed\n  secret HOME from environment\nStage 5 values [Y]es/[n]o? ".to_owned(),
                None,
                "",
            ),
        ];
        let mut answers = script.iter();
        let result = fill_values(
            &client,
            "demo",
            &policy,
            Values::Prompt(&mut |question: &Question| {
                let (expected, secret, answer) = answers.next().expect("unexpected question");
                match question {
                    Question::Line(text) => assert_eq!((text, None), (expected, *secret)),
                    Question::Value {
                        text,
                        secret: actual,
                    } => {
                        assert_eq!((text, Some(*actual)), (expected, *secret));
                    }
                }
                Ok((*answer).to_owned())
            }),
        )
        .await
        .unwrap();
        assert!(answers.next().is_none());
        assert!(!result.to_string().contains("typed-"));
        assert!(
            result["declarations"].as_array().unwrap().contains(
                &json!({"kind":"env","name":"HOME","optional":true,"action":"unchanged"})
            )
        );
        let mut staged = state
            .lock()
            .unwrap()
            .received
            .iter()
            .filter(|(route, _)| route.starts_with("PUT"))
            .map(|(route, body)| (route.clone(), body["value"].as_str().unwrap().to_owned()))
            .collect::<Vec<_>>();
        staged.sort();
        assert_eq!(
            staged,
            [
                ("PUT /prefix/v1/apps/demo/env/ALREADY_SET", "replaced"),
                ("PUT /prefix/v1/apps/demo/env/OBELISK_CLOUD_TEST_EMPTY", ""),
                ("PUT /prefix/v1/apps/demo/env/PATH", path_value.as_str()),
                ("PUT /prefix/v1/apps/demo/secrets/HOME", home_value.as_str()),
                (
                    "PUT /prefix/v1/apps/demo/secrets/OBELISK_CLOUD_TEST_PROMPT_SECRET",
                    "typed-secret"
                ),
            ]
            .map(|(route, value)| (route.to_owned(), value.to_owned()))
        );
        state.lock().unwrap().received.clear();
        let error = fill_values(
            &client,
            "demo",
            &policy,
            Values::Prompt(&mut |question: &Question| {
                Ok(match question {
                    Question::Value { .. } => "value".to_owned(),
                    Question::Line(text) if text.starts_with("Will stage:") => "n".to_owned(),
                    Question::Line(_) => String::new(),
                })
            }),
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("nothing was staged"));
        assert!(
            state
                .lock()
                .unwrap()
                .received
                .iter()
                .all(|(route, _)| route.starts_with("GET"))
        );
        task.abort();
    }
    #[tokio::test]
    async fn cloud_fill_validates_before_mutation_and_secret_errors_are_redacted() {
        let (origin, state, task) = server().await;
        let client = Client::new(&args(&origin, &["app", "get"])).unwrap();
        let policy = client
            .app("demo", Method::GET, &["policy"], None, Kind::Read)
            .await
            .unwrap();
        let error = fill_values(&client, "demo", &policy, Values::Check)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("CLOUD_TEST_MISSING_REQUIRED"));
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("app.toml");
        tokio::fs::write(&path, "[public_env]\nREGION={}\n")
            .await
            .unwrap();
        let error = args(&origin, &["app", "fill", "-a", path.to_str().unwrap()])
            .execute()
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("Run `obelisk cloud app --app demo stage -a"),
            "{error}"
        );
        assert!(
            state
                .lock()
                .unwrap()
                .received
                .iter()
                .all(|(method, _)| method.starts_with("GET"))
        );
        state.lock().unwrap().error = true;
        let error = client
            .app(
                "demo",
                Method::PUT,
                &["secrets", "KEY"],
                Some(json!({"value":"DO-NOT-ECHO-SECRET"})),
                Kind::Secret,
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("409"));
        assert!(!error.to_string().contains("DO-NOT-ECHO-SECRET"));
        let error = client
            .request(
                Method::POST,
                &["v1", "apps"],
                Some(json!({"app_name":"demo","secrets":{"KEY":"DO-NOT-ECHO-SECRET"}})),
                Kind::SecretCommand(None),
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("409"));
        assert!(error.to_string().contains("Command ID"));
        assert!(!error.to_string().contains("DO-NOT-ECHO-SECRET"));
        state.lock().unwrap().error = false;
        state.lock().unwrap().redirect = true;
        let error = client
            .app(
                "demo",
                Method::PUT,
                &["secrets", "KEY"],
                Some(json!({"value":"DO-NOT-ECHO-SECRET"})),
                Kind::Secret,
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("307"));
        assert!(
            !state
                .lock()
                .unwrap()
                .received
                .iter()
                .any(|(uri, _)| uri.contains("leaked"))
        );
        task.abort();
    }
}
