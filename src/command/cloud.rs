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
        let command_id = if let Kind::Command(id) = kind {
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
        let status = response.status();
        if !status.is_success() {
            if matches!(kind, Kind::Secret) {
                bail!("Cloud secret operation failed: HTTP {status}");
            }
            let body = response.text().await.unwrap_or_default();
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
fn dns_label(name: &str, max: usize) -> bool {
    !name.is_empty()
        && name.len() <= max
        && !name.starts_with('-')
        && !name.ends_with('-')
        && name
            .bytes()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'-')
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
    ensure!(
        dns_label(&name, 40) && name != "default" && !name.starts_with("system-"),
        "Invalid or reserved cloud app name: {name}"
    );
    Ok(name)
}
fn config_name(name: &str) -> anyhow::Result<()> {
    ensure!(
        !name.is_empty()
            && name.len() <= 128
            && !name.as_bytes()[0].is_ascii_digit()
            && name.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'_')
            && !name.starts_with("OBELISK_"),
        "Invalid or reserved configuration name"
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
            let token = match options.open(token_file) {
                Ok(mut file) => {
                    let token = crate::api::generate_token();
                    file.write_all(token.as_bytes())?;
                    file.sync_all()?;
                    token
                }
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                    #[cfg(unix)]
                    {
                        use std::os::unix::fs::PermissionsExt as _;
                        ensure!(
                            std::fs::metadata(token_file)?
                                .permissions()
                                .mode()
                                .trailing_zeros()
                                >= 6,
                            "Token file must be private (chmod 600)"
                        );
                    }
                    std::fs::read_to_string(token_file)?.trim().to_owned()
                }
                Err(error) => return Err(error).context("Cannot persist token file"),
            };
            ensure!(
                token.len() == 64 && token.bytes().all(|c| c.is_ascii_hexdigit()),
                "Invalid token file"
            );
            let expires_at = expires_at
                .map(|date| u64::try_from(date.timestamp()))
                .transpose()
                .context("Expiry must be after the Unix epoch")?;
            if client.verbose {
                eprintln!(
                    "Token saved in {}; reuse this file and parameters to retry",
                    token_file.display()
                );
            }
            let mut value = client.request(Method::POST,&path,Some(json!({"hash":crate::api::token_hash(&token).to_string(),"name":name,"global":scope.is_empty(),"apps":scope,"expires_at":expires_at})),Kind::Read).await
                .with_context(|| format!("Token saved in {}; reuse this file and parameters to retry", token_file.display()))?;
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
        let client = Client::new(self)?;
        let name = app_name(self.selected_app()).await?;
        let app = client
            .app(&name, Method::GET, &[], None, Kind::Read)
            .await?;
        let webhook = match app["webhook_url"].as_str() {
            Some(url) => url.to_owned(),
            None => format!(
                "https://{}",
                app["hostname"]
                    .as_str()
                    .context("Provisioner response is missing hostname")?
            ),
        };
        Ok(json!({
            "api_url": app["api_url"].as_str().context("Provisioner response is missing api_url")?,
            "webhook_url": webhook,
            "webui_url": app["webui_url"].as_str().context("Provisioner response is missing webui_url")?,
        }))
    }
    async fn direct_api_url(&self) -> anyhow::Result<String> {
        let client = Client::new(self)?;
        let name = app_name(self.selected_app()).await?;
        let app = client
            .app(&name, Method::GET, &[], None, Kind::Read)
            .await?;
        let value = app["api_url"]
            .as_str()
            .context("Provisioner response is missing api_url")?;
        Ok(api_origin(value)?.as_str().trim_end_matches('/').to_owned())
    }
    pub(crate) async fn run(self) -> anyhow::Result<()> {
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
                Cloud::Apps(_) | Cloud::App(_) | Cloud::Token(_) => unreachable!(),
            };
            return crate::command_future(command, Some(startup))?.await;
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
                        command_id,
                    },
            }) => {
                let config = match crate::config::app::resolve_app_config(app_config)? {
                    Some(path) => Some(policy(&path).await?),
                    None => None,
                };
                let name = app_name(
                    name.as_deref()
                        .or(app.as_deref())
                        .or_else(|| config.as_ref().and_then(|c| c.app_name.as_deref())),
                )
                .await?;
                let mut body = json!({"app_slug":name});
                if let Some(config) = config {
                    body["policy"] = serde_json::to_value(config.policy())?;
                }
                client
                    .request(Method::POST, &["v1", "apps"], Some(body), command_id.into())
                    .await
            }
            Cloud::App(CloudAppTarget { app, command }) => {
                let name = &app_name(app.as_deref()).await?;
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
                    CloudApp::Update {
                        app_config,
                        command_id,
                    } => {
                        client
                            .app(
                                name,
                                Method::PUT,
                                &["policy"],
                                Some(serde_json::to_value(policy(&app_config).await?.policy())?),
                                command_id.into(),
                            )
                            .await
                    }
                    CloudApp::Apply { force, command_id } => {
                        client
                            .app(
                                name,
                                Method::POST,
                                &["apply"],
                                Some(json!({"force":force})),
                                command_id.into(),
                            )
                            .await
                    }
                    CloudApp::Start { command_id } => {
                        client
                            .app(name, Method::POST, &["start"], None, command_id.into())
                            .await
                    }
                    CloudApp::Stop { command_id } => {
                        client
                            .app(name, Method::POST, &["stop"], None, command_id.into())
                            .await
                    }
                    CloudApp::Delete { command_id } => {
                        client
                            .app(name, Method::DELETE, &[], None, command_id.into())
                            .await
                    }
                    CloudApp::Fill { app_config } => {
                        let declarations = match app_config {
                            Some(path) => serde_json::to_value(policy(&path).await?.policy())?,
                            None => {
                                client
                                    .app(name, Method::GET, &["policy"], None, Kind::Read)
                                    .await?
                            }
                        };
                        fill(&client, name, &declarations).await
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
                }
            }
        }
    }
}

async fn fill(client: &Client, app: &str, policy: &Value) -> anyhow::Result<Value> {
    let env = client
        .app(app, Method::GET, &["env"], None, Kind::Read)
        .await?;
    let secrets = client
        .app(app, Method::GET, &["secrets"], None, Kind::Secret)
        .await?;
    let mut pending = Vec::new();
    let mut missing = Vec::new();
    for declaration in policy["public_env"]
        .as_array()
        .context("Invalid policy public_env declarations")?
    {
        let name = declaration
            .as_str()
            .or_else(|| declaration["name"].as_str())
            .context("Invalid env declaration")?;
        config_name(name)?;
        match std::env::var(name) {
            Ok(value) => pending.push(("env", name.to_owned(), value)),
            Err(std::env::VarError::NotPresent) => {
                if declaration["optional"] != true && env["env"].get(name).is_none() {
                    missing.push(format!("env {name}"));
                }
            }
            Err(_) => bail!("Environment variable {name} is not UTF-8"),
        }
    }
    for (name, declaration) in policy["secrets"]
        .as_object()
        .context("Invalid policy secret declarations")?
    {
        config_name(name)?;
        match std::env::var(name) {
            Ok(value) => pending.push(("secrets", name.clone(), value)),
            Err(std::env::VarError::NotPresent) => {
                let exists = secrets["secrets"]
                    .as_array()
                    .is_some_and(|values| values.iter().any(|s| s["name"] == *name));
                if declaration["optional"] != true && !exists {
                    missing.push(format!("secret {name}"));
                }
            }
            Err(_) => bail!("Secret source {name} is not UTF-8"),
        }
    }
    ensure!(
        missing.is_empty(),
        "Missing configuration in process environment and Fly: {}",
        missing.join(", ")
    );
    let mut staged = Vec::new();
    for (kind, name, value) in pending {
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
    Ok(json!({"staged":staged,"apply_required":true}))
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
    }
    #[tokio::test]
    async fn cloud_token_retries_keep_private_secret_out_of_requests() {
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
        let token = std::fs::read_to_string(&path).unwrap();
        assert_eq!(token.len(), 64);
        state.lock().unwrap().error = false;
        args(&origin, &command).execute().await.unwrap();
        assert_eq!(std::fs::read_to_string(&path).unwrap(), token);
        let state = state.lock().unwrap();
        assert_eq!(state.received.len(), 2);
        assert_eq!(state.received[0], state.received[1]);
        assert_eq!(state.received[0].0, "POST /prefix/v1/tokens");
        assert_eq!(state.received[0].1["global"], true);
        assert_eq!(state.received[0].1["apps"], json!([]));
        assert_eq!(
            state.received[0].1["hash"],
            crate::api::token_hash(&token).to_string()
        );
        assert!(!state.received[0].1.to_string().contains(&token));
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
        state.received.push((format!("{method} {uri}"), body));
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
            "/prefix/v1/apps/demo" => {
                json!({"api_url":state.api_url.clone().unwrap_or_else(||format!("http://{}/prefix/tenant",headers["host"].to_str().unwrap()))})
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
                app_name: "demo".to_owned(),
                obelisk_version: "0.0.0".to_owned(),
                deployment_id: concepts::prefixed_ulid::DeploymentId::generate(),
                node_run_id: concepts::prefixed_ulid::NodeRunId::generate(),
                app_config_digest: "digest".to_owned(),
                started_at: chrono::Utc::now(),
            }),
            "/prefix/v1/apps/demo/policy" if method == Method::GET => {
                json!({"public_env":["CLOUD_TEST_MISSING_REQUIRED"],"secrets":{}})
            }
            "/prefix/v1/apps/demo/env" if state.existing_config => {
                json!({"env":{"ALREADY_SET":"stored"}})
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
    fn args(origin: &str, command: &[&str]) -> CloudArgs {
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
    async fn cloud_api_commands_use_direct_api_and_cloud_token() {
        let (origin, state, task) = server().await;
        let (target, target_state, target_task) = server().await;
        let api_url = format!("{target}tenant");
        state.lock().unwrap().api_url = Some(api_url.clone());
        let execution_id = concepts::ExecutionId::generate().to_string();
        for command in [
            vec!["app", "deployment", "active"],
            vec!["app", "execution", "pause", &execution_id],
            vec!["app", "execution", "cancel", &execution_id],
            vec!["app", "component", "list", "--imports"],
            vec!["app", "admin", "node-run-id"],
        ] {
            args(&origin, &command).run().await.unwrap();
        }
        assert_eq!(
            state.lock().unwrap().received,
            vec![("GET /prefix/v1/apps/demo".to_owned(), Value::Null); 5]
        );
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
        state.lock().unwrap().error = true;
        assert!(
            args(&origin, &["app", "admin", "node-run-id"])
                .run()
                .await
                .is_err()
        );
        assert_eq!(state.lock().unwrap().received.len(), 6);
        assert_eq!(target_state.lock().unwrap().received.len(), 5);
        task.abort();
        target_task.abort();
    }

    #[tokio::test]
    async fn cloud_api_commands_do_not_follow_redirects() {
        let (origin, state, task) = server().await;
        state.lock().unwrap().redirect = true;
        let error = args(&origin, &["app", "deployment", "active"])
            .run()
            .await
            .unwrap_err();
        assert!(error.to_string().contains("307"));
        assert_eq!(state.lock().unwrap().received.len(), 1);
        let (target, target_state, target_task) = server().await;
        state.lock().unwrap().redirect = false;
        state.lock().unwrap().api_url = Some(format!("{target}tenant"));
        target_state.lock().unwrap().redirect = true;
        assert!(
            args(&origin, &["app", "deployment", "active"])
                .run()
                .await
                .unwrap_err()
                .to_string()
                .contains("307")
        );
        assert_eq!(target_state.lock().unwrap().received.len(), 1);
        task.abort();
        target_task.abort();
    }
    #[tokio::test]
    async fn cloud_commands_keep_policy_staged_and_force_explicit() {
        let (origin, state, task) = server().await;
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("app.toml");
        tokio::fs::write(
            &path,
            "app_name='from-file'\n[public_env]\nREGION={}\n[secrets]\nKEY={optional=true}\n[[outbound_http.allowed_host]]\npattern='${REGION}'\n",
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
        args(&origin, &["app", "update", "-a", path.to_str().unwrap()])
            .execute()
            .await
            .unwrap();
        args(&origin, &["app", "apply"]).execute().await.unwrap();
        args(&origin, &["app", "apply", "--force"])
            .execute()
            .await
            .unwrap();
        args(&origin, &["app", "env", "set", "OPTIONAL", "value"])
            .execute()
            .await
            .unwrap();
        let received = &state.lock().unwrap().received;
        assert_eq!(received[0].0, "POST /prefix/v1/apps");
        assert_eq!(received[0].1["app_slug"], "explicit");
        assert_eq!(received[0].1["policy"]["format"], "obelisk-app-config-v1");
        assert_eq!(received[1].0, "PUT /prefix/v1/apps/demo/policy");
        assert!(received[1].1.get("app_name").is_none());
        assert_eq!(received[1].1["outbound_http"][0]["pattern"], "${REGION}");
        assert_eq!(received[2].1, json!({"force":false}));
        assert_eq!(received[3].1, json!({"force":true}));
        assert_eq!(received[4].0, "PUT /prefix/v1/apps/demo/env/OPTIONAL");
        task.abort();
    }
    #[tokio::test]
    async fn cloud_fill_stages_optional_values_and_preserves_existing_required_config() {
        let (origin, state, task) = server().await;
        state.lock().unwrap().existing_config = true;
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("app.toml");
        tokio::fs::write(&path,
            "[public_env]\nPATH={optional=true}\nALREADY_SET={}\n[secrets]\nHOME={optional=true}\nALREADY_SET={}\nCLOUD_TEST_OPTIONAL_ABSENT={optional=true}\n")
            .await.unwrap();
        let result = args(&origin, &["app", "fill", "-a", path.to_str().unwrap()])
            .execute()
            .await
            .unwrap();
        assert_eq!(result["staged"].as_array().unwrap().len(), 2);
        let path_value = std::env::var("PATH").unwrap();
        let home_value = std::env::var("HOME").unwrap();
        assert!(!result.to_string().contains(&home_value));
        args(
            &origin,
            &["app", "secrets", "set", "KEY", "--from-env", "PATH"],
        )
        .execute()
        .await
        .unwrap();
        let received = &state.lock().unwrap().received;
        assert!(
            received
                .iter()
                .any(|(route, body)| route == "PUT /prefix/v1/apps/demo/env/PATH"
                    && body["value"] == path_value)
        );
        assert!(received.iter().any(|(route, body)| route
            == "PUT /prefix/v1/apps/demo/secrets/HOME"
            && body["value"] == home_value));
        assert!(received.iter().any(|(route, body)| route
            == "PUT /prefix/v1/apps/demo/secrets/KEY"
            && body["value"] == path_value));
        assert!(!received.iter().any(|(route, _)| route.contains("/apply")));
        task.abort();
    }
    #[tokio::test]
    async fn cloud_fill_validates_before_mutation_and_secret_errors_are_redacted() {
        let (origin, state, task) = server().await;
        let error = args(&origin, &["app", "fill"]).execute().await.unwrap_err();
        assert!(error.to_string().contains("CLOUD_TEST_MISSING_REQUIRED"));
        assert!(
            state
                .lock()
                .unwrap()
                .received
                .iter()
                .all(|(method, _)| method.starts_with("GET"))
        );
        state.lock().unwrap().error = true;
        let client = Client::new(&args(&origin, &["app", "get"])).unwrap();
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
