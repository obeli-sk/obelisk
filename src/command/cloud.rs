use super::output::print_json;
use crate::args::CloudToken;
use crate::args::{
    AdminArgs, App, AppArgs, ClientConnection, Cloud, CloudApp, CloudAppTarget, CloudArgs,
    CloudCommandId, CloudDb, CloudDraft, CloudEnv, CloudPush, CloudSecrets, CloudStage,
    CloudValues, ComponentArgs, DeploymentArgs, ExecutionArgs, SelectedApp, Subcommand,
};
use crate::client::ClientStartup;
use crate::config::app::{AppConfigToml, generate_app_name_hint, with_app_name};
use anyhow::{Context as _, bail, ensure};
use concepts::prefixed_ulid::ExecutionIdTopLevel;
use reqwest::{Method, Url, header};
use secrecy::{ExposeSecret as _, SecretString};
use serde_json::{Value, json};
use std::{
    fmt::Write as _,
    io::Read as _,
    path::{Path, PathBuf},
    time::Duration,
};

mod auth;
mod db;
mod output;
mod push;
mod stage;
use output::Summary;

const DEFAULT_API_URL: &str = "https://cloud.obeli.sk/api";
const DEFAULT_APPS_DOMAIN: &str = "obelisk.works";

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

enum Body {
    Json(Value),
    /// Verbatim app.toml, which the provisioner parses and stages.
    Toml(String),
}

struct Client {
    http: crate::client::HttpClient,
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
    fn build(api_url: &str, token: Option<&SecretString>, verbose: bool) -> anyhow::Result<Self> {
        let origin = api_origin(api_url)?;
        let mut headers = header::HeaderMap::new();
        if let Some(token) = token {
            let mut value =
                header::HeaderValue::from_str(&format!("Bearer {}", token.expose_secret()))
                    .map_err(|_| {
                        anyhow::anyhow!("Cloud API token contains invalid header characters")
                    })?;
            value.set_sensitive(true);
            headers.insert(header::AUTHORIZATION, value);
        }
        Ok(Self {
            http: crate::client::HttpClient::new(
                reqwest::Client::builder()
                    .user_agent(crate::client::USER_AGENT)
                    .default_headers(headers)
                    .redirect(reqwest::redirect::Policy::none())
                    .timeout(Duration::from_secs(360))
                    .build()?,
                verbose,
            ),
            origin,
            verbose,
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
        self.send(method, self.url(path)?, body.map(Body::Json), kind, None)
            .await
    }
    /// `draft_id` is sent as `If-Match`, failing with 412 when the label names another draft.
    async fn send(
        &self,
        method: Method,
        url: Url,
        body: Option<Body>,
        kind: Kind,
        draft_id: Option<u64>,
    ) -> anyhow::Result<Value> {
        let mut request = self.http.request(method, url);
        if let Some(id) = draft_id {
            request = request.header(header::IF_MATCH, format!("\"draft-{id}\""));
        }
        let command_id = if let Kind::Command(id) | Kind::SecretCommand(id) = kind {
            let id = id.unwrap_or_else(ExecutionIdTopLevel::generate);
            crate::client::trace(format_args!("Cloud command execution ID: {id}"));
            request = request.header("Idempotency-Key", id.to_string());
            Some(id)
        } else {
            None
        };
        match body {
            Some(Body::Json(body)) => request = request.json(&body),
            Some(Body::Toml(text)) => {
                request = request
                    .header(header::CONTENT_TYPE, "application/toml")
                    .body(text);
            }
            None => {}
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
        self.optional(&["v1", "apps", name]).await
    }
    /// GET that maps 404 to `None`.
    async fn optional(&self, path: &[&str]) -> anyhow::Result<Option<Value>> {
        let response = self
            .http
            .get(self.url(path)?)
            .send()
            .await
            .map_err(reqwest::Error::without_url)?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            Ok(None)
        } else {
            self.response(response, Kind::Read, None).await.map(Some)
        }
    }
    /// Result of a command the provisioner already accepted under `--command-id`, so a retry
    /// reports it instead of re-reading inputs that the first attempt may have changed.
    async fn accepted(&self, command_id: &CloudCommandId) -> anyhow::Result<Option<Value>> {
        let Some(id) = command_id.id else {
            return Ok(None);
        };
        let response = self
            .http
            .get(self.url(&["v1", "commands", &id.to_string()])?)
            .send()
            .await
            .map_err(reqwest::Error::without_url)
            .with_context(|| {
                format!("Cloud request failed; retry the same command with --command-id {id}")
            })?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        crate::client::trace(format_args!("Cloud command {id} was already accepted"));
        self.response(response, Kind::Command(Some(id)), Some(id))
            .await
            .map(Some)
    }
    /// Bodiless app lifecycle command, reporting an already accepted `--command-id`.
    async fn command(
        &self,
        name: &str,
        method: Method,
        suffix: &[&str],
        command_id: CloudCommandId,
    ) -> anyhow::Result<Value> {
        if let Some(result) = self.accepted(&command_id).await? {
            return Ok(result);
        }
        self.app(name, method, suffix, None, command_id.into())
            .await
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
    /// Opens draft `key` on `parent_revision`, or returns the existing draft with that parent.
    async fn open_draft(
        &self,
        name: &str,
        key: &str,
        parent_revision: u64,
    ) -> anyhow::Result<Value> {
        let url = self.url(&["v1", "apps", name, "drafts", key])?;
        let body = Body::Json(json!({"parent_revision":parent_revision}));
        self.send(Method::PUT, url, Some(body), Kind::Read, None)
            .await
    }
    /// ID of draft `key`, opening it on the applied revision when missing.
    async fn resolve_draft(&self, name: &str, key: &str) -> anyhow::Result<Value> {
        if let Some(draft) = self.optional(&["v1", "apps", name, "drafts", key]).await? {
            return Ok(draft);
        }
        let current = self.app(name, Method::GET, &[], None, Kind::Read).await?;
        self.open_draft(name, key, applied_revision(&current)?)
            .await
    }
    /// Discards or edits draft `id` under label `key`.
    /// Edits are not lifecycle commands and take no idempotency key.
    async fn fenced(
        &self,
        name: &str,
        key: &str,
        id: u64,
        method: Method,
        suffix: &[&str],
        body: Option<Body>,
    ) -> anyhow::Result<Value> {
        let mut path = vec!["v1", "apps", name, "drafts", key];
        path.extend_from_slice(suffix);
        let kind = if method == Method::PUT && suffix.first() == Some(&"secrets") {
            Kind::Secret
        } else {
            Kind::Read
        };
        self.send(method, self.url(&path)?, body, kind, Some(id))
            .await
    }
    /// Edits draft `key`, opening it when missing.
    async fn draft(
        &self,
        name: &str,
        key: &str,
        method: Method,
        suffix: &[&str],
        body: Option<Body>,
    ) -> anyhow::Result<Value> {
        let id = draft_id(&self.resolve_draft(name, key).await?)?;
        self.fenced(name, key, id, method, suffix, body).await
    }
    /// Applies `body`; `draft_id` pins a draft apply to the draft that was checked.
    async fn apply(
        &self,
        name: &str,
        body: Value,
        draft_id: Option<u64>,
        kind: Kind,
    ) -> anyhow::Result<Value> {
        let url = self.url(&["v1", "apps", name, "apply"])?;
        self.send(Method::POST, url, Some(Body::Json(body)), kind, draft_id)
            .await
    }
    async fn events(&self, name: &str, limit: u8) -> anyhow::Result<Value> {
        self.events_at(&["v1", "apps", name, "events"], limit).await
    }
    async fn account_events(&self, limit: u8) -> anyhow::Result<Value> {
        self.events_at(&["v1", "account", "events"], limit).await
    }
    async fn events_at(&self, path: &[&str], limit: u8) -> anyhow::Result<Value> {
        let mut url = self.url(path)?;
        url.query_pairs_mut()
            .append_pair("limit", &limit.to_string());
        let response = self
            .http
            .get(url)
            .send()
            .await
            .map_err(reqwest::Error::without_url)
            .context("Cloud request failed")?;
        self.response(response, Kind::Read, None).await
    }
    async fn logs(
        &self,
        name: &str,
        limit: u16,
        since: Option<jiff::Timestamp>,
    ) -> anyhow::Result<Value> {
        let mut url = self.url(&["v1", "apps", name, "logs"])?;
        url.query_pairs_mut()
            .append_pair("limit", &limit.to_string());
        if let Some(since) = since {
            // The cursor is the Unix nanosecond after which lines are returned.
            let after = u64::try_from(since.as_nanosecond()).context("`--since` is before 1970")?;
            url.query_pairs_mut()
                .append_pair("after", &after.to_string());
        }
        let response = self
            .http
            .get(url)
            .send()
            .await
            .map_err(reqwest::Error::without_url)
            .context("Cloud request failed")?;
        self.response(response, Kind::Read, None).await
    }
    async fn app_toml(
        &self,
        name: &str,
        method: Method,
        suffix: &[&str],
        app_toml: &str,
        kind: Kind,
    ) -> anyhow::Result<Value> {
        let mut path = vec!["v1", "apps", name];
        path.extend_from_slice(suffix);
        self.send(
            method,
            self.url(&path)?,
            Some(Body::Toml(app_toml.to_owned())),
            kind,
            None,
        )
        .await
    }
}
fn draft_id(draft: &Value) -> anyhow::Result<u64> {
    draft["draft_id"]
        .as_u64()
        .context("Cloud draft response is missing draft_id")
}
fn applied_revision(app: &Value) -> anyhow::Result<u64> {
    app["applied_revision"]
        .as_u64()
        .context("Cloud app response is missing applied_revision")
}
async fn app_name(name: Option<&str>) -> anyhow::Result<String> {
    let name = match name {
        Some(name) => name.to_owned(),
        None => {
            policy(Path::new("app.toml"))
                .await
                .context("Select an app with --app, OBELISK_APP_NAME or app_name in ./app.toml")?
                .config
                .app_name
        }
    };
    validate_cloud_app_name(&name)?;
    Ok(name)
}
fn validate_cloud_app_name(name: &str) -> anyhow::Result<()> {
    crate::config::app::validate_app_name(name)?;
    ensure!(
        name.len() <= 20
            && !name.contains("--")
            && !name.starts_with("system-")
            && !name.starts_with("integrations-"),
        "Cloud app names must contain 4-20 lowercase letters, digits or single interior hyphens; system- and integrations- are reserved"
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
/// A local app config, parsed for prompts and kept verbatim for the provisioner.
#[derive(Clone)]
struct LocalConfig {
    config: AppConfigToml,
    text: String,
}
impl std::ops::Deref for LocalConfig {
    type Target = AppConfigToml;
    fn deref(&self) -> &AppConfigToml {
        &self.config
    }
}
impl LocalConfig {
    fn parse(text: String) -> anyhow::Result<Self> {
        let config: AppConfigToml = toml::from_str(&text)?;
        config.validate_cloud_integrations()?;
        Ok(Self { config, text })
    }
}
async fn policy(path: &Path) -> anyhow::Result<LocalConfig> {
    let text = tokio::fs::read_to_string(path)
        .await
        .with_context(|| format!("Cannot read app config {}", path.display()))?;
    let config = LocalConfig::parse(text)
        .with_context(|| format!("Invalid app config {}", path.display()))?;
    validate_cloud_app_name(&config.app_name).with_context(|| generate_app_name_hint(path))?;
    Ok(config)
}
/// App config path of commands that read one, with whether `-a` was given.
fn discover_app_config(app_config: Option<PathBuf>) -> anyhow::Result<Option<(PathBuf, bool)>> {
    let explicit = app_config.is_some();
    Ok(crate::config::app::resolve_app_config(app_config)?.map(|path| (path, explicit)))
}
/// Never use another app's config: an explicit or required one is an error, a discovered optional one is ignored.
fn config_for_app(
    name: &str,
    config: Option<(LocalConfig, (PathBuf, bool))>,
    required: bool,
) -> anyhow::Result<Option<(LocalConfig, PathBuf)>> {
    let Some((config, (path, explicit))) = config else {
        ensure!(
            !required,
            "No app config: ./app.toml not found; pass --app-config"
        );
        return Ok(None);
    };
    if config.app_name != name {
        let other = &config.app_name;
        ensure!(
            !explicit && !required,
            "{} is for app `{other}`, but the selected app is `{name}` (from --app or OBELISK_APP_NAME); pass `--app {other}` or fix `app_name`",
            path.display()
        );
        return Ok(None);
    }
    Ok(Some((config, path)))
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
                .map(|date| u64::try_from(date.as_second()))
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
/// The resolved connection and output flags, once the command is taken out of `CloudArgs`.
struct Session {
    api_url: String,
    apps_domain: Option<String>,
    api_token: Option<SecretString>,
    verbose: bool,
    json: bool,
}

impl Session {
    fn client(&self) -> anyhow::Result<Client> {
        Client::build(&self.api_url, self.api_token.as_ref(), self.verbose)
    }
    /// The tenant API is reachable without asking the provisioner.
    fn direct_api_url(&self, name: &str) -> anyhow::Result<String> {
        let domain = self
            .apps_domain
            .as_deref()
            .unwrap_or(DEFAULT_APPS_DOMAIN)
            .to_ascii_lowercase();
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
            hostname.len() + "internal--".len() <= 253,
            "App hostname exceeds the DNS length limit"
        );
        Ok(format!("https://api--{hostname}"))
    }
    async fn selected_api_url(&self, selected: &SelectedApp) -> anyhow::Result<String> {
        self.direct_api_url(&app_name(selected.app.as_deref()).await?)
    }
    /// Runs a tenant API command against an app's API with the cloud token.
    async fn run_app_api(
        &self,
        api_url: String,
        command: impl FnOnce(ClientConnection) -> Subcommand,
    ) -> anyhow::Result<()> {
        let startup = ClientStartup::cloud(api_url.clone(), self.api_token.clone(), self.verbose);
        let command = command(ClientConnection {
            api_url,
            api_token: None,
            verbose: self.verbose,
        });
        crate::command_future(command, Some(startup))?.await
    }
}

impl CloudArgs {
    fn into_session(self) -> (Session, Cloud) {
        let CloudArgs {
            json,
            verbose,
            api_token,
            account: _,
            api_url,
            apps_domain,
            command,
        } = self;
        let session = Session {
            api_url: api_url.unwrap_or_else(|| DEFAULT_API_URL.to_owned()),
            apps_domain,
            api_token,
            verbose,
            json,
        };
        (session, command)
    }
    pub(crate) async fn run(mut self) -> anyhow::Result<()> {
        if self.verbose {
            crate::client::enable_trace();
        }
        let credentials = match self.command {
            Cloud::Auth(command) => return auth::run(command, self.json, self.verbose).await,
            Cloud::Token(ref mut args) => &mut args.credentials,
            Cloud::App(ref mut args) => &mut args.credentials,
            Cloud::Status(ref mut args) => &mut args.credentials,
            Cloud::Events(ref mut args) => &mut args.credentials,
        };
        self.api_token = credentials.api_token.take();
        self.account = credentials.account.take();
        auth::resolve(&mut self)?;
        let (session, command) = self.into_session();
        dispatch(&session, command).await
    }
    /// Dispatches with the credentials as set, skipping the saved login.
    #[cfg(test)]
    async fn dispatch(self) -> anyhow::Result<()> {
        let (session, command) = self.into_session();
        dispatch(&session, command).await
    }
    #[cfg(test)]
    async fn execute(self) -> anyhow::Result<Value> {
        let (session, command) = self.into_session();
        execute(&session, command).await
    }
}

async fn dispatch(session: &Session, command: Cloud) -> anyhow::Result<()> {
    let (command, credentials) = match command {
        Cloud::App(CloudAppTarget {
            command,
            credentials,
        }) => (command, credentials),
        command => return print_result(session, command).await,
    };
    match command {
        CloudApp::Push(args) => push::push(session, args).await,
        CloudApp::Stage(args) => stage::stage(session, args).await,
        CloudApp::Db {
            selected,
            command: CloudDb::Pull { output, force },
        } => {
            let name = app_name(selected.app.as_deref()).await?;
            db::DbRestore::new(session, name, output, force)
                .await?
                .run()
                .await
        }
        CloudApp::Pull { selected, pull, db } => {
            let name = app_name(selected.app.as_deref()).await?;
            // Fails on a missing or old `litestream` before any file is written.
            let restore = match db {
                Some(output) => {
                    Some(db::DbRestore::new(session, name.clone(), output, pull.force).await?)
                }
                None => None,
            };
            session
                .run_app_api(session.direct_api_url(&name)?, |token| {
                    Subcommand::App(AppArgs {
                        command: App::Pull(pull),
                        token,
                    })
                })
                .await?;
            match restore {
                Some(restore) => restore.run().await,
                None => Ok(()),
            }
        }
        CloudApp::Deployment { selected, command } => {
            let api_url = session.selected_api_url(&selected).await?;
            session
                .run_app_api(api_url, |token| {
                    Subcommand::Deployment(DeploymentArgs {
                        command: command.into(),
                        token,
                    })
                })
                .await
        }
        CloudApp::Execution { selected, command } => {
            let api_url = session.selected_api_url(&selected).await?;
            session
                .run_app_api(api_url, |token| {
                    Subcommand::Execution(ExecutionArgs { command, token })
                })
                .await
        }
        CloudApp::Component { selected, command } => {
            let api_url = session.selected_api_url(&selected).await?;
            session
                .run_app_api(api_url, |token| {
                    Subcommand::Component(ComponentArgs { command, token })
                })
                .await
        }
        CloudApp::Admin { selected, command } => {
            let api_url = session.selected_api_url(&selected).await?;
            session
                .run_app_api(api_url, |token| {
                    Subcommand::Admin(AdminArgs { command, token })
                })
                .await
        }
        command => {
            print_result(
                session,
                Cloud::App(CloudAppTarget {
                    credentials,
                    command,
                }),
            )
            .await
        }
    }
}

/// Runs a command whose result is a JSON value and prints it.
async fn print_result(session: &Session, command: Cloud) -> anyhow::Result<()> {
    let summary = Summary::from(&command);
    let value = execute(session, command).await?;
    if session.json {
        print_json(&value)
    } else {
        eprint!(
            "{}",
            if session.verbose {
                human_output(&value)
            } else {
                summary.render(&value)
            }
        );
        Ok(())
    }
}

async fn execute(session: &Session, command: Cloud) -> anyhow::Result<Value> {
    let client = session.client()?;
    match command {
        Cloud::Auth(_) => unreachable!("handled in run"),
        Cloud::Token(args) => token_command(&client, args.command).await,
        Cloud::Status(_) => {
            client
                .request(Method::GET, &["v1", "account"], None, Kind::Read)
                .await
        }
        Cloud::Events(args) => client.account_events(args.limit).await,
        Cloud::App(CloudAppTarget {
            command: CloudApp::List,
            ..
        }) => {
            client
                .request(Method::GET, &["v1", "apps"], None, Kind::Read)
                .await
        }
        Cloud::App(CloudAppTarget {
            command:
                CloudApp::New {
                    selected: SelectedApp { app },
                    app_config,
                    values,
                    command_id,
                },
            ..
        }) => {
            let config_path = discover_app_config(app_config)?;
            let config = match &config_path {
                Some((path, _)) => Some(policy(path).await?),
                None => None,
            };
            let name = app_name(
                app.as_deref()
                    .or_else(|| config.as_ref().map(|c| c.app_name.as_str())),
            )
            .await?;
            let config =
                config_for_app(&name, config.zip(config_path), false)?.map(|(config, _)| config);
            let explicit = explicit_values(&values)?;
            ensure!(
                !(values.fill || values.fill_non_interactive || !explicit.is_empty())
                    || config.is_some(),
                "Initial values require an app config"
            );
            let mut body = json!({"app_toml":with_app_name("", &name)?});
            if let Some(config) = config {
                body["app_toml"] = json!(config.text);
                let mut ask = ask;
                let values = initial_values(
                    &user_supplied_policy(&desired_config(&config)),
                    explicit,
                    Values::new(
                        values.fill,
                        values.fill_non_interactive,
                        terminal().then_some(&mut ask),
                    )?,
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
        Cloud::App(CloudAppTarget { command, .. }) => {
            let app = command.selected_app().map(str::to_owned);
            let config_path = match &command {
                CloudApp::Status { app_config, .. } => discover_app_config(app_config.clone())?,
                _ => None,
            };
            let config = match &config_path {
                // Status only reports a readable discovered ./app.toml.
                Some((path, false)) if matches!(command, CloudApp::Status { .. }) => {
                    policy(path).await.ok()
                }
                Some((path, _)) => Some(policy(path).await?),
                None => None,
            };
            let name = &app_name(
                app.as_deref()
                    .or_else(|| config.as_ref().map(|c| c.app_name.as_str())),
            )
            .await?;
            let local = config_for_app(name, config.zip(config_path), false)?;
            match command {
                CloudApp::Deployment { .. }
                | CloudApp::Execution { .. }
                | CloudApp::Component { .. }
                | CloudApp::Admin { .. }
                | CloudApp::Pull { .. }
                | CloudApp::Db { .. } => unreachable!("dispatched in run"),
                CloudApp::List => unreachable!("handled above"),
                CloudApp::Status { .. } => {
                    let mut value = client
                        .app(name, Method::GET, &["status"], None, Kind::Read)
                        .await?;
                    let drafts = client
                        .app(name, Method::GET, &["drafts"], None, Kind::Read)
                        .await?;
                    value["drafts"] = drafts["drafts"].clone();
                    if let Some((config, path)) = local {
                        let comparison = compare_config(&client, name, &config.text).await;
                        value["local_config"] = json!({
                            "path": path.display().to_string(),
                            "comparison": comparison.as_ref().ok(),
                            "error": comparison.err().map(|error| format!("{error:#}")),
                        });
                    }
                    Ok(value)
                }
                CloudApp::Events { limit, .. } => client.events(name, limit).await,
                CloudApp::Logs { limit, since, .. } => client.logs(name, limit, since).await,
                CloudApp::Unstage {
                    draft: CloudDraft { key },
                    ..
                } => match client
                    .optional(&["v1", "apps", name, "drafts", &key])
                    .await?
                {
                    Some(draft) => {
                        client
                            .fenced(name, &key, draft_id(&draft)?, Method::DELETE, &[], None)
                            .await
                    }
                    None => Ok(json!({"discarded":false})),
                },
                CloudApp::Apply {
                    force,
                    reapply,
                    draft: CloudDraft { key },
                    command_id,
                    ..
                } => {
                    if let Some(result) = client.accepted(&command_id).await? {
                        return Ok(result);
                    }
                    let current = client.app(name, Method::GET, &[], None, Kind::Read).await?;
                    let draft = if reapply {
                        None
                    } else {
                        Some(
                            client
                                .optional(&["v1", "apps", name, "drafts", &key])
                                .await?
                                .with_context(|| {
                                    format!(
                                        "Nothing staged in draft `{key}`; stage changes with `obelisk cloud app stage`, `env set` or `secrets set`{}, or reconcile the applied revision with `--reapply`",
                                        output::draft_arg(&key)
                                    )
                                })?,
                        )
                    };
                    let body =
                        apply_request(name, &key, draft.as_ref(), &current, force, session.json)?;
                    let id = draft.as_ref().map(draft_id).transpose()?;
                    client.apply(name, body, id, command_id.into()).await
                }
                CloudApp::Enable { command_id, .. } => {
                    client
                        .command(name, Method::POST, &["enable"], command_id)
                        .await
                }
                CloudApp::Disable { command_id, .. } => {
                    client
                        .command(name, Method::POST, &["disable"], command_id)
                        .await
                }
                CloudApp::Delete { command_id, .. } => {
                    client.command(name, Method::DELETE, &[], command_id).await
                }
                CloudApp::Env {
                    command: CloudEnv::List,
                    ..
                } => {
                    client
                        .app(name, Method::GET, &["env"], None, Kind::Read)
                        .await
                }
                CloudApp::Env {
                    command:
                        CloudEnv::Set {
                            name: variable,
                            value,
                            draft: CloudDraft { key },
                        },
                    ..
                } => {
                    config_name(&variable)?;
                    client
                        .draft(
                            name,
                            &key,
                            Method::PUT,
                            &["env", &variable],
                            Some(Body::Json(json!({"value":value}))),
                        )
                        .await
                }
                CloudApp::Env {
                    command:
                        CloudEnv::Unset {
                            name: variable,
                            draft: CloudDraft { key },
                        },
                    ..
                } => {
                    config_name(&variable)?;
                    client
                        .draft(name, &key, Method::DELETE, &["env", &variable], None)
                        .await
                }
                CloudApp::Secrets {
                    command: CloudSecrets::List,
                    ..
                } => {
                    client
                        .app(name, Method::GET, &["secrets"], None, Kind::Secret)
                        .await
                }
                CloudApp::Secrets {
                    command:
                        CloudSecrets::Set {
                            name: secret,
                            from_env,
                            stdin,
                            draft: CloudDraft { key },
                        },
                    ..
                } => {
                    config_name(&secret)?;
                    let value = if stdin {
                        let mut value = String::new();
                        std::io::stdin()
                            .read_to_string(&mut value)
                            .context("Cannot read secret from stdin")?;
                        value
                    } else {
                        std::env::var(from_env.context("Select --from-env or --stdin")?).map_err(
                            |_| {
                                anyhow::anyhow!(
                                    "Secret source environment variable is missing or not UTF-8"
                                )
                            },
                        )?
                    };
                    client
                        .draft(
                            name,
                            &key,
                            Method::PUT,
                            &["secrets", &secret],
                            Some(Body::Json(json!({"value":value}))),
                        )
                        .await
                }
                CloudApp::Secrets {
                    command:
                        CloudSecrets::Unset {
                            name: secret,
                            draft: CloudDraft { key },
                        },
                    ..
                } => {
                    config_name(&secret)?;
                    client
                        .draft(name, &key, Method::DELETE, &["secrets", &secret], None)
                        .await
                }
                CloudApp::New { .. } => unreachable!(),
                CloudApp::Push { .. } | CloudApp::Stage { .. } => {
                    unreachable!("command has its own output")
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

/// Asks the provisioner how `app_toml` differs from the staged configuration.
async fn compare_config(client: &Client, name: &str, app_toml: &str) -> anyhow::Result<Value> {
    client
        .app_toml(
            name,
            Method::POST,
            &["config", "compare"],
            app_toml,
            Kind::Read,
        )
        .await
}

#[cfg(test)]
const DEFAULT_DRAFT: &str = "default";

/// Applies the current labeled proposal, or re-applies the installed revision without one.
fn apply_request(
    name: &str,
    key: &str,
    draft: Option<&Value>,
    current: &Value,
    force: bool,
    quiet: bool,
) -> anyhow::Result<Value> {
    let mut body = json!({"force":force});
    let applied = &current["applied_revision"];
    let Some(draft) = draft else {
        if !quiet {
            eprintln!("Re-applying revision {applied}");
        }
        body["revision"] = applied.clone();
        return Ok(body);
    };
    ensure!(
        draft["stale"] != true,
        "Draft `{key}` was opened on revision {}, but revision {applied} was applied since; discard it with `obelisk cloud app unstage --app {name}{}`",
        draft["parent_revision"],
        output::draft_arg(key)
    );
    let missing = output::draft_values(draft, "missing");
    ensure!(
        missing.is_empty(),
        "Missing values in draft `{key}`: {}. Stage them with `obelisk cloud app env set` or `obelisk cloud app secrets set`{}",
        missing.join(", "),
        output::draft_arg(key)
    );
    if !quiet {
        eprintln!(
            "Applying draft `{key}` revision {} over revision {applied}: {}",
            draft["revision"],
            output::draft_changes(draft)
        );
        let unused = output::draft_values(draft, "unused");
        if !unused.is_empty() {
            eprintln!("Not declared in app.toml: {}", unused.join(", "));
        }
    }
    body["draft"] = json!(key);
    Ok(body)
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

fn user_supplied_policy(config: &Value) -> Value {
    let mut policy = config["policy"].clone();
    if let Some(declarations) = policy["public_env"].as_array_mut() {
        declarations.retain(|declaration| {
            let name = declaration
                .as_str()
                .or_else(|| declaration["name"].as_str());
            !config["cloud"]["integrations"]
                .as_object()
                .into_iter()
                .flatten()
                .any(|(_, integration)| integration["env"].as_str() == name)
        });
    }
    policy
}

/// Reads `--env` and `--secret` values; a bare name reads the same-named environment variable.
fn explicit_values(values: &CloudValues) -> anyhow::Result<Vec<PendingValue>> {
    let read = |name: &str| {
        std::env::var(name)
            .map_err(|_| anyhow::anyhow!("Environment variable {name} is missing or not UTF-8"))
    };
    let mut pending = Vec::new();
    for item in &values.env {
        let (name, value, origin) = match item.split_once('=') {
            Some((name, value)) => (name, Some(value.to_owned()), "given"),
            None => (item.as_str(), None, "from environment"),
        };
        config_name(name)?;
        let value = match value {
            Some(value) => value,
            None => read(name)?,
        };
        pending.push(("env", name.to_owned(), value, origin));
    }
    for name in &values.secrets {
        if let Some((name, _)) = name.split_once('=') {
            bail!(
                "`--secret` takes only a name and reads the same-named environment variable, e.g. `{name}=... obelisk cloud app push --secret {name}`"
            );
        }
        config_name(name)?;
        pending.push(("secrets", name.clone(), read(name)?, "from environment"));
    }
    Ok(pending)
}

/// Drops declarations of `explicit` values, which are not collected again.
fn without(mut policy: Value, explicit: &[PendingValue]) -> Value {
    let given = |kind: &str, name: Option<&str>| {
        explicit
            .iter()
            .any(|(k, n, _, _)| *k == kind && Some(n.as_str()) == name)
    };
    if let Some(declarations) = policy["public_env"].as_array_mut() {
        declarations.retain(|declaration| {
            !given(
                "env",
                declaration
                    .as_str()
                    .or_else(|| declaration["name"].as_str()),
            )
        });
    }
    if let Some(declarations) = policy["secrets"].as_object_mut() {
        declarations.retain(|name, _| !given("secrets", Some(name)));
    }
    policy
}

/// Stages `explicit` values into `draft`, then the remaining declared values taken from `values`.
async fn stage_values(
    client: &Client,
    app: &str,
    key: &str,
    mut draft: Value,
    explicit: Vec<PendingValue>,
    values: Values<'_>,
) -> anyhow::Result<(Value, Value)> {
    let id = draft_id(&draft)?;
    let mut staged = Vec::new();
    let mut put = async |kind: &'static str, name: String, value: String| {
        let view = client
            .fenced(
                app,
                key,
                id,
                Method::PUT,
                &[kind, &name],
                Some(Body::Json(json!({"value":value}))),
            )
            .await
            .with_context(|| {
                format!(
                    "Could not stage {} {name} in draft `{key}`; earlier values may already be staged",
                    kind_label(kind)
                )
            })?;
        staged.push(json!({"kind":kind,"name":name}));
        anyhow::Ok(view)
    };
    for (kind, name, value, _) in explicit.iter().cloned() {
        draft = put(kind, name, value).await?;
    }
    let mut report = Vec::new();
    if !matches!(values, Values::Check) {
        let pending;
        (report, pending) = collect_values(
            "Retry with `--fill` or `--fill-non-interactive`",
            &without(user_supplied_policy(&draft), &explicit),
            draft["env"]
                .as_object()
                .context("Invalid cloud draft environment")?,
            draft["secrets"]
                .as_array()
                .context("Invalid cloud draft secrets")?,
            values,
        )?;
        for (kind, name, value, _) in pending {
            draft = put(kind, name, value).await?;
        }
    }
    Ok((
        draft,
        json!({"draft":key,"staged":staged,"declarations":report}),
    ))
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
    secret_names: &[Value],
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
                secret_names.iter().any(|s| s == name),
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

fn initial_values(
    policy: &Value,
    explicit: Vec<PendingValue>,
    values: Values,
) -> anyhow::Result<Value> {
    let (_, pending) = collect_values(
        "Retry with `--fill`, or `--fill-non-interactive` to read same-named environment variables",
        &without(policy.clone(), &explicit),
        &serde_json::Map::new(),
        &[],
        values,
    )?;
    let mut values = json!({"env":{},"secrets":{}});
    for (kind, name, value, _) in explicit.into_iter().chain(pending) {
        values[kind][name] = json!(value);
    }
    Ok(values)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cloud_integration_values_are_never_collected_from_local_environment() {
        let config = json!({"policy":{"public_env":["PATH","OTHER_REQUIRED"],"secrets":{}},"cloud":{"integrations":{"llm":{"type":"llm","env":"PATH"}}}});
        let policy = user_supplied_policy(&config);
        let existing = serde_json::from_value(json!({"OTHER_REQUIRED":"value"})).unwrap();
        let (_, pending) = collect_values("", &policy, &existing, &[], Values::Env).unwrap();
        assert!(pending.is_empty());
        assert_eq!(policy["public_env"], json!(["OTHER_REQUIRED"]));
        assert!(validate_cloud_app_name("integrations-llm").is_err());
        assert!(validate_cloud_app_name("system-example").is_err());
    }
    use crate::args::{Args, Subcommand};
    use axum::{Router, body::Bytes, extract::State, http::HeaderMap, routing::any};
    use clap::Parser as _;
    use std::sync::{Arc, Mutex};

    #[derive(Default)]
    struct Requests {
        received: Vec<(String, Value)>,
        error: bool,
        redirect: bool,
        api_url: Option<String>,
        drafts: MockDrafts,
        /// Results of accepted commands by execution ID.
        commands: std::collections::BTreeMap<String, Value>,
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
        let body = request_body(&headers, &body);
        let mut state = state.lock().unwrap();
        if uri.path().contains("/drafts") {
            assert_eq!(uri.query(), None);
        }
        state
            .received
            .push((format!("{method} {uri}"), body.clone()));
        if method != Method::GET
            && uri.path().starts_with("/prefix/v1/apps")
            && !uri.path().contains("/drafts")
            && !uri.path().contains("/secrets/")
            && !uri.path().ends_with("/config/compare")
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
        if let Some(path) = uri.path().strip_prefix("/prefix/v1/apps/demo/")
            && let Some(result) = state.drafts.route(&method, path, if_match(&headers), &body)
        {
            return match result {
                Ok(view) => axum::Json(view).into_response(),
                Err(status) => status.into_response(),
            };
        }
        let result = match uri.path() {
            "/prefix/v1/apps/demo/apply" => {
                if let Err(error) = state.drafts.apply(&body, if_match(&headers)) {
                    return (axum::http::StatusCode::CONFLICT, error).into_response();
                }
                json!({"ok":true})
            }
            "/prefix/v1/apps/demo/config/compare" => {
                let desired = staged_config(body.as_str().unwrap());
                let staged = staged_config(state.drafts.app_toml(DEFAULT_DRAFT));
                let changes: Vec<_> = ["policy", "cloud"]
                    .into_iter()
                    .filter(|key| staged[key] != desired[key])
                    .map(|key| format!("/{key}"))
                    .collect();
                json!({"changes":changes,"revision":state.drafts.revision})
            }
            "/prefix/v1/apps/demo" => {
                let applied = staged_config(state.drafts.app_toml(""));
                json!({
                    "endpoints":{"api":state.api_url.clone().unwrap_or_else(||format!("http://{}/prefix/tenant",headers["host"].to_str().unwrap()))},
                    "policy":applied["policy"],
                    "cloud":applied["cloud"],
                    "app_toml":state.drafts.applied["app_toml"],
                    "revision":state.drafts.revision,
                    "applied_revision":state.drafts.applied_revision,
                })
            }
            path if path.starts_with("/prefix/tenant/v1/executions/")
                && path.ends_with("/cancel") =>
            {
                json!({"ok":"cancelled"})
            }
            path if let Some(id) = path.strip_prefix("/prefix/v1/commands/") => {
                match state.commands.get(id) {
                    Some(result) => result.clone(),
                    None => return axum::http::StatusCode::NOT_FOUND.into_response(),
                }
            }
            "/prefix/tenant/v1/components" => json!([]),
            "/prefix/tenant/v1/info" => json!(rest_types::server::ServerInfoResponse {
                app_env_prefix: String::new(),
                app_name: "demo".to_owned(),
                obelisk_version: "0.0.0".to_owned(),
                deployment_id: concepts::prefixed_ulid::DeploymentId::generate().to_string(),
                node_run_id: concepts::prefixed_ulid::NodeRunId::generate().to_string(),
                switch_count: 0,
                app_config_digest: "digest".to_owned(),
                gc: None,
                http_servers: Vec::new(),
                started_at: jiff::Timestamp::now(),
            }),
            "/prefix/v1/tokens" if method == Method::POST => {
                json!({"token_id":"a".repeat(32),"token":format!("obk_{}_{}", "a".repeat(32), "b".repeat(64))})
            }
            "/prefix/v1/apps/demo/policy" if method == Method::GET => {
                json!({"public_env":["CLOUD_TEST_MISSING_REQUIRED"],"secrets":{}})
            }
            "/prefix/v1/apps/demo/env" => json!({"env":{}}),
            "/prefix/v1/apps/demo/secrets" => json!({"secrets":[]}),
            _ => json!({"ok":true}),
        };
        axum::Json(result).into_response()
    }
    /// Draft ID of a `"draft-<id>"` If-Match header.
    pub(super) fn if_match(headers: &HeaderMap) -> Option<u64> {
        let value = headers.get("if-match")?.to_str().unwrap();
        Some(
            value
                .strip_prefix("\"draft-")
                .and_then(|id| id.strip_suffix('"'))
                .unwrap()
                .parse()
                .unwrap(),
        )
    }
    /// Request body as JSON, or as a string when it is app.toml text.
    pub(super) fn request_body(headers: &HeaderMap, body: &Bytes) -> Value {
        if headers
            .get("content-type")
            .is_some_and(|value| value == "application/toml")
        {
            Value::String(String::from_utf8(body.to_vec()).unwrap())
        } else if body.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(body).unwrap()
        }
    }
    /// Drafts of the mock app, following the provisioner's draft API.
    pub(super) struct MockDrafts {
        /// `{app_toml, env, secrets}` of the applied revision; secrets map names to values.
        pub(super) applied: Value,
        pub(super) applied_revision: u64,
        pub(super) revision: u64,
        pub(super) drafts: std::collections::BTreeMap<String, Value>,
        last_draft_id: u64,
    }
    impl Default for MockDrafts {
        fn default() -> Self {
            Self::new("app_name='demo'\n", 1)
        }
    }
    impl MockDrafts {
        pub(super) fn new(app_toml: &str, revision: u64) -> Self {
            Self {
                applied: json!({"app_toml":app_toml,"env":{},"secrets":{}}),
                applied_revision: revision,
                revision,
                drafts: std::collections::BTreeMap::new(),
                last_draft_id: 0,
            }
        }
        /// app.toml of draft `key`, or the applied one when there is no such draft.
        pub(super) fn app_toml(&self, key: &str) -> &str {
            self.drafts.get(key).unwrap_or(&self.applied)["app_toml"]
                .as_str()
                .unwrap()
        }
        fn view(&self, key: &str) -> Value {
            let draft = &self.drafts[key];
            let config = staged_config(draft["app_toml"].as_str().unwrap());
            let applied = staged_config(self.applied["app_toml"].as_str().unwrap());
            let diff = |kind: &str| {
                let (new, old) = (
                    draft[kind].as_object().unwrap(),
                    self.applied[kind].as_object().unwrap(),
                );
                json!({
                    "added":new.keys().filter(|name| !old.contains_key(*name)).collect::<Vec<_>>(),
                    "removed":old.keys().filter(|name| !new.contains_key(*name)).collect::<Vec<_>>(),
                    "changed":new.iter().filter(|(name, value)| old.get(*name).is_some_and(|old| old != *value)).map(|(name, _)| name).collect::<Vec<_>>(),
                })
            };
            let policy = user_supplied_policy(&config);
            let declared_env = policy["public_env"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|declaration| declaration["type"] != "internal")
                .map(|declaration| {
                    let name = declaration
                        .as_str()
                        .or_else(|| declaration["name"].as_str())
                        .unwrap();
                    (name.to_owned(), declaration["optional"] == true)
                })
                .collect::<Vec<_>>();
            let declared_secrets = policy["secrets"]
                .as_object()
                .unwrap()
                .iter()
                .filter(|(_, declaration)| declaration["type"] != "internal")
                .map(|(name, declaration)| (name.clone(), declaration["optional"] == true))
                .collect::<Vec<_>>();
            let values = |kind: &str, declared: &[(String, bool)]| {
                let set = draft[kind].as_object().unwrap();
                (
                    declared
                        .iter()
                        .filter(|(name, optional)| !optional && !set.contains_key(name))
                        .map(|(name, _)| name.clone())
                        .collect::<Vec<_>>(),
                    set.keys()
                        .filter(|name| !declared.iter().any(|(declared, _)| declared == *name))
                        .cloned()
                        .collect::<Vec<_>>(),
                )
            };
            let (missing_env, unused_env) = values("env", &declared_env);
            let (missing_secrets, unused_secrets) = values("secrets", &declared_secrets);
            let (env, secrets) = (diff("env"), diff("secrets"));
            let app_toml = draft["app_toml"] != self.applied["app_toml"];
            let deployment_id = draft.get("deployment_id").cloned().unwrap_or(Value::Null);
            let changed = app_toml
                || !deployment_id.is_null()
                || [&env, &secrets].iter().any(|diff| {
                    ["added", "removed", "changed"]
                        .iter()
                        .any(|key| !diff[key].as_array().unwrap().is_empty())
                });
            json!({
                "key":key,"draft_id":draft["draft_id"],"parent_revision":draft["parent_revision"],"revision":draft["revision"],
                "applied_revision":self.applied_revision,
                "stale":draft["parent_revision"] != self.applied_revision,
                "app_toml":draft["app_toml"],"policy":config["policy"],"cloud":config["cloud"],
                "env":draft["env"],"secrets":draft["secrets"].as_object().unwrap().keys().collect::<Vec<_>>(),
                "deployment_id":deployment_id,
                "summary":{"changed":changed,"deployment":!deployment_id.is_null(),"app_toml":app_toml,"policy":config["policy"] != applied["policy"],"cloud":config["cloud"] != applied["cloud"],"env":env,"secrets":secrets},
                "values":{"missing_env":missing_env,"missing_secrets":missing_secrets,"unused_env":unused_env,"unused_secrets":unused_secrets},
            })
        }
        fn open(&mut self, key: &str) {
            self.revision += 1;
            self.last_draft_id += 1;
            let mut draft = self.applied.clone();
            draft["draft_id"] = json!(self.last_draft_id);
            draft["parent_revision"] = json!(self.applied_revision);
            draft["revision"] = json!(self.revision);
            self.drafts.insert(key.to_owned(), draft);
        }
        /// Draft `key`, which must be `if_match` when given.
        fn fenced(
            &self,
            key: &str,
            if_match: Option<u64>,
        ) -> Result<Option<&Value>, axum::http::StatusCode> {
            match (self.drafts.get(key), if_match) {
                (Some(draft), Some(id)) if draft["draft_id"] != id => {
                    Err(axum::http::StatusCode::PRECONDITION_FAILED)
                }
                (None, Some(_)) => Err(axum::http::StatusCode::PRECONDITION_FAILED),
                (draft, _) => Ok(draft),
            }
        }
        /// Serves `/drafts...` below the app path; `None` for other paths.
        pub(super) fn route(
            &mut self,
            method: &Method,
            path: &str,
            if_match: Option<u64>,
            body: &Value,
        ) -> Option<Result<Value, axum::http::StatusCode>> {
            let rest = path.strip_prefix("drafts")?;
            let segments: Vec<&str> = rest.split('/').skip(1).collect();
            let key = match segments.first() {
                None => {
                    let drafts = self
                        .drafts
                        .keys()
                        .map(|key| self.view(key))
                        .collect::<Vec<_>>();
                    return Some(Ok(
                        json!({"drafts":drafts,"applied_revision":self.applied_revision}),
                    ));
                }
                Some(key) => *key,
            };
            let draft = match self.fenced(key, if_match) {
                Ok(draft) => draft.cloned(),
                Err(status) => return Some(Err(status)),
            };
            let parent = body["parent_revision"].as_u64();
            Some(match (method, &segments[1..], draft) {
                (&Method::PUT, [], Some(draft))
                    if parent.is_some_and(|parent| draft["parent_revision"] != parent) =>
                {
                    Err(axum::http::StatusCode::PRECONDITION_FAILED)
                }
                (&Method::GET | &Method::PUT, [], Some(_)) => Ok(self.view(key)),
                (&Method::PUT, [], None)
                    if parent.is_some_and(|parent| parent != self.applied_revision) =>
                {
                    Err(axum::http::StatusCode::PRECONDITION_FAILED)
                }
                (&Method::PUT, [], None) => {
                    self.open(key);
                    Ok(self.view(key))
                }
                (&Method::DELETE, [], draft) => {
                    self.drafts.remove(key);
                    Ok(json!({"discarded":draft.is_some()}))
                }
                (_, _, None) => Err(axum::http::StatusCode::NOT_FOUND),
                (method, edit, Some(_)) => {
                    let draft = self.drafts.get_mut(key).unwrap();
                    let before = draft.clone();
                    match (method, edit) {
                        (&Method::PUT, ["config"]) => draft["app_toml"] = body.clone(),
                        (&Method::PUT, ["deployment"]) => {
                            draft["deployment_id"] = body["deployment_id"].clone();
                        }
                        (&Method::DELETE, ["deployment"]) => draft["deployment_id"] = Value::Null,
                        (&Method::PUT, [kind, name]) => draft[*kind][*name] = body["value"].clone(),
                        (&Method::DELETE, [kind, name]) => {
                            draft[*kind].as_object_mut().unwrap().remove(*name);
                        }
                        _ => panic!("unexpected draft request: {method} {path}"),
                    }
                    let changed = *draft != before;
                    let mut view = self.view(key);
                    view["changed"] = json!(changed);
                    Ok(view)
                }
            })
        }
        /// Applies the current proposal by label, or the applied revision again.
        pub(super) fn apply(&mut self, body: &Value, if_match: Option<u64>) -> Result<(), String> {
            let Some(key) = body["draft"].as_str() else {
                let revision = body["revision"].as_u64().unwrap();
                return if revision == self.applied_revision {
                    Ok(())
                } else {
                    Err(format!("revision {revision} is not applied"))
                };
            };
            let draft = self
                .fenced(key, if_match)
                .map_err(|_| "draft identity changed".to_owned())?
                .ok_or("draft not found")?
                .clone();
            if draft["parent_revision"] != self.applied_revision {
                return Err(format!("draft {key} changed"));
            }
            self.applied =
                json!({"app_toml":draft["app_toml"],"env":draft["env"],"secrets":draft["secrets"]});
            self.applied_revision = draft["revision"].as_u64().unwrap();
            let parent = draft["parent_revision"].clone();
            self.drafts
                .retain(|_, draft| draft["parent_revision"] != parent);
            Ok(())
        }
    }
    /// What the provisioner stages from an app.toml.
    pub(super) fn staged_config(app_toml: &str) -> Value {
        let config = LocalConfig::parse(app_toml.to_owned()).unwrap();
        let mut value = desired_config(&config);
        value["app_name"] = json!(config.app_name);
        value
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
        let mut args = vec!["obelisk", "cloud"];
        let position = command.iter().position(|arg| *arg == "app");
        if let Some(position) = position
            && command.get(position + 1) != Some(&"list")
        {
            args.extend_from_slice(&command[..=position + 1]);
            args.extend_from_slice(&["--app", "demo"]);
            args.extend_from_slice(&command[position + 2..]);
        } else {
            args.extend_from_slice(command);
        }
        let Subcommand::Cloud(mut args) = Args::try_parse_from(args).unwrap().command else {
            panic!()
        };
        args.api_token = Some(SecretString::from("cloud-token"));
        args.api_url = Some(origin.to_owned());
        args.apps_domain = Some("example.test".to_owned());
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
                    command: CloudApp::Admin {
                        command: crate::args::Admin::NodeRunId { json: true },
                        ..
                    },
                    ..
                })
            ));
        }
    }
    /// Runs a tenant API command against `api_url` instead of the app's API.
    async fn run_app_api(args: CloudArgs, api_url: String) -> anyhow::Result<()> {
        let (session, command) = args.into_session();
        let Cloud::App(CloudAppTarget { command, .. }) = command else {
            panic!("expected `cloud app`")
        };
        session
            .run_app_api(api_url, |token| match command {
                CloudApp::Deployment { command, .. } => Subcommand::Deployment(DeploymentArgs {
                    command: command.into(),
                    token,
                }),
                CloudApp::Execution { command, .. } => {
                    Subcommand::Execution(ExecutionArgs { command, token })
                }
                CloudApp::Component { command, .. } => {
                    Subcommand::Component(ComponentArgs { command, token })
                }
                CloudApp::Admin { command, .. } => Subcommand::Admin(AdminArgs { command, token }),
                _ => panic!("not a tenant API command"),
            })
            .await
    }
    #[tokio::test]
    async fn cloud_api_url_rejects_invalid_domains() {
        for domain in [
            "",
            "https://obelisk.works",
            "obelisk.works/path",
            "obelisk.works:443",
            "*.obelisk.works",
            "-invalid.example",
            "bad..example",
        ] {
            let mut args = args("http://127.0.0.1:1", &["app", "status"]);
            args.apps_domain = Some(domain.to_owned());
            let (session, _) = args.into_session();
            assert!(session.direct_api_url("demo").is_err(), "{domain}");
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
            run_app_api(args(&origin, &command), api_url.clone())
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
                "GET /prefix/tenant/v1/info".to_owned(),
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
            run_app_api(
                args("http://127.0.0.1:1", &["app", "deployment", "active"]),
                format!("{origin}tenant")
            )
            .await
            .unwrap_err()
            .to_string()
            .contains("307")
        );
        assert_eq!(state.lock().unwrap().received.len(), 1);
        task.abort();
    }
    #[test]
    fn with_app_name_keeps_matching_text_verbatim() {
        let text = "# comment\napp_name='demo'\n[cloud]\nmemory.mib=512\n";
        assert_eq!(with_app_name(text, "demo").unwrap(), text);
        assert_eq!(
            with_app_name(text, "renamed").unwrap(),
            "# comment\napp_name= \"renamed\"\n[cloud]\nmemory.mib=512\n"
        );
        assert_eq!(with_app_name("", "demo").unwrap(), "app_name = \"demo\"\n");
    }
    #[tokio::test]
    async fn cloud_commands_keep_policy_staged_and_force_explicit() {
        let (origin, state, task) = server().await;
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("app.toml");
        tokio::fs::write(
            &path,
            "app_name='demo'\n[cloud]\nmemory.mib=1024\n[public_env]\nREGION={optional=true}\n[secrets]\nKEY={optional=true}\n[[outbound_http.allowed_host]]\npattern='${REGION}'\n",
        )
        .await
        .unwrap();
        args(&origin, &["app", "new", "-a", path.to_str().unwrap()])
            .execute()
            .await
            .unwrap();
        args(&origin, &["app", "stage", "-a", path.to_str().unwrap()])
            .dispatch()
            .await
            .unwrap();
        args(&origin, &["app", "apply"]).execute().await.unwrap();
        let error = args(&origin, &["app", "apply", "--force"])
            .execute()
            .await
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("Nothing staged in draft `default`"),
            "{error}"
        );
        args(&origin, &["app", "apply", "--reapply", "--force"])
            .execute()
            .await
            .unwrap();
        let before_env = state.lock().unwrap().received.len();
        args(&origin, &["app", "env", "set", "OBELISK_API_URL", "value"])
            .execute()
            .await
            .unwrap();
        let received = &state.lock().unwrap().received;
        // The applied draft is gone, so the edit reopens it on the applied revision.
        assert_eq!(received.len(), before_env + 4);
        let app_toml = std::fs::read_to_string(&path).unwrap();
        assert_eq!(received[0].0, "POST /prefix/v1/apps");
        assert_eq!(received[0].1["app_toml"], app_toml);
        assert!(received[0].1.get("policy").is_none());
        // Stage looks the app up before staging, creating it only when missing.
        assert_eq!(received[1].0, "GET /prefix/v1/apps/demo");
        assert_eq!(received[2].0, "GET /prefix/v1/apps/demo/drafts/default");
        assert_eq!(received[4].0, "PUT /prefix/v1/apps/demo/drafts/default");
        assert_eq!(received[4].1, json!({"parent_revision":1}));
        assert_eq!(
            received[5].0,
            "PUT /prefix/v1/apps/demo/drafts/default/config"
        );
        assert_eq!(received[5].1, app_toml);
        let applies: Vec<_> = received
            .iter()
            .filter(|(path, _)| path.ends_with("/apply"))
            .collect();
        assert_eq!(applies.len(), 2);
        // The second apply has no draft left and re-applies the applied revision explicitly.
        for (request, (force, draft)) in applies
            .iter()
            .zip([(false, json!("default")), (true, Value::Null)])
        {
            assert_eq!(request.1["force"], force);
            assert_eq!(request.1["draft"], draft);
            // The provisioner takes the deployment from the draft.
            assert!(request.1.get("deployment_id").is_none());
            if draft.is_null() {
                assert_eq!(request.1["revision"], 2);
            } else {
                assert!(request.1.get("revision").is_none());
            }
            assert!(request.1.get("config").is_none());
        }
        assert_eq!(
            received.last().unwrap().0,
            "PUT /prefix/v1/apps/demo/drafts/default/env/OBELISK_API_URL"
        );
        task.abort();
    }
    #[test]
    fn explicit_values_read_bare_names_from_environment() {
        let values = |env: &[&str], secrets: &[&str]| CloudValues {
            env: env.iter().map(|value| (*value).to_owned()).collect(),
            secrets: secrets.iter().map(|value| (*value).to_owned()).collect(),
            fill: false,
            fill_non_interactive: false,
        };
        let path = std::env::var("PATH").unwrap();
        let home = std::env::var("HOME").unwrap();
        assert_eq!(
            explicit_values(&values(&["PATH", "REGION=eu=1"], &["HOME"])).unwrap(),
            [
                ("env", "PATH".to_owned(), path, "from environment"),
                ("env", "REGION".to_owned(), "eu=1".to_owned(), "given"),
                ("secrets", "HOME".to_owned(), home, "from environment"),
            ]
        );
        let error = explicit_values(&values(&[], &["KEY=value"]))
            .unwrap_err()
            .to_string();
        assert!(error.contains("`--secret` takes only a name"), "{error}");
        assert!(!error.contains("value"), "{error}");
        let unset: &[&str] = &["OBELISK_CLOUD_TEST_UNSET"];
        for (env, secrets) in [(unset, &[][..]), (&[][..], unset)] {
            assert!(
                explicit_values(&values(env, secrets))
                    .unwrap_err()
                    .to_string()
                    .contains("OBELISK_CLOUD_TEST_UNSET is missing")
            );
        }
        let policy =
            json!({"public_env":["PATH",{"name":"REGION"}],"secrets":{"HOME":{},"KEY":{}}});
        let explicit = explicit_values(&values(&["PATH"], &["HOME"])).unwrap();
        assert_eq!(
            without(policy, &explicit),
            json!({"public_env":[{"name":"REGION"}],"secrets":{"KEY":{}}})
        );
    }
    #[tokio::test]
    async fn cloud_apply_applies_only_staged_draft() {
        let (origin, state, task) = server().await;
        state.lock().unwrap().drafts =
            MockDrafts::new("app_name='demo'\n[public_env]\nPATH={}\n", 3);
        let error = args(&origin, &["app", "apply", "--draft", "ci"])
            .execute()
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("Nothing staged in draft `ci`; stage changes with"),
            "{error}"
        );
        args(
            &origin,
            &["app", "env", "set", "PATH", "/bin", "--draft", "ci"],
        )
        .execute()
        .await
        .unwrap();
        args(&origin, &["app", "apply", "--draft", "ci"])
            .execute()
            .await
            .unwrap();
        let state = state.lock().unwrap();
        // The unprefixed route records that the write created the draft via `create_or_reuse`.
        assert!(state.received.iter().any(|(route, body)| route
            == "PUT /prefix/v1/apps/demo/drafts/ci/env/PATH"
            && body["value"] == "/bin"));
        let (_, apply) = state.received.last().unwrap();
        assert_eq!(apply["draft"], "ci");
        assert!(apply.get("revision").is_none());
        assert_eq!(state.drafts.applied["env"]["PATH"], "/bin");
        assert!(state.drafts.drafts.is_empty());
        task.abort();
    }
    #[tokio::test]
    async fn cloud_command_id_reports_accepted_command() {
        let (origin, state, task) = server().await;
        let accepted = concepts::prefixed_ulid::ExecutionIdTopLevel::generate().to_string();
        let unknown = concepts::prefixed_ulid::ExecutionIdTopLevel::generate().to_string();
        state.lock().unwrap().commands.insert(
            accepted.clone(),
            json!({"revision":7,"execution_id":accepted}),
        );
        // Nothing is staged, so only the recorded result lets the retry succeed.
        for command in ["apply", "enable", "disable", "delete"] {
            let result = args(&origin, &["app", command, "--command-id", &accepted])
                .execute()
                .await
                .unwrap();
            assert_eq!(result["revision"], 7, "{command}");
        }
        assert!(
            state
                .lock()
                .unwrap()
                .received
                .iter()
                .all(|(route, _)| route == &format!("GET /prefix/v1/commands/{accepted}"))
        );
        state.lock().unwrap().received.clear();
        args(&origin, &["app", "enable", "--command-id", &unknown])
            .execute()
            .await
            .unwrap();
        let routes: Vec<_> = state
            .lock()
            .unwrap()
            .received
            .iter()
            .map(|(route, _)| route.clone())
            .collect();
        assert_eq!(
            routes,
            [
                format!("GET /prefix/v1/commands/{unknown}"),
                "POST /prefix/v1/apps/demo/enable".to_owned()
            ]
        );
        task.abort();
    }
    #[tokio::test]
    async fn cloud_commands_reject_explicit_config_of_another_app() {
        let (origin, state, task) = server().await;
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("app.toml");
        tokio::fs::write(&path, "app_name='other'\n").await.unwrap();
        let path = path.to_str().unwrap();
        for command in ["new", "status", "stage"] {
            let error = args(&origin, &["app", command, "-a", path])
                .dispatch()
                .await
                .unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("is for app `other`, but the selected app is `demo`"),
                "{command}: {error}"
            );
        }
        assert!(state.lock().unwrap().received.is_empty());
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
                .any(|(route, body)| route
                    == "PUT /prefix/v1/apps/demo/drafts/default/secrets/OBELISK_API_TOKEN"
                    && body["value"] == path_value)
        );
        task.abort();
    }
    #[tokio::test]
    async fn cloud_fill_asks_per_value_and_collects_before_writing() {
        let (origin, state, task) = server().await;
        let client = args(&origin, &["app", "status"])
            .into_session()
            .0
            .client()
            .unwrap();
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
        let draft = json!({
            "policy":policy,"cloud":{},
            "env":{"ALREADY_SET":"stored","HOME":home_value},
            "secrets":["ALREADY_SET"],"draft_id":1,
        });
        state.lock().unwrap().drafts.open("default");
        let mut answers = script.iter();
        let (_, result) = stage_values(
            &client,
            "demo",
            "default",
            draft.clone(),
            Vec::new(),
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
                ("PUT /prefix/v1/apps/demo/drafts/default/env/ALREADY_SET", "replaced"),
                (
                    "PUT /prefix/v1/apps/demo/drafts/default/env/OBELISK_CLOUD_TEST_EMPTY",
                    ""
                ),
                ("PUT /prefix/v1/apps/demo/drafts/default/env/PATH", path_value.as_str()),
                (
                    "PUT /prefix/v1/apps/demo/drafts/default/secrets/HOME",
                    home_value.as_str()
                ),
                (
                    "PUT /prefix/v1/apps/demo/drafts/default/secrets/OBELISK_CLOUD_TEST_PROMPT_SECRET",
                    "typed-secret"
                ),
            ]
            .map(|(route, value)| (route.to_owned(), value.to_owned()))
        );
        state.lock().unwrap().received.clear();
        let error = stage_values(
            &client,
            "demo",
            "default",
            draft,
            Vec::new(),
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
        assert!(state.lock().unwrap().received.is_empty());
        task.abort();
    }
    #[tokio::test]
    async fn cloud_fill_validates_before_mutation_and_secret_errors_are_redacted() {
        let (origin, state, task) = server().await;
        let client = args(&origin, &["app", "status"])
            .into_session()
            .0
            .client()
            .unwrap();
        let draft = json!({
            "policy":{"public_env":["CLOUD_TEST_MISSING_REQUIRED"],"secrets":{}},
            "cloud":{},"env":{},"secrets":[],"draft_id":1,
        });
        let error = stage_values(&client, "demo", "default", draft, Vec::new(), Values::Env)
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("Missing required configuration: env CLOUD_TEST_MISSING_REQUIRED"),
            "{error}"
        );
        assert!(state.lock().unwrap().received.is_empty());
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
