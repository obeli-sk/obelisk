use crate::args::{
    AdminArgs, ClientConnection, Cloud, CloudApp, CloudApps, CloudArgs, CloudEnv, CloudSecrets,
    ComponentArgs, DeploymentArgs, ExecutionArgs, Subcommand,
};
use crate::client::ClientStartup;
use crate::config::app::AppConfigToml;
use anyhow::{Context as _, bail, ensure};
use reqwest::{Method, Url, header};
use secrecy::ExposeSecret as _;
use serde_json::{Value, json};
use std::{io::Read as _, path::Path, time::Duration};

struct Client {
    http: reqwest::Client,
    origin: Url,
}
impl Client {
    fn new(args: &CloudArgs) -> anyhow::Result<Self> {
        let origin = args
            .api_url
            .as_ref()
            .context("Set --api-url or OBELISK_CLOUD_API_URL")?
            .parse::<Url>()?;
        ensure!(
            matches!(origin.scheme(), "http" | "https") && origin.host_str().is_some(),
            "Cloud API URL must use HTTP or HTTPS"
        );
        ensure!(
            origin.username().is_empty()
                && origin.password().is_none()
                && origin.query().is_none()
                && origin.fragment().is_none(),
            "Cloud API URL must not contain credentials, query or fragment"
        );
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
        secret: bool,
    ) -> anyhow::Result<Value> {
        let mut request = self.http.request(method, self.url(path)?);
        if let Some(body) = body {
            request = request.json(&body);
        }
        let response = request.send().await.map_err(reqwest::Error::without_url)?;
        let status = response.status();
        if !status.is_success() {
            if secret {
                bail!("Cloud secret operation failed: HTTP {status}");
            }
            let body = response.text().await.unwrap_or_default();
            bail!("Cloud API returned {status}: {body}");
        }
        Ok(response.json().await?)
    }
    async fn app(
        &self,
        name: &str,
        method: Method,
        suffix: &[&str],
        body: Option<Value>,
        secret: bool,
    ) -> anyhow::Result<Value> {
        let mut path = vec!["v1", "apps", name];
        path.extend_from_slice(suffix);
        self.request(method, &path, body, secret).await
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
impl CloudArgs {
    pub(crate) async fn run(self) -> anyhow::Result<()> {
        if matches!(self.command, Cloud::App(CloudApp::Endpoint)) {
            let client = Client::new(&self)?;
            let name = app_name(self.app.as_deref()).await?;
            println!("{}", client.url(&["proxy", &name])?);
            return Ok(());
        }
        if matches!(
            self.command,
            Cloud::Deployment(_) | Cloud::Execution(_) | Cloud::Component(_) | Cloud::Admin(_)
        ) {
            let client = Client::new(&self)?;
            let name = app_name(self.app.as_deref()).await?;
            let api_url = client.url(&["proxy", &name])?.to_string();
            let startup = ClientStartup::cloud(api_url.clone(), self.api_token);
            let token = ClientConnection {
                api_url,
                api_token: None,
            };
            let command = match self.command {
                Cloud::Deployment(command) => {
                    Subcommand::Deployment(DeploymentArgs { command, token })
                }
                Cloud::Execution(command) => {
                    Subcommand::Execution(ExecutionArgs { command, token })
                }
                Cloud::Component(command) => {
                    Subcommand::Component(ComponentArgs { command, token })
                }
                Cloud::Admin(command) => Subcommand::Admin(AdminArgs { command, token }),
                Cloud::Apps(_) | Cloud::App(_) => unreachable!(),
            };
            return crate::command_future(command, Some(startup))?.await;
        }
        let value = self.execute().await?;
        println!("{}", serde_json::to_string_pretty(&value)?);
        Ok(())
    }
    async fn execute(self) -> anyhow::Result<Value> {
        let client = Client::new(&self)?;
        match self.command {
            Cloud::Deployment(_) | Cloud::Execution(_) | Cloud::Component(_) | Cloud::Admin(_) => {
                unreachable!("API commands are dispatched in run")
            }
            Cloud::Apps(CloudApps::List) => {
                client
                    .request(Method::GET, &["v1", "apps"], None, false)
                    .await
            }
            Cloud::App(CloudApp::New { name, app_config }) => {
                let config = match app_config {
                    Some(path) => Some(policy(&path).await?),
                    None => None,
                };
                let name = app_name(
                    name.as_deref()
                        .or(self.app.as_deref())
                        .or_else(|| config.as_ref().and_then(|c| c.app_name.as_deref())),
                )
                .await?;
                let mut body = json!({"app_slug":name});
                if let Some(config) = config {
                    body["policy"] = serde_json::to_value(config.policy())?;
                }
                client
                    .request(Method::POST, &["v1", "apps"], Some(body), false)
                    .await
            }
            Cloud::App(command) => {
                let name = &app_name(self.app.as_deref()).await?;
                match command {
                    CloudApp::Get => client.app(name, Method::GET, &[], None, false).await,
                    CloudApp::Status => {
                        client
                            .app(name, Method::GET, &["status"], None, false)
                            .await
                    }
                    CloudApp::Update { app_config } => {
                        client
                            .app(
                                name,
                                Method::PUT,
                                &["policy"],
                                Some(serde_json::to_value(policy(&app_config).await?.policy())?),
                                false,
                            )
                            .await
                    }
                    CloudApp::Apply { force } => {
                        client
                            .app(
                                name,
                                Method::POST,
                                &["apply"],
                                Some(json!({"force":force})),
                                false,
                            )
                            .await
                    }
                    CloudApp::Start => {
                        client
                            .app(name, Method::POST, &["start"], None, false)
                            .await
                    }
                    CloudApp::Stop => client.app(name, Method::POST, &["stop"], None, false).await,
                    CloudApp::Delete => client.app(name, Method::DELETE, &[], None, false).await,
                    CloudApp::Fill { app_config } => {
                        let declarations = match app_config {
                            Some(path) => serde_json::to_value(policy(&path).await?.policy())?,
                            None => {
                                client
                                    .app(name, Method::GET, &["policy"], None, false)
                                    .await?
                            }
                        };
                        fill(&client, name, &declarations).await
                    }
                    CloudApp::Env(CloudEnv::List) => {
                        client.app(name, Method::GET, &["env"], None, false).await
                    }
                    CloudApp::Env(CloudEnv::Set { name: key, value }) => {
                        config_name(&key)?;
                        client
                            .app(
                                name,
                                Method::PUT,
                                &["env", &key],
                                Some(json!({"value":value})),
                                false,
                            )
                            .await
                    }
                    CloudApp::Env(CloudEnv::Unset { name: key }) => {
                        config_name(&key)?;
                        client
                            .app(name, Method::DELETE, &["env", &key], None, false)
                            .await
                    }
                    CloudApp::Secrets(CloudSecrets::List) => {
                        client
                            .app(name, Method::GET, &["secrets"], None, true)
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
                                true,
                            )
                            .await?;
                        Ok(json!({"staged_secret":key}))
                    }
                    CloudApp::Secrets(CloudSecrets::Unset { name: key }) => {
                        config_name(&key)?;
                        client
                            .app(name, Method::DELETE, &["secrets", &key], None, true)
                            .await?;
                        Ok(json!({"deleted_secret":key}))
                    }
                    CloudApp::Endpoint => unreachable!("endpoint is printed in run"),
                    CloudApp::New { .. } => unreachable!(),
                }
            }
        }
    }
}

async fn fill(client: &Client, app: &str, policy: &Value) -> anyhow::Result<Value> {
    let env = client.app(app, Method::GET, &["env"], None, false).await?;
    let secrets = client
        .app(app, Method::GET, &["secrets"], None, true)
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
                kind == "secrets",
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
            path if path.starts_with("/prefix/proxy/demo/v1/executions/")
                && path.ends_with("/cancel") =>
            {
                json!({"ok":"cancelled"})
            }
            "/prefix/proxy/demo/v1/deployment-id" => {
                json!(concepts::prefixed_ulid::DeploymentId::generate())
            }
            "/prefix/proxy/demo/v1/components" => json!([]),
            "/prefix/proxy/demo/v1/admin/node-run-id" => {
                json!(concepts::prefixed_ulid::NodeRunId::generate())
            }
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
            "--app",
            "demo",
        ];
        args.extend_from_slice(command);
        let Subcommand::Cloud(args) = Args::try_parse_from(args).unwrap().command else {
            panic!()
        };
        args
    }
    #[tokio::test]
    async fn cloud_api_commands_use_app_proxy_and_cloud_token() {
        let (origin, state, task) = server().await;
        let execution_id = concepts::ExecutionId::generate().to_string();
        for command in [
            vec!["deployment", "active"],
            vec!["execution", "pause", &execution_id],
            vec!["execution", "cancel", &execution_id],
            vec!["component", "list", "--imports"],
            vec!["admin", "node-run-id"],
        ] {
            args(&origin, &command).run().await.unwrap();
        }
        let routes: Vec<_> = state
            .lock()
            .unwrap()
            .received
            .iter()
            .map(|(route, _)| route.clone())
            .collect();
        assert_eq!(
            routes,
            vec![
                "GET /prefix/proxy/demo/v1/deployment-id".to_owned(),
                format!("PUT /prefix/proxy/demo/v1/executions/{execution_id}/pause"),
                format!("PUT /prefix/proxy/demo/v1/executions/{execution_id}/cancel"),
                "GET /prefix/proxy/demo/v1/components?exports=true&imports=true&extensions=false"
                    .to_owned(),
                "GET /prefix/proxy/demo/v1/admin/node-run-id".to_owned(),
            ]
        );
        task.abort();
    }

    #[tokio::test]
    async fn cloud_api_commands_do_not_follow_redirects() {
        let (origin, state, task) = server().await;
        state.lock().unwrap().redirect = true;
        let error = args(&origin, &["deployment", "active"])
            .run()
            .await
            .unwrap_err();
        assert!(error.to_string().contains("307"));
        assert_eq!(state.lock().unwrap().received.len(), 1);
        task.abort();
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
                true,
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
                true,
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
