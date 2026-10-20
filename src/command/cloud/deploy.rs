use super::*;
use crate::config::deployment::prepare_deployment_manifest_from_disk;

impl CloudArgs {
    pub(super) async fn deploy(self) -> anyhow::Result<()> {
        let prompt = (terminal() && !self.json).then_some(ask);
        self.deploy_with_prompt(prompt).await
    }

    async fn deploy_with_prompt(
        self,
        mut prompt: Option<impl FnMut(&Question) -> anyhow::Result<String>>,
    ) -> anyhow::Result<()> {
        let client = Client::new(&self)?;
        let Cloud::App(CloudAppTarget {
            app,
            command:
                CloudApp::Deploy {
                    app_config,
                    deployment,
                    fill: interactive,
                },
        }) = self.command
        else {
            unreachable!()
        };
        let config = policy(&app_config).await?;
        let mut name = app_name(app.as_deref().or(config.app_name.as_deref())).await?;
        let save_name = app.is_none();
        let desired = desired_config(&config);
        let prepared = prepare_deployment_manifest_from_disk(&deployment).await?;
        eprintln!("Deploying {name}");
        let step = |command: &str| {
            eprintln!("→ obelisk cloud app {command}");
        };
        let mut current = if let Some(current) = client.lookup_app(&name).await? {
            if config_differs(&current, &desired) {
                step("stage");
                client
                    .app(
                        &name,
                        Method::PUT,
                        &["config"],
                        Some(desired.clone()),
                        Kind::Command(None),
                    )
                    .await?
            } else {
                eprintln!("App configuration unchanged.");
                current
            }
        } else {
            let original_name = name.clone();
            let current = loop {
                step("new");
                let mut body = desired.clone();
                body["app_name"] = json!(name);
                match client
                    .request(
                        Method::POST,
                        &["v1", "apps"],
                        Some(body),
                        Kind::Command(None),
                    )
                    .await
                {
                    Ok(app) => break app,
                    Err(error) if error.is::<AppNameTaken>() => {
                        let Some(prompt) = prompt.as_mut() else {
                            return Err(error);
                        };
                        name = choose_app_name(&name, prompt)?;
                    }
                    Err(error) => return Err(error),
                }
            };
            if save_name && name != original_name {
                let source = std::fs::read_to_string(&app_config)?;
                let mut document = source.parse::<toml_edit::DocumentMut>()?;
                document["app_name"] = toml_edit::value(&name);
                std::fs::write(&app_config, document.to_string()).with_context(|| {
                    format!(
                        "App {name} was created, but {} could not be updated; use --app {name}",
                        app_config.display()
                    )
                })?;
                eprintln!("Updated app_name to {name} in {}", app_config.display());
            }
            current
        };
        if interactive {
            step("fill");
        }
        let filled = fill(&client, &name, &desired["policy"], interactive).await?;
        let unset = filled["declarations"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|declaration| declaration["action"] == "unset")
            .map(|declaration| {
                format!(
                    "{} {}",
                    kind_label(declaration["kind"].as_str().unwrap_or_default()),
                    declaration["name"].as_str().unwrap_or_default()
                )
            })
            .collect::<Vec<_>>();
        if !interactive && !unset.is_empty() {
            eprintln!(
                "Optional configuration not set: {}. Run `obelisk cloud app --app {name} fill` or `deploy --fill` to set it.",
                unset.join(", ")
            );
        }
        let mut outcomes = Vec::new();
        if current["potentially_stale"] == true
            || current["applied_revision"] == 0
            || filled["apply_required"] == true
        {
            step("apply");
            current = client
                .app(
                    &name,
                    Method::POST,
                    &["apply"],
                    Some(json!({"force":false})),
                    Kind::Command(None),
                )
                .await?;
            eprintln!("{}", output::lifecycle_outcome(&current));
            if let Some(outcome) = current.get("outcome") {
                outcomes.push(outcome.clone());
            }
        }
        if current["status"] == "stopped" {
            step("start");
            current = client
                .app(&name, Method::POST, &["start"], None, Kind::Command(None))
                .await?;
            eprintln!("{}", output::lifecycle_outcome(&current));
            if let Some(outcome) = current.get("outcome") {
                outcomes.push(outcome.clone());
            }
        }
        ensure!(
            current["status"] == "running",
            "App is {}; inspect it with `obelisk cloud app --app {name} status`",
            current["status"]
        );
        let api_url = api_origin(
            current["api_url"]
                .as_str()
                .context("Control plane response is missing api_url")?,
        )?;
        let startup = ClientStartup::cloud(
            api_url.as_str().trim_end_matches('/').to_owned(),
            self.api_token,
            self.verbose,
        );
        step("deployment apply");
        let id = crate::command::deployment::apply_prepared(&startup, prepared).await
            .with_context(|| format!("Deployment did not complete; inspect `obelisk cloud app --app {name} deployment active` to see what is serving, then retry `obelisk cloud app --app {name} deploy -a {} -d {}`", app_config.display(), deployment.display()))?;
        let result = json!({"app":current,"deployment_id":id,"outcomes":outcomes});
        if self.json {
            print_json(&result)
        } else {
            eprintln!("Deployed {name}");
            if let Some(url) = current["webhook_url"].as_str() {
                println!("{url}");
            }
            Ok(())
        }
    }
}

fn choose_app_name(
    name: &str,
    prompt: &mut impl FnMut(&Question) -> anyhow::Result<String>,
) -> anyhow::Result<String> {
    let suffix = ExecutionIdTopLevel::generate()
        .to_string()
        .to_ascii_lowercase();
    let suggestion = format!(
        "{}-{}",
        name[..name.len().min(13)].trim_end_matches('-'),
        &suffix[suffix.len() - 6..]
    );
    loop {
        let answer = prompt(&Question::Line(format!(
            "App name {name} is taken. New app name [{suggestion}]: "
        )))?;
        let answer = if answer.trim().is_empty() {
            suggestion.clone()
        } else {
            answer.trim().to_owned()
        };
        match validate_cloud_app_name(&answer) {
            Ok(()) => break Ok(answer),
            Err(error) => eprintln!("{error}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        Router, body::Bytes, extract::State, http::StatusCode, response::IntoResponse as _,
        routing::any,
    };
    use std::sync::{Arc, Mutex};

    struct Server {
        app: Option<Value>,
        calls: Vec<String>,
        api_url: String,
        reject_deployment: bool,
        name_taken: bool,
        active: concepts::prefixed_ulid::DeploymentId,
    }

    async fn handle(
        State(state): State<Arc<Mutex<Server>>>,
        method: Method,
        uri: axum::http::Uri,
        body: Bytes,
    ) -> axum::response::Response {
        let mut state = state.lock().unwrap();
        state.calls.push(format!("{method} {}", uri.path()));
        let routed_path = uri.path().replace("/v1/apps/renamed", "/v1/apps/demo");
        let path = routed_path.as_str();
        let body: Value = if body.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&body).unwrap()
        };
        let mut value = match (method, path) {
            (Method::GET, "/v1/apps/demo") => match &state.app {
                Some(app) => app.clone(),
                None => return (StatusCode::NOT_FOUND, "missing").into_response(),
            },
            (Method::POST, "/v1/apps") => {
                if state.name_taken && body["app_name"] == "demo" {
                    return (
                        StatusCode::CONFLICT,
                        axum::Json(
                            json!({"error":"App name is already taken","code":"app_name_taken"}),
                        ),
                    )
                        .into_response();
                }
                let mut app = body;
                app["status"] = json!("stopped");
                app["potentially_stale"] = json!(false);
                app["applied_revision"] = json!(0);
                app["desired_state"] = json!("running");
                state.app = Some(app.clone());
                app
            }
            (Method::PUT, "/v1/apps/demo/config") => {
                let app = state.app.as_mut().unwrap();
                app["policy"] = body["policy"].clone();
                app["cloud"] = body["cloud"].clone();
                app["potentially_stale"] = json!(true);
                app.clone()
            }
            (Method::POST, "/v1/apps/demo/apply" | "/v1/apps/demo/start") => {
                let app = state.app.as_mut().unwrap();
                if path.ends_with("/start") {
                    app["desired_state"] = json!("running");
                }
                let running = app["desired_state"] == "running";
                app["status"] = json!(if running { "running" } else { "stopped" });
                app["outcome"] = json!({"vm":if running { "started" } else { "remained_stopped" }});
                if path.ends_with("/apply") {
                    app["outcome"]["configuration"] = json!("applied");
                }
                app["potentially_stale"] = json!(false);
                app["applied_revision"] = json!(1);
                app.clone()
            }
            (Method::GET, "/v1/apps/demo/env") => json!({"env":{}}),
            (Method::GET, "/v1/apps/demo/secrets") => json!({"secrets":[]}),
            (Method::GET, "/tenant/v1/deployment-id") => json!(state.active),
            (Method::GET, _) if path.starts_with("/tenant/v1/deployments/") => json!({
                "deployment_id":state.active,"digest":"sha256:old","status":"active",
                "created_at":chrono::Utc::now(),"deployment_toml":"[defaults]", "files":[]
            }),
            (Method::PUT, _)
                if path.starts_with("/tenant/v1/deployments/") && !path.ends_with("/switch") =>
            {
                if state.reject_deployment {
                    return (StatusCode::UNPROCESSABLE_ENTITY, "invalid deployment")
                        .into_response();
                }
                json!({"ok":true})
            }
            (Method::PUT, _) if path.ends_with("/switch") => {
                assert_eq!(body["apply"], true);
                json!({"ok":"switched"})
            }
            _ => panic!("unexpected request: {}", state.calls.last().unwrap()),
        };
        if value.get("policy").is_some() {
            value["api_url"] = json!(state.api_url);
        }
        axum::Json(value).into_response()
    }

    #[tokio::test]
    async fn deploy_creates_stages_and_starts_only_when_needed() {
        test_utils::set_up();
        for scenario in [
            "new",
            "code-only",
            "config",
            "stopped",
            "stopped-config",
            "pending",
            "missing",
            "invalid",
            "deployment-error",
            "name-conflict",
            "rename",
            "rename-override",
        ] {
            let directory = tempfile::tempdir().unwrap();
            let app_path = directory.path().join("app.toml");
            let deployment_path = directory.path().join("deployment.toml");
            let config = if scenario == "missing" {
                "app_name='demo'\n[public_env]\nPATH={}\n"
            } else {
                "app_name='demo'\n"
            };
            std::fs::write(&app_path, config).unwrap();
            std::fs::write(
                &deployment_path,
                if scenario == "invalid" {
                    "invalid TOML"
                } else {
                    ""
                },
            )
            .unwrap();
            let desired = policy(&app_path).await.unwrap();
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let origin = format!("http://{}", listener.local_addr().unwrap());
            let mut app = json!({"policy":desired.policy(),"cloud":{},"status":"running","desired_state":"running","potentially_stale":false,"applied_revision":1,"api_url":format!("{origin}/tenant")});
            if matches!(scenario, "config" | "stopped-config") {
                app["cloud"] = json!({"memory_mb":512});
            }
            if matches!(scenario, "stopped" | "stopped-config") {
                app["status"] = json!("stopped");
                app["desired_state"] = json!("stopped");
            }
            if scenario == "pending" {
                app["potentially_stale"] = json!(true);
            }
            let state = Arc::new(Mutex::new(Server {
                app: (!matches!(
                    scenario,
                    "new" | "name-conflict" | "rename" | "rename-override"
                ))
                .then_some(app),
                calls: Vec::new(),
                api_url: format!("{origin}/tenant"),
                reject_deployment: scenario == "deployment-error",
                name_taken: matches!(scenario, "name-conflict" | "rename" | "rename-override"),
                active: concepts::prefixed_ulid::DeploymentId::generate(),
            }));
            let service = Router::new()
                .fallback(any(handle))
                .with_state(state.clone());
            let task = tokio::spawn(async move { axum::serve(listener, service).await.unwrap() });
            let mut args = super::super::tests::args(
                &origin,
                &[
                    "app",
                    "deploy",
                    "-a",
                    app_path.to_str().unwrap(),
                    "-d",
                    deployment_path.to_str().unwrap(),
                ],
            );
            if scenario == "rename" {
                let Cloud::App(target) = &mut args.command else {
                    unreachable!()
                };
                target.app = None;
            }
            let result = if matches!(scenario, "rename" | "rename-override") {
                args.deploy_with_prompt(Some(|_: &Question| Ok("renamed".to_owned())))
                    .await
            } else {
                args.deploy_with_prompt(None::<fn(&Question) -> anyhow::Result<String>>)
                    .await
            };
            let state = state.lock().unwrap();
            if scenario == "name-conflict" {
                assert!(result.unwrap_err().is::<AppNameTaken>());
                assert!(state.app.is_none());
                assert_eq!(std::fs::read_to_string(&app_path).unwrap(), config);
            } else if matches!(scenario, "rename" | "rename-override") {
                result.unwrap();
                assert_eq!(state.app.as_ref().unwrap()["app_name"], "renamed");
                let saved: AppConfigToml =
                    toml::from_str(&std::fs::read_to_string(&app_path).unwrap()).unwrap();
                assert_eq!(
                    saved.app_name.as_deref(),
                    Some(if scenario == "rename" {
                        "renamed"
                    } else {
                        "demo"
                    })
                );
                assert!(
                    state
                        .calls
                        .contains(&"POST /v1/apps/renamed/apply".to_owned())
                );
                assert!(state.calls.last().unwrap().ends_with("/switch"));
            } else if matches!(scenario, "missing" | "invalid") {
                let error = result.unwrap_err().to_string();
                if scenario == "missing" {
                    assert!(error.contains("PATH"));
                }
                assert!(state.calls.iter().all(|call| call.starts_with("GET")));
            } else if scenario == "deployment-error" {
                assert!(
                    result
                        .unwrap_err()
                        .to_string()
                        .contains("deployment active")
                );
                assert!(
                    state
                        .calls
                        .last()
                        .unwrap()
                        .starts_with("PUT /tenant/v1/deployments/")
                );
                assert!(!state.calls.contains(&"POST /v1/apps/demo/apply".to_owned()));
            } else {
                result.unwrap();
                assert_eq!(
                    state.calls.contains(&"POST /v1/apps".to_owned()),
                    scenario == "new"
                );
                assert_eq!(
                    state.calls.contains(&"PUT /v1/apps/demo/config".to_owned()),
                    matches!(scenario, "config" | "stopped-config")
                );
                assert_eq!(
                    state.calls.contains(&"POST /v1/apps/demo/apply".to_owned()),
                    matches!(scenario, "new" | "config" | "pending" | "stopped-config")
                );
                assert_eq!(
                    state.calls.contains(&"POST /v1/apps/demo/start".to_owned()),
                    matches!(scenario, "stopped" | "stopped-config")
                );
                assert!(
                    state
                        .calls
                        .iter()
                        .any(|call| call.starts_with("PUT /tenant/v1/deployments/")
                            && !call.ends_with("/switch"))
                );
                assert!(state.calls.last().unwrap().ends_with("/switch"));
            }
            task.abort();
        }
    }
}
