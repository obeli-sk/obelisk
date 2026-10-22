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
                    fill_non_interactive,
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
        let existing = client.lookup_app(&name).await?;
        let is_new = existing.is_none();
        let mut current = if existing.is_some() {
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
            if interactive || fill_non_interactive {
                step("fill");
            }
            let values = initial_values(
                &desired["policy"],
                Values::new(
                    interactive,
                    fill_non_interactive,
                    prompt.as_mut().map(|p| p as Prompt),
                )?,
            )?;
            let original_name = name.clone();
            let current = loop {
                step("new");
                let mut body = desired.clone();
                body["app_name"] = json!(name);
                body["env"] = values["env"].clone();
                body["secrets"] = values["secrets"].clone();
                match client
                    .request(
                        Method::POST,
                        &["v1", "apps"],
                        Some(body),
                        Kind::SecretCommand(None),
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
        step("deployment enqueue");
        let id = crate::command::deployment::enqueue_prepared(&startup, prepared).await
            .with_context(|| format!("Deployment staging failed; inspect `obelisk cloud app --app {name} status` and retry deploy"))?;
        let filled = if is_new {
            json!({"declarations":[]})
        } else {
            if interactive || fill_non_interactive {
                step("fill");
            }
            fill_values(
                &client,
                &name,
                &desired["policy"],
                Values::new(
                    interactive,
                    fill_non_interactive,
                    prompt.as_mut().map(|p| p as Prompt),
                )?,
            )
            .await?
        };
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
        {
            step("apply");
            current = client
                .app(
                    &name,
                    Method::POST,
                    &["apply"],
                    Some(json!({"force":false,"config":desired,"deployment_id":id})),
                    Kind::Command(None),
                )
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
        submitted: Option<String>,
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
                if body["policy"]["secrets"].get("INITIAL_KEY").is_some() {
                    assert_eq!(body["env"]["INITIAL_REGION"], "initial-value");
                    assert_eq!(body["secrets"]["INITIAL_KEY"], "initial-value");
                }
                let mut app = body;
                app["status"] = json!("running");
                app["potentially_stale"] = json!(false);
                app["applied_revision"] = json!(1);
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
            (Method::POST, "/v1/apps/demo/apply") => {
                assert_eq!(body["deployment_id"].as_str(), state.submitted.as_deref());
                assert!(body["config"]["policy"].is_object());
                let app = state.app.as_mut().unwrap();
                if path.ends_with("/apply") {
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
                state.submitted = Some(
                    path.strip_prefix("/tenant/v1/deployments/")
                        .unwrap()
                        .to_owned(),
                );
                json!({"ok":true})
            }
            (Method::PUT, _) if path.ends_with("/switch") => {
                assert_eq!(body["apply"], false);
                assert_eq!(body["allow_unavailable_runtime_config"], true);
                json!({"ok":"restart_required"})
            }
            _ => panic!("unexpected request: {}", state.calls.last().unwrap()),
        };
        if value.get("policy").is_some() {
            value["api_url"] = json!(state.api_url);
        }
        axum::Json(value).into_response()
    }

    #[tokio::test]
    async fn deploy_passes_its_uploaded_id_and_policy_to_apply() {
        test_utils::set_up();
        for scenario in [
            "new",
            "new-values",
            "new-missing",
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
            let config = if matches!(scenario, "new-values" | "new-missing") {
                "app_name='demo'\n[public_env]\nINITIAL_REGION={}\n[secrets]\nINITIAL_KEY={}\n"
            } else if scenario == "missing" {
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
                    "new"
                        | "new-values"
                        | "new-missing"
                        | "name-conflict"
                        | "rename"
                        | "rename-override"
                ))
                .then_some(app),
                calls: Vec::new(),
                api_url: format!("{origin}/tenant"),
                reject_deployment: scenario == "deployment-error",
                name_taken: matches!(scenario, "name-conflict" | "rename" | "rename-override"),
                active: concepts::prefixed_ulid::DeploymentId::generate(),
                submitted: None,
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
            let result = if scenario == "new-values" {
                let Cloud::App(target) = &mut args.command else {
                    unreachable!()
                };
                let CloudApp::Deploy { fill, .. } = &mut target.command else {
                    unreachable!()
                };
                *fill = true;
                args.deploy_with_prompt(Some(|q: &Question| {
                    Ok(match q {
                        Question::Value { .. } => "initial-value",
                        Question::Line(_) => "",
                    }
                    .to_owned())
                }))
                .await
            } else if matches!(scenario, "rename" | "rename-override") {
                args.deploy_with_prompt(Some(|_: &Question| Ok("renamed".to_owned())))
                    .await
            } else {
                args.deploy_with_prompt(None::<fn(&Question) -> anyhow::Result<String>>)
                    .await
            };
            let state = state.lock().unwrap();
            if scenario == "new-missing" {
                let error = result.unwrap_err().to_string();
                assert!(error.contains("Missing required configuration"));
                assert!(error.contains("Retry with `--fill`"));
                assert_eq!(state.calls, ["GET /v1/apps/demo"]);
            } else if scenario == "name-conflict" {
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
                assert!(state.calls.last().unwrap().ends_with("/apply"));
            } else if matches!(scenario, "missing" | "invalid") {
                let error = result.unwrap_err().to_string();
                if scenario == "missing" {
                    assert!(error.contains("PATH"));
                }
                if scenario == "invalid" {
                    assert!(state.calls.is_empty());
                } else {
                    assert!(state.calls.iter().any(|call| call.ends_with("/switch")));
                    assert!(!state.calls.contains(&"POST /v1/apps/demo/apply".to_owned()));
                }
            } else if scenario == "deployment-error" {
                assert!(
                    result
                        .unwrap_err()
                        .to_string()
                        .contains("Deployment staging failed")
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
                    matches!(scenario, "new" | "new-values")
                );
                assert_eq!(
                    state.calls.contains(&"PUT /v1/apps/demo/config".to_owned()),
                    !matches!(scenario, "new" | "new-values")
                );
                assert!(state.calls.contains(&"POST /v1/apps/demo/apply".to_owned()));
                assert!(!state.calls.contains(&"POST /v1/apps/demo/start".to_owned()));
                assert!(
                    state
                        .calls
                        .iter()
                        .any(|call| call.starts_with("PUT /tenant/v1/deployments/")
                            && !call.ends_with("/switch"))
                );
                let enqueue = state
                    .calls
                    .iter()
                    .position(|call| call.ends_with("/switch"))
                    .unwrap();
                let apply = state
                    .calls
                    .iter()
                    .position(|call| call == "POST /v1/apps/demo/apply")
                    .unwrap();
                assert!(enqueue < apply);
                assert!(state.calls.last().unwrap().ends_with("/apply"));
            }
            task.abort();
        }
    }
}
