use super::*;
use crate::config::deployment::prepare_deployment_manifest_from_disk;

impl CloudArgs {
    pub(super) async fn deploy(self) -> anyhow::Result<()> {
        let client = Client::new(&self)?;
        let Cloud::App(CloudAppTarget {
            app,
            command:
                CloudApp::Deploy {
                    app_config,
                    deployment,
                    interactive,
                },
        }) = self.command
        else {
            unreachable!()
        };
        let config = policy(&app_config).await?;
        let name = app_name(app.as_deref().or(config.app_name.as_deref())).await?;
        let desired = json!({"policy":config.policy(), "cloud":config.cloud});
        let prepared = prepare_deployment_manifest_from_disk(&deployment).await?;
        let step = |command: &str, message: &str| {
            eprintln!("→ obelisk cloud app --app {name} {command}  {message}");
        };
        let mut current = if let Some(current) = client.lookup_app(&name).await? {
            if current["policy"] != desired["policy"] || current["cloud"] != desired["cloud"] {
                step(
                    &format!("stage -a {}", app_config.display()),
                    "Staging app configuration",
                );
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
            step(&format!("new -a {}", app_config.display()), "Creating app");
            let mut body = desired.clone();
            body["app_slug"] = json!(name);
            client
                .request(
                    Method::POST,
                    &["v1", "apps"],
                    Some(body),
                    Kind::Command(None),
                )
                .await?
        };
        if interactive {
            step("fill --interactive", "Resolving missing configuration");
        }
        let filled = fill(
            &client,
            &name,
            &desired["policy"],
            if interactive {
                FillMode::Interactive
            } else {
                FillMode::Check
            },
        )
        .await?;
        let mut outcomes = Vec::new();
        if current["potentially_stale"] == true
            || current["applied_revision"] == 0
            || filled["apply_required"] == true
        {
            step("apply", "Applying staged configuration");
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
            step("start", "Starting VM");
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
                .context("Provisioner response is missing api_url")?,
        )?;
        let startup = ClientStartup::cloud(
            api_url.as_str().trim_end_matches('/').to_owned(),
            self.api_token,
            self.verbose,
        );
        step(
            &format!("deployment apply {}", deployment.display()),
            "Uploading and activating deployment",
        );
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
        let path = uri.path();
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
                app: (scenario != "new").then_some(app),
                calls: Vec::new(),
                api_url: format!("{origin}/tenant"),
                reject_deployment: scenario == "deployment-error",
                active: concepts::prefixed_ulid::DeploymentId::generate(),
            }));
            let service = Router::new()
                .fallback(any(handle))
                .with_state(state.clone());
            let task = tokio::spawn(async move { axum::serve(listener, service).await.unwrap() });
            let args = super::super::tests::args(
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
            let result = args.run().await;
            let state = state.lock().unwrap();
            if matches!(scenario, "missing" | "invalid") {
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
