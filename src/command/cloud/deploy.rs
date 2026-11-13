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
        let Cloud::App(target) = &self.command else {
            unreachable!()
        };
        let config_path = app_config_path(&target.command)?;
        let Cloud::App(CloudAppTarget {
            command:
                CloudApp::Deploy {
                    selected: SelectedApp { app },
                    deployment,
                    values,
                    draft,
                    description,
                    ..
                },
            ..
        }) = self.command
        else {
            unreachable!()
        };
        let config = match &config_path {
            Some((path, _)) => Some(policy(path).await?),
            None => None,
        };
        let mut name = app_name(
            app.as_deref()
                .or_else(|| config.as_ref().map(|c| c.app_name.as_str())),
        )
        .await?;
        let (config, app_config) = config_for_app(&name, config.zip(config_path), true)?
            .expect("deploy requires an app config");
        let save_name = app.is_none();
        let explicit = explicit_values(&values)?;
        let prepared = prepare_deployment_manifest_from_disk(&deployment).await?;
        eprintln!("Deploying {name}");
        let step =
            |command: &str| crate::client::step(format_args!("→ obelisk cloud app {command}"));
        let fill = values.fill || values.fill_non_interactive;
        let existing = client.lookup_app(&name).await?;
        // Existing apps stage into a draft; `None` re-applies the applied revision.
        let (current, staged) = if let Some(current) = existing {
            let generated = draft.is_none();
            let key = draft.unwrap_or_else(|| {
                let id = ExecutionIdTopLevel::generate().to_string();
                let ulid = id.split_once('_').map_or(id.as_str(), |(_, ulid)| ulid);
                format!("deploy-{}", ulid.to_ascii_lowercase())
            });
            let resume = |error: anyhow::Error| {
                error.context(format!(
                    "Draft `{key}` keeps the staged changes; retry with `obelisk cloud app deploy --app {name} --draft {key}` or discard it with `obelisk cloud app unstage --app {name} --draft {key}`"
                ))
            };
            step(&format!("stage --draft {key}"));
            let staged = client
                .draft(
                    &name,
                    &key,
                    Method::PUT,
                    &["config"],
                    Some(Body::Toml(config.text.clone())),
                )
                .await?;
            if fill {
                step("fill");
            }
            let (staged, filled) = stage_values(
                &client,
                &name,
                &key,
                Some(staged),
                explicit,
                Values::new(
                    values.fill,
                    values.fill_non_interactive,
                    prompt.as_mut().map(|p| p as Prompt),
                )
                .map_err(&resume)?,
            )
            .await
            .map_err(&resume)?;
            let staged = staged.expect("draft was staged");
            let missing = output::draft_values(&staged, "missing");
            if !missing.is_empty() {
                return Err(resume(anyhow::anyhow!(
                    "Missing values: {}. Pass them with `--env NAME[=VALUE]` or `--secret NAME`, or retry with `--fill` or `--fill-non-interactive`",
                    missing.join(", ")
                )));
            }
            optional_unset(&filled, values.fill);
            if let Ok(drafts) = client
                .app(&name, Method::GET, &["drafts"], None, Kind::Read)
                .await
            {
                for other in drafts["drafts"].as_array().into_iter().flatten() {
                    if other["key"] != key.as_str() && other["summary"]["changed"] == true {
                        eprintln!(
                            "Draft `{}` ({}) will be discarded when this deploy is applied",
                            other["key"].as_str().unwrap_or_default(),
                            output::draft_changes(other)
                        );
                    }
                }
            }
            (current, Some((key, staged, generated)))
        } else {
            if fill {
                step("fill");
            }
            let values = initial_values(
                &user_supplied_policy(&desired_config(&config)),
                explicit,
                Values::new(
                    values.fill,
                    values.fill_non_interactive,
                    prompt.as_mut().map(|p| p as Prompt),
                )?,
            )?;
            let original_name = name.clone();
            let current = loop {
                step("new");
                let mut body = json!({"app_toml":with_app_name(&config.text, &name)?});
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
                std::fs::write(&app_config, with_app_name(&config.text, &name)?).with_context(
                    || {
                        format!(
                            "App {name} was created, but {} could not be updated; use --app {name}",
                            app_config.display()
                        )
                    },
                )?;
                eprintln!("Updated app_name to {name} in {}", app_config.display());
            }
            (current, None)
        };
        let api_url = api_origin(
            current["endpoints"]["api"]
                .as_str()
                .context("Control plane response is missing endpoints.api")?,
        )?;
        let startup = ClientStartup::cloud(
            api_url.as_str().trim_end_matches('/').to_owned(),
            self.api_token,
            self.verbose,
        );
        step("deployment submit");
        let submitted = crate::command::deployment::submit_prepared(
            &startup,
            startup.api_url(),
            prepared,
            true,
            description,
            None,
        )
        .await
        .with_context(|| format!("Deployment staging failed; inspect `obelisk cloud app status --app {name}` and retry deploy"));
        let id = match (submitted, &staged) {
            (Ok(id), _) => id,
            (Err(error), Some((key, ..))) => {
                return Err(error.context(format!("Draft `{key}` keeps the staged changes; retry with `obelisk cloud app deploy --app {name} --draft {key}`")));
            }
            (Err(error), None) => return Err(error),
        };
        let key = staged
            .as_ref()
            .map_or(DEFAULT_DRAFT, |(key, ..)| key.as_str());
        let body = apply_request(
            &name,
            key,
            staged.as_ref().map(|(_, draft, _)| draft),
            &current,
            &id.to_string(),
            false,
            self.json,
        )?;
        let mut outcomes = Vec::new();
        step("apply");
        let current = client
            .app(
                &name,
                Method::POST,
                &["apply"],
                Some(body),
                Kind::Command(None),
            )
            .await;
        let current = match (current, &staged) {
            (Ok(current), _) => current,
            (Err(error), Some((key, ..))) => {
                return Err(error.context(format!("Draft `{key}` keeps the staged changes; retry with `obelisk cloud app deploy --app {name} --draft {key}`")));
            }
            (Err(error), None) => return Err(error),
        };
        // An unchanged generated draft was not applied and would linger.
        if let Some((key, draft, true)) = &staged
            && draft["summary"]["changed"] != true
        {
            let _ = client.draft(&name, key, Method::DELETE, &[], None).await;
        }
        eprintln!("{}", output::lifecycle_outcome(&current));
        if let Some(outcome) = current.get("outcome") {
            outcomes.push(outcome.clone());
        }
        ensure!(
            current["status"] == "running",
            "App is {}; inspect it with `obelisk cloud app status --app {name}`",
            current["status"]
        );
        let result = json!({"app":current,"deployment_id":id,"outcomes":outcomes});
        if self.json {
            print_json(&result)
        } else {
            let mut out = format!("Deployed {name}\n");
            output::endpoints(&mut out, &current["endpoints"], "");
            eprint!("{out}");
            Ok(())
        }
    }
}

/// Reports optional declarations left unset by a non-interactive fill.
fn optional_unset(filled: &Value, interactive: bool) {
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
            "Optional configuration not set: {}. Pass them with `--env` or `--secret`, or deploy with `--fill`.",
            unset.join(", ")
        );
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
    use super::super::tests::{MockDrafts, request_body, staged_config};
    use super::*;
    use axum::{
        Router,
        body::Bytes,
        extract::State,
        http::{HeaderMap, StatusCode},
        response::IntoResponse as _,
        routing::any,
    };
    use std::sync::{Arc, Mutex};

    struct Server {
        app: Option<Value>,
        drafts: MockDrafts,
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
        headers: HeaderMap,
        uri: axum::http::Uri,
        body: Bytes,
    ) -> axum::response::Response {
        let mut state = state.lock().unwrap();
        state.calls.push(format!("{method} {}", uri.path()));
        let routed_path = uri.path().replace("/v1/apps/renamed", "/v1/apps/demo");
        let path = routed_path.as_str();
        let body = request_body(&headers, &body);
        if let Some(rest) = path.strip_prefix("/v1/apps/demo/")
            && let Some(result) = state.drafts.route(&method, rest, &body)
        {
            return match result {
                Ok(view) => axum::Json(view).into_response(),
                Err(status) => status.into_response(),
            };
        }
        let mut value = match (method, path) {
            (Method::GET, "/v1/apps/demo") => match &state.app {
                Some(app) => app.clone(),
                None => return (StatusCode::NOT_FOUND, "missing").into_response(),
            },
            (Method::POST, "/v1/apps") => {
                let mut app = staged_config(body["app_toml"].as_str().unwrap());
                if state.name_taken && app["app_name"] == "demo" {
                    return (
                        StatusCode::CONFLICT,
                        axum::Json(
                            json!({"error":"App name is already taken","code":"app_name_taken"}),
                        ),
                    )
                        .into_response();
                }
                if app["policy"]["secrets"].get("INITIAL_KEY").is_some() {
                    assert_eq!(body["env"]["INITIAL_REGION"], "initial-value");
                    assert_eq!(body["secrets"]["INITIAL_KEY"], "initial-value");
                }
                app["env"] = body["env"].clone();
                state.drafts = MockDrafts::new(body["app_toml"].as_str().unwrap(), 1);
                state.drafts.applied["env"] = body["env"].clone();
                app["status"] = json!("running");
                app["potentially_stale"] = json!(false);
                app["revision"] = json!(1);
                app["applied_revision"] = json!(1);
                app["desired_state"] = json!("running");
                state.app = Some(app.clone());
                app
            }
            (Method::POST, "/v1/apps/demo/apply") => {
                assert_eq!(body["deployment_id"].as_str(), state.submitted.as_deref());
                assert!(body.get("config").is_none());
                state.drafts.apply(&body).unwrap();
                let (revision, applied) = (state.drafts.revision, state.drafts.applied_revision);
                let applied_config = staged_config(state.drafts.app_toml(""));
                let app = state.app.as_mut().unwrap();
                app["revision"] = json!(revision);
                app["applied_revision"] = json!(applied);
                app["policy"] = applied_config["policy"].clone();
                app["cloud"] = applied_config["cloud"].clone();
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
                app.clone()
            }
            (Method::GET, "/tenant/v1/info") => json!(rest_types::server::ServerInfoResponse {
                app_name: "demo".to_owned(),
                app_env_prefix: String::new(),
                obelisk_version: "0.0.0".to_owned(),
                deployment_id: state.active.to_string(),
                node_run_id: concepts::prefixed_ulid::NodeRunId::generate().to_string(),
                switch_count: 0,
                app_config_digest: "digest".to_owned(),
                gc: None,
                started_at: chrono::Utc::now(),
            }),
            (Method::GET, _) if path.starts_with("/tenant/v1/deployment-digests/") => {
                return StatusCode::NOT_FOUND.into_response();
            }
            (Method::PUT, _)
                if path.starts_with("/tenant/v1/deployments/") && !path.ends_with("/switch") =>
            {
                assert_eq!(body["allow_unavailable_runtime_config"], true);
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
            _ => panic!("unexpected request: {}", state.calls.last().unwrap()),
        };
        if value.get("policy").is_some() {
            value["endpoints"] = json!({"api": state.api_url});
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
            "new-integration",
            "integration",
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
            } else if matches!(scenario, "new-integration" | "integration") {
                "app_name='demo'\n[cloud.integrations]\nllm={type='llm',env='PATH'}\n[public_env]\nPATH={}\n"
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
            // The applied app.toml differs from the local one only in the config scenarios.
            let applied_toml = if matches!(scenario, "config" | "stopped-config") {
                "app_name='demo'\n[cloud]\nmemory_mb=512\n"
            } else {
                config
            };
            let mut app = json!({"policy":desired.policy(),"cloud":{},"status":"running","desired_state":"running","potentially_stale":false,"revision":1,"applied_revision":1,"endpoints":{"api":format!("{origin}/tenant")}});
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
                        | "new-integration"
                        | "name-conflict"
                        | "rename"
                        | "rename-override"
                ))
                .then_some(app),
                drafts: MockDrafts::new(applied_toml, 1),
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
                target.command.selected_mut().unwrap().app = None;
            }
            let result = if scenario == "new-values" {
                let Cloud::App(target) = &mut args.command else {
                    unreachable!()
                };
                let CloudApp::Deploy { values, .. } = &mut target.command else {
                    unreachable!()
                };
                values.fill = true;
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
            assert!(!state.calls.iter().any(|call| call.ends_with("/switch")));
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
                    saved.app_name,
                    if scenario == "rename" {
                        "renamed"
                    } else {
                        "demo"
                    }
                );
                assert!(
                    state
                        .calls
                        .contains(&"POST /v1/apps/renamed/apply".to_owned())
                );
                assert!(state.calls.last().unwrap().ends_with("/apply"));
            } else if matches!(scenario, "missing" | "invalid") {
                let error = format!("{:#}", result.unwrap_err());
                if scenario == "invalid" {
                    assert!(state.calls.is_empty());
                } else {
                    // Missing values fail before the deployment is submitted and keep the draft.
                    assert!(error.contains("Missing values: env PATH"), "{error}");
                    assert!(
                        error.contains(
                            "retry with `obelisk cloud app deploy --app demo --draft deploy-"
                        ),
                        "{error}"
                    );
                    assert!(state.submitted.is_none());
                    assert_eq!(state.drafts.drafts.len(), 1);
                }
            } else if scenario == "deployment-error" {
                let error = format!("{:#}", result.unwrap_err());
                assert!(error.contains("Deployment staging failed"), "{error}");
                assert!(error.contains("--draft deploy-"), "{error}");
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
                    matches!(scenario, "new" | "new-values" | "new-integration")
                );
                let new = matches!(scenario, "new" | "new-values" | "new-integration");
                assert_eq!(
                    state
                        .calls
                        .iter()
                        .any(|call| call.starts_with("PUT /v1/apps/demo/drafts/deploy-")
                            && call.ends_with("/config")),
                    !new
                );
                // Changed drafts are applied; unchanged generated drafts are discarded.
                assert!(state.drafts.drafts.is_empty());
                let changed = matches!(scenario, "config" | "stopped-config");
                assert_eq!(
                    state.drafts.applied_revision,
                    if changed { 3 } else { 1 },
                    "{scenario}"
                );
                assert_eq!(
                    state
                        .calls
                        .iter()
                        .any(|call| call.starts_with("DELETE /v1/apps/demo/drafts/deploy-")),
                    !new && !changed
                );
                if scenario == "new-integration" {
                    assert_eq!(state.app.as_ref().unwrap()["env"], json!({}));
                }
                assert!(state.calls.contains(&"POST /v1/apps/demo/apply".to_owned()));
                assert!(!state.calls.contains(&"POST /v1/apps/demo/start".to_owned()));
                assert!(
                    state
                        .calls
                        .iter()
                        .any(|call| call.starts_with("PUT /tenant/v1/deployments/")
                            && !call.ends_with("/switch"))
                );
                let submit = state
                    .calls
                    .iter()
                    .position(|call| call.starts_with("PUT /tenant/v1/deployments/"))
                    .unwrap();
                let apply = state
                    .calls
                    .iter()
                    .position(|call| call == "POST /v1/apps/demo/apply")
                    .unwrap();
                assert!(submit < apply);
                assert!(
                    state.calls[apply..]
                        .iter()
                        .all(|call| !call.starts_with("PUT "))
                );
            }
            task.abort();
        }
    }
}
