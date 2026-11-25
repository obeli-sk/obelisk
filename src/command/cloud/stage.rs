use super::*;
use crate::args::DeploymentSource;
use crate::config::deployment::{
    PreparedDeploymentManifest, prepare_deployment_manifest_from_disk,
};

pub(super) async fn stage(session: &Session, args: CloudStage) -> anyhow::Result<()> {
    let prompt = (terminal() && !session.json).then_some(ask);
    stage_with_prompt(session, args, prompt, Path::new("deployment.toml")).await
}

async fn stage_with_prompt(
    session: &Session,
    CloudStage {
        selected: SelectedApp { app },
        app_config,
        draft: CloudDraft { key },
        deployment,
        empty_deployment,
        values,
        description,
    }: CloudStage,
    mut prompt: Option<impl FnMut(&Question) -> anyhow::Result<String>>,
    default_deployment: &Path,
) -> anyhow::Result<()> {
    let client = session.client()?;
    let config_path = discover_app_config(app_config)?;
    // Like push, ./deployment.toml is staged by default; without it the draft's deployment is kept.
    let deployment = match deployment {
        None if !empty_deployment && default_deployment.is_file() => {
            Some(DeploymentSource::File(default_deployment.to_owned()))
        }
        deployment => deployment,
    };
    ensure!(
        description.is_none()
            || matches!(deployment, Some(DeploymentSource::File(_)))
            || empty_deployment,
        "`--description` requires a deployment manifest or `--empty-deployment`"
    );
    let config = match &config_path {
        Some((path, _)) => Some(policy(path).await?),
        None => None,
    };
    let name = app_name(
        app.as_deref()
            .or_else(|| config.as_ref().map(|c| c.app_name.as_str())),
    )
    .await?;
    let config = config_for_app(&name, config.zip(config_path), false)?;
    let explicit = explicit_values(&values)?;
    ensure!(
        config.is_some()
            || deployment.is_some()
            || empty_deployment
            || values.fill
            || values.fill_non_interactive
            || !explicit.is_empty(),
        "Nothing to stage: ./app.toml and ./deployment.toml not found; pass --app-config, --deployment or values"
    );
    let (prepared, existing) = match (deployment, empty_deployment) {
        (Some(DeploymentSource::Id(id)), _) => (None, Some(id.to_string())),
        (Some(DeploymentSource::File(path)), _) => (
            Some(prepare_deployment_manifest_from_disk(&path).await?),
            None,
        ),
        (None, true) => (Some(PreparedDeploymentManifest::empty()), None),
        (None, false) => (None, None),
    };
    let step = |command: &str| crate::client::step(format_args!("→ obelisk cloud app {command}"));
    let current = if let Some(current) = client.lookup_app(&name).await? {
        current
    } else {
        ensure!(
            existing.is_none(),
            "App {name} does not exist, so it has no deployment to stage"
        );
        // Applying the draft of a new app should bring up the whole app, not an empty deployment.
        ensure!(
            prepared.is_some(),
            "App {name} does not exist and ./deployment.toml was not found; pass `--deployment PATH` or `--empty-deployment`"
        );
        // A bare app runs an empty deployment, so its API can receive the submission.
        eprintln!("App {name} does not exist; creating it");
        step("new");
        client
            .request(
                Method::POST,
                &["v1", "apps"],
                Some(json!({"app_toml":with_app_name("", &name)?})),
                Kind::Command(None),
            )
            .await?
    };
    // Submitting first leaves nothing staged when the submission fails.
    let deployment_id = if let Some(prepared) = prepared {
        let api_url = api_origin(
            current["endpoints"]["api"]
                .as_str()
                .context("Control plane response is missing endpoints.api")?,
        )?;
        let startup = ClientStartup::cloud(
            api_url.as_str().trim_end_matches('/').to_owned(),
            session.api_token.clone(),
            session.verbose,
        );
        step("deployment submit");
        let id = crate::command::deployment::submit_prepared(
            &startup,
            startup.api_url(),
            prepared,
            true,
            description,
            None,
        )
        .await
        .context("The deployment was not submitted, nothing was staged")?;
        Some(id.to_string())
    } else {
        existing
    };
    let mut staged = client.resolve_draft(&name, &key).await?;
    let id = draft_id(&staged)?;
    if let Some((config, _)) = config {
        staged = client
            .fenced(
                &name,
                &key,
                id,
                Method::PUT,
                &["config"],
                Some(Body::Toml(config.text)),
            )
            .await?;
    }
    let values = Values::new(
        values.fill,
        values.fill_non_interactive,
        prompt.as_mut().map(|p| p as Prompt),
    )?;
    let (mut staged, _) = stage_values(&client, &name, &key, staged, explicit, values).await?;
    if let Some(deployment_id) = deployment_id {
        staged = client
            .fenced(
                &name,
                &key,
                id,
                Method::PUT,
                &["deployment"],
                Some(Body::Json(json!({"deployment_id":deployment_id}))),
            )
            .await?;
    }
    if session.json {
        return print_json(&staged);
    }
    let apply = format!(
        "obelisk cloud app apply --app {name}{}",
        output::draft_arg(&key)
    );
    let mut out = output::Summary::Staged("Changes staged.", apply).render(&staged);
    if !output::draft_values(&staged, "missing").is_empty() {
        let _ = writeln!(
            out,
            "Set them with `obelisk cloud app env set` or `obelisk cloud app secrets set --app {name}{}`, or in the dashboard, before applying.",
            output::draft_arg(&key)
        );
    }
    eprint!("{out}");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::tests::{MockDrafts, if_match, request_body, staged_config};
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
        submitted: Option<String>,
        applied_deployment: Option<String>,
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
        let path = uri.path().to_owned();
        let body = request_body(&headers, &body);
        assert_eq!(uri.query(), None);
        if let Some(rest) = path.strip_prefix("/v1/apps/demo/")
            && let Some(result) = state.drafts.route(&method, rest, if_match(&headers), &body)
        {
            return match result {
                Ok(view) => axum::Json(view).into_response(),
                Err(status) => status.into_response(),
            };
        }
        let mut value = match (method, path.as_str()) {
            (Method::GET, "/v1/apps/demo") => match &state.app {
                Some(app) => app.clone(),
                None => return (StatusCode::NOT_FOUND, "missing").into_response(),
            },
            (Method::POST, "/v1/apps") => {
                let app_toml = body["app_toml"].as_str().unwrap();
                // The app starts bare; its configuration goes to the draft.
                assert_eq!(staged_config(app_toml), staged_config("app_name='demo'\n"));
                state.drafts = MockDrafts::new(app_toml, 1);
                let mut app = staged_config(app_toml);
                app["status"] = json!("running");
                app["applied_revision"] = json!(1);
                state.app = Some(app.clone());
                app
            }
            (Method::POST, "/v1/apps/demo/apply") => {
                assert!(body.get("deployment_id").is_none());
                // The provisioner takes the deployment from the draft.
                let staged = body["draft"]
                    .as_str()
                    .and_then(|key| state.drafts.drafts.get(key))
                    .and_then(|draft| draft["deployment_id"].as_str())
                    .map(str::to_owned);
                state.drafts.apply(&body, if_match(&headers)).unwrap();
                state.applied_deployment = staged;
                let mut app = state.app.clone().unwrap();
                app["applied_revision"] = json!(state.drafts.applied_revision);
                app["outcome"] = json!({"configuration":"applied","vm":"started"});
                app
            }
            (Method::GET, _) if path.starts_with("/tenant/v1/deployment-digests/") => {
                return StatusCode::NOT_FOUND.into_response();
            }
            (Method::PUT, _)
                if path.starts_with("/tenant/v1/deployments/") && !path.ends_with("/switch") =>
            {
                assert_eq!(body["allow_unavailable_runtime_config"], true);
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

    const EXISTING: &str = "Dep_01M7K4M11EH34V1WVJYFZ4JSS2";

    #[tokio::test]
    async fn stage_creates_a_missing_app_and_stages_its_deployment() {
        test_utils::set_up();
        for scenario in [
            "new",
            "new-default",
            "new-without-deployment",
            "config-only",
            "existing-default",
            "empty",
            "id",
            "manifest-only",
        ] {
            let directory = tempfile::tempdir().unwrap();
            let app_path = directory.path().join("app.toml");
            let deployment_path = directory.path().join("deployment.toml");
            let new = scenario.starts_with("new");
            let config = if new {
                "app_name='demo'\n[secrets]\nKEY={}\n"
            } else {
                "app_name='demo'\n[cloud]\nmemory.mib=512\n"
            };
            std::fs::write(&app_path, config).unwrap();
            // Without ./deployment.toml an existing app keeps the draft's deployment.
            if !matches!(scenario, "new-without-deployment" | "config-only") {
                std::fs::write(&deployment_path, "").unwrap();
            }
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let origin = format!("http://{}", listener.local_addr().unwrap());
            let existing = staged_config("app_name='demo'\n");
            let state = Arc::new(Mutex::new(Server {
                app: (!new).then(|| {
                    let mut app = existing;
                    app["status"] = json!("running");
                    app["applied_revision"] = json!(1);
                    app
                }),
                drafts: MockDrafts::default(),
                calls: Vec::new(),
                api_url: format!("{origin}/tenant"),
                submitted: None,
                applied_deployment: None,
            }));
            let service = Router::new()
                .fallback(any(handle))
                .with_state(state.clone());
            let task = tokio::spawn(async move { axum::serve(listener, service).await.unwrap() });
            // Without app.toml, stage keeps the draft's config.
            let without_config = matches!(scenario, "id" | "manifest-only");
            let mut command = vec!["app", "stage"];
            if !without_config {
                command.extend(["-a", app_path.to_str().unwrap()]);
            }
            match scenario {
                "new" | "manifest-only" => {
                    command.extend(["-d", deployment_path.to_str().unwrap()]);
                }
                "empty" => command.push("--empty-deployment"),
                "id" => command.extend(["-d", EXISTING]),
                _ => {}
            }
            let (session, command) = super::super::tests::args(&origin, &command).into_session();
            let Cloud::App(CloudAppTarget {
                command: CloudApp::Stage(stage_args),
                ..
            }) = command
            else {
                panic!("expected `app stage`")
            };
            // ./deployment.toml is staged unless a deployment is given, as with push.
            let result = stage_with_prompt(
                &session,
                stage_args,
                None::<fn(&Question) -> anyhow::Result<String>>,
                &deployment_path,
            )
            .await;
            if scenario == "new-without-deployment" {
                let error = result.unwrap_err().to_string();
                assert!(error.contains("--empty-deployment"), "{error}");
                // Nothing is created when there is nothing to deploy.
                assert_eq!(state.lock().unwrap().calls, ["GET /v1/apps/demo"]);
                task.abort();
                continue;
            }
            result.unwrap();
            {
                let state = state.lock().unwrap();
                assert_eq!(state.calls.contains(&"POST /v1/apps".to_owned()), new);
                assert!(!state.calls.iter().any(|call| call.ends_with("/apply")));
                assert!(!state.calls.iter().any(|call| call.ends_with("/switch")));
                let draft = state.drafts.drafts.get("default").unwrap();
                if without_config {
                    assert!(!state.calls.iter().any(|call| call.ends_with("/config")));
                } else {
                    assert_eq!(draft["app_toml"], config);
                }
                let expected = if scenario == "id" {
                    Some(EXISTING)
                } else {
                    state.submitted.as_deref()
                };
                assert_eq!(
                    draft.get("deployment_id").and_then(Value::as_str),
                    expected,
                    "{scenario}"
                );
                assert_eq!(
                    state.submitted.is_some(),
                    !matches!(scenario, "config-only" | "id")
                );
                // The deployment is submitted before anything is staged.
                let position =
                    |prefix: &str| state.calls.iter().position(|call| call.starts_with(prefix));
                if let Some(submit) = position("PUT /tenant/v1/deployments/") {
                    assert!(submit < position("PUT /v1/apps/demo/drafts/").unwrap());
                }
            }
            if scenario == "empty" {
                // Apply activates the staged deployment without naming it.
                let args = super::super::tests::args(&origin, &["app", "apply"]);
                args.execute().await.unwrap();
                let state = state.lock().unwrap();
                assert_eq!(state.applied_deployment, state.submitted);
                assert!(!state.calls.iter().any(|call| call.ends_with("/v1/info")));
            }
            task.abort();
        }
    }
}
