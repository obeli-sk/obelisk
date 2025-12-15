use axum::{
    Json, Router,
    extract::{Path, Query, State},
    response::{IntoResponse, Response},
    routing,
};
use axum_accept::AcceptExtractor;
use concepts::{
    ExecutionId, FinishedExecutionError, FunctionFqn, SupportedFunctionReturnValue,
    storage::{DbErrorRead, DbErrorWrite, DbErrorWriteNonRetriable, DbPool, ExecutionEventInner},
};
use http::StatusCode;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::Arc;
use val_json::wast_val::WastVal;

use crate::{
    command::server::{self, ComponentConfigRegistryRO, SubmitError},
    server::web_api::components::components_list,
};

#[derive(Clone)]
pub(crate) struct WebApiState {
    pub(crate) db_pool: Arc<dyn DbPool>,
    pub(crate) component_registry_ro: ComponentConfigRegistryRO,
}

pub(crate) fn app_router(state: WebApiState) -> Router {
    Router::new()
        .nest("/v1", v1_router())
        .with_state(Arc::new(state))
}

fn v1_router() -> Router<Arc<WebApiState>> {
    Router::new()
        .route("/execution-id", routing::get(execution_id_generate))
        .route(
            "/executions/{execution-id}/status",
            routing::get(execution_status_get),
        )
        .route("/executions/{execution-id}", routing::get(execution_get))
        .route("/executions/{execution-id}", routing::put(execution_submit))
        .route("/components", routing::get(components_list))
}

async fn execution_id_generate(_: State<Arc<WebApiState>>, accept: ExecutionIdAccept) -> Response {
    let id = ExecutionId::generate();
    match accept {
        ExecutionIdAccept::Json => Json(json!(id)).into_response(),
        ExecutionIdAccept::Text => id.to_string().into_response(),
    }
}

async fn execution_status_get(
    Path(execution_id): Path<ExecutionId>,
    state: State<Arc<WebApiState>>,
    accept: ExecutionIdAccept,
) -> Result<Response, ErrorWrapper<DbErrorRead>> {
    let pending_state = state
        .db_pool
        .connection()
        .get_pending_state(&execution_id)
        .await
        .map_err(|e| ErrorWrapper(e, accept))?;
    Ok(match accept {
        ExecutionIdAccept::Json => Json(json!(pending_state)).into_response(),
        ExecutionIdAccept::Text => pending_state.to_string().into_response(),
    })
}

#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
enum RetVal {
    Ok(Option<WastVal>),
    Err(Option<WastVal>),
    ExecutionError(FinishedExecutionError),
}
impl From<SupportedFunctionReturnValue> for RetVal {
    fn from(value: SupportedFunctionReturnValue) -> RetVal {
        match value {
            SupportedFunctionReturnValue::Ok { ok: val_with_type } => {
                RetVal::Ok(val_with_type.map(|it| it.value))
            }
            SupportedFunctionReturnValue::Err { err: val_with_type } => {
                RetVal::Err(val_with_type.map(|it| it.value))
            }
            SupportedFunctionReturnValue::ExecutionError(err) => RetVal::ExecutionError(err),
        }
    }
}

async fn execution_get(
    Path(execution_id): Path<ExecutionId>,
    state: State<Arc<WebApiState>>,
) -> Result<Response, ErrorWrapper<DbErrorRead>> {
    let last_event = state
        .db_pool
        .connection()
        .get_last_execution_event(&execution_id)
        .await
        .map_err(|e| ErrorWrapper(e, ExecutionIdAccept::Json))?;
    Ok(
        if let ExecutionEventInner::Finished { result, .. } = last_event.event {
            let result = RetVal::from(result);
            Json(json!(result)).into_response()
        } else {
            (
                StatusCode::TOO_EARLY,
                Json(json!({"error":"not finished yet"})),
            )
                .into_response()
        },
    )
}

#[derive(Deserialize, Debug)]
struct ExecutionPutPayload {
    ffqn: FunctionFqn,
    params: Vec<serde_json::Value>,
}

async fn execution_submit(
    Path(execution_id): Path<ExecutionId>,
    state: State<Arc<WebApiState>>,
    Json(payload): Json<ExecutionPutPayload>,
) -> Response {
    match server::submit(
        state.db_pool.connection().as_ref(),
        execution_id,
        payload.ffqn,
        payload.params,
        &state.component_registry_ro,
    )
    .await
    {
        Ok(()) => (StatusCode::CREATED, Json(json!({ "ok": "created" }))).into_response(),
        Err(SubmitError::DbErrorWrite(DbErrorWrite::NonRetriable(
            DbErrorWriteNonRetriable::Conflict,
        ))) => (
            StatusCode::CONFLICT,
            Json(json!({ "err": "already exists" })),
        )
            .into_response(),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "err": err.to_string() })),
        )
            .into_response(),
    }
}

pub(crate) mod components {
    use super::{
        Arc, Deserialize, ExecutionIdAccept, FunctionFqn, IntoResponse, Json, Query, Response,
        Serialize, State, WebApiState, json,
    };
    use concepts::{
        ComponentId, ComponentType, FunctionExtension, FunctionMetadata, ParameterType,
    };
    use std::fmt::Write as _;

    #[derive(Deserialize, Debug)]
    pub(crate) struct ComponentsListParams {
        r#type: Option<ComponentType>,
        name: Option<String>,
        digest: Option<String>,
        exports: Option<bool>,
        imports: Option<bool>,
        extensions: Option<bool>,
        submittable: Option<bool>,
    }

    pub(crate) async fn components_list(
        state: State<Arc<WebApiState>>,
        Query(params): Query<ComponentsListParams>,
        accept: ExecutionIdAccept,
    ) -> Response {
        let extensions = params.extensions.unwrap_or_default();
        let mut components = state.component_registry_ro.list(extensions);

        if let Some(name) = params.name {
            components.retain(|c| c.component_id.name.as_ref() == name);
        }
        if let Some(digest) = params.digest {
            components
                .retain(|c| c.component_id.input_digest.digest_base16_without_prefix() == digest);
        }
        if let Some(ty) = params.r#type {
            components.retain(|c| c.component_id.component_type == ty);
        }
        let exports = params.exports.unwrap_or_default();
        let imports = params.imports.unwrap_or_default();
        let components: Vec<_> = components
            .into_iter()
            .map(|c| {
                let (exports, exports_ext) = if exports {
                    let mut exports = Vec::new();
                    let mut exports_ext = Vec::new();
                    for export in c
                        .workflow_or_activity_config
                        .into_iter()
                        .flat_map(|c| c.exports_ext)
                        .filter(|e| {
                            if let Some(submittable) = params.submittable {
                                e.submittable == submittable
                            } else {
                                true
                            }
                        })
                    {
                        if export.extension.is_none() {
                            exports.push(FunctionMetadataLite::from(export));
                        } else if extensions {
                            exports_ext.push(FunctionMetadataLite::from(export));
                        }
                    }

                    (
                        Some(exports),
                        if extensions { Some(exports_ext) } else { None },
                    )
                } else {
                    (None, None)
                };

                ComponentConfig {
                    component_id: c.component_id,
                    imports: if imports {
                        Some(
                            c.imports
                                .into_iter()
                                .map(FunctionMetadataLite::from)
                                .collect(),
                        )
                    } else {
                        None
                    },
                    exports,
                    exports_ext,
                }
            })
            .collect();

        match accept {
            ExecutionIdAccept::Json => Json(json!(components)).into_response(),
            ExecutionIdAccept::Text => {
                let mut output = String::new();
                for component in components {
                    writeln!(output, "{}", component.component_id).expect("writing to string");
                }
                output.into_response()
            }
        }
    }

    #[derive(Serialize)]
    pub(crate) struct ComponentConfig {
        component_id: ComponentId,
        #[serde(skip_serializing_if = "Option::is_none")]
        imports: Option<Vec<FunctionMetadataLite>>,
        #[serde(skip_serializing_if = "Option::is_none")]
        exports: Option<Vec<FunctionMetadataLite>>,
        #[serde(skip_serializing_if = "Option::is_none")]
        exports_ext: Option<Vec<FunctionMetadataLite>>,
    }

    #[derive(serde::Serialize)]
    struct FunctionMetadataLite {
        ffqn: FunctionFqn,
        parameter_types: Vec<ParameterTypeLite>,
        return_type: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        extension: Option<FunctionExtension>,
        /// Externally submittable: primary functions + `-schedule` extended, but no activity stubs
        submittable: bool,
    }
    impl From<FunctionMetadata> for FunctionMetadataLite {
        fn from(value: FunctionMetadata) -> Self {
            FunctionMetadataLite {
                ffqn: value.ffqn,
                parameter_types: value
                    .parameter_types
                    .0
                    .into_iter()
                    .map(ParameterTypeLite::from)
                    .collect(),
                return_type: value.return_type.wit_type().to_string(),
                extension: value.extension,
                submittable: value.submittable,
            }
        }
    }

    #[derive(serde::Serialize)]
    struct ParameterTypeLite {
        name: String,
        wit_type: String,
    }

    impl From<ParameterType> for ParameterTypeLite {
        fn from(value: ParameterType) -> Self {
            ParameterTypeLite {
                name: value.name.to_string(),
                wit_type: value.wit_type.to_string(),
            }
        }
    }
}

#[derive(AcceptExtractor, Clone, Copy)]
pub(crate) enum ExecutionIdAccept {
    #[accept(mediatype = "text/plain")]
    Text,
    #[accept(mediatype = "application/json")]
    Json,
}

struct ErrorWrapper<E>(E, ExecutionIdAccept);

impl IntoResponse for ErrorWrapper<DbErrorRead> {
    fn into_response(self) -> Response {
        let (status, message) = match self.0 {
            DbErrorRead::NotFound => (StatusCode::NOT_FOUND, "Not found".to_string()),
            DbErrorRead::Generic(err) => (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
        };
        match self.1 {
            ExecutionIdAccept::Json => (status, Json(json!({ "error": message }))).into_response(),
            ExecutionIdAccept::Text => (status, message).into_response(),
        }
    }
}
