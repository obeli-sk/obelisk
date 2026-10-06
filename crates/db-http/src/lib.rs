use async_trait::async_trait;
use chrono::{DateTime, Utc};
use concepts::cas::Cas;
use concepts::component_id::ComponentDigest;
use concepts::prefixed_ulid::{DelayId, DeploymentId, ExecutionIdDerived, ExecutorId, RunId};
use concepts::storage::{
    AppendBatchResponse, AppendDelayResponseOutcome, AppendEventsToExecution, AppendRequest,
    AppendResponse, AppendResponseToExecution, BacktraceFilter, BacktraceInfo, CancelOutcome,
    CasGc, CasGcResult, CleanupResult, ComponentMetadataRecord, ComponentUpgradeReason,
    CreateRequest, DbAdmin, DbConnection, DbErrorGeneric, DbErrorRead, DbErrorReadWithTimeout,
    DbErrorStubResponse, DbErrorWrite, DbExecutor, DbExternalApi, DbPool, DbPoolCloseable,
    DeleteDeploymentResult, DeleteExecutionTreeResult, DeploymentComponentDetail,
    DeploymentComponentFileRecord, DeploymentComponentRecord, DeploymentExecutionCounts,
    DeploymentFileRecord, DeploymentRecord, DeploymentState, EnqueueOutcome, ExecutionEvent,
    ExecutionEventBounds, ExecutionGcResult, ExecutionListPagination, ExecutionLog,
    ExecutionWithState, ExecutionWithStateRequestsResponses, ExpiredTimer, HttpPolicyEventIds,
    ListExecutionEventsResponse, ListExecutionsFilter, ListLogsResponse, ListResponsesResponse,
    LockPendingResponse, LogCursor, LogFilter, LogInfoAppendRow, Pagination, ResponseCursor,
    ResponseSubscriptionEnd, ResponseWithCursor, RetentionPolicy, StorageStatus,
    SubscribeToResponsesError, SystemEvent, SystemEventFilter, SystemEventRetentionResult,
    TimeoutOutcome, Version, VersionType,
};
use concepts::{
    ComponentId, ComponentRetryConfig, ContentDigest, ExecutionId, FunctionFqn, JoinSetId,
    SupportedFunctionReturnValue,
};
use secrecy::{ExposeSecret, SecretString};
use serde::{Serialize, de::DeserializeOwned};
use std::future::Future;
use std::num::NonZeroU16;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;

#[cfg(feature = "test")]
use concepts::storage::{DbConnectionTest, JoinSetResponseEvent, LockedExecution};

mod cas;
#[doc(hidden)]
pub mod error;

#[doc(hidden)]
pub const LONG_POLL_MILLIS: u64 = 30_000;
#[doc(hidden)]
pub const REQUEST_TIMEOUT: Duration = Duration::from_secs(60);

pub fn client() -> Result<reqwest::Client, reqwest::Error> {
    reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(5))
        .timeout(REQUEST_TIMEOUT)
        .redirect(reqwest::redirect::Policy::none())
        .retry(reqwest::retry::never())
        .build()
}

#[derive(Clone)]
pub struct HttpPool {
    client: reqwest::Client,
    endpoint: reqwest::Url,
    token: SecretString,
    closed: watch::Sender<bool>,
}

impl HttpPool {
    pub fn new(
        endpoint: &str,
        token: SecretString,
        client: reqwest::Client,
    ) -> Result<Self, DbErrorGeneric> {
        let mut endpoint =
            reqwest::Url::parse(endpoint).map_err(|err| error::generic(err.to_string()))?;
        if !matches!(endpoint.scheme(), "http" | "https")
            || endpoint.host_str().is_none()
            || !endpoint.username().is_empty()
            || endpoint.password().is_some()
            || endpoint.query().is_some()
            || endpoint.fragment().is_some()
        {
            return Err(error::generic(
                "storage URL must be an HTTP(S) base URL without credentials, query or fragment",
            ));
        }
        if token.expose_secret().is_empty() {
            return Err(error::generic("storage token must not be empty"));
        }
        if !endpoint.path().ends_with('/') {
            endpoint.set_path(&format!("{}/", endpoint.path()));
        }
        Ok(Self {
            client,
            endpoint,
            token,
            closed: watch::channel(false).0,
        })
    }

    pub async fn verify(&self) -> Result<(), DbErrorGeneric> {
        self.admin_conn()
            .await?
            .get_storage_status()
            .await
            .map(|_| ())
            .map_err(|err| match err {
                DbErrorRead::Generic(err) => err,
                other @ DbErrorRead::NotFound => error::generic(other.to_string()),
            })
    }

    async fn call<T: DeserializeOwned>(
        &self,
        interface: &str,
        method: &str,
        args: impl Serialize + Send,
    ) -> Result<T, error::WireError> {
        let mut closed = self.closed.subscribe();
        if *closed.borrow() {
            return Err(error::WireError::Closed);
        }
        let url = self
            .endpoint
            .join(&format!("v1/{interface}/{method}"))
            .map_err(|err| error::WireError::Generic(err.to_string()))?;
        let request = self
            .client
            .post(url)
            .bearer_auth(self.token.expose_secret())
            .timeout(REQUEST_TIMEOUT)
            .json(&args);
        tokio::select! {
            result = async {
                // A failed reply may follow a committed write; callers receive the uncertainty without replay.
                let response = request.send().await.map_err(|err| error::WireError::Generic(err.to_string()))?;
                if !response.status().is_success() {
                    return Err(error::WireError::Generic(format!("storage HTTP status {}", response.status())));
                }
                response.json::<Result<T, error::WireError>>().await.map_err(|err| error::WireError::Generic(err.to_string()))?
            } => result,
            _ = closed.changed() => Err(error::WireError::Closed),
        }
    }

    fn ensure_open(&self) -> Result<(), DbErrorGeneric> {
        if *self.closed.borrow() {
            Err(DbErrorGeneric::Close)
        } else {
            Ok(())
        }
    }
}

#[async_trait]
impl DbPoolCloseable for HttpPool {
    async fn close(&self) {
        self.closed.send_replace(true);
    }
}

macro_rules! pool_connections {
    ($($method:ident => $interface:ident),* $(,)?) => {
        #[async_trait]
        impl DbPool for HttpPool {
            $(async fn $method(&self) -> Result<Box<dyn $interface>, DbErrorGeneric> {
                self.ensure_open()?;
                Ok(Box::new(self.clone()))
            })*
            #[cfg(feature = "test")]
            async fn connection_test(&self) -> Result<Box<dyn DbConnectionTest>, DbErrorGeneric> {
                self.ensure_open()?;
                Ok(Box::new(self.clone()))
            }
        }
    };
}
pool_connections! { db_exec_conn => DbExecutor, connection => DbConnection, external_api_conn => DbExternalApi, admin_conn => DbAdmin, cas_conn => Cas, cas_gc_conn => CasGc }

macro_rules! rpc_interface {
    ($interface:ident, $connection:ident; $( $(#[$attr:meta])* fn $method:ident($($arg:ident: ($($ty:tt)*)),* $(,)?) -> $out:ty, $err:ty; )* @extras { $($extra:item)* }) => {
        #[async_trait]
        impl $interface for HttpPool {
            $( $(#[$attr])*
                #[allow(clippy::too_many_arguments)]
                async fn $method(&self, $($arg: $($ty)*),*) -> Result<$out, $err> {
                    self.call(stringify!($interface), stringify!($method), ($($arg,)* )).await.map_err(<$err>::from)
                }
            )*
            $($extra)*
        }
    };
}
mod methods;
rpc_methods!(rpc_interface);

pub mod openapi;
