use crate::postgres_dao::ddl::{ADMIN_DB_NAME, T_METADATA_EXPECTED_SCHEMA_VERSION};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use concepts::{
    ComponentId, ComponentRetryConfig, ContentDigest, ExecutionId, FunctionFqn, JoinSetId,
    StrVariant, SupportedFunctionReturnValue,
    component_id::{Digest, InputContentDigest},
    prefixed_ulid::{DelayId, ExecutionIdDerived, ExecutorId, RunId},
    storage::{
        AppendBatchResponse, AppendDelayResponseOutcome, AppendEventsToExecution, AppendRequest,
        AppendResponse, AppendResponseToExecution, BacktraceFilter, BacktraceInfo, CreateRequest,
        DUMMY_CREATED, DUMMY_HISTORY_EVENT, DbConnection, DbErrorGeneric, DbErrorRead,
        DbErrorReadWithTimeout, DbErrorWrite, DbErrorWriteNonRetriable, DbExecutor, DbExternalApi,
        DbPool, DbPoolCloseable, ExecutionEvent, ExecutionListPagination, ExecutionRequest,
        ExecutionWithState, ExpiredDelay, ExpiredLock, ExpiredTimer, HISTORY_EVENT_TYPE_JOIN_NEXT,
        HistoryEvent, JoinSetRequest, JoinSetResponse, JoinSetResponseEvent,
        JoinSetResponseEventOuter, LockPendingResponse, Locked, LockedBy, LockedExecution,
        Pagination, PendingState, PendingStateFinished, PendingStateFinishedResultKind,
        PendingStateLocked, ResponseWithCursor, STATE_BLOCKED_BY_JOIN_SET, STATE_FINISHED,
        STATE_LOCKED, STATE_PENDING_AT, TimeoutOutcome, Version, VersionType, WasmBacktrace,
    },
};
use deadpool_postgres::{Client, Config, ManagerConfig, Pool, RecyclingMethod};
use hashbrown::HashMap;
use std::fmt::Write as _;
use std::{collections::VecDeque, pin::Pin, str::FromStr as _, sync::Arc, time::Duration};
use tokio::sync::{mpsc, oneshot};
use tokio_postgres::{
    NoTls, Row, Transaction,
    types::{FromSql, Json, ToSql},
};
use tracing::{Level, debug, error, info, instrument, trace, warn};

fn get<'a, T: FromSql<'a>>(row: &'a Row, name: &str) -> Result<T, DbErrorGeneric> {
    row.try_get(name)
        .map_err(|err| consistency_db_err(format!("Failed to retrieve column '{name}': {err:?}")))
}

mod ddl {
    use concepts::storage::HISTORY_EVENT_TYPE_JOIN_NEXT;

    pub const ADMIN_DB_NAME: &str = "postgres";

    pub const T_METADATA_EXPECTED_SCHEMA_VERSION: i32 = 1;

    // CREATE_TABLE_T_METADATA
    pub const CREATE_TABLE_T_METADATA: &str = r"
CREATE TABLE IF NOT EXISTS t_metadata (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    schema_version INTEGER NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);
";

    // CREATE_TABLE_T_EXECUTION_LOG
    pub const CREATE_TABLE_T_EXECUTION_LOG: &str = r"
CREATE TABLE IF NOT EXISTS t_execution_log (
    execution_id TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    json_value JSON NOT NULL,
    version BIGINT NOT NULL CHECK (version >= 0),
    variant TEXT NOT NULL,
    join_set_id TEXT,
    history_event_type TEXT GENERATED ALWAYS AS (json_value #>> '{HistoryEvent,event,type}') STORED,
    PRIMARY KEY (execution_id, version)
);
";

    // Indexes for t_execution_log
    pub const CREATE_INDEX_IDX_T_EXECUTION_LOG_EXECUTION_ID_VERSION: &str = r"
CREATE INDEX IF NOT EXISTS idx_t_execution_log_execution_id_version ON t_execution_log (execution_id, version);
";

    pub const CREATE_INDEX_IDX_T_EXECUTION_LOG_EXECUTION_ID_VARIANT: &str = r"
CREATE INDEX IF NOT EXISTS idx_t_execution_log_execution_id_variant ON t_execution_log (execution_id, variant);
";

    pub const CREATE_INDEX_IDX_T_EXECUTION_LOG_EXECUTION_ID_JOIN_SET: &str = const_format::formatcp!(
        "CREATE INDEX IF NOT EXISTS idx_t_execution_log_execution_id_join_set ON t_execution_log (execution_id, join_set_id, history_event_type) WHERE history_event_type='{}';",
        HISTORY_EVENT_TYPE_JOIN_NEXT
    );

    // CREATE_TABLE_T_JOIN_SET_RESPONSE
    pub const CREATE_TABLE_T_JOIN_SET_RESPONSE: &str = r"
CREATE TABLE IF NOT EXISTS t_join_set_response (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL,
    execution_id TEXT NOT NULL,
    join_set_id TEXT NOT NULL,

    delay_id TEXT,
    delay_success BOOLEAN,

    child_execution_id TEXT,
    finished_version BIGINT CHECK (finished_version >= 0),

    UNIQUE (execution_id, join_set_id, delay_id, child_execution_id)
);
";

    // Indexes for t_join_set_response
    pub const CREATE_INDEX_IDX_T_JOIN_SET_RESPONSE_EXECUTION_ID_ID: &str = r"
CREATE INDEX IF NOT EXISTS idx_t_join_set_response_execution_id_id ON t_join_set_response (execution_id, id);
";

    pub const CREATE_INDEX_IDX_JOIN_SET_RESPONSE_UNIQUE_CHILD_ID: &str = r"
CREATE UNIQUE INDEX IF NOT EXISTS idx_join_set_response_unique_child_id
ON t_join_set_response (child_execution_id) WHERE child_execution_id IS NOT NULL;
";

    pub const CREATE_INDEX_IDX_JOIN_SET_RESPONSE_UNIQUE_DELAY_ID: &str = r"
CREATE UNIQUE INDEX IF NOT EXISTS idx_join_set_response_unique_delay_id
ON t_join_set_response (delay_id) WHERE delay_id IS NOT NULL;
";

    // CREATE_TABLE_T_STATE
    pub const CREATE_TABLE_T_STATE: &str = r"
CREATE TABLE IF NOT EXISTS t_state (
    execution_id TEXT NOT NULL,
    is_top_level BOOLEAN NOT NULL,
    corresponding_version BIGINT NOT NULL CHECK (corresponding_version >= 0),
    ffqn TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    component_id_input_digest BYTEA NOT NULL,
    first_scheduled_at TIMESTAMPTZ NOT NULL,

    pending_expires_finished TIMESTAMPTZ NOT NULL,
    state TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    intermittent_event_count BIGINT NOT NULL CHECK (intermittent_event_count >=0),

    max_retries BIGINT CHECK (max_retries >= 0),
    retry_exp_backoff_millis BIGINT CHECK (retry_exp_backoff_millis >= 0),
    last_lock_version BIGINT CHECK (last_lock_version >= 0),
    executor_id TEXT,
    run_id TEXT,

    join_set_id TEXT,
    join_set_closing BOOLEAN,

    result_kind JSONB,

    PRIMARY KEY (execution_id)
);
";

    // Indexes for t_state
    pub const IDX_T_STATE_LOCK_PENDING: &str = r"
CREATE INDEX IF NOT EXISTS idx_t_state_lock_pending ON t_state (state, pending_expires_finished, ffqn);
";

    pub const IDX_T_STATE_EXPIRED_TIMERS: &str = r"
CREATE INDEX IF NOT EXISTS idx_t_state_expired_timers ON t_state (pending_expires_finished) WHERE executor_id IS NOT NULL;
";

    pub const IDX_T_STATE_EXECUTION_ID_IS_TOP_LEVEL: &str = r"
CREATE INDEX IF NOT EXISTS idx_t_state_execution_id_is_root ON t_state (execution_id, is_top_level);
";

    pub const IDX_T_STATE_FFQN: &str = r"
CREATE INDEX IF NOT EXISTS idx_t_state_ffqn ON t_state (ffqn);
";

    pub const IDX_T_STATE_CREATED_AT: &str = r"
CREATE INDEX IF NOT EXISTS idx_t_state_created_at ON t_state (created_at);
";

    // CREATE_TABLE_T_DELAY
    pub const CREATE_TABLE_T_DELAY: &str = r"
CREATE TABLE IF NOT EXISTS t_delay (
    execution_id TEXT NOT NULL,
    join_set_id TEXT NOT NULL,
    delay_id TEXT NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (execution_id, join_set_id, delay_id)
);
";

    // CREATE_TABLE_T_BACKTRACE
    pub const CREATE_TABLE_T_BACKTRACE: &str = r"
CREATE TABLE IF NOT EXISTS t_backtrace (
    execution_id TEXT NOT NULL,
    component_id JSONB NOT NULL,
    version_min_including BIGINT NOT NULL CHECK (version_min_including >= 0),
    version_max_excluding BIGINT NOT NULL CHECK (version_max_excluding >= 0),
    wasm_backtrace JSONB NOT NULL,
    PRIMARY KEY (execution_id, version_min_including, version_max_excluding)
);
";

    pub const IDX_T_BACKTRACE_EXECUTION_ID_VERSION: &str = r"
CREATE INDEX IF NOT EXISTS idx_t_backtrace_execution_id_version ON t_backtrace (execution_id, version_min_including, version_max_excluding);
";
}

#[derive(derive_more::Debug, Clone)]
pub struct PostgresConfig {
    pub host: String,
    pub user: String,
    #[debug(skip)]
    pub password: String,
    pub db_name: String,
}

#[derive(Debug, thiserror::Error)]
#[error("initialization error")]
pub struct InitializationError;

async fn create_database(
    config: &PostgresConfig,
) -> Result<DbInitialzationOutcome, InitializationError> {
    let mut cfg = Config::new();
    cfg.host = Some(config.host.clone());
    cfg.user = Some(config.user.clone());
    cfg.password = Some(config.password.clone());
    cfg.dbname = Some(ADMIN_DB_NAME.into());
    cfg.manager = Some(ManagerConfig {
        recycling_method: RecyclingMethod::Fast,
    });

    let pool = cfg.create_pool(None, NoTls).map_err(|err| {
        error!("Cannot create the default pool - {err:?}");
        InitializationError
    })?;

    let client = pool.get().await.map_err(|err| {
        error!("Cannot get a connection from the default pool - {err:?}");
        InitializationError
    })?;

    let row = client
        .query_opt(
            &format!(
                "SELECT 1 FROM pg_database WHERE datname = '{}'",
                config.db_name
            ),
            &[],
        )
        .await
        .map_err(|err| {
            error!("Cannot select from the default database - {err:?}");
            InitializationError
        })?;

    let outcome = if row.is_none() {
        client
            .execute(&format!("CREATE DATABASE {}", config.db_name), &[])
            .await
            .map_err(|err| {
                error!("Cannot create the database - {err:?}");
                InitializationError
            })?;

        DbInitialzationOutcome::Created
    } else {
        DbInitialzationOutcome::Existing
    };
    match outcome {
        DbInitialzationOutcome::Created => info!("Database '{}' created.", config.db_name),
        DbInitialzationOutcome::Existing => info!("Database '{}' exists.", config.db_name),
    }
    Ok(outcome)
}

// All mutexes here have a very short critical section completely controlled by this module, thus using std mutex.
type ResponseSubscribers =
    Arc<std::sync::Mutex<HashMap<ExecutionId, (oneshot::Sender<JoinSetResponseEventOuter>, u64)>>>;
type PendingSubscribers = Arc<std::sync::Mutex<PendingFfqnSubscribersHolder>>;
type ExecutionFinishedSubscribers = std::sync::Mutex<
    HashMap<ExecutionId, HashMap<u64, oneshot::Sender<SupportedFunctionReturnValue>>>,
>;

fn map_pool_error(err: deadpool_postgres::PoolError) -> DbErrorGeneric {
    match err {
        deadpool_postgres::PoolError::Backend(err) => DbErrorGeneric::from(err),
        deadpool_postgres::PoolError::Closed => DbErrorGeneric::Close,
        other => DbErrorGeneric::Uncategorized(other.to_string().into()),
    }
}

pub struct PostgresPool {
    pool: Pool,
    response_subscribers: ResponseSubscribers,
    pending_subscribers: PendingSubscribers,
    execution_finished_subscribers: Arc<ExecutionFinishedSubscribers>,
    #[allow(dead_code)] // only for deleting the db in tests.
    config: PostgresConfig,
}

#[async_trait]
impl DbPool for PostgresPool {
    async fn db_exec_conn(&self) -> Result<Box<dyn DbExecutor>, DbErrorGeneric> {
        let client = self.pool.get().await.map_err(map_pool_error)?;

        Ok(Box::new(PostgresConnection {
            client: tokio::sync::Mutex::new(client),
            response_subscribers: self.response_subscribers.clone(),
            pending_subscribers: self.pending_subscribers.clone(),
            execution_finished_subscribers: self.execution_finished_subscribers.clone(),
        }))
    }

    async fn connection(&self) -> Result<Box<dyn DbConnection>, DbErrorGeneric> {
        let client = self.pool.get().await.map_err(map_pool_error)?;

        Ok(Box::new(PostgresConnection {
            client: tokio::sync::Mutex::new(client),
            response_subscribers: self.response_subscribers.clone(),
            pending_subscribers: self.pending_subscribers.clone(),
            execution_finished_subscribers: self.execution_finished_subscribers.clone(),
        }))
    }

    async fn external_api_conn(&self) -> Result<Box<dyn DbExternalApi>, DbErrorGeneric> {
        let client = self.pool.get().await.map_err(map_pool_error)?;

        Ok(Box::new(PostgresConnection {
            client: tokio::sync::Mutex::new(client),
            response_subscribers: self.response_subscribers.clone(),
            pending_subscribers: self.pending_subscribers.clone(),
            execution_finished_subscribers: self.execution_finished_subscribers.clone(),
        }))
    }

    #[cfg(feature = "test")]
    async fn connection_test(
        &self,
    ) -> Result<Box<dyn concepts::storage::DbConnectionTest>, DbErrorGeneric> {
        let client = self.pool.get().await.map_err(map_pool_error)?;

        Ok(Box::new(PostgresConnection {
            client: tokio::sync::Mutex::new(client),
            response_subscribers: self.response_subscribers.clone(),
            pending_subscribers: self.pending_subscribers.clone(),
            execution_finished_subscribers: self.execution_finished_subscribers.clone(),
        }))
    }
}

pub struct PostgresConnection {
    client: tokio::sync::Mutex<Client>, // Callers should not hold onto a connection for too long but it is not controlled by this module, thus tokio mutex.
    response_subscribers: ResponseSubscribers,
    pending_subscribers: PendingSubscribers,
    execution_finished_subscribers: Arc<ExecutionFinishedSubscribers>,
}

#[derive(Default)]
struct PendingFfqnSubscribersHolder {
    by_ffqns: HashMap<FunctionFqn, (mpsc::Sender<()>, u64)>,
    by_component: HashMap<InputContentDigest /* input digest */, (mpsc::Sender<()>, u64)>,
}
impl PendingFfqnSubscribersHolder {
    fn notify(&self, notifier: &NotifierPendingAt) {
        if let Some((subscription, _)) = self.by_ffqns.get(&notifier.ffqn) {
            debug!("Notifying pending subscriber by ffqn");
            // Does not block
            let _ = subscription.try_send(());
        }
        if let Some((subscription, _)) = self.by_component.get(&notifier.component_input_digest) {
            debug!("Notifying pending subscriber by component");
            // Does not block
            let _ = subscription.try_send(());
        }
    }

    fn insert_ffqn(&mut self, ffqn: FunctionFqn, value: (mpsc::Sender<()>, u64)) {
        self.by_ffqns.insert(ffqn, value);
    }

    fn remove_ffqn(&mut self, ffqn: &FunctionFqn) -> Option<(mpsc::Sender<()>, u64)> {
        self.by_ffqns.remove(ffqn)
    }

    fn insert_by_component(
        &mut self,
        input_content_digest: InputContentDigest,
        value: (mpsc::Sender<()>, u64),
    ) {
        self.by_component.insert(input_content_digest, value);
    }

    fn remove_by_component(
        &mut self,
        input_content_digest: &InputContentDigest,
    ) -> Option<(mpsc::Sender<()>, u64)> {
        self.by_component.remove(input_content_digest)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProvisionPolicy {
    Never,
    /// Create database if it does not exist.
    Auto,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DbInitialzationOutcome {
    Created,
    Existing,
}

impl PostgresPool {
    #[instrument(skip_all, name = "postgres_new")]
    pub async fn new(
        config: PostgresConfig,
        provision_policy: ProvisionPolicy,
    ) -> Result<PostgresPool, InitializationError> {
        Self::new_with_outcome(config, provision_policy)
            .await
            .map(|(db, _)| db)
    }

    pub async fn new_with_outcome(
        config: PostgresConfig,
        provision_policy: ProvisionPolicy,
    ) -> Result<(PostgresPool, DbInitialzationOutcome), InitializationError> {
        let outcome = if provision_policy == ProvisionPolicy::Auto {
            create_database(&config).await?
        } else {
            DbInitialzationOutcome::Existing
        };
        let mut cfg = Config::new();
        cfg.host = Some(config.host.clone());
        cfg.user = Some(config.user.clone());
        cfg.password = Some(config.password.clone());
        cfg.dbname = Some(config.db_name.clone());
        cfg.manager = Some(ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        });

        let pool = cfg.create_pool(None, NoTls).map_err(|err| {
            error!("Cannot create the database pool - {err:?}");
            InitializationError
        })?;
        let client = pool.get().await.map_err(|err| {
            error!("Cannot get a connection from the database pool - {err:?}");
            InitializationError
        })?;

        let statements = vec![
            ddl::CREATE_TABLE_T_METADATA,
            ddl::CREATE_TABLE_T_EXECUTION_LOG,
            ddl::CREATE_INDEX_IDX_T_EXECUTION_LOG_EXECUTION_ID_VERSION,
            ddl::CREATE_INDEX_IDX_T_EXECUTION_LOG_EXECUTION_ID_VARIANT,
            ddl::CREATE_INDEX_IDX_T_EXECUTION_LOG_EXECUTION_ID_JOIN_SET,
            ddl::CREATE_TABLE_T_JOIN_SET_RESPONSE,
            ddl::CREATE_INDEX_IDX_T_JOIN_SET_RESPONSE_EXECUTION_ID_ID,
            ddl::CREATE_INDEX_IDX_JOIN_SET_RESPONSE_UNIQUE_CHILD_ID,
            ddl::CREATE_INDEX_IDX_JOIN_SET_RESPONSE_UNIQUE_DELAY_ID,
            ddl::CREATE_TABLE_T_STATE,
            ddl::IDX_T_STATE_LOCK_PENDING,
            ddl::IDX_T_STATE_EXPIRED_TIMERS,
            ddl::IDX_T_STATE_EXECUTION_ID_IS_TOP_LEVEL,
            ddl::IDX_T_STATE_FFQN,
            ddl::IDX_T_STATE_CREATED_AT,
            ddl::CREATE_TABLE_T_DELAY,
            ddl::CREATE_TABLE_T_BACKTRACE,
            ddl::IDX_T_BACKTRACE_EXECUTION_ID_VERSION,
        ];

        // Combine into one batch execution for atomicity per round-trip (or efficiency)
        let batch_sql = statements.join("\n");
        client.batch_execute(&batch_sql).await.map_err(|err| {
            error!("Cannot run the DDL import - {err:?}");
            InitializationError
        })?;

        let row = client
            .query_opt(
                "SELECT schema_version FROM t_metadata ORDER BY id DESC LIMIT 1",
                &[],
            )
            .await
            .map_err(|err| {
                error!("Cannot select schema version - {err:?}");
                InitializationError
            })?;

        // Postgres INTEGER maps to i32
        let actual_version = match row {
            Some(r) => Some(r.try_get::<_, i32>("schema_version").map_err(|e| {
                error!("Failed to get schema_version column: {e}");
                InitializationError
            })?),
            None => None,
        };

        match actual_version {
            None => {
                client
                    .execute(
                        "INSERT INTO t_metadata (schema_version, created_at) VALUES ($1, $2)",
                        &[&(T_METADATA_EXPECTED_SCHEMA_VERSION), &Utc::now()],
                    )
                    .await
                    .map_err(|err| {
                        error!("Cannot insert schema version - {err:?}");
                        InitializationError
                    })?;
            }
            Some(actual_version) => {
                // Fail on unexpected `schema_version`.
                if (actual_version) != T_METADATA_EXPECTED_SCHEMA_VERSION {
                    error!(
                        "Wrong schema version, expected {T_METADATA_EXPECTED_SCHEMA_VERSION}, got {actual_version}"
                    );
                    return Err(InitializationError);
                }
            }
        }

        debug!("Database schema initialized.");

        Ok((
            PostgresPool {
                pool,
                execution_finished_subscribers: Arc::default(),
                pending_subscribers: Arc::default(),
                response_subscribers: Arc::default(),
                config,
            },
            outcome,
        ))
    }
}

fn consistency_db_err(err: impl Into<StrVariant>) -> DbErrorGeneric {
    let err = err.into();
    warn!(
        backtrace = %std::backtrace::Backtrace::capture(),
        "Consistency error: {err}"
    );
    DbErrorGeneric::Uncategorized(err)
}

#[derive(Debug)]
struct CombinedStateDTO {
    state: String,
    ffqn: String,
    component_id_input_digest: InputContentDigest,
    pending_expires_finished: DateTime<Utc>,
    // Locked:
    last_lock_version: Option<Version>,
    executor_id: Option<ExecutorId>,
    run_id: Option<RunId>,
    // Blocked by join set:
    join_set_id: Option<JoinSetId>,
    join_set_closing: Option<bool>,
    // Finished:
    result_kind: Option<PendingStateFinishedResultKind>,
}
#[derive(Debug)]
struct CombinedState {
    ffqn: FunctionFqn,
    pending_state: PendingState,
    corresponding_version: Version,
}

impl CombinedState {
    fn new(dto: CombinedStateDTO, corresponding_version: Version) -> Result<Self, DbErrorGeneric> {
        let ffqn = FunctionFqn::from_str(&dto.ffqn).map_err(|parse_err| {
            error!("Error parsing ffqn of {dto:?} - {parse_err:?}");
            consistency_db_err("invalid ffqn value in `t_state`")
        })?;
        let pending_state = match dto {
            // Pending - just created
            CombinedStateDTO {
                state,
                ffqn: _,
                component_id_input_digest,
                pending_expires_finished: scheduled_at,
                last_lock_version: None,
                executor_id: None,
                run_id: None,
                join_set_id: None,
                join_set_closing: None,
                result_kind: None,
            } if state == STATE_PENDING_AT => PendingState::PendingAt {
                scheduled_at,
                last_lock: None,
                component_id_input_digest,
            },
            // Pending, previously locked
            CombinedStateDTO {
                state,
                ffqn: _,
                component_id_input_digest,
                pending_expires_finished: scheduled_at,
                last_lock_version: None,
                executor_id: Some(executor_id),
                run_id: Some(run_id),
                join_set_id: None,
                join_set_closing: None,
                result_kind: None,
            } if state == STATE_PENDING_AT => PendingState::PendingAt {
                scheduled_at,
                last_lock: Some(LockedBy {
                    executor_id,
                    run_id,
                }),
                component_id_input_digest,
            },
            CombinedStateDTO {
                state,
                ffqn: _,
                component_id_input_digest,
                pending_expires_finished: lock_expires_at,
                last_lock_version: Some(_),
                executor_id: Some(executor_id),
                run_id: Some(run_id),
                join_set_id: None,
                join_set_closing: None,
                result_kind: None,
            } if state == STATE_LOCKED => PendingState::Locked(PendingStateLocked {
                locked_by: LockedBy {
                    executor_id,
                    run_id,
                },
                lock_expires_at,
                component_id_input_digest,
            }),
            CombinedStateDTO {
                state,
                ffqn: _,
                component_id_input_digest,
                pending_expires_finished: lock_expires_at,
                last_lock_version: None,
                executor_id: _,
                run_id: _,
                join_set_id: Some(join_set_id),
                join_set_closing: Some(join_set_closing),
                result_kind: None,
            } if state == STATE_BLOCKED_BY_JOIN_SET => PendingState::BlockedByJoinSet {
                join_set_id: join_set_id.clone(),
                closing: join_set_closing,
                lock_expires_at,
                component_id_input_digest,
            },
            CombinedStateDTO {
                state,
                ffqn: _,
                component_id_input_digest,
                pending_expires_finished: finished_at,
                last_lock_version: None,
                executor_id: None,
                run_id: None,
                join_set_id: None,
                join_set_closing: None,
                result_kind: Some(result_kind),
            } if state == STATE_FINISHED => PendingState::Finished {
                finished: PendingStateFinished {
                    finished_at,
                    version: corresponding_version.0,
                    result_kind,
                },
                component_id_input_digest,
            },

            _ => {
                error!("Cannot deserialize pending state from  {dto:?}");
                return Err(consistency_db_err("invalid `t_state`"));
            }
        };
        Ok(Self {
            ffqn,
            pending_state,
            corresponding_version,
        })
    }

    fn get_next_version_assert_not_finished(&self) -> Version {
        assert!(!self.pending_state.is_finished());
        self.corresponding_version.increment()
    }

    #[cfg(feature = "test")]
    fn get_next_version_or_finished(&self) -> Version {
        if self.pending_state.is_finished() {
            self.corresponding_version.clone()
        } else {
            self.corresponding_version.increment()
        }
    }
}

#[derive(Debug)]
struct NotifierPendingAt {
    scheduled_at: DateTime<Utc>,
    ffqn: FunctionFqn,
    component_input_digest: InputContentDigest,
}

#[derive(Debug)]
struct NotifierExecutionFinished {
    execution_id: ExecutionId,
    retval: SupportedFunctionReturnValue,
}

#[derive(Debug, Default)]
struct AppendNotifier {
    pending_at: Option<NotifierPendingAt>,
    execution_finished: Option<NotifierExecutionFinished>,
    response: Option<(ExecutionId, JoinSetResponseEventOuter)>,
}

#[derive(Debug, Clone)]
struct DelayReq {
    join_set_id: JoinSetId,
    delay_id: DelayId,
    expires_at: DateTime<Utc>,
}

async fn fetch_created_event(
    tx: &Transaction<'_>,
    execution_id: &ExecutionId,
) -> Result<CreateRequest, DbErrorRead> {
    let stmt = "SELECT created_at, json_value FROM t_execution_log WHERE \
                execution_id = $1 AND version = 0";

    let row = tx.query_one(stmt, &[&execution_id.to_string()]).await?;

    let created_at = get(&row, "created_at")?;
    let event: Json<ExecutionRequest> = get(&row, "json_value")?;
    let event = event.0;

    if let ExecutionRequest::Created {
        ffqn,
        params,
        parent,
        scheduled_at,
        component_id,
        metadata,
        scheduled_by,
    } = event
    {
        Ok(CreateRequest {
            created_at,
            execution_id: execution_id.clone(),
            ffqn,
            params,
            parent,
            scheduled_at,
            component_id,
            metadata,
            scheduled_by,
        })
    } else {
        error!("Row with version=0 must be a `Created` event - {event:?}");
        Err(consistency_db_err("expected `Created` event").into())
    }
}

fn check_expected_next_and_appending_version(
    expected_version: &Version,
    appending_version: &Version,
) -> Result<(), DbErrorWrite> {
    if *expected_version != *appending_version {
        debug!(
            "Version conflict - expected: {expected_version:?}, appending: {appending_version:?}"
        );
        return Err(DbErrorWrite::NonRetriable(
            DbErrorWriteNonRetriable::VersionConflict {
                expected: expected_version.clone(),
                requested: appending_version.clone(),
            },
        ));
    }
    Ok(())
}

#[instrument(level = Level::TRACE, skip_all, fields(execution_id = %req.execution_id))]
async fn create_inner(
    tx: &Transaction<'_>,
    req: CreateRequest,
) -> Result<(AppendResponse, AppendNotifier), DbErrorWrite> {
    trace!("create_inner");

    let version = Version::default();
    let execution_id = req.execution_id.clone();
    let execution_id_str = execution_id.to_string();
    let ffqn = req.ffqn.clone();
    let created_at = req.created_at;
    let scheduled_at = req.scheduled_at;
    let component_id = req.component_id.clone();

    let event = ExecutionRequest::from(req);
    let event = Json(event);

    tx.execute(
        "INSERT INTO t_execution_log (
            execution_id, created_at, version, json_value, variant, join_set_id
        ) VALUES ($1, $2, $3, $4, $5, $6)",
        &[
            &execution_id_str,
            &created_at,
            &i64::from(version.0), // BIGINT
            &event,
            &event.0.variant(),
            &event.0.join_set_id().map(std::string::ToString::to_string),
        ],
    )
    .await?;

    let pending_at = {
        debug!("Creating with `Pending(`{scheduled_at:?}`)");

        tx.execute(
            r"
            INSERT INTO t_state (
                execution_id,
                is_top_level,
                corresponding_version,
                pending_expires_finished,
                ffqn,
                state,
                created_at,
                component_id_input_digest,
                updated_at,
                first_scheduled_at,
                intermittent_event_count
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, CURRENT_TIMESTAMP, $9, 0
            )",
            &[
                &execution_id_str,
                &execution_id.is_top_level(),
                &i64::from(version.0),
                &scheduled_at,
                &ffqn.to_string(),
                &STATE_PENDING_AT,
                &created_at,
                &component_id.input_digest.as_slice(),
                &scheduled_at,
            ],
        )
        .await?;

        AppendNotifier {
            pending_at: Some(NotifierPendingAt {
                scheduled_at,
                ffqn,
                component_input_digest: component_id.input_digest,
            }),
            execution_finished: None,
            response: None,
        }
    };

    let next_version = Version::new(version.0 + 1);
    Ok((next_version, pending_at))
}

#[instrument(level = Level::DEBUG, skip_all, fields(%execution_id, %scheduled_at, %corresponding_version))]
async fn update_state_pending_after_response_appended(
    tx: &Transaction<'_>,
    execution_id: &ExecutionId,
    scheduled_at: DateTime<Utc>,     // Changing to state PendingAt
    corresponding_version: &Version, // t_execution_log is not changed
    component_input_digest: InputContentDigest,
) -> Result<AppendNotifier, DbErrorWrite> {
    debug!("Setting t_state to Pending(`{scheduled_at:?}`) after response appended");

    // Convert types for Postgres arguments
    let execution_id_str = execution_id.to_string();
    let version = i64::from(corresponding_version.0);

    let updated = tx
        .execute(
            r"
            UPDATE t_state
            SET
                corresponding_version = $1,
                pending_expires_finished = $2,
                state = $3,
                updated_at = CURRENT_TIMESTAMP,

                last_lock_version = NULL,

                join_set_id = NULL,
                join_set_closing = NULL,

                result_kind = NULL
            WHERE execution_id = $4
            ",
            &[
                &version,          // $1
                &scheduled_at,     // $2
                &STATE_PENDING_AT, // $3
                &execution_id_str, // $4
            ],
        )
        .await?;

    if updated == 0 {
        return Err(DbErrorWrite::NotFound);
    }

    Ok(AppendNotifier {
        pending_at: Some(NotifierPendingAt {
            scheduled_at,
            ffqn: fetch_created_event(tx, execution_id).await?.ffqn,
            component_input_digest,
        }),
        execution_finished: None,
        response: None,
    })
}

#[instrument(level = Level::DEBUG, skip_all, fields(%execution_id, %scheduled_at, %appending_version))]
async fn update_state_pending_after_event_appended(
    tx: &Transaction<'_>,
    execution_id: &ExecutionId,
    appending_version: &Version,
    scheduled_at: DateTime<Utc>,
    intermittent_failure: bool,
    component_input_digest: InputContentDigest,
) -> Result<(AppendResponse, AppendNotifier), DbErrorWrite> {
    debug!("Setting t_state to Pending(`{scheduled_at:?}`) after event appended");

    let intermittent_delta = i64::from(intermittent_failure); // 0 or 1

    let updated = tx
        .execute(
            r"
            UPDATE t_state
            SET
                corresponding_version = $1,
                pending_expires_finished = $2,
                state = $3,
                updated_at = CURRENT_TIMESTAMP,
                intermittent_event_count = intermittent_event_count + $4,

                last_lock_version = NULL,

                join_set_id = NULL,
                join_set_closing = NULL,

                result_kind = NULL
            WHERE execution_id = $5;
            ",
            &[
                &i64::from(appending_version.0), // $1
                &scheduled_at,                   // $2
                &STATE_PENDING_AT,               // $3
                &intermittent_delta,             // $4
                &execution_id.to_string(),       // $5
            ],
        )
        .await?;

    if updated != 1 {
        return Err(DbErrorWrite::NotFound);
    }

    Ok((
        appending_version.increment(),
        AppendNotifier {
            pending_at: Some(NotifierPendingAt {
                scheduled_at,
                ffqn: fetch_created_event(tx, execution_id).await?.ffqn,
                component_input_digest,
            }),
            execution_finished: None,
            response: None,
        },
    ))
}

async fn update_state_locked_get_intermittent_event_count(
    tx: &Transaction<'_>,
    execution_id: &ExecutionId,
    executor_id: ExecutorId,
    run_id: RunId,
    lock_expires_at: DateTime<Utc>,
    appending_version: &Version,
    retry_config: ComponentRetryConfig,
) -> Result<u32, DbErrorWrite> {
    debug!("Setting t_state to Locked(`{lock_expires_at:?}`)");
    let backoff_millis = i64::try_from(retry_config.retry_exp_backoff.as_millis())
        .map_err(|_| DbErrorGeneric::Uncategorized("backoff too big".into()))?; // BIGINT = i64

    let execution_id_str = execution_id.to_string();

    let updated = tx
        .execute(
            r"
            UPDATE t_state
            SET
                corresponding_version = $1,
                pending_expires_finished = $2,
                state = $3,
                updated_at = CURRENT_TIMESTAMP,

                max_retries = $4,
                retry_exp_backoff_millis = $5,
                last_lock_version = $1, -- appending_version again
                executor_id = $6,
                run_id = $7,

                join_set_id = NULL,
                join_set_closing = NULL,

                result_kind = NULL
            WHERE execution_id = $8
            ",
            &[
                &i64::from(appending_version.0),          // $1
                &lock_expires_at,                         // $2
                &STATE_LOCKED,                            // $3
                &retry_config.max_retries.map(i64::from), // $4
                &backoff_millis,                          // $5
                &executor_id.to_string(),                 // $6
                &run_id.to_string(),                      // $7
                &execution_id_str,                        // $8
            ],
        )
        .await?;

    if updated != 1 {
        return Err(DbErrorWrite::NotFound);
    }

    // fetch intermittent event count
    let row = tx
        .query_one(
            "SELECT intermittent_event_count FROM t_state WHERE execution_id = $1",
            &[&execution_id_str],
        )
        .await
        .map_err(|err| DbErrorGeneric::Uncategorized(err.to_string().into()))?;

    let count: i64 = get(&row, "intermittent_event_count")?; // Postgres BIGINT
    let count = u32::try_from(count)
        .map_err(|_| consistency_db_err("`intermittent_event_count` must not be negative"))?;
    Ok(count)
}

async fn update_state_blocked(
    tx: &Transaction<'_>,
    execution_id: &ExecutionId,
    appending_version: &Version,
    join_set_id: &JoinSetId,
    lock_expires_at: DateTime<Utc>,
    join_set_closing: bool,
) -> Result<AppendResponse, DbErrorWrite> {
    debug!("Setting t_state to BlockedByJoinSet(`{join_set_id}`)");

    let updated = tx
        .execute(
            r"
            UPDATE t_state
            SET
                corresponding_version = $1,
                pending_expires_finished = $2,
                state = $3,
                updated_at = CURRENT_TIMESTAMP,

                last_lock_version = NULL,

                join_set_id = $4,
                join_set_closing = $5,

                result_kind = NULL
            WHERE execution_id = $6
            ",
            &[
                &i64::from(appending_version.0), // $1
                &lock_expires_at,                // $2
                &STATE_BLOCKED_BY_JOIN_SET,      // $3
                &join_set_id.to_string(),        // $4
                &join_set_closing,               // $5 (BOOLEAN)
                &execution_id.to_string(),       // $6
            ],
        )
        .await?;

    if updated != 1 {
        return Err(DbErrorWrite::NotFound);
    }
    Ok(appending_version.increment())
}

async fn update_state_finished(
    tx: &Transaction<'_>,
    execution_id: &ExecutionId,
    appending_version: &Version,
    finished_at: DateTime<Utc>,
    result_kind: PendingStateFinishedResultKind,
) -> Result<(), DbErrorWrite> {
    debug!("Setting t_state to Finished");

    let result_kind_json = Json(result_kind);

    let updated = tx
        .execute(
            r"
            UPDATE t_state
            SET
                corresponding_version = $1,
                pending_expires_finished = $2,
                state = $3,
                updated_at = CURRENT_TIMESTAMP,

                last_lock_version = NULL,
                executor_id = NULL,
                run_id = NULL,

                join_set_id = NULL,
                join_set_closing = NULL,

                result_kind = $4
            WHERE execution_id = $5
            ",
            &[
                &i64::from(appending_version.0), // $1
                &finished_at,                    // $2
                &STATE_FINISHED,                 // $3
                &result_kind_json,               // $4
                &execution_id.to_string(),       // $5
            ],
        )
        .await?;

    if updated != 1 {
        return Err(DbErrorWrite::NotFound);
    }
    Ok(())
}

#[instrument(level = Level::TRACE, skip_all, fields(%execution_id, %appending_version))]
async fn bump_state_next_version(
    tx: &Transaction<'_>,
    execution_id: &ExecutionId,
    appending_version: &Version,
    delay_req: Option<DelayReq>,
) -> Result<AppendResponse, DbErrorWrite> {
    debug!("update_index_version");
    let execution_id_str = execution_id.to_string();

    let updated = tx
        .execute(
            r"
            UPDATE t_state
            SET
                corresponding_version = $1,
                updated_at = CURRENT_TIMESTAMP
            WHERE execution_id = $2
            ",
            &[
                &i64::from(appending_version.0), // $1
                &execution_id_str,               // $2
            ],
        )
        .await?;

    if updated != 1 {
        return Err(DbErrorWrite::NotFound);
    }

    if let Some(DelayReq {
        join_set_id,
        delay_id,
        expires_at,
    }) = delay_req
    {
        debug!("Inserting delay to `t_delay`");
        tx.execute(
            "INSERT INTO t_delay (execution_id, join_set_id, delay_id, expires_at) VALUES ($1, $2, $3, $4)",
            &[
                &execution_id_str,
                &join_set_id.to_string(),
                &delay_id.to_string(),
                &expires_at,
            ],
        )
        .await?;
    }
    Ok(appending_version.increment())
}

async fn get_combined_state(
    tx: &Transaction<'_>,
    execution_id: &ExecutionId,
) -> Result<CombinedState, DbErrorRead> {
    let row = tx
        .query_one(
            r"
            SELECT
                state, ffqn, component_id_input_digest, corresponding_version, pending_expires_finished,
                last_lock_version, executor_id, run_id,
                join_set_id, join_set_closing,
                result_kind
            FROM t_state
            WHERE execution_id = $1
            ",
            &[&execution_id.to_string()],
        )
        .await
        .map_err(DbErrorRead::from)?;

    // Parsing columns
    let digest_bytes: Vec<u8> = get(&row, "component_id_input_digest")?;
    let digest = Digest::try_from(digest_bytes.as_slice())
        .map_err(|err| consistency_db_err(err.to_string()))?;
    let component_id_input_digest = InputContentDigest(ContentDigest(digest));

    let state: String = get(&row, "state")?;
    let ffqn: String = get(&row, "ffqn")?;
    let pending_expires_finished: DateTime<Utc> = get(&row, "pending_expires_finished")?;

    let last_lock_version_raw: Option<i64> = get(&row, "last_lock_version")?;
    let last_lock_version = last_lock_version_raw
        .map(Version::try_from)
        .transpose()
        .map_err(|_| consistency_db_err("version must be non-negative"))?;

    let executor_id_raw: Option<String> = get(&row, "executor_id")?;
    let executor_id = executor_id_raw
        .map(|id| ExecutorId::from_str(&id))
        .transpose()
        .map_err(|err| DbErrorGeneric::Uncategorized(err.to_string().into()))?;

    let run_id_raw: Option<String> = get(&row, "run_id")?;
    let run_id = run_id_raw
        .map(|id| RunId::from_str(&id))
        .transpose()
        .map_err(|err| DbErrorGeneric::Uncategorized(err.to_string().into()))?;

    let join_set_id_raw: Option<String> = get(&row, "join_set_id")?;
    let join_set_id = join_set_id_raw
        .map(|id| JoinSetId::from_str(&id))
        .transpose()
        .map_err(|err| DbErrorGeneric::Uncategorized(err.to_string().into()))?;

    let join_set_closing: Option<bool> = get(&row, "join_set_closing")?;

    let result_kind: Option<Json<PendingStateFinishedResultKind>> = get(&row, "result_kind")?;
    let result_kind = result_kind.map(|it| it.0);

    let corresponding_version = get::<i64>(&row, "corresponding_version")?;
    let corresponding_version = Version::new(
        VersionType::try_from(corresponding_version)
            .map_err(|_| consistency_db_err("version must be non-negative"))?,
    );

    let dto = CombinedStateDTO {
        state,
        ffqn,
        component_id_input_digest,
        pending_expires_finished,
        last_lock_version,
        executor_id,
        run_id,
        join_set_id,
        join_set_closing,
        result_kind,
    };
    CombinedState::new(dto, corresponding_version).map_err(DbErrorRead::from)
}

async fn list_executions(
    read_tx: &Transaction<'_>,
    ffqn: Option<&FunctionFqn>,
    top_level_only: bool,
    pagination: &ExecutionListPagination,
) -> Result<Vec<ExecutionWithState>, DbErrorGeneric> {
    // Helper to manage dynamic WHERE clauses and positional parameters ($1, $2...)
    struct QueryBuilder {
        where_clauses: Vec<String>,
        params: Vec<Box<dyn ToSql + Send + Sync>>,
    }

    impl QueryBuilder {
        fn new() -> Self {
            Self {
                where_clauses: Vec::new(),
                params: Vec::new(),
            }
        }

        fn add_param<T>(&mut self, param: T) -> String
        where
            T: ToSql + Sync + Send + 'static,
        {
            self.params.push(Box::new(param));
            format!("${}", self.params.len())
        }

        fn add_where(&mut self, clause: String) {
            self.where_clauses.push(clause);
        }
    }

    let mut qb = QueryBuilder::new();

    // 1. Pagination Logic
    let (limit, limit_desc) = match pagination {
        ExecutionListPagination::CreatedBy(p) => {
            let limit = p.length();
            let is_desc = p.is_desc();
            if let Some(cursor) = p.cursor() {
                let placeholder = qb.add_param(*cursor);
                qb.add_where(format!("created_at {} {}", p.rel(), placeholder));
            }
            (limit, is_desc)
        }
        ExecutionListPagination::ExecutionId(p) => {
            let limit = p.length();
            let is_desc = p.is_desc();
            if let Some(cursor) = p.cursor() {
                let placeholder = qb.add_param(cursor.to_string());
                qb.add_where(format!("execution_id {} {}", p.rel(), placeholder));
            }
            (limit, is_desc)
        }
    };

    // 2. Top Level Filter
    if top_level_only {
        qb.add_where("is_top_level = true".to_string());
    }

    // 3. FFQN Filter
    if let Some(ffqn) = ffqn {
        let placeholder = qb.add_param(ffqn.to_string());
        qb.add_where(format!("ffqn = {placeholder}"));
    }

    // 4. Construct Query
    let where_str = if qb.where_clauses.is_empty() {
        String::new()
    } else {
        format!("WHERE {}", qb.where_clauses.join(" AND "))
    };

    let order_col = match pagination {
        ExecutionListPagination::CreatedBy(_) => "created_at",
        ExecutionListPagination::ExecutionId(_) => "execution_id",
    };

    let desc_str = if limit_desc { "DESC" } else { "" };

    let sql = format!(
        r"
            SELECT created_at, first_scheduled_at, component_id_input_digest,
            state, execution_id, ffqn, corresponding_version, pending_expires_finished,
            last_lock_version, executor_id, run_id,
            join_set_id, join_set_closing,
            result_kind
            FROM t_state {where_str} ORDER BY {order_col} {desc_str} LIMIT {limit}
            "
    );

    let params_refs: Vec<&(dyn ToSql + Sync)> = qb
        .params
        .iter()
        .map(|p| p.as_ref() as &(dyn ToSql + Sync))
        .collect();

    let rows = read_tx
        .query(&sql, &params_refs)
        .await
        .map_err(|err| DbErrorGeneric::Uncategorized(err.to_string().into()))?;

    let mut vec = Vec::with_capacity(rows.len());

    for row in rows {
        // If parsing of the row fails, log and skip it.
        let unpack = || -> Result<ExecutionWithState, DbErrorGeneric> {
            let execution_id_str: String = get(&row, "execution_id")?;
            let execution_id = ExecutionId::from_str(&execution_id_str)
                .map_err(|err| consistency_db_err(err.to_string()))?;

            let digest_bytes: Vec<u8> = get(&row, "component_id_input_digest")?;
            let digest = Digest::try_from(digest_bytes.as_slice())
                .map_err(|err| consistency_db_err(err.to_string()))?;
            let component_id_input_digest = InputContentDigest(ContentDigest(digest));

            let created_at: DateTime<Utc> = get(&row, "created_at")?;
            let first_scheduled_at: DateTime<Utc> = get(&row, "first_scheduled_at")?;

            let result_kind: Option<Json<PendingStateFinishedResultKind>> =
                get(&row, "result_kind")?;
            let result_kind = result_kind.map(|it| it.0);

            let corresponding_version: i64 = get(&row, "corresponding_version")?;
            let corresponding_version = Version::try_from(corresponding_version)
                .map_err(|_| consistency_db_err("version must be non-negative"))?;

            let executor_id_str: Option<String> = get(&row, "executor_id")?;
            let executor_id = executor_id_str
                .map(|id| ExecutorId::from_str(&id))
                .transpose()
                .map_err(|err| DbErrorGeneric::Uncategorized(err.to_string().into()))?;

            let last_lock_version_raw: Option<i64> = get(&row, "last_lock_version")?;
            let last_lock_version = last_lock_version_raw
                .map(Version::try_from)
                .transpose()
                .map_err(|_| consistency_db_err("version must be non-negative"))?;

            let run_id_str: Option<String> = get(&row, "run_id")?;
            let run_id = run_id_str
                .map(|id| RunId::from_str(&id))
                .transpose()
                .map_err(|err| DbErrorGeneric::Uncategorized(err.to_string().into()))?;

            let join_set_id_str: Option<String> = get(&row, "join_set_id")?;
            let join_set_id = join_set_id_str
                .map(|id| JoinSetId::from_str(&id))
                .transpose()
                .map_err(|err| DbErrorGeneric::Uncategorized(err.to_string().into()))?;

            let combined_state_dto = CombinedStateDTO {
                component_id_input_digest: component_id_input_digest.clone(),
                state: get(&row, "state")?,
                ffqn: get(&row, "ffqn")?,
                pending_expires_finished: get(&row, "pending_expires_finished")?,
                executor_id,
                last_lock_version,
                run_id,
                join_set_id,
                join_set_closing: get(&row, "join_set_closing")?,
                result_kind,
            };

            let combined_state = CombinedState::new(combined_state_dto, corresponding_version)?;

            Ok(ExecutionWithState {
                execution_id,
                ffqn: combined_state.ffqn,
                pending_state: combined_state.pending_state,
                created_at,
                first_scheduled_at,
                component_digest: component_id_input_digest,
            })
        };

        match unpack() {
            Ok(execution) => vec.push(execution),
            Err(err) => {
                warn!("Skipping corrupted row in t_state: {err:?}");
            }
        }
    }

    if !limit_desc {
        // the list must be sorted in descending order
        vec.reverse();
    }
    Ok(vec)
}

async fn list_responses(
    tx: &Transaction<'_>,
    execution_id: &ExecutionId,
    pagination: Option<Pagination<u32>>,
) -> Result<Vec<ResponseWithCursor>, DbErrorRead> {
    // Helper to manage params dynamically
    let mut params: Vec<Box<dyn ToSql + Send + Sync>> = Vec::new();
    let mut add_param = |p: Box<dyn ToSql + Send + Sync>| {
        params.push(p);
        format!("${}", params.len())
    };

    // 1. Base Query
    let p_execution_id = add_param(Box::new(execution_id.to_string()));

    let mut sql = format!(
        "SELECT \
            r.id, r.created_at, r.join_set_id, r.delay_id, r.delay_success, r.child_execution_id, r.finished_version, l.json_value \
            FROM t_join_set_response r LEFT OUTER JOIN t_execution_log l ON r.child_execution_id = l.execution_id \
            WHERE \
            r.execution_id = {p_execution_id} \
            AND ( r.finished_version = l.version OR r.child_execution_id IS NULL )"
    );

    // 2. Pagination Logic
    let limit = match &pagination {
        Some(p @ (Pagination::NewerThan { cursor, .. } | Pagination::OlderThan { cursor, .. })) => {
            // Postgres BIGINT is i64.
            let p_cursor = add_param(Box::new(i64::from(*cursor)));

            // Add WHERE clause for cursor
            write!(sql, " AND r.id {} {}", p.rel(), p_cursor).unwrap();

            Some(p.length())
        }
        None => None,
    };

    // 3. Ordering
    sql.push_str(" ORDER BY r.id");
    if pagination.as_ref().is_some_and(Pagination::is_desc) {
        sql.push_str(" DESC");
    }

    // 4. Limit
    if let Some(limit) = limit {
        // Postgres limit expects i64
        let p_limit = add_param(Box::new(i64::from(limit)));
        write!(sql, " LIMIT {p_limit}").unwrap();
    }

    let params_refs: Vec<&(dyn ToSql + Sync)> = params
        .iter()
        .map(|p| p.as_ref() as &(dyn ToSql + Sync))
        .collect();

    let rows = tx
        .query(&sql, &params_refs)
        .await
        .map_err(DbErrorRead::from)?;

    let mut results = Vec::with_capacity(rows.len());
    for row in rows {
        results.push(parse_response_with_cursor(&row)?);
    }

    Ok(results)
}

fn parse_response_with_cursor(
    row: &tokio_postgres::Row,
) -> Result<ResponseWithCursor, DbErrorRead> {
    // Postgres BIGINT = i64.
    let id = u32::try_from(get::<i64>(row, "id")?)
        .map_err(|_| consistency_db_err("id must not be negative"))?;

    let created_at: DateTime<Utc> = get(row, "created_at")?;
    let join_set_id_str: String = get(row, "join_set_id")?;
    let join_set_id = JoinSetId::from_str(&join_set_id_str)
        .map_err(|err| DbErrorGeneric::Uncategorized(err.to_string().into()))?;

    // Extract Optionals
    let delay_id: Option<String> = get(row, "delay_id")?;
    let delay_id = delay_id
        .map(|id| DelayId::from_str(&id))
        .transpose()
        .map_err(|err| DbErrorGeneric::Uncategorized(err.to_string().into()))?;
    let delay_success: Option<bool> = get(row, "delay_success")?;
    let child_execution_id: Option<String> = get(row, "child_execution_id")?;
    let child_execution_id = child_execution_id
        .map(|id| ExecutionIdDerived::from_str(&id))
        .transpose()
        .map_err(|err| DbErrorGeneric::Uncategorized(err.to_string().into()))?;
    let finished_version = get::<Option<i64>>(row, "finished_version")?
        .map(Version::try_from)
        .transpose()
        .map_err(|_| consistency_db_err("version must be non-negative"))?;
    let json_value: Option<Json<ExecutionRequest>> = get(row, "json_value")?;
    let json_value = json_value.map(|it| it.0);

    let event = match (
        delay_id,
        delay_success,
        child_execution_id,
        finished_version,
        json_value,
    ) {
        (Some(delay_id), Some(delay_success), None, None, None) => JoinSetResponse::DelayFinished {
            delay_id,
            result: delay_success.then_some(()).ok_or(()),
        },
        (None, None, Some(child_execution_id), Some(finished_version), Some(json_val)) => {
            if let ExecutionRequest::Finished { result, .. } = json_val {
                JoinSetResponse::ChildExecutionFinished {
                    child_execution_id,
                    finished_version,
                    result,
                }
            } else {
                error!("Joined log entry must be 'Finished'");
                return Err(consistency_db_err("joined log entry must be 'Finished'").into());
            }
        }
        (delay, delay_success, child, finished, result) => {
            error!(
                "Invalid row in t_join_set_response {id} - {delay:?} {delay_success:?} {child:?} {finished:?} {result:?}",
            );
            return Err(consistency_db_err("invalid row in t_join_set_response").into());
        }
    };

    Ok(ResponseWithCursor {
        cursor: id,
        event: JoinSetResponseEventOuter {
            event: JoinSetResponseEvent { join_set_id, event },
            created_at,
        },
    })
}

#[instrument(level = Level::TRACE, skip_all, fields(%execution_id, %run_id, %executor_id))]
#[expect(clippy::too_many_arguments)]
async fn lock_single_execution(
    tx: &Transaction<'_>,
    created_at: DateTime<Utc>,
    component_id: ComponentId,
    execution_id: &ExecutionId,
    run_id: RunId,
    appending_version: &Version,
    executor_id: ExecutorId,
    lock_expires_at: DateTime<Utc>,
    retry_config: ComponentRetryConfig,
) -> Result<LockedExecution, DbErrorWrite> {
    debug!("lock_single_execution");

    // 1. Check State
    let combined_state = get_combined_state(tx, execution_id).await?;
    combined_state.pending_state.can_append_lock(
        created_at,
        executor_id,
        run_id,
        lock_expires_at,
    )?;
    let expected_version = combined_state.get_next_version_assert_not_finished();
    check_expected_next_and_appending_version(&expected_version, appending_version)?;

    // 2. Prepare Event
    let locked_event = Locked {
        component_id,
        executor_id,
        lock_expires_at,
        run_id,
        retry_config,
    };
    let event = ExecutionRequest::Locked(locked_event.clone());

    let event = Json(event);

    // 3. Append to execution_log
    tx.execute(
        "INSERT INTO t_execution_log \
            (execution_id, created_at, json_value, version, variant) \
            VALUES ($1, $2, $3, $4, $5)",
        &[
            &execution_id.to_string(),
            &created_at,
            &event,
            &i64::from(appending_version.0),
            &event.0.variant(),
        ],
    )
    .await
    .map_err(|err| {
        warn!("Cannot lock execution - {err:?}");
        DbErrorWrite::NonRetriable(DbErrorWriteNonRetriable::IllegalState("cannot lock".into()))
    })?;

    // 4. Update t_state
    let responses_dto = list_responses(tx, execution_id, None).await?;
    let responses = responses_dto.into_iter().map(|resp| resp.event).collect();
    trace!("Responses: {responses:?}");

    let intermittent_event_count = update_state_locked_get_intermittent_event_count(
        tx,
        execution_id,
        executor_id,
        run_id,
        lock_expires_at,
        appending_version,
        retry_config,
    )
    .await?;

    // 5. Fetch History
    // Fetch event_history and `Created` event.
    let rows = tx
        .query(
            "SELECT json_value, version FROM t_execution_log WHERE \
                execution_id = $1 AND (variant = $2 OR variant = $3) \
                ORDER BY version",
            &[
                &execution_id.to_string(),
                &DUMMY_CREATED.variant(),
                &DUMMY_HISTORY_EVENT.variant(),
            ],
        )
        .await
        .map_err(|err| DbErrorGeneric::Uncategorized(err.to_string().into()))?;

    let mut events: VecDeque<ExecutionEvent> = VecDeque::new();

    for row in rows {
        let event: Json<ExecutionRequest> = get(&row, "json_value")?;
        let event = event.0;

        let version: i64 = get(&row, "version")?;
        let version = Version::try_from(version)
            .map_err(|_| consistency_db_err("version must be non-negative"))?;

        events.push_back(ExecutionEvent {
            created_at: DateTime::from_timestamp_nanos(0), // not used, only the inner event and version
            event,
            backtrace_id: None,
            version,
        });
    }

    // 6. Extract Created Event
    let Some(ExecutionRequest::Created {
        ffqn,
        params,
        parent,
        metadata,
        ..
    }) = events.pop_front().map(|outer| outer.event)
    else {
        error!("Execution log must contain at least `Created` event");
        return Err(consistency_db_err("execution log must contain `Created` event").into());
    };

    // 7. Extract History Events
    let mut event_history = Vec::new();
    for ExecutionEvent { event, version, .. } in events {
        if let ExecutionRequest::HistoryEvent { event } = event {
            event_history.push((event, version));
        } else {
            error!("Rows can only contain `Created` and `HistoryEvent` event kinds");
            return Err(consistency_db_err(
                "rows can only contain `Created` and `HistoryEvent` event kinds",
            )
            .into());
        }
    }

    Ok(LockedExecution {
        execution_id: execution_id.clone(),
        metadata,
        next_version: appending_version.increment(),
        ffqn,
        params,
        event_history,
        responses,
        parent,
        intermittent_event_count,
        locked_event,
    })
}

async fn count_join_next(
    tx: &Transaction<'_>,
    execution_id: &ExecutionId,
    join_set_id: &JoinSetId,
) -> Result<u32, DbErrorRead> {
    let row = tx
            .query_one(
                "SELECT COUNT(*) as count FROM t_execution_log WHERE execution_id = $1 AND join_set_id = $2 \
                AND history_event_type = $3",
                &[
                    &execution_id.to_string(),
                    &join_set_id.to_string(),
                    &HISTORY_EVENT_TYPE_JOIN_NEXT,
                ],
            )
            .await
            .map_err(DbErrorRead::from)?;

    let count = u32::try_from(get::<i64>(&row, "count")?).expect("COUNT cannot be negative");
    Ok(count)
}

async fn nth_response(
    tx: &Transaction<'_>,
    execution_id: &ExecutionId,
    join_set_id: &JoinSetId,
    skip_rows: u32,
) -> Result<Option<ResponseWithCursor>, DbErrorRead> {
    let row = tx
            .query_opt(
                "SELECT r.id, r.created_at, r.join_set_id, \
                 r.delay_id, r.delay_success, \
                 r.child_execution_id, r.finished_version, l.json_value \
                 FROM t_join_set_response r LEFT OUTER JOIN t_execution_log l ON r.child_execution_id = l.execution_id \
                 WHERE \
                 r.execution_id = $1 AND r.join_set_id = $2 AND \
                 ( \
                 r.finished_version = l.version \
                 OR \
                 r.child_execution_id IS NULL \
                 ) \
                 ORDER BY id \
                 LIMIT 1 OFFSET $3",
                 &[
                     &execution_id.to_string(),
                     &join_set_id.to_string(),
                     &i64::from(skip_rows),
                 ]
            )
            .await
            .map_err(DbErrorRead::from)?;

    match row {
        Some(r) => Ok(Some(parse_response_with_cursor(&r)?)),
        None => Ok(None),
    }
}

#[instrument(level = Level::TRACE, skip_all, fields(%execution_id))]
async fn append(
    tx: &Transaction<'_>,
    execution_id: &ExecutionId,
    req: AppendRequest,
    appending_version: Version,
) -> Result<(AppendResponse, AppendNotifier), DbErrorWrite> {
    if matches!(req.event, ExecutionRequest::Created { .. }) {
        return Err(DbErrorWrite::NonRetriable(
            DbErrorWriteNonRetriable::ValidationFailed(
                "cannot append `Created` event - use `create` instead".into(),
            ),
        ));
    }

    if let AppendRequest {
        event:
            ExecutionRequest::Locked(Locked {
                component_id,
                executor_id,
                run_id,
                lock_expires_at,
                retry_config,
            }),
        created_at,
    } = req
    {
        return lock_single_execution(
            tx,
            created_at,
            component_id,
            execution_id,
            run_id,
            &appending_version,
            executor_id,
            lock_expires_at,
            retry_config,
        )
        .await
        .map(|locked_execution| (locked_execution.next_version, AppendNotifier::default()));
    }

    let combined_state = get_combined_state(tx, execution_id).await?;
    if combined_state.pending_state.is_finished() {
        debug!("Execution is already finished");
        return Err(DbErrorWrite::NonRetriable(
            DbErrorWriteNonRetriable::IllegalState("already finished".into()),
        ));
    }

    check_expected_next_and_appending_version(
        &combined_state.get_next_version_assert_not_finished(),
        &appending_version,
    )?;

    let event = Json(req.event);

    // Insert into t_execution_log
    tx.execute(
            "INSERT INTO t_execution_log (execution_id, created_at, json_value, version, variant, join_set_id) \
             VALUES ($1, $2, $3, $4, $5, $6)",
            &[
                &execution_id.to_string(),
                &req.created_at,
                &event,
                &i64::from(appending_version.0),
                &event.0.variant(),
                &event.0.join_set_id().map(std::string::ToString::to_string),
            ],
        )
        .await?;

    // Calculate current pending state
    match &event.0 {
        ExecutionRequest::Created { .. } => {
            unreachable!("handled in the caller")
        }

        ExecutionRequest::Locked { .. } => {
            unreachable!("handled above")
        }

        ExecutionRequest::TemporarilyFailed {
            backoff_expires_at, ..
        }
        | ExecutionRequest::TemporarilyTimedOut {
            backoff_expires_at, ..
        } => {
            let (next_version, notifier) = update_state_pending_after_event_appended(
                tx,
                execution_id,
                &appending_version,
                *backoff_expires_at,
                true, // an intermittent failure
                combined_state.pending_state.component_digest().clone(),
            )
            .await?;
            return Ok((next_version, notifier));
        }

        ExecutionRequest::Unlocked {
            backoff_expires_at, ..
        } => {
            let (next_version, notifier) = update_state_pending_after_event_appended(
                tx,
                execution_id,
                &appending_version,
                *backoff_expires_at,
                false, // not an intermittent failure
                combined_state.pending_state.component_digest().clone(),
            )
            .await?;
            return Ok((next_version, notifier));
        }

        ExecutionRequest::Finished { result, .. } => {
            update_state_finished(
                tx,
                execution_id,
                &appending_version,
                req.created_at,
                PendingStateFinishedResultKind::from(result),
            )
            .await?;
            return Ok((
                appending_version,
                AppendNotifier {
                    pending_at: None,
                    execution_finished: Some(NotifierExecutionFinished {
                        execution_id: execution_id.clone(),
                        retval: result.clone(),
                    }),
                    response: None,
                },
            ));
        }

        ExecutionRequest::HistoryEvent {
            event:
                HistoryEvent::JoinSetCreate { .. }
                | HistoryEvent::JoinSetRequest {
                    request: JoinSetRequest::ChildExecutionRequest { .. },
                    ..
                }
                | HistoryEvent::Persist { .. }
                | HistoryEvent::Schedule { .. }
                | HistoryEvent::Stub { .. }
                | HistoryEvent::JoinNextTooMany { .. },
        } => {
            return Ok((
                bump_state_next_version(tx, execution_id, &appending_version, None).await?,
                AppendNotifier::default(),
            ));
        }

        ExecutionRequest::HistoryEvent {
            event:
                HistoryEvent::JoinSetRequest {
                    join_set_id,
                    request:
                        JoinSetRequest::DelayRequest {
                            delay_id,
                            expires_at,
                            ..
                        },
                },
        } => {
            return Ok((
                bump_state_next_version(
                    tx,
                    execution_id,
                    &appending_version,
                    Some(DelayReq {
                        join_set_id: join_set_id.clone(),
                        delay_id: delay_id.clone(),
                        expires_at: *expires_at,
                    }),
                )
                .await?,
                AppendNotifier::default(),
            ));
        }

        ExecutionRequest::HistoryEvent {
            event:
                HistoryEvent::JoinNext {
                    join_set_id,
                    run_expires_at,
                    closing,
                    requested_ffqn: _,
                },
        } => {
            // Did the response arrive already?
            let join_next_count = count_join_next(tx, execution_id, join_set_id).await?;

            // Fetch the response corresponding to this JoinNext (skip n-1)
            let nth_response =
                nth_response(tx, execution_id, join_set_id, join_next_count - 1).await?;

            trace!("join_next_count: {join_next_count}, nth_response: {nth_response:?}");
            assert!(join_next_count > 0);

            if let Some(ResponseWithCursor {
                event:
                    JoinSetResponseEventOuter {
                        created_at: nth_created_at,
                        ..
                    },
                cursor: _,
            }) = nth_response
            {
                let scheduled_at = std::cmp::max(*run_expires_at, nth_created_at);
                let (next_version, notifier) = update_state_pending_after_event_appended(
                    tx,
                    execution_id,
                    &appending_version,
                    scheduled_at,
                    false, // not an intermittent failure
                    combined_state.pending_state.component_digest().clone(),
                )
                .await?;
                return Ok((next_version, notifier));
            }

            return Ok((
                update_state_blocked(
                    tx,
                    execution_id,
                    &appending_version,
                    join_set_id,
                    *run_expires_at,
                    *closing,
                )
                .await?,
                AppendNotifier::default(),
            ));
        }
    }
}

async fn append_response(
    tx: &Transaction<'_>,
    execution_id: &ExecutionId,
    response_outer: JoinSetResponseEventOuter,
) -> Result<AppendNotifier, DbErrorWrite> {
    let join_set_id = &response_outer.event.join_set_id;

    let (delay_id, delay_success) = match &response_outer.event.event {
        JoinSetResponse::DelayFinished { delay_id, result } => {
            (Some(delay_id.to_string()), Some(result.is_ok()))
        }
        JoinSetResponse::ChildExecutionFinished { .. } => (None, None),
    };

    let (child_execution_id, finished_version) = match &response_outer.event.event {
        JoinSetResponse::ChildExecutionFinished {
            child_execution_id,
            finished_version,
            result: _,
        } => (
            Some(child_execution_id.to_string()),
            Some(i64::from(finished_version.0)),
        ),
        JoinSetResponse::DelayFinished { .. } => (None, None),
    };

    tx.execute(
            "INSERT INTO t_join_set_response (execution_id, created_at, join_set_id, delay_id, delay_success, child_execution_id, finished_version) \
             VALUES ($1, $2, $3, $4, $5, $6, $7)",
             &[
                 &execution_id.to_string(),
                 &response_outer.created_at,
                 &join_set_id.to_string(),
                 &delay_id,
                 &delay_success,
                 &child_execution_id,
                 &finished_version,
             ]
        ).await?;

    // if the execution is going to be unblocked by this response...
    let combined_state = get_combined_state(tx, execution_id).await?;
    debug!("previous_pending_state: {combined_state:?}");

    let mut notifier = if let PendingState::BlockedByJoinSet {
        join_set_id: found_join_set_id,
        lock_expires_at,
        closing: _,
        component_id_input_digest,
    } = combined_state.pending_state
        && *join_set_id == found_join_set_id
    {
        let scheduled_at = std::cmp::max(lock_expires_at, response_outer.created_at);
        // Unblock the state.
        update_state_pending_after_response_appended(
            tx,
            execution_id,
            scheduled_at,
            &combined_state.corresponding_version,
            component_id_input_digest,
        )
        .await?
    } else {
        AppendNotifier::default()
    };

    if let JoinSetResponseEvent {
        join_set_id,
        event:
            JoinSetResponse::DelayFinished {
                delay_id,
                result: _,
            },
    } = &response_outer.event
    {
        debug!(%join_set_id, %delay_id, "Deleting from `t_delay`");
        tx.execute(
            "DELETE FROM t_delay WHERE execution_id = $1 AND join_set_id = $2 AND delay_id = $3",
            &[
                &execution_id.to_string(),
                &join_set_id.to_string(),
                &delay_id.to_string(),
            ],
        )
        .await?;
    }

    notifier.response = Some((execution_id.clone(), response_outer));
    Ok(notifier)
}

async fn append_backtrace(
    tx: &Transaction<'_>,
    backtrace_info: &BacktraceInfo,
) -> Result<(), DbErrorWrite> {
    let backtrace_json = Json(&backtrace_info.wasm_backtrace);

    tx.execute(
            "INSERT INTO t_backtrace (execution_id, component_id, version_min_including, version_max_excluding, wasm_backtrace) \
             VALUES ($1, $2, $3, $4, $5)",
            &[
                &backtrace_info.execution_id.to_string(),
                &Json(&backtrace_info.component_id),
                &i64::from(backtrace_info.version_min_including.0),
                &i64::from(backtrace_info.version_max_excluding.0),
                &backtrace_json,
            ],
        )
        .await?;

    Ok(())
}

#[cfg(feature = "test")]
async fn get_execution_log(
    tx: &Transaction<'_>,
    execution_id: &ExecutionId,
) -> Result<concepts::storage::ExecutionLog, DbErrorRead> {
    let rows = tx
        .query(
            "SELECT created_at, json_value, version FROM t_execution_log WHERE \
                 execution_id = $1 ORDER BY version",
            &[&execution_id.to_string()],
        )
        .await
        .map_err(DbErrorRead::from)?;

    if rows.is_empty() {
        return Err(DbErrorRead::NotFound);
    }

    let mut events = Vec::with_capacity(rows.len());
    for row in rows {
        let created_at: DateTime<Utc> = get(&row, "created_at")?;
        let event: Json<ExecutionRequest> = get(&row, "json_value")?;
        let event = event.0;
        let version: i64 = get(&row, "version")?;
        let version = Version::try_from(version)
            .map_err(|_| consistency_db_err("version must be non-negative"))?;

        events.push(ExecutionEvent {
            created_at,
            event,
            backtrace_id: None,
            version,
        });
    }

    let combined_state = get_combined_state(tx, execution_id).await?;
    let responses_dto = list_responses(tx, execution_id, None).await?;
    let responses = responses_dto.into_iter().map(|resp| resp.event).collect();

    Ok(concepts::storage::ExecutionLog {
        execution_id: execution_id.clone(),
        events,
        responses,
        next_version: combined_state.get_next_version_or_finished(),
        pending_state: combined_state.pending_state,
    })
}

async fn list_execution_events(
    tx: &Transaction<'_>,
    execution_id: &ExecutionId,
    version_min: VersionType,
    version_max_excluding: VersionType,
    include_backtrace_id: bool,
) -> Result<Vec<ExecutionEvent>, DbErrorRead> {
    let sql = if include_backtrace_id {
        "SELECT
                log.created_at,
                log.json_value,
                log.version,
                bt.version_min_including AS backtrace_id
            FROM
                t_execution_log AS log
            LEFT OUTER JOIN
                t_backtrace AS bt ON log.execution_id = bt.execution_id
                                AND log.version >= bt.version_min_including
                                AND log.version < bt.version_max_excluding
            WHERE
                log.execution_id = $1
                AND log.version >= $2
                AND log.version < $3
            ORDER BY
                log.version"
    } else {
        "SELECT
                created_at, json_value, NULL::BIGINT as backtrace_id, version
            FROM t_execution_log WHERE
                execution_id = $1 AND version >= $2 AND version < $3
            ORDER BY version"
    };

    let rows = tx
        .query(
            sql,
            &[
                &execution_id.to_string(),
                &i64::from(version_min),
                &i64::from(version_max_excluding),
            ],
        )
        .await
        .map_err(DbErrorRead::from)?;

    let mut events = Vec::with_capacity(rows.len());
    for row in rows {
        let created_at: DateTime<Utc> = get(&row, "created_at")?;
        let backtrace_id = get::<Option<i64>>(&row, "backtrace_id")?
            .map(Version::try_from)
            .transpose()
            .map_err(|_| consistency_db_err("version must be non-negative"))?;

        let version = get::<i64>(&row, "version")?;
        let version = Version::new(
            VersionType::try_from(version)
                .map_err(|_| consistency_db_err("version must be non-negative"))?,
        );
        let event_req: Json<ExecutionRequest> = get(&row, "json_value")?;
        let event_req = event_req.0;

        events.push(ExecutionEvent {
            created_at,
            event: event_req,
            backtrace_id,
            version,
        });
    }
    Ok(events)
}

async fn get_execution_event(
    tx: &Transaction<'_>,
    execution_id: &ExecutionId,
    version: VersionType,
) -> Result<ExecutionEvent, DbErrorRead> {
    let row = tx
        .query_one(
            "SELECT created_at, json_value, version FROM t_execution_log WHERE \
                 execution_id = $1 AND version = $2",
            &[&execution_id.to_string(), &i64::from(version)],
        )
        .await
        .map_err(DbErrorRead::from)?;

    let created_at: DateTime<Utc> = get(&row, "created_at")?;
    let json_val: Json<ExecutionRequest> = get(&row, "json_value")?;
    let version = get::<i64>(&row, "version")?;
    let version = Version::try_from(version)
        .map_err(|_| consistency_db_err("version must be non-negative"))?;
    let event = json_val.0;

    Ok(ExecutionEvent {
        created_at,
        event,
        backtrace_id: None,
        version,
    })
}

async fn get_last_execution_event(
    tx: &Transaction<'_>,
    execution_id: &ExecutionId,
) -> Result<ExecutionEvent, DbErrorRead> {
    let row = tx
        .query_one(
            "SELECT created_at, json_value, version FROM t_execution_log WHERE \
                 execution_id = $1 ORDER BY version DESC LIMIT 1",
            &[&execution_id.to_string()],
        )
        .await
        .map_err(DbErrorRead::from)?;

    let created_at: DateTime<Utc> = get(&row, "created_at")?;
    let event: Json<ExecutionRequest> = get(&row, "json_value")?;
    let event = event.0;
    let version = get::<i64>(&row, "version")?;
    let version = Version::try_from(version)
        .map_err(|_| consistency_db_err("version must be non-negative"))?;

    Ok(ExecutionEvent {
        created_at,
        event,
        backtrace_id: None,
        version,
    })
}

async fn delay_response(
    tx: &Transaction<'_>,
    execution_id: &ExecutionId,
    delay_id: &DelayId,
) -> Result<Option<bool>, DbErrorRead> {
    let row = tx
        .query_opt(
            "SELECT delay_success \
                 FROM t_join_set_response \
                 WHERE \
                 execution_id = $1 AND delay_id = $2",
            &[&execution_id.to_string(), &delay_id.to_string()],
        )
        .await
        .map_err(DbErrorRead::from)?;

    match row {
        Some(r) => Ok(Some(get::<bool>(&r, "delay_success")?)),
        None => Ok(None),
    }
}

#[instrument(level = Level::TRACE, skip_all)]
async fn get_responses_with_offset(
    tx: &Transaction<'_>,
    execution_id: &ExecutionId,
    skip_rows: u32,
) -> Result<Vec<JoinSetResponseEventOuter>, DbErrorRead> {
    let rows = tx
            .query(
                "SELECT r.id, r.created_at, r.join_set_id, \
                 r.delay_id, r.delay_success, \
                 r.child_execution_id, r.finished_version, l.json_value \
                 FROM t_join_set_response r LEFT OUTER JOIN t_execution_log l ON r.child_execution_id = l.execution_id \
                 WHERE \
                 r.execution_id = $1 AND \
                 ( \
                 r.finished_version = l.version \
                 OR r.child_execution_id IS NULL \
                 ) \
                 ORDER BY id \
                 OFFSET $2",
                 &[
                     &execution_id.to_string(),
                     &(i64::from(skip_rows)),
                 ]
            )
            .await
            .map_err(DbErrorRead::from)?;

    let mut results = Vec::with_capacity(rows.len());
    for row in rows {
        let resp = parse_response_with_cursor(&row)?;
        results.push(resp.event);
    }
    Ok(results)
}

async fn get_pending_of_single_ffqn(
    tx: &Transaction<'_>,
    batch_size: u32,
    pending_at_or_sooner: DateTime<Utc>,
    ffqn: &FunctionFqn,
    select_strategy: SelectStrategy,
) -> Result<Vec<(ExecutionId, Version)>, ()> {
    let rows = tx
        .query(
            &format!(
                "SELECT execution_id, corresponding_version FROM t_state \
                WHERE \
                state = '{STATE_PENDING_AT}' AND \
                pending_expires_finished <= $1 AND ffqn = $2 \
                ORDER BY pending_expires_finished \
                {} \
                LIMIT $3",
                if select_strategy == SelectStrategy::LockForUpdate {
                    "FOR UPDATE SKIP LOCKED"
                } else {
                    ""
                }
            ),
            &[
                &pending_at_or_sooner,
                &ffqn.to_string(),
                &(i64::from(batch_size)),
            ],
        )
        .await
        .map_err(|err| {
            warn!("Ignoring consistency error {err:?}");
        })?;

    let mut result = Vec::with_capacity(rows.len());
    for row in rows {
        let unpack = || -> Result<(ExecutionId, Version), DbErrorGeneric> {
            let eid_str: String = get(&row, "execution_id")?;
            let corresponding_version: i64 = get(&row, "corresponding_version")?;
            let corresponding_version = Version::try_from(corresponding_version)
                .map_err(|_| consistency_db_err("version must be non-negative"))?;

            if let Ok(eid) = ExecutionId::from_str(&eid_str) {
                return Ok((eid, corresponding_version.increment()));
            }
            Err(consistency_db_err("invalid execution_id"))
        };

        match unpack() {
            Ok(val) => result.push(val),
            Err(err) => warn!("Ignoring corrupted row in pending check: {err:?}"),
        }
    }
    Ok(result)
}

/// Get executions and their next versions
async fn get_pending_by_ffqns(
    tx: &Transaction<'_>,
    batch_size: u32,
    pending_at_or_sooner: DateTime<Utc>,
    ffqns: &[FunctionFqn],
    select_strategy: SelectStrategy,
) -> Result<Vec<(ExecutionId, Version)>, DbErrorGeneric> {
    let batch_size = usize::try_from(batch_size).expect("16 bit systems are unsupported");
    let mut execution_ids_versions = Vec::with_capacity(batch_size);

    for ffqn in ffqns {
        let needed = batch_size - execution_ids_versions.len();
        if needed == 0 {
            break;
        }
        let needed = u32::try_from(needed).expect("u32 - usize cannot overflow an 32");
        if let Ok(execs) =
            get_pending_of_single_ffqn(tx, needed, pending_at_or_sooner, ffqn, select_strategy)
                .await
        {
            execution_ids_versions.extend(execs);
        }
    }

    Ok(execution_ids_versions)
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
enum SelectStrategy {
    Read,
    LockForUpdate,
}

async fn get_pending_by_component_input_digest(
    tx: &Transaction<'_>,
    batch_size: u32,
    pending_at_or_sooner: DateTime<Utc>,
    input_digest: &InputContentDigest,
    select_strategy: SelectStrategy,
) -> Result<Vec<(ExecutionId, Version)>, DbErrorGeneric> {
    let rows = tx
        .query(
            &format!(
                "SELECT execution_id, corresponding_version FROM t_state WHERE \
                state = '{STATE_PENDING_AT}' AND \
                pending_expires_finished <= $1 AND \
                component_id_input_digest = $2 \
                ORDER BY pending_expires_finished \
                {} \
                LIMIT $3",
                if select_strategy == SelectStrategy::LockForUpdate {
                    "FOR UPDATE SKIP LOCKED"
                } else {
                    ""
                }
            ),
            &[
                &pending_at_or_sooner,
                &input_digest.as_slice(), // BYTEA
                &i64::from(batch_size),
            ],
        )
        .await
        .map_err(|err| DbErrorGeneric::Uncategorized(err.to_string().into()))?;

    let mut result = Vec::with_capacity(rows.len());
    for row in rows {
        let unpack = || -> Result<(ExecutionId, Version), DbErrorGeneric> {
            let eid_str: String = get(&row, "execution_id")?;
            let corresponding_version: i64 = get(&row, "corresponding_version")?;
            let corresponding_version = Version::try_from(corresponding_version)
                .map_err(|_| consistency_db_err("version must be non-negative"))?;

            let eid = ExecutionId::from_str(&eid_str)
                .map_err(|err| consistency_db_err(err.to_string()))?;
            Ok((eid, corresponding_version.increment()))
        };

        match unpack() {
            Ok(val) => result.push(val),
            Err(err) => {
                warn!("Skipping corrupted row in get_pending_by_component_input_digest: {err:?}");
            }
        }
    }

    Ok(result)
}

fn notify_pending_locked(
    notifier: &NotifierPendingAt,
    current_time: DateTime<Utc>,
    ffqn_to_pending_subscription: &std::sync::MutexGuard<PendingFfqnSubscribersHolder>,
) {
    if notifier.scheduled_at <= current_time {
        ffqn_to_pending_subscription.notify(notifier);
    }
}

async fn upgrade_execution_component(
    tx: &Transaction<'_>,
    execution_id: &ExecutionId,
    old: &InputContentDigest,
    new: &InputContentDigest,
) -> Result<(), DbErrorWrite> {
    debug!("Updating t_state to component {new}");

    let updated = tx
        .execute(
            r"
                UPDATE t_state
                SET
                    updated_at = CURRENT_TIMESTAMP,
                    component_id_input_digest = $1
                WHERE
                    execution_id = $2 AND
                    component_id_input_digest = $3
                ",
            &[
                &new.as_slice(),           // $1: BYTEA
                &execution_id.to_string(), // $2: TEXT
                &old.as_slice(),           // $3: BYTEA
            ],
        )
        .await
        .map_err(|err| DbErrorGeneric::Uncategorized(err.to_string().into()))?;

    if updated != 1 {
        return Err(DbErrorWrite::NotFound);
    }
    Ok(())
}

impl PostgresConnection {
    // Must be called after write transaction commit for a correct happens-before relationship.
    #[instrument(level = Level::TRACE, skip_all)]
    fn notify_all(&self, notifiers: Vec<AppendNotifier>, current_time: DateTime<Utc>) {
        let (pending_ats, finished_execs, responses) = {
            let (mut pending_ats, mut finished_execs, mut responses) =
                (Vec::new(), Vec::new(), Vec::new());
            for notifier in notifiers {
                if let Some(pending_at) = notifier.pending_at {
                    pending_ats.push(pending_at);
                }
                if let Some(finished) = notifier.execution_finished {
                    finished_execs.push(finished);
                }
                if let Some(response) = notifier.response {
                    responses.push(response);
                }
            }
            (pending_ats, finished_execs, responses)
        };

        // Notify pending_at subscribers.
        if !pending_ats.is_empty() {
            let guard = self.pending_subscribers.lock().unwrap();
            for pending_at in pending_ats {
                notify_pending_locked(&pending_at, current_time, &guard);
            }
        }
        // Notify execution finished subscribers.
        if !finished_execs.is_empty() {
            let mut guard = self.execution_finished_subscribers.lock().unwrap();
            for finished in finished_execs {
                if let Some(listeners_of_exe_id) = guard.remove(&finished.execution_id) {
                    for (_tag, sender) in listeners_of_exe_id {
                        let _ = sender.send(finished.retval.clone());
                    }
                }
            }
        }
        // Notify response subscribers.
        if !responses.is_empty() {
            let mut guard = self.response_subscribers.lock().unwrap();
            for (execution_id, response) in responses {
                if let Some((sender, _)) = guard.remove(&execution_id) {
                    let _ = sender.send(response);
                }
            }
        }
    }
}

#[async_trait]
impl DbExecutor for PostgresConnection {
    #[instrument(level = Level::TRACE, skip(self))]
    async fn lock_pending_by_ffqns(
        &self,
        batch_size: u32,
        pending_at_or_sooner: DateTime<Utc>,
        ffqns: Arc<[FunctionFqn]>,
        created_at: DateTime<Utc>,
        component_id: ComponentId,
        executor_id: ExecutorId,
        lock_expires_at: DateTime<Utc>,
        run_id: RunId,
        retry_config: ComponentRetryConfig,
    ) -> Result<LockPendingResponse, DbErrorGeneric> {
        let mut client_guard = self.client.lock().await;
        let tx = client_guard
            .transaction()
            .await
            .map_err(DbErrorGeneric::from)?;

        let execution_ids_versions = get_pending_by_ffqns(
            &tx,
            batch_size,
            pending_at_or_sooner,
            &ffqns,
            SelectStrategy::LockForUpdate,
        )
        .await?;

        if execution_ids_versions.is_empty() {
            // Commit is required to release the connection state cleanly,
            // though rollback/drop works too for read-only.
            tx.commit().await.map_err(DbErrorGeneric::from)?;
            return Ok(vec![]);
        }

        debug!("Locking {execution_ids_versions:?}");

        // Lock using the same transaction
        let mut locked_execs = Vec::with_capacity(execution_ids_versions.len());
        for (execution_id, version) in execution_ids_versions {
            match lock_single_execution(
                &tx,
                created_at,
                component_id.clone(),
                &execution_id,
                run_id,
                &version,
                executor_id,
                lock_expires_at,
                retry_config,
            )
            .await
            {
                Ok(locked) => locked_execs.push(locked),
                Err(err) => {
                    warn!("Locking row {execution_id} failed - {err:?}");
                }
            }
        }

        tx.commit().await.map_err(DbErrorGeneric::from)?;

        Ok(locked_execs)
    }

    #[instrument(level = Level::TRACE, skip(self))]
    async fn lock_pending_by_component_id(
        &self,
        batch_size: u32,
        pending_at_or_sooner: DateTime<Utc>,
        component_id: &ComponentId,
        created_at: DateTime<Utc>,
        executor_id: ExecutorId,
        lock_expires_at: DateTime<Utc>,
        run_id: RunId,
        retry_config: ComponentRetryConfig,
    ) -> Result<LockPendingResponse, DbErrorGeneric> {
        let mut client_guard = self.client.lock().await;
        let tx = client_guard
            .transaction()
            .await
            .map_err(DbErrorGeneric::from)?;

        let execution_ids_versions = get_pending_by_component_input_digest(
            &tx,
            batch_size,
            pending_at_or_sooner,
            &component_id.input_digest,
            SelectStrategy::LockForUpdate,
        )
        .await?;

        if execution_ids_versions.is_empty() {
            tx.commit().await.map_err(DbErrorGeneric::from)?;
            return Ok(vec![]);
        }

        debug!("Locking {execution_ids_versions:?}");

        let mut locked_execs = Vec::with_capacity(execution_ids_versions.len());
        for (execution_id, version) in execution_ids_versions {
            match lock_single_execution(
                &tx,
                created_at,
                component_id.clone(),
                &execution_id,
                run_id,
                &version,
                executor_id,
                lock_expires_at,
                retry_config,
            )
            .await
            {
                Ok(locked) => locked_execs.push(locked),
                Err(err) => {
                    warn!("Locking row {execution_id} failed - {err:?}");
                }
            }
        }

        tx.commit().await.map_err(DbErrorGeneric::from)?;
        Ok(locked_execs)
    }

    #[instrument(level = Level::DEBUG, skip(self))]
    async fn lock_one(
        &self,
        created_at: DateTime<Utc>,
        component_id: ComponentId,
        execution_id: &ExecutionId,
        run_id: RunId,
        version: Version,
        executor_id: ExecutorId,
        lock_expires_at: DateTime<Utc>,
        retry_config: ComponentRetryConfig,
    ) -> Result<LockedExecution, DbErrorWrite> {
        debug!(%execution_id, "lock_one");
        let mut client_guard = self.client.lock().await;
        let tx = client_guard
            .transaction()
            .await
            .map_err(DbErrorGeneric::from)?;

        let res = lock_single_execution(
            &tx,
            created_at,
            component_id,
            execution_id,
            run_id,
            &version,
            executor_id,
            lock_expires_at,
            retry_config,
        )
        .await?;

        tx.commit().await.map_err(DbErrorGeneric::from)?;
        Ok(res)
    }

    #[instrument(level = Level::DEBUG, skip(self, req))]
    async fn append(
        &self,
        execution_id: ExecutionId,
        version: Version,
        req: AppendRequest,
    ) -> Result<AppendResponse, DbErrorWrite> {
        debug!(%req, "append");
        trace!(?req, "append");
        let created_at = req.created_at;

        let mut client_guard = self.client.lock().await;
        let tx = client_guard
            .transaction()
            .await
            .map_err(DbErrorGeneric::from)?;

        let (new_version, notifier) = append(&tx, &execution_id, req, version).await?;

        tx.commit().await.map_err(DbErrorGeneric::from)?;

        // Explicitly drop guard (optional, happens at end of scope anyway)
        drop(client_guard);

        self.notify_all(vec![notifier], created_at);
        Ok(new_version)
    }

    #[instrument(level = Level::DEBUG, skip_all)]
    async fn append_batch_respond_to_parent(
        &self,
        events: AppendEventsToExecution,
        response: AppendResponseToExecution,
        current_time: DateTime<Utc>,
    ) -> Result<AppendBatchResponse, DbErrorWrite> {
        debug!("append_batch_respond_to_parent");
        if events.execution_id == response.parent_execution_id {
            return Err(DbErrorWrite::NonRetriable(
                DbErrorWriteNonRetriable::ValidationFailed(
                    "Parameters `execution_id` and `parent_execution_id` cannot be the same".into(),
                ),
            ));
        }
        if events.batch.is_empty() {
            return Err(DbErrorWrite::NonRetriable(
                DbErrorWriteNonRetriable::ValidationFailed("batch cannot be empty".into()),
            ));
        }

        let mut client_guard = self.client.lock().await;
        let tx = client_guard
            .transaction()
            .await
            .map_err(DbErrorGeneric::from)?;

        let mut version = events.version;
        let mut notifiers = Vec::new();

        for append_request in events.batch {
            let (v, n) = append(&tx, &events.execution_id, append_request, version).await?;
            version = v;
            notifiers.push(n);
        }

        let pending_at_parent = append_response(
            &tx,
            &response.parent_execution_id,
            JoinSetResponseEventOuter {
                created_at: response.created_at,
                event: JoinSetResponseEvent {
                    join_set_id: response.join_set_id,
                    event: JoinSetResponse::ChildExecutionFinished {
                        child_execution_id: response.child_execution_id,
                        finished_version: response.finished_version,
                        result: response.result,
                    },
                },
            },
        )
        .await?;
        notifiers.push(pending_at_parent);

        tx.commit().await.map_err(DbErrorGeneric::from)?;
        drop(client_guard);

        self.notify_all(notifiers, current_time);
        Ok(version)
    }

    #[instrument(level = Level::TRACE, skip(self, timeout_fut))]
    async fn wait_for_pending_by_ffqn(
        &self,
        pending_at_or_sooner: DateTime<Utc>,
        ffqns: Arc<[FunctionFqn]>,
        timeout_fut: Pin<Box<dyn Future<Output = ()> + Send>>,
    ) {
        let unique_tag: u64 = rand::random();
        let (sender, mut receiver) = mpsc::channel(1);
        {
            let mut pending_subscribers = self.pending_subscribers.lock().unwrap();
            for ffqn in ffqns.as_ref() {
                pending_subscribers.insert_ffqn(ffqn.clone(), (sender.clone(), unique_tag));
            }
        }

        async {
            let mut db_has_pending = false;
            {
                // Scope the lock so we don't hold it while waiting for timeout
                let mut client_guard = self.client.lock().await;
                // Read-only transaction check
                if let Ok(tx) = client_guard.transaction().await {
                    if let Ok(res) = get_pending_by_ffqns(
                        &tx,
                        1,
                        pending_at_or_sooner,
                        &ffqns,
                        SelectStrategy::Read,
                    )
                    .await
                        && !res.is_empty()
                    {
                        db_has_pending = true;
                    }
                    // Commit/Rollback read transaction
                    let _ = tx.commit().await;
                }
            }

            if db_has_pending {
                trace!("Not waiting, database already contains new pending executions");
                return;
            }

            tokio::select! {
                _ = receiver.recv() => {
                    trace!("Received a notification");
                }
                () = timeout_fut => {
                }
            }
        }
        .await;

        // Cleanup
        {
            let mut pending_subscribers = self.pending_subscribers.lock().unwrap();
            for ffqn in ffqns.as_ref() {
                match pending_subscribers.remove_ffqn(ffqn) {
                    Some((_, tag)) if tag == unique_tag => {}
                    Some(other) => {
                        pending_subscribers.insert_ffqn(ffqn.clone(), other);
                    }
                    None => {}
                }
            }
        }
    }

    async fn wait_for_pending_by_component_id(
        &self,
        pending_at_or_sooner: DateTime<Utc>,
        component_id: &ComponentId,
        timeout_fut: Pin<Box<dyn Future<Output = ()> + Send>>,
    ) {
        let unique_tag: u64 = rand::random();
        let (sender, mut receiver) = mpsc::channel(1);
        {
            let mut pending_subscribers = self.pending_subscribers.lock().unwrap();
            pending_subscribers.insert_by_component(
                component_id.input_digest.clone(),
                (sender.clone(), unique_tag),
            );
        }

        async {
            let mut db_has_pending = false;
            {
                let mut client_guard = self.client.lock().await;
                if let Ok(tx) = client_guard.transaction().await {
                    if let Ok(res) = get_pending_by_component_input_digest(
                        &tx,
                        1,
                        pending_at_or_sooner,
                        &component_id.input_digest,
                        SelectStrategy::Read,
                    )
                    .await
                        && !res.is_empty()
                    {
                        db_has_pending = true;
                    }
                    let _ = tx.commit().await;
                }
            }

            if db_has_pending {
                trace!("Not waiting, database already contains new pending executions");
                return;
            }

            tokio::select! {
                _ = receiver.recv() => {
                    trace!("Received a notification");
                }
                () = timeout_fut => {
                }
            }
        }
        .await;

        // Cleanup
        {
            let mut pending_subscribers = self.pending_subscribers.lock().unwrap();
            match pending_subscribers.remove_by_component(&component_id.input_digest) {
                Some((_, tag)) if tag == unique_tag => {}
                Some(other) => {
                    pending_subscribers
                        .insert_by_component(component_id.input_digest.clone(), other);
                }
                None => {}
            }
        }
    }

    async fn get_last_execution_event(
        &self,
        execution_id: &ExecutionId,
    ) -> Result<ExecutionEvent, DbErrorRead> {
        let mut client_guard = self.client.lock().await;
        let tx = client_guard
            .transaction()
            .await
            .map_err(DbErrorGeneric::from)?;

        let event = get_last_execution_event(&tx, execution_id).await?;

        tx.commit().await.map_err(DbErrorGeneric::from)?;
        Ok(event)
    }
}
#[async_trait]
impl DbConnection for PostgresConnection {
    #[instrument(level = Level::DEBUG, skip_all, fields(execution_id = %req.execution_id))]
    async fn create(&self, req: CreateRequest) -> Result<AppendResponse, DbErrorWrite> {
        debug!("create");
        trace!(?req, "create");
        let created_at = req.created_at;

        let mut client_guard = self.client.lock().await;
        let tx = client_guard
            .transaction()
            .await
            .map_err(DbErrorGeneric::from)?;

        let (version, notifier) = create_inner(&tx, req.clone()).await?;

        tx.commit().await.map_err(DbErrorGeneric::from)?;
        drop(client_guard); // Release DB lock before notifying

        self.notify_all(vec![notifier], created_at);
        Ok(version)
    }

    #[instrument(level = Level::DEBUG, skip(self, batch))]
    async fn append_batch(
        &self,
        current_time: DateTime<Utc>,
        batch: Vec<AppendRequest>,
        execution_id: ExecutionId,
        version: Version,
    ) -> Result<AppendBatchResponse, DbErrorWrite> {
        debug!("append_batch");
        trace!(?batch, "append_batch");
        assert!(!batch.is_empty(), "Empty batch request");

        let mut client_guard = self.client.lock().await;
        let tx = client_guard
            .transaction()
            .await
            .map_err(DbErrorGeneric::from)?;

        let mut version = version;
        let mut notifier = None;

        for append_request in batch {
            let (v, n) = append(&tx, &execution_id, append_request, version).await?;
            version = v;
            notifier = Some(n);
        }

        tx.commit().await.map_err(DbErrorGeneric::from)?;
        drop(client_guard);

        self.notify_all(
            vec![notifier.expect("checked that the batch is not empty")],
            current_time,
        );
        Ok(version)
    }

    #[instrument(level = Level::DEBUG, skip(self, batch, child_req))]
    async fn append_batch_create_new_execution(
        &self,
        current_time: DateTime<Utc>,
        batch: Vec<AppendRequest>,
        execution_id: ExecutionId,
        version: Version,
        child_req: Vec<CreateRequest>,
    ) -> Result<AppendBatchResponse, DbErrorWrite> {
        debug!("append_batch_create_new_execution");
        trace!(?batch, ?child_req, "append_batch_create_new_execution");
        assert!(!batch.is_empty(), "Empty batch request");

        let mut client_guard = self.client.lock().await;
        let tx = client_guard
            .transaction()
            .await
            .map_err(DbErrorGeneric::from)?;

        let mut version = version;
        let mut notifier = None;

        for append_request in batch {
            let (v, n) = append(&tx, &execution_id, append_request, version).await?;
            version = v;
            notifier = Some(n);
        }

        let mut notifiers = Vec::new();
        notifiers.push(notifier.expect("checked that the batch is not empty"));

        for req in child_req {
            let (_, n) = create_inner(&tx, req).await?;
            notifiers.push(n);
        }

        tx.commit().await.map_err(DbErrorGeneric::from)?;
        drop(client_guard);

        self.notify_all(notifiers, current_time);
        Ok(version)
    }

    #[instrument(level = Level::DEBUG, skip(self, timeout_fut))]
    async fn subscribe_to_next_responses(
        &self,
        execution_id: &ExecutionId,
        start_idx: u32,
        timeout_fut: Pin<Box<dyn Future<Output = TimeoutOutcome> + Send>>,
    ) -> Result<Vec<JoinSetResponseEventOuter>, DbErrorReadWithTimeout> {
        debug!("next_responses");
        let unique_tag: u64 = rand::random();
        let execution_id_clone = execution_id.clone();

        let cleanup = || {
            let mut guard = self.response_subscribers.lock().unwrap();
            match guard.remove(&execution_id_clone) {
                Some((_, tag)) if tag == unique_tag => {}
                Some(other) => {
                    guard.insert(execution_id_clone.clone(), other);
                }
                None => {}
            }
        };

        let receiver = {
            let mut client_guard = self.client.lock().await;
            let tx = client_guard
                .transaction()
                .await
                .map_err(DbErrorRead::from)?;

            // Register listener before fetching from database.
            // This is a best-effort mechanism that shortens the polling time, if it
            // does not detect the response it will sleep using `timeout_fut`. Consumers
            // are expected to poll this function in a loop.
            // Currently the notification mechanism only works on a single node deployment.
            let (sender, receiver) = oneshot::channel();
            self.response_subscribers
                .lock()
                .unwrap()
                .insert(execution_id.clone(), (sender, unique_tag));

            let responses = get_responses_with_offset(&tx, execution_id, start_idx).await?;

            if responses.is_empty() {
                // Commit read transaction
                tx.commit().await.map_err(|err| {
                    cleanup(); // Remove the just inserted subscriber.
                    DbErrorRead::from(err)
                })?;
                receiver
            } else {
                cleanup(); // Remove the just inserted subscriber as we already have the answer.
                tx.commit().await.map_err(DbErrorRead::from)?;
                return Ok(responses);
            }
        };

        let res = tokio::select! {
            resp = receiver => {
                match resp {
                    Ok(resp) => Ok(vec![resp]),
                    Err(_) => Err(DbErrorReadWithTimeout::from(DbErrorGeneric::Close)),
                }
            }
            outcome = timeout_fut => Err(DbErrorReadWithTimeout::Timeout(outcome)),
        };

        cleanup();
        res
    }

    async fn wait_for_finished_result(
        &self,
        execution_id: &ExecutionId,
        timeout_fut: Option<Pin<Box<dyn Future<Output = TimeoutOutcome> + Send>>>,
    ) -> Result<SupportedFunctionReturnValue, DbErrorReadWithTimeout> {
        let unique_tag: u64 = rand::random();
        let execution_id_clone = execution_id.clone();

        let cleanup = || {
            let mut guard = self.execution_finished_subscribers.lock().unwrap();
            if let Some(subscribers) = guard.get_mut(&execution_id_clone) {
                subscribers.remove(&unique_tag);
            }
        };

        let receiver = {
            let mut client_guard = self.client.lock().await;
            let tx = client_guard
                .transaction()
                .await
                .map_err(DbErrorRead::from)?;

            // Register listener
            let (sender, receiver) = oneshot::channel();
            {
                let mut guard = self.execution_finished_subscribers.lock().unwrap();
                guard
                    .entry(execution_id.clone())
                    .or_default()
                    .insert(unique_tag, sender);
            }

            let pending_state = get_combined_state(&tx, execution_id).await?.pending_state;

            if let PendingState::Finished { finished, .. } = pending_state {
                let event = get_execution_event(&tx, execution_id, finished.version).await?;
                tx.commit().await.map_err(DbErrorRead::from)?;
                cleanup();

                if let ExecutionRequest::Finished { result, .. } = event.event {
                    return Ok(result);
                }
                error!("Mismatch, expected Finished row: {event:?} based on t_state {finished}");
                return Err(DbErrorReadWithTimeout::from(consistency_db_err(
                    "cannot get finished event based on t_state version",
                )));
            }
            tx.commit().await.map_err(DbErrorRead::from)?;
            receiver
        };

        let timeout_fut = timeout_fut.unwrap_or_else(|| Box::pin(std::future::pending()));
        let res = tokio::select! {
            resp = receiver => {
                match resp {
                    Ok(retval) => Ok(retval),
                    Err(_recv_err) => Err(DbErrorGeneric::Close.into())
                }
            }
            outcome = timeout_fut => Err(DbErrorReadWithTimeout::Timeout(outcome)),
        };

        cleanup();
        res
    }

    #[instrument(level = Level::DEBUG, skip_all, fields(%join_set_id, %execution_id))]
    async fn append_delay_response(
        &self,
        created_at: DateTime<Utc>,
        execution_id: ExecutionId,
        join_set_id: JoinSetId,
        delay_id: DelayId,
        result: Result<(), ()>,
    ) -> Result<AppendDelayResponseOutcome, DbErrorWrite> {
        debug!("append_delay_response");
        let event = JoinSetResponseEventOuter {
            created_at,
            event: JoinSetResponseEvent {
                join_set_id,
                event: JoinSetResponse::DelayFinished {
                    delay_id: delay_id.clone(),
                    result,
                },
            },
        };

        let mut client_guard = self.client.lock().await;
        let tx = client_guard
            .transaction()
            .await
            .map_err(DbErrorGeneric::from)?;

        let res = append_response(&tx, &execution_id, event).await;

        match res {
            Ok(notifier) => {
                tx.commit().await.map_err(DbErrorGeneric::from)?;
                drop(client_guard);
                self.notify_all(vec![notifier], created_at);
                Ok(AppendDelayResponseOutcome::Success)
            }
            Err(DbErrorWrite::NonRetriable(DbErrorWriteNonRetriable::Conflict)) => {
                // Check if already finished
                // Roll back the failed tx, start new read tx.
                tx.rollback().await.map_err(DbErrorGeneric::from)?;

                // Start new tx for check
                let tx = client_guard
                    .transaction()
                    .await
                    .map_err(DbErrorGeneric::from)?;
                let delay_success = delay_response(&tx, &execution_id, &delay_id).await?;
                tx.commit().await.map_err(DbErrorGeneric::from)?;

                match delay_success {
                    Some(true) => Ok(AppendDelayResponseOutcome::AlreadyFinished),
                    Some(false) => Ok(AppendDelayResponseOutcome::AlreadyCancelled),
                    None => Err(DbErrorWrite::Generic(DbErrorGeneric::Uncategorized(
                        "insert failed yet select did not find the response".into(),
                    ))),
                }
            }
            Err(err) => {
                let _ = tx.rollback().await; // cleanup
                Err(err)
            }
        }
    }

    #[instrument(level = Level::DEBUG, skip_all)]
    async fn append_backtrace(&self, append: BacktraceInfo) -> Result<(), DbErrorWrite> {
        debug!("append_backtrace");
        let mut client_guard = self.client.lock().await;
        let tx = client_guard
            .transaction()
            .await
            .map_err(DbErrorGeneric::from)?;

        append_backtrace(&tx, &append).await?;

        tx.commit().await.map_err(DbErrorGeneric::from)?;
        Ok(())
    }

    #[instrument(level = Level::DEBUG, skip_all)]
    async fn append_backtrace_batch(&self, batch: Vec<BacktraceInfo>) -> Result<(), DbErrorWrite> {
        debug!("append_backtrace_batch");
        let mut client_guard = self.client.lock().await;
        let tx = client_guard
            .transaction()
            .await
            .map_err(DbErrorGeneric::from)?;

        for append in batch {
            append_backtrace(&tx, &append).await?;
        }

        tx.commit().await.map_err(DbErrorGeneric::from)?;
        Ok(())
    }

    /// Get currently expired delays and locks.
    #[instrument(level = Level::TRACE, skip(self))]
    async fn get_expired_timers(
        &self,
        at: DateTime<Utc>,
    ) -> Result<Vec<ExpiredTimer>, DbErrorGeneric> {
        let mut client_guard = self.client.lock().await;
        let tx = client_guard
            .transaction()
            .await
            .map_err(DbErrorGeneric::from)?;

        // 1. Expired Delays
        let rows = tx
            .query(
                "SELECT execution_id, join_set_id, delay_id FROM t_delay WHERE expires_at <= $1",
                &[&at],
            )
            .await
            .map_err(DbErrorGeneric::from)?;

        let mut expired_timers = Vec::with_capacity(rows.len());
        for row in rows {
            let unpack = || -> Result<ExpiredTimer, DbErrorGeneric> {
                let execution_id: String = get(&row, "execution_id")?;
                let execution_id = ExecutionId::from_str(&execution_id)
                    .map_err(|err| consistency_db_err(err.to_string()))?;
                let join_set_id: String = get(&row, "join_set_id")?;
                let join_set_id = JoinSetId::from_str(&join_set_id)
                    .map_err(|err| consistency_db_err(err.to_string()))?;
                let delay_id: String = get(&row, "delay_id")?;
                let delay_id = DelayId::from_str(&delay_id)
                    .map_err(|err| consistency_db_err(err.to_string()))?;

                Ok(ExpiredTimer::Delay(ExpiredDelay {
                    execution_id,
                    join_set_id,
                    delay_id,
                }))
            };

            match unpack() {
                Ok(timer) => expired_timers.push(timer),
                Err(err) => warn!("Skipping corrupted row in get_expired_timers (delays): {err:?}"),
            }
        }

        // 2. Expired Locks
        let rows = tx.query(
            &format!(
                "SELECT execution_id, last_lock_version, corresponding_version, intermittent_event_count, max_retries, retry_exp_backoff_millis, executor_id, run_id \
                 FROM t_state \
                 WHERE pending_expires_finished <= $1 AND state = '{STATE_LOCKED}'"
            ),
            &[&at]
        ).await.map_err(DbErrorGeneric::from)?;

        for row in rows {
            let unpack = || -> Result<ExpiredTimer, DbErrorGeneric> {
                let execution_id: String = get(&row, "execution_id")?;
                let execution_id = ExecutionId::from_str(&execution_id)
                    .map_err(|err| consistency_db_err(err.to_string()))?;
                let last_lock_version: i64 = get(&row, "last_lock_version")?;
                let last_lock_version = Version::try_from(last_lock_version)
                    .map_err(|_| consistency_db_err("version must be non-negative"))?;

                let corresponding_version: i64 = get(&row, "corresponding_version")?;
                let corresponding_version = Version::try_from(corresponding_version)
                    .map_err(|_| consistency_db_err("version must be non-negative"))?;

                let intermittent_event_count =
                    u32::try_from(get::<i64>(&row, "intermittent_event_count")?).map_err(|_| {
                        consistency_db_err("`intermittent_event_count` must not be negative")
                    })?;

                let max_retries = get::<Option<i64>>(&row, "max_retries")?
                    .map(u32::try_from)
                    .transpose()
                    .map_err(|_| consistency_db_err("`max_retries` must not be negative"))?;
                let retry_exp_backoff_millis =
                    u32::try_from(get::<i64>(&row, "retry_exp_backoff_millis")?).map_err(|_| {
                        consistency_db_err("`retry_exp_backoff_millis` must not be negative")
                    })?;
                let executor_id: String = get(&row, "executor_id")?;
                let executor_id = ExecutorId::from_str(&executor_id)
                    .map_err(|err| consistency_db_err(err.to_string()))?;
                let run_id: String = get(&row, "run_id")?;
                let run_id =
                    RunId::from_str(&run_id).map_err(|err| consistency_db_err(err.to_string()))?;

                Ok(ExpiredTimer::Lock(ExpiredLock {
                    execution_id,
                    locked_at_version: last_lock_version,
                    next_version: corresponding_version.increment(),
                    intermittent_event_count,
                    max_retries,
                    retry_exp_backoff: Duration::from_millis(u64::from(retry_exp_backoff_millis)),
                    locked_by: LockedBy {
                        executor_id,
                        run_id,
                    },
                }))
            };

            match unpack() {
                Ok(timer) => expired_timers.push(timer),
                Err(err) => warn!("Skipping corrupted row in get_expired_timers (locks): {err:?}"),
            }
        }

        tx.commit().await.map_err(DbErrorGeneric::from)?;

        if !expired_timers.is_empty() {
            debug!("get_expired_timers found {expired_timers:?}");
        }
        Ok(expired_timers)
    }

    async fn get_execution_event(
        &self,
        execution_id: &ExecutionId,
        version: &Version,
    ) -> Result<ExecutionEvent, DbErrorRead> {
        let mut client_guard = self.client.lock().await;
        let tx = client_guard
            .transaction()
            .await
            .map_err(DbErrorRead::from)?;

        let event = get_execution_event(&tx, execution_id, version.0).await?;

        tx.commit().await.map_err(DbErrorRead::from)?;
        Ok(event)
    }

    async fn get_pending_state(
        &self,
        execution_id: &ExecutionId,
    ) -> Result<PendingState, DbErrorRead> {
        let mut client_guard = self.client.lock().await;
        let tx = client_guard
            .transaction()
            .await
            .map_err(DbErrorRead::from)?;

        let state = get_combined_state(&tx, execution_id).await?.pending_state;

        tx.commit().await.map_err(DbErrorRead::from)?;
        Ok(state)
    }
}

#[async_trait]
impl DbExternalApi for PostgresConnection {
    #[instrument(level = Level::DEBUG, skip_all)]
    async fn get_backtrace(
        &self,
        execution_id: &ExecutionId,
        filter: BacktraceFilter,
    ) -> Result<BacktraceInfo, DbErrorRead> {
        debug!("get_backtrace");

        let mut client_guard = self.client.lock().await;
        let tx = client_guard
            .transaction()
            .await
            .map_err(DbErrorRead::from)?;

        let mut params: Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>> = Vec::new();

        params.push(Box::new(execution_id.to_string())); // $1
        let p_execution_id_idx = format!("${}", params.len()); // $1

        let mut sql = String::new();
        write!(
            &mut sql,
            "SELECT component_id, version_min_including, version_max_excluding, wasm_backtrace \
     FROM t_backtrace WHERE execution_id = {p_execution_id_idx}"
        )
        .unwrap();

        match &filter {
            BacktraceFilter::Specific(version) => {
                params.push(Box::new(i64::from(version.0))); // $2
                let p_ver_idx = format!("${}", params.len()); // $2
                write!(
                    &mut sql,
                    " AND version_min_including <= {p_ver_idx} AND version_max_excluding > {p_ver_idx}"
                )
                .unwrap();
            }
            BacktraceFilter::First => {
                sql.push_str(" ORDER BY version_min_including LIMIT 1");
            }
            BacktraceFilter::Last => {
                sql.push_str(" ORDER BY version_min_including DESC LIMIT 1");
            }
        }

        let params_refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
            params.iter().map(|p| p.as_ref() as _).collect();

        let row = tx
            .query_one(&sql, &params_refs)
            .await
            .map_err(DbErrorRead::from)?;

        let component_id: Json<ComponentId> = get(&row, "component_id")?;
        let component_id = component_id.0;

        let version_min_including =
            Version::try_from(get::<i64>(&row, "version_min_including")?)
                .map_err(|_| consistency_db_err("version must be non-negative"))?;

        let version_max_excluding =
            Version::try_from(get::<i64>(&row, "version_max_excluding")?)
                .map_err(|_| consistency_db_err("version must be non-negative"))?;

        // wasm_backtrace stored as JSONB
        let wasm_backtrace: Json<WasmBacktrace> = get(&row, "wasm_backtrace")?;
        let wasm_backtrace = wasm_backtrace.0;

        tx.commit().await.map_err(DbErrorRead::from)?;

        Ok(BacktraceInfo {
            execution_id: execution_id.clone(),
            component_id,
            version_min_including,
            version_max_excluding,
            wasm_backtrace,
        })
    }

    async fn list_executions(
        &self,
        ffqn: Option<FunctionFqn>,
        top_level_only: bool,
        pagination: ExecutionListPagination,
    ) -> Result<Vec<ExecutionWithState>, DbErrorGeneric> {
        let mut client_guard = self.client.lock().await;
        let tx = client_guard
            .transaction()
            .await
            .map_err(DbErrorGeneric::from)?;

        let result = list_executions(&tx, ffqn.as_ref(), top_level_only, &pagination).await?;

        tx.commit().await.map_err(DbErrorGeneric::from)?;
        Ok(result)
    }

    #[instrument(level = Level::DEBUG, skip(self))]
    async fn list_execution_events(
        &self,
        execution_id: &ExecutionId,
        since: &Version,
        max_length: VersionType,
        include_backtrace_id: bool,
    ) -> Result<Vec<ExecutionEvent>, DbErrorRead> {
        let mut client_guard = self.client.lock().await;
        let tx = client_guard
            .transaction()
            .await
            .map_err(DbErrorRead::from)?;

        let result = list_execution_events(
            &tx,
            execution_id,
            since.0,
            since.0 + max_length,
            include_backtrace_id,
        )
        .await?;

        tx.commit().await.map_err(DbErrorRead::from)?;
        Ok(result)
    }

    async fn list_responses(
        &self,
        execution_id: &ExecutionId,
        pagination: Pagination<u32>,
    ) -> Result<Vec<ResponseWithCursor>, DbErrorRead> {
        let mut client_guard = self.client.lock().await;
        let tx = client_guard
            .transaction()
            .await
            .map_err(DbErrorRead::from)?;

        let result = list_responses(&tx, execution_id, Some(pagination)).await?;

        tx.commit().await.map_err(DbErrorRead::from)?;
        Ok(result)
    }

    async fn upgrade_execution_component(
        &self,
        execution_id: &ExecutionId,
        old: &InputContentDigest,
        new: &InputContentDigest,
    ) -> Result<(), DbErrorWrite> {
        let mut client_guard = self.client.lock().await;
        let tx = client_guard
            .transaction()
            .await
            .map_err(DbErrorGeneric::from)?;

        upgrade_execution_component(&tx, execution_id, old, new).await?;

        tx.commit().await.map_err(DbErrorGeneric::from)?;
        Ok(())
    }
}

#[async_trait]
impl DbPoolCloseable for PostgresPool {
    async fn close(&self) {
        self.pool.close();
    }
}

#[cfg(feature = "test")]
#[async_trait]
impl concepts::storage::DbConnectionTest for PostgresConnection {
    #[instrument(level = Level::DEBUG, skip(self, response_event), fields(join_set_id = %response_event.join_set_id))]
    async fn append_response(
        &self,
        created_at: DateTime<Utc>,
        execution_id: ExecutionId,
        response_event: JoinSetResponseEvent,
    ) -> Result<(), DbErrorWrite> {
        debug!("append_response");
        let event = JoinSetResponseEventOuter {
            created_at,
            event: response_event,
        };

        let mut client_guard = self.client.lock().await;
        let tx = client_guard
            .transaction()
            .await
            .map_err(DbErrorGeneric::from)?;

        let notifier = append_response(&tx, &execution_id, event).await?;

        tx.commit().await.map_err(DbErrorGeneric::from)?;
        drop(client_guard);

        self.notify_all(vec![notifier], created_at);
        Ok(())
    }

    #[instrument(level = Level::DEBUG, skip(self))]
    async fn get(
        &self,
        execution_id: &ExecutionId,
    ) -> Result<concepts::storage::ExecutionLog, DbErrorRead> {
        trace!("get");
        let mut client_guard = self.client.lock().await;
        let tx = client_guard
            .transaction()
            .await
            .map_err(DbErrorRead::from)?;

        let res = get_execution_log(&tx, execution_id).await?;

        tx.commit().await.map_err(DbErrorRead::from)?;
        Ok(res)
    }
}

#[cfg(feature = "test")]
impl PostgresPool {
    pub async fn drop_database(&self) {
        let mut cfg = Config::new();
        cfg.host = Some(self.config.host.clone());
        cfg.user = Some(self.config.user.clone());
        cfg.password = Some(self.config.password.clone());
        cfg.dbname = Some(ADMIN_DB_NAME.into());
        cfg.manager = Some(ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        });

        let pool = cfg
            .create_pool(None, NoTls)
            .map_err(|err| {
                error!("Cannot create the default pool - {err:?}");
                InitializationError
            })
            .unwrap();

        let client = pool
            .get()
            .await
            .map_err(|err| {
                error!("Cannot get a connection from the default pool - {err:?}");
                InitializationError
            })
            .unwrap();
        for _ in 0..3 {
            let res = client
                .execute(&format!("DROP DATABASE {}", self.config.db_name), &[])
                .await; // Waits 1s on error, no need to sleep more.
            if res.is_ok() {
                debug!("Database '{}' dropped.", self.config.db_name);
                return;
            }
            debug!("Dropping db failed - {res:?}",);
        }
        warn!("Did not drop database {}", self.config.db_name);
    }
}
