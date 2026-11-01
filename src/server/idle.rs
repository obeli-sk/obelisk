use crate::config::server::IdleReportConfig;
use chrono::{DateTime, Utc};
use concepts::storage::{DbPool, SystemEventCode};
use db_sqlite::sqlite_dao::{IdleState, SqlitePool};
use std::time::Duration;
use tokio::time::Instant;
use utils::http_activity::{HttpActivity, Snapshot};

struct Detector {
    config: IdleReportConfig,
    previous: (DateTime<Utc>, Instant),
    generation: u64,
    /// The idle event is recorded once per idle period, not per report.
    recorded: bool,
    /// Wall time skipped by the last detected resume, until recorded.
    resumed: Option<chrono::TimeDelta>,
}

impl Detector {
    fn new(config: IdleReportConfig, now: DateTime<Utc>, monotonic: Instant) -> Self {
        Self {
            config,
            previous: (now, monotonic),
            generation: 0,
            recorded: false,
            resumed: None,
        }
    }

    /// Whether to report. Every idle check reports until the Machine is suspended, so a
    /// failed or skipped suspension needs no response handling.
    fn check(
        &mut self,
        now: DateTime<Utc>,
        monotonic: Instant,
        http: Snapshot,
        db: IdleState,
    ) -> bool {
        let wall_delta = now.signed_duration_since(self.previous.0);
        let monotonic_delta = chrono::TimeDelta::from_std(monotonic - self.previous.1).unwrap();
        let gap = wall_delta - monotonic_delta;
        let resumed =
            gap > chrono::TimeDelta::from_std(self.config.resume_threshold.into()).unwrap();
        if resumed {
            self.resumed = Some(gap);
        }
        self.previous = (now, monotonic);
        let idle = http.in_flight == 0
            && now.signed_duration_since(http.last_finished_at)
                >= chrono::TimeDelta::from_std(self.config.quiet_period.into()).unwrap()
            && !db.busy
            && db
                .next_run_at
                .is_none_or(|at| at > now + Duration::from(self.config.horizon));
        if !idle || resumed || self.generation != http.generation {
            self.recorded = false;
        }
        self.generation = http.generation;
        // The connection that woke the Machine may not be accepted yet; skip one check.
        idle && !resumed
    }
}

pub(crate) async fn run(config: IdleReportConfig, db: SqlitePool, http: HttpActivity) {
    let client = match reqwest::Client::builder()
        .timeout(config.request_timeout.into())
        .redirect(reqwest::redirect::Policy::none())
        .build()
    {
        Ok(client) => client,
        Err(err) => {
            tracing::warn!("Cannot create idle report client: {err}");
            return;
        }
    };
    let mut detector = Detector::new(config, Utc::now(), Instant::now());
    loop {
        tokio::time::sleep(detector.config.check_interval.into()).await;
        let generation = http.snapshot().generation;
        match db.idle_state().await {
            Ok(state) => {
                let activity = http.snapshot();
                let idle = detector.check(Utc::now(), Instant::now(), activity, state);
                record_resume(&db, &mut detector).await;
                if idle && activity.generation == generation {
                    let report = serde_json::json!({"next_run_at": state.next_run_at});
                    if !detector.recorded {
                        detector.recorded = true;
                        super::system_event_writer::record(
                            &db as &dyn DbPool,
                            SystemEventCode::ServerIdleReported,
                            None,
                            None,
                            report.clone(),
                        )
                        .await;
                    }
                    // Event persistence can yield to an incoming request.
                    let activity = http.snapshot();
                    if activity.generation == generation
                        && detector.check(Utc::now(), Instant::now(), activity, state)
                    {
                        // The status is informational: an unsuspended Machine reports again.
                        match client
                            .post(detector.config.url.as_ref().unwrap())
                            .json(&report)
                            .send()
                            .await
                        {
                            Ok(response) if response.status().is_success() => {}
                            Ok(response) => {
                                tracing::debug!(status = %response.status(), "Idle report not accepted");
                            }
                            Err(err) => tracing::warn!("Idle report failed: {err}"),
                        }
                    }
                }
            }
            Err(err) => {
                detector.recorded = false;
                tracing::warn!("Cannot read idle state: {err}");
            }
        }
    }
}

async fn record_resume(db: &SqlitePool, detector: &mut Detector) {
    if let Some(gap) = detector.resumed.take() {
        super::system_event_writer::record(
            db as &dyn DbPool,
            SystemEventCode::ServerResumeDetected,
            None,
            None,
            serde_json::json!({"suspended_ms": gap.num_milliseconds()}),
        )
        .await;
    }
}

pub(crate) async fn middleware(
    axum::extract::State(activity): axum::extract::State<HttpActivity>,
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    let guard = activity.begin();
    next.run(request)
        .await
        .map(|body| axum::body::Body::new(utils::http_activity::TrackedBody::new(body, guard)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::deployment::DurationConfig;
    use concepts::storage::DbPoolCloseable;

    #[tokio::test]
    async fn idle_http_tracks_stream_until_completion_or_drop() {
        use axum::{Router, body::Body, routing::get};
        use tower::ServiceExt;
        let http = HttpActivity::default();
        let app = Router::new()
            .route(
                "/stream",
                get(|| async {
                    Body::from_stream(futures_util::stream::pending::<
                        Result<hyper::body::Bytes, std::io::Error>,
                    >())
                }),
            )
            .route("/finite", get(|| async { "hello" }))
            .layer(axum::middleware::from_fn_with_state(
                http.clone(),
                middleware,
            ));
        let request = |path| {
            axum::http::Request::builder()
                .uri(path)
                .body(Body::empty())
                .unwrap()
        };
        let response = app.clone().oneshot(request("/stream")).await.unwrap();
        assert_eq!(http.snapshot().in_flight, 1);
        drop(response);
        assert_eq!(http.snapshot().in_flight, 0);
        let response = app.clone().oneshot(request("/finite")).await.unwrap();
        assert_eq!(http.snapshot().in_flight, 1);
        axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(http.snapshot().in_flight, 0);
        let response = app.oneshot(request("/missing")).await.unwrap();
        assert_eq!(response.status(), axum::http::StatusCode::NOT_FOUND);
        drop(response);
        assert_eq!(http.snapshot().in_flight, 0);
        assert_eq!(http.snapshot().generation, 3);
    }

    #[test]
    fn idle_decision_boundaries_and_activity() {
        let now = Utc::now();
        let instant = Instant::now();
        let http = Snapshot {
            in_flight: 0,
            last_finished_at: now - Duration::from_secs(60),
            generation: 0,
        };
        let mut detector = Detector::new(IdleReportConfig::default(), now, instant);
        assert!(!detector.check(
            now,
            instant,
            Snapshot {
                in_flight: 1,
                ..http
            },
            IdleState::default()
        ));
        assert!(!detector.check(
            now,
            instant,
            Snapshot {
                last_finished_at: now - Duration::from_secs(59),
                ..http
            },
            IdleState::default()
        ));
        assert!(!detector.check(
            now,
            instant,
            http,
            IdleState {
                busy: true,
                next_run_at: None
            }
        ));
        for seconds in [-1, 0, 60] {
            assert!(!detector.check(
                now,
                instant,
                http,
                IdleState {
                    busy: false,
                    next_run_at: Some(now + chrono::TimeDelta::seconds(seconds))
                }
            ));
        }
        assert!(detector.check(
            now,
            instant,
            http,
            IdleState {
                busy: false,
                next_run_at: Some(now + Duration::from_secs(61))
            }
        ));
        // Reports repeat until the Machine is suspended.
        detector.recorded = true;
        assert!(detector.check(now, instant, http, IdleState::default()));
        assert!(detector.recorded);
        assert!(detector.check(
            now,
            instant,
            Snapshot {
                generation: 1,
                ..http
            },
            IdleState::default()
        ));
        assert!(!detector.recorded);
    }

    #[test]
    fn idle_resume_skips_one_check() {
        let now = Utc::now();
        let instant = Instant::now();
        let http = Snapshot {
            in_flight: 0,
            last_finished_at: now - Duration::from_secs(60),
            generation: 0,
        };
        let mut detector = Detector::new(IdleReportConfig::default(), now, instant);
        detector.recorded = true;
        // A gap within the threshold is not a resume.
        assert!(detector.check(
            now + Duration::from_secs(35),
            instant + Duration::from_secs(30),
            http,
            IdleState::default()
        ));
        assert!(detector.resumed.is_none() && detector.recorded);
        // Suspended for 10 minutes, possibly with a report in flight.
        assert!(!detector.check(
            now + Duration::from_mins(11),
            instant + Duration::from_secs(60),
            http,
            IdleState::default()
        ));
        assert_eq!(
            detector.resumed.take(),
            Some(chrono::TimeDelta::seconds(595))
        );
        assert!(!detector.recorded);
        assert!(detector.check(
            now + Duration::from_secs(690),
            instant + Duration::from_secs(90),
            http,
            IdleState::default()
        ));
        assert!(detector.resumed.is_none());
    }

    #[tokio::test]
    async fn idle_report_repeats_while_idle() {
        use wiremock::{
            Mock, MockServer, ResponseTemplate,
            matchers::{body_json, header, method},
        };
        test_utils::set_up();
        let sink = MockServer::start().await;
        Mock::given(method("POST"))
            .and(header("content-type", "application/json"))
            .and(body_json(serde_json::json!({"next_run_at": null})))
            .respond_with(ResponseTemplate::new(503))
            .mount(&sink)
            .await;
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = SqlitePool::new(file.path(), db_sqlite::sqlite_dao::SqliteConfig::default())
            .await
            .unwrap();
        let http = HttpActivity::default();
        let config = IdleReportConfig {
            url: Some(sink.uri()),
            check_interval: DurationConfig::Milliseconds(20),
            quiet_period: DurationConfig::Milliseconds(0),
            ..Default::default()
        };
        let task = tokio::spawn(run(config, db.clone(), http.clone()));
        let task = executor::AbortOnDropHandle::new(task.abort_handle());
        let count = async || sink.received_requests().await.unwrap().len();
        async fn wait_for(sink: &MockServer, count: usize) {
            tokio::time::timeout(Duration::from_secs(5), async {
                while sink.received_requests().await.unwrap().len() < count {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap();
        }
        wait_for(&sink, 3).await;
        let request = http.begin();
        tokio::time::sleep(Duration::from_millis(60)).await;
        let busy = count().await;
        tokio::time::sleep(Duration::from_millis(60)).await;
        assert_eq!(count().await, busy);
        drop(request);
        wait_for(&sink, busy + 1).await;
        // One event per idle period, not per report.
        let events = (&db as &dyn DbPool)
            .admin_conn()
            .await
            .unwrap()
            .list_system_events(concepts::storage::SystemEventFilter {
                code: Some(SystemEventCode::ServerIdleReported.as_str().to_owned()),
                limit: 10,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(events.len(), 2);
        drop(task);
        db.close().await;
    }
}
