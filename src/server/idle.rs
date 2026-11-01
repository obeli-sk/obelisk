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
    reported: bool,
    backoff: Duration,
}

impl Detector {
    fn new(config: IdleReportConfig, now: DateTime<Utc>, monotonic: Instant) -> Self {
        Self {
            config,
            previous: (now, monotonic),
            generation: 0,
            reported: false,
            backoff: Duration::from_secs(1),
        }
    }

    fn check(
        &mut self,
        now: DateTime<Utc>,
        monotonic: Instant,
        http: Snapshot,
        db: IdleState,
    ) -> bool {
        let wall_delta = now.signed_duration_since(self.previous.0);
        let monotonic_delta = chrono::TimeDelta::from_std(monotonic - self.previous.1).unwrap();
        let resumed = wall_delta - monotonic_delta
            > chrono::TimeDelta::from_std(self.config.resume_threshold.into()).unwrap();
        self.previous = (now, monotonic);
        let idle = http.in_flight == 0
            && now.signed_duration_since(http.last_finished_at)
                >= chrono::TimeDelta::from_std(self.config.quiet_period.into()).unwrap()
            && !db.busy
            && db
                .next_run_at
                .is_none_or(|at| at > now + Duration::from(self.config.horizon));
        if !idle || resumed || self.generation != http.generation {
            self.reset();
        }
        self.generation = http.generation;
        idle && !self.reported && !resumed
    }

    fn reset(&mut self) {
        self.reported = false;
        self.backoff = Duration::from_secs(1);
    }

    fn failed(&mut self) -> Duration {
        let delay = self.backoff.min(self.config.check_interval.into());
        self.backoff = delay
            .saturating_mul(2)
            .min(self.config.check_interval.into());
        delay
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
    let mut delay = Duration::from(detector.config.check_interval);
    loop {
        tokio::time::sleep(delay).await;
        delay = detector.config.check_interval.into();
        let generation = http.snapshot().generation;
        match db.idle_state().await {
            Ok(state) => {
                let activity = http.snapshot();
                if detector.check(Utc::now(), Instant::now(), activity, state)
                    && activity.generation == generation
                {
                    let report = serde_json::json!({"next_run_at": state.next_run_at});
                    super::system_event_writer::record(
                        &db as &dyn DbPool,
                        SystemEventCode::ServerIdleReported,
                        None,
                        None,
                        report.clone(),
                    )
                    .await;
                    // Event persistence can yield to an incoming request.
                    let activity = http.snapshot();
                    if activity.generation == generation
                        && detector.check(Utc::now(), Instant::now(), activity, state)
                    {
                        let result = client
                            .post(detector.config.url.as_ref().unwrap())
                            .json(&report)
                            .send()
                            .await;
                        match result {
                            Ok(response) if response.status().is_success() => {
                                detector.reported = true;
                            }
                            Ok(response) => {
                                tracing::warn!(status = %response.status(), "Idle report failed");
                                delay = detector.failed();
                            }
                            Err(err) => {
                                tracing::warn!("Idle report failed: {err}");
                                delay = detector.failed();
                            }
                        }
                    }
                }
            }
            Err(err) => {
                detector.reset();
                tracing::warn!("Cannot read idle state: {err}");
            }
        }
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
        detector.reported = true;
        assert!(!detector.check(now, instant, http, IdleState::default()));
        assert!(detector.check(
            now,
            instant,
            Snapshot {
                generation: 1,
                ..http
            },
            IdleState::default()
        ));
        detector.reported = true;
        assert!(!detector.check(
            now,
            instant,
            http,
            IdleState {
                busy: true,
                next_run_at: None
            }
        ));
        assert!(detector.check(now, instant, http, IdleState::default()));
    }

    #[test]
    fn idle_resume_threshold_and_retry_abort() {
        let now = Utc::now();
        let instant = Instant::now();
        let http = Snapshot {
            in_flight: 0,
            last_finished_at: now - Duration::from_secs(60),
            generation: 0,
        };
        let mut detector = Detector::new(IdleReportConfig::default(), now, instant);
        detector.reported = true;
        assert!(!detector.check(
            now + Duration::from_secs(35),
            instant + Duration::from_secs(30),
            http,
            IdleState::default()
        ));
        assert!(detector.reported);
        assert!(!detector.check(
            now + Duration::from_secs(72),
            instant + Duration::from_secs(60),
            http,
            IdleState::default()
        ));
        assert!(!detector.reported);
        assert!(detector.check(
            now + Duration::from_secs(102),
            instant + Duration::from_secs(90),
            http,
            IdleState::default()
        ));
        for expected in [1, 2, 4, 8, 16, 30, 30] {
            assert_eq!(detector.failed(), Duration::from_secs(expected));
        }
        assert!(!detector.check(
            now + Duration::from_secs(102),
            instant + Duration::from_secs(90),
            Snapshot {
                in_flight: 1,
                ..http
            },
            IdleState::default()
        ));
        assert_eq!(detector.failed(), Duration::from_secs(1));
    }

    #[tokio::test]
    async fn idle_report_once_per_period_and_retry() {
        use wiremock::{
            Mock, MockServer, ResponseTemplate,
            matchers::{body_json, header, method},
        };
        test_utils::set_up();
        let sink = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(503))
            .up_to_n_times(1)
            .with_priority(1)
            .expect(1)
            .mount(&sink)
            .await;
        Mock::given(method("POST"))
            .and(header("content-type", "application/json"))
            .and(body_json(serde_json::json!({"next_run_at": null})))
            .respond_with(ResponseTemplate::new(204))
            .expect(2)
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
        async fn wait_for(sink: &MockServer, count: usize) {
            tokio::time::timeout(Duration::from_secs(5), async {
                while sink.received_requests().await.unwrap().len() < count {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap();
        }
        wait_for(&sink, 2).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(sink.received_requests().await.unwrap().len(), 2);
        let request = http.begin();
        tokio::time::sleep(Duration::from_millis(60)).await;
        assert_eq!(sink.received_requests().await.unwrap().len(), 2);
        drop(request);
        wait_for(&sink, 3).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(sink.received_requests().await.unwrap().len(), 3);
        drop(task);
        db.close().await;
    }
}
