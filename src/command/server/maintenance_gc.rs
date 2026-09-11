use super::DeploymentSwitchManagerHandle;
use crate::config::deployment::DurationConfig;
#[cfg(test)]
use crate::config::server::RetentionTomlConfig;
use crate::config::server::{GarbageCollectionTomlConfig, RetentionPolicyTomlConfig};
use anyhow::{Context as _, bail};
use concepts::storage::{DbPool, RetentionPolicy, SystemEventCode};
use executor::AbortOnDropHandle;
use std::{sync::Arc, time::Duration};
use tokio::sync::watch;
use tracing::{debug, info, warn};

#[derive(Clone, Copy)]
pub(super) struct ValidatedConfig {
    interval: Duration,
    batch_size: u32,
    batch_delay: Duration,
    executions: Option<Duration>,
    deployments: Option<Duration>,
    system_events: Option<Duration>,
}

impl ValidatedConfig {
    pub(super) fn new(config: GarbageCollectionTomlConfig) -> anyhow::Result<Self> {
        let interval = duration("maintenance.gc.interval", config.interval)?;
        if interval.is_zero() {
            bail!("`maintenance.gc.interval` must be greater than zero");
        }
        if config.batch_size == 0 {
            bail!("`maintenance.gc.batch_size` must be greater than zero");
        }
        Ok(Self {
            interval,
            batch_size: config.batch_size,
            batch_delay: duration("maintenance.gc.batch_delay", config.batch_delay)?,
            executions: retention_duration("executions", config.retention.executions)?,
            deployments: retention_duration("deployments", config.retention.deployments)?,
            system_events: retention_duration("system_events", config.retention.system_events)?,
        })
    }
}

fn retention_duration(
    name: &str,
    policy: RetentionPolicyTomlConfig,
) -> anyhow::Result<Option<Duration>> {
    if !policy.enabled {
        return Ok(None);
    }
    let duration = duration(
        &format!("maintenance.gc.retention.{name}.max_age"),
        policy.max_age,
    )?;
    if duration.is_zero() {
        bail!("`maintenance.gc.retention.{name}.max_age` must be greater than zero");
    }
    let age = chrono::Duration::from_std(duration)
        .with_context(|| format!("`maintenance.gc.retention.{name}.max_age` is too large"))?;
    chrono::Utc::now()
        .checked_sub_signed(age)
        .with_context(|| format!("`maintenance.gc.retention.{name}.max_age` is too large"))?;
    Ok(Some(duration))
}

fn duration(name: &str, value: DurationConfig) -> anyhow::Result<Duration> {
    let seconds = match value {
        DurationConfig::Milliseconds(milliseconds) => {
            return Ok(Duration::from_millis(milliseconds));
        }
        DurationConfig::Seconds(seconds) => seconds,
        DurationConfig::Minutes(minutes) => minutes
            .checked_mul(60)
            .with_context(|| format!("`{name}` is too large"))?,
        DurationConfig::Hours(hours) => hours
            .checked_mul(60 * 60)
            .with_context(|| format!("`{name}` is too large"))?,
    };
    Ok(Duration::from_secs(seconds))
}

#[derive(Clone, Copy, Default)]
struct CategoryStats {
    affected: u64,
    active: Duration,
}

#[derive(Clone, Copy, Default)]
struct SweepStats {
    execution_retention: CategoryStats,
    deployment_retention: CategoryStats,
    system_event_retention: CategoryStats,
    execution_gc: CategoryStats,
    cas_gc: CategoryStats,
    cas_bytes: u64,
}

impl SweepStats {
    fn log(self, total: Duration) {
        debug!(
            ?total,
            execution_trees_deleted = self.execution_retention.affected,
            execution_retention_active = ?self.execution_retention.active,
            deployments_deleted = self.deployment_retention.affected,
            deployment_retention_active = ?self.deployment_retention.active,
            system_events_deleted = self.system_event_retention.affected,
            system_event_retention_active = ?self.system_event_retention.active,
            execution_rows_deleted = self.execution_gc.affected,
            execution_gc_active = ?self.execution_gc.active,
            cas_blobs_deleted = self.cas_gc.affected,
            cas_bytes_deleted = self.cas_bytes,
            cas_gc_active = ?self.cas_gc.active,
            "Periodic garbage collection finished"
        );
    }

    fn affected(self) -> u64 {
        self.execution_retention
            .affected
            .saturating_add(self.deployment_retention.affected)
            .saturating_add(self.system_event_retention.affected)
            .saturating_add(self.execution_gc.affected)
            .saturating_add(self.cas_gc.affected)
    }

    fn details(self, total: Duration) -> serde_json::Value {
        serde_json::json!({
            "total_ms": duration_millis(total),
            "execution_retention": category_details(self.execution_retention),
            "deployment_retention": category_details(self.deployment_retention),
            "system_event_retention": category_details(self.system_event_retention),
            "execution_gc": category_details(self.execution_gc),
            "cas_gc": {
                "affected": self.cas_gc.affected,
                "active_ms": duration_millis(self.cas_gc.active),
                "bytes_deleted": self.cas_bytes,
            },
        })
    }
}

struct SweepFailure {
    category: &'static str,
    stats: SweepStats,
    source: anyhow::Error,
}

impl SweepFailure {
    fn new(category: &'static str, stats: SweepStats, source: impl Into<anyhow::Error>) -> Self {
        Self {
            category,
            stats,
            source: source.into(),
        }
    }
}

fn category_details(stats: CategoryStats) -> serde_json::Value {
    serde_json::json!({
        "affected": stats.affected,
        "active_ms": duration_millis(stats.active),
    })
}

fn duration_millis(duration: Duration) -> u64 {
    duration.as_millis().try_into().unwrap_or(u64::MAX)
}

pub(super) fn spawn(
    db_pool: Arc<dyn DbPool>,
    deployment_switch_manager: DeploymentSwitchManagerHandle,
    mut termination_watcher: watch::Receiver<()>,
    config: ValidatedConfig,
) -> AbortOnDropHandle {
    let handle = utils::spawn::spawn_named("maintenance_gc", async move {
        debug!("Spawned maintenance garbage collector");
        for (record_type, retention) in [
            ("executions", config.executions),
            ("deployments", config.deployments),
            ("system_events", config.system_events),
        ] {
            if let Some(max_age) = retention {
                info!(
                    record_type,
                    max_age_seconds = max_age.as_secs(),
                    "Periodic retention enabled"
                );
            } else {
                info!(record_type, "Periodic retention disabled");
            }
        }
        loop {
            tokio::select! {
                biased;
                _ = termination_watcher.changed() => break,
                () = tokio::time::sleep(config.interval) => {}
            }
            let started = std::time::Instant::now();
            let sweep = run_sweep(&db_pool, &deployment_switch_manager, config);
            let result = tokio::select! {
                biased;
                _ = termination_watcher.changed() => break,
                result = sweep => result,
            };
            let total = started.elapsed();
            match result {
                Ok(stats) => {
                    stats.log(total);
                    if stats.affected() > 0 {
                        crate::server::system_event_writer::record(
                            db_pool.as_ref(),
                            SystemEventCode::MaintenanceGcCompleted,
                            None,
                            None,
                            stats.details(total),
                        )
                        .await;
                    }
                }
                Err(failure) => {
                    warn!(
                        category = failure.category,
                        "Periodic garbage collection failed: {:#}", failure.source
                    );
                    let mut details = failure.stats.details(total);
                    details["category"] = failure.category.into();
                    details["error"] = format!("{:#}", failure.source).into();
                    crate::server::system_event_writer::record(
                        db_pool.as_ref(),
                        SystemEventCode::MaintenanceGcFailed,
                        None,
                        None,
                        details,
                    )
                    .await;
                }
            }
        }
        debug!("Ending maintenance garbage collector");
    });
    AbortOnDropHandle::new(handle.abort_handle())
}

async fn run_sweep(
    db_pool: &Arc<dyn DbPool>,
    deployment_switch_manager: &DeploymentSwitchManagerHandle,
    config: ValidatedConfig,
) -> Result<SweepStats, Box<SweepFailure>> {
    let mut stats = SweepStats::default();
    if let Some(max_age) = config.executions {
        let cutoff = cutoff(max_age);
        loop {
            let started = std::time::Instant::now();
            let result = async {
                db_pool
                    .admin_conn()
                    .await?
                    .retain_executions(
                        RetentionPolicy::CreatedAtOrAfter(cutoff),
                        config.batch_size,
                        false,
                        false,
                    )
                    .await
            }
            .await
            .map_err(|source| Box::new(SweepFailure::new("execution_retention", stats, source)))?;
            stats.execution_retention.active += started.elapsed();
            stats.execution_retention.affected += result.deleted_execution_trees;
            delay_after_work(result.deleted_execution_trees, config.batch_delay).await;
            if !result.has_more {
                break;
            }
        }
    }
    if let Some(max_age) = config.deployments {
        let cutoff = cutoff(max_age);
        loop {
            let started = std::time::Instant::now();
            let result = async {
                db_pool
                    .admin_conn()
                    .await?
                    .retain_deployments(
                        RetentionPolicy::CreatedAtOrAfter(cutoff),
                        config.batch_size,
                        true,
                        false,
                        false,
                    )
                    .await
            }
            .await
            .map_err(|source| Box::new(SweepFailure::new("deployment_retention", stats, source)))?;
            stats.deployment_retention.active += started.elapsed();
            stats.deployment_retention.affected += result.deleted_deployments;
            delay_after_work(result.deleted_deployments, config.batch_delay).await;
            if !result.has_more {
                break;
            }
        }
    }
    if let Some(max_age) = config.system_events {
        let cutoff = cutoff(max_age);
        loop {
            let started = std::time::Instant::now();
            let result = async {
                db_pool
                    .admin_conn()
                    .await?
                    .retain_system_events(cutoff, config.batch_size)
                    .await
            }
            .await
            .map_err(|source| {
                Box::new(SweepFailure::new("system_event_retention", stats, source))
            })?;
            stats.system_event_retention.active += started.elapsed();
            stats.system_event_retention.affected += result.deleted;
            delay_after_work(result.deleted, config.batch_delay).await;
            if !result.has_more {
                break;
            }
        }
    }
    loop {
        let started = std::time::Instant::now();
        let result = async {
            db_pool
                .admin_conn()
                .await?
                .gc_executions(config.batch_size)
                .await
        }
        .await
        .map_err(|source| Box::new(SweepFailure::new("execution_gc", stats, source)))?;
        stats.execution_gc.active += started.elapsed();
        stats.execution_gc.affected += result.deleted_rows;
        delay_after_work(result.deleted_rows, config.batch_delay).await;
        if !result.has_more {
            break;
        }
    }
    loop {
        let started = std::time::Instant::now();
        let result = deployment_switch_manager
            .gc_cas(false, config.batch_size)
            .await
            .map_err(|source| {
                Box::new(SweepFailure::new("cas_gc", stats, anyhow::anyhow!(source)))
            })?;
        stats.cas_gc.active += started.elapsed();
        stats.cas_gc.affected += result.deleted_blobs;
        stats.cas_bytes += result.deleted_bytes;
        delay_after_work(result.deleted_blobs, config.batch_delay).await;
        if !result.has_more {
            break;
        }
    }
    Ok(stats)
}

fn cutoff(max_age: Duration) -> chrono::DateTime<chrono::Utc> {
    let age = chrono::Duration::from_std(max_age).expect("retention age was validated at startup");
    chrono::Utc::now()
        .checked_sub_signed(age)
        .expect("retention age was validated at startup")
}

async fn delay_after_work(affected: u64, delay: Duration) {
    if affected > 0 && !delay.is_zero() {
        tokio::time::sleep(delay).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_invalid_interval_and_retention_before_starting() {
        let config = GarbageCollectionTomlConfig {
            interval: DurationConfig::Seconds(0),
            ..GarbageCollectionTomlConfig::default()
        };
        assert!(ValidatedConfig::new(config).is_err());

        let retention = RetentionTomlConfig {
            system_events: RetentionPolicyTomlConfig {
                max_age: DurationConfig::Hours(u64::MAX),
                ..RetentionPolicyTomlConfig::default()
            },
            ..RetentionTomlConfig::default()
        };
        let config = GarbageCollectionTomlConfig {
            retention,
            ..GarbageCollectionTomlConfig::default()
        };
        assert!(ValidatedConfig::new(config).is_err());
    }
}
