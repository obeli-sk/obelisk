pub(crate) const PENDING_BY_FFQN: &str = "
    SELECT execution_id, corresponding_version FROM t_state
    WHERE state = 'pending_at' AND pending_expires_finished <= $1 AND ffqn = $2
    AND lifecycle = 'active' AND tombstoned = FALSE
    ORDER BY pending_expires_finished";

pub(crate) const PENDING_BY_FFQN_AUTO: &str = "
    SELECT execution_id, corresponding_version FROM t_state
    WHERE state = 'pending_at' AND pending_expires_finished <= $1 AND ffqn = $2
    AND lifecycle = 'active' AND tombstoned = FALSE
    AND (incompatible_digest IS NULL OR incompatible_digest <> $3)
    ORDER BY pending_expires_finished";

pub(crate) const PENDING_BY_COMPONENT: &str = "
    SELECT execution_id, corresponding_version FROM t_state
    WHERE state = 'pending_at' AND pending_expires_finished <= $1
    AND component_id_input_digest = $2 AND lifecycle = 'active' AND tombstoned = FALSE
    ORDER BY pending_expires_finished";

pub(crate) const CANCELLING: &str = "
    SELECT execution_id FROM t_state WHERE lifecycle = 'cancelling'
    AND NOT EXISTS (SELECT 1 FROM t_state root WHERE root.is_top_level = TRUE
        AND root.tombstoned = TRUE AND root.execution_id = split_part(t_state.execution_id, '.', 1))
    ORDER BY created_at LIMIT $1";

pub(crate) const EXPIRED_DELAYS: &str = "
    SELECT d.execution_id, d.join_set_id, d.delay_id FROM t_delay d
    WHERE d.expires_at <= $1 AND NOT d.is_paused
    AND NOT EXISTS (SELECT 1 FROM t_state root WHERE root.is_top_level = TRUE
        AND root.tombstoned = TRUE AND root.execution_id = split_part(d.execution_id, '.', 1))";

pub(crate) const EXPIRED_LOCKS: &str = "
    SELECT execution_id, last_lock_version, corresponding_version, intermittent_event_count,
        max_retries, retry_exp_backoff_millis, executor_id, run_id, lifecycle
    FROM t_state WHERE pending_expires_finished <= $1 AND state = 'locked'";

#[cfg(test)]
mod tests {
    use super::*;
    use crate::postgres_dao::{PostgresConfig, PostgresPool, ProvisionPolicy};
    use concepts::storage::DbPoolCloseable;
    use secrecy::SecretString;
    use serde_json::Value;
    use tokio_postgres::types::Json;

    #[tokio::test]
    async fn idle_polling_work_is_bounded() {
        let env = |key| std::env::var(key).unwrap_or_else(|_| panic!("missing {key}"));
        let config = PostgresConfig {
            host: env("TEST_POSTGRES_HOST"),
            user: env("TEST_POSTGRES_USER"),
            password: SecretString::from(env("TEST_POSTGRES_PASSWORD")),
            db_name: format!(
                "{}_polling_{}",
                env("TEST_POSTGRES_DATABASE_PREFIX"),
                rand::random::<u64>()
            ),
        };
        let pool = PostgresPool::new(config.clone(), ProvisionPolicy::MustCreate)
            .await
            .unwrap();
        let mut pg = tokio_postgres::Config::new();
        pg.host(&config.host)
            .user(&config.user)
            .dbname(&config.db_name);
        use secrecy::ExposeSecret;
        pg.password(config.password.expose_secret());
        let (client, connection) = pg.connect(tokio_postgres::NoTls).await.unwrap();
        let task = tokio::spawn(connection);
        client.batch_execute("
            INSERT INTO t_state (
                execution_id, is_top_level, corresponding_version, ffqn, created_at,
                component_id_input_digest, component_type, first_scheduled_at, deployment_id,
                pending_expires_finished, state, updated_at, intermittent_event_count, lifecycle, tombstoned
            ) SELECT 'Execution_' || state || '_' || lifecycle || '_' || tombstoned || '_' || i, TRUE, 2, ffqn, '2026-10-29',
                digest, 'workflow', '2026-10-29', 'Dep_' || lpad(i::text, 26, '0'),
                (CASE WHEN state = 'locked' THEN '2099-01-01'::timestamptz ELSE '2026-10-29'::timestamptz END) + i * interval '1 second',
                state, '2026-10-29', 0, lifecycle, tombstoned FROM generate_series(1, 10000) n(i) CROSS JOIN (VALUES
                    ('finished', 'ns:pkg/ifc.fn', decode('00', 'hex'), 'active', FALSE),
                    ('pending_at', 'other:pkg/ifc.fn', decode('02', 'hex'), 'active', FALSE),
                    ('locked', 'other:pkg/ifc.fn', decode('02', 'hex'), 'active', FALSE),
                    ('pending_at', 'ns:pkg/ifc.fn', decode('00', 'hex'), 'paused', FALSE),
                    ('pending_at', 'ns:pkg/ifc.fn', decode('00', 'hex'), 'active', TRUE)
                ) kinds(state, ffqn, digest, lifecycle, tombstoned);
            INSERT INTO t_delay (execution_id, join_set_id, delay_id, expires_at, is_paused)
            SELECT 'Execution_delay_' || paused || '_' || i, 'join-set', 'Delay_' || paused || '_' || i,
                expires_at::timestamptz, paused FROM generate_series(1, 10000) n(i) CROSS JOIN
                (VALUES (FALSE, '2099-01-01'), (TRUE, '2026-10-29')) kinds(paused, expires_at);
            ANALYZE t_state; ANALYZE t_delay;
        ").await.unwrap();
        for mode in ["force_custom_plan", "force_generic_plan"] {
            client
                .batch_execute(&format!("SET plan_cache_mode = {mode}"))
                .await
                .unwrap();
            for (sql, args, pending) in [
                (PENDING_BY_FFQN, "'2027-01-01', 'ns:pkg/ifc.fn', 5", true),
                (
                    PENDING_BY_FFQN_AUTO,
                    "'2027-01-01', 'ns:pkg/ifc.fn', decode('00', 'hex'), 5",
                    true,
                ),
                (
                    PENDING_BY_COMPONENT,
                    "'2027-01-01', decode('00', 'hex'), 5",
                    true,
                ),
                (CANCELLING, "5", false),
                (EXPIRED_DELAYS, "'2027-01-01'", false),
                (EXPIRED_LOCKS, "'2027-01-01'", false),
                (
                    "SELECT execution_id FROM t_state WHERE execution_id >= $1 AND execution_id < $2 ORDER BY execution_id LIMIT 1",
                    "'Execution_missing.', 'Execution_missing/'",
                    false,
                ),
                (
                    "SELECT execution_id FROM t_delay WHERE execution_id >= $1 AND execution_id < $2 LIMIT 1",
                    "'Execution_missing.', 'Execution_missing/'",
                    false,
                ),
            ] {
                for locking in [false, true] {
                    if pending || !locking {
                        let sql = if pending {
                            let limit = if sql == PENDING_BY_FFQN_AUTO { 4 } else { 3 };
                            format!(
                                "{sql} {} LIMIT ${limit}",
                                if locking {
                                    "FOR UPDATE SKIP LOCKED"
                                } else {
                                    ""
                                }
                            )
                        } else {
                            sql.to_owned()
                        };
                        client
                            .batch_execute(&format!("PREPARE poll AS {sql}"))
                            .await
                            .unwrap();
                        let row = client
                            .query_one(
                                &format!(
                                    "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) EXECUTE poll({args})"
                                ),
                                &[],
                            )
                            .await
                            .unwrap();
                        let Json(plan): Json<Value> = row.get(0);
                        let root = &plan[0]["Plan"];
                        assert!(
                            root["Actual Rows"].as_f64().unwrap() < f64::EPSILON,
                            "{plan}"
                        );
                        let blocks = root["Shared Hit Blocks"].as_u64().unwrap()
                            + root["Shared Read Blocks"].as_u64().unwrap();
                        assert!(
                            blocks < 32,
                            "{mode}, {sql}: accessed {blocks} blocks: {plan}"
                        );
                        client.batch_execute("DEALLOCATE poll").await.unwrap();
                    }
                }
            }
        }
        client.batch_execute("UPDATE t_state SET lifecycle = 'cancelling' WHERE execution_id = (SELECT MIN(execution_id) FROM t_state WHERE state = 'finished')").await.unwrap();
        client
            .batch_execute(&format!("PREPARE poll AS {CANCELLING}"))
            .await
            .unwrap();
        let row = client
            .query_one(
                "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) EXECUTE poll(5)",
                &[],
            )
            .await
            .unwrap();
        let Json(plan): Json<Value> = row.get(0);
        let root = &plan[0]["Plan"];
        assert!(
            (root["Actual Rows"].as_f64().unwrap() - 1.0).abs() < f64::EPSILON,
            "{plan}"
        );
        let blocks = root["Shared Hit Blocks"].as_u64().unwrap()
            + root["Shared Read Blocks"].as_u64().unwrap();
        assert!(
            blocks < 32,
            "cancellation with unrelated tombstones accessed {blocks} blocks: {plan}"
        );
        drop(client);
        task.await.unwrap().unwrap();
        pool.close().await;
        let mut pg = pg.clone();
        pg.dbname("postgres");
        let (admin, connection) = pg.connect(tokio_postgres::NoTls).await.unwrap();
        let task = tokio::spawn(connection);
        admin
            .batch_execute(&format!("DROP DATABASE {} WITH (FORCE)", config.db_name))
            .await
            .unwrap();
        drop(admin);
        task.await.unwrap().unwrap();
    }
}
