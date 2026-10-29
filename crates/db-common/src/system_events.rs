use concepts::storage::SystemEventFilter;

pub fn system_event_list_sql(
    filter: &SystemEventFilter,
    parameter: impl Fn(usize) -> String,
) -> String {
    let mut predicates = Vec::new();
    for (index, column, present) in [
        (1, "event_id", filter.event_id.is_some()),
        (2, "node_run_id", filter.node_run_id.is_some()),
        (3, "level", filter.level.is_some()),
        (4, "code", filter.code.is_some()),
        (5, "deployment_id", filter.deployment_id.is_some()),
    ] {
        let param = parameter(index);
        predicates.push(if present {
            format!("{column} = {param}")
        } else {
            format!("{param} IS NULL")
        });
    }
    let cursor = parameter(6);
    predicates.push(if filter.before_event_id.is_some() {
        format!("(created_at, event_id) < (SELECT created_at, event_id FROM t_system_event WHERE event_id = {cursor})")
    } else {
        format!("{cursor} IS NULL")
    });
    for (index, op, present) in [
        (8, ">=", filter.created_from.is_some()),
        (9, "<", filter.created_to.is_some()),
    ] {
        let param = parameter(index);
        predicates.push(if present {
            format!("created_at {op} {param}")
        } else {
            format!("{param} IS NULL")
        });
    }
    format!(
        "SELECT event_id, node_run_id, created_at, level, code, execution_id, deployment_id, details, cas_digest, app_config_digest FROM t_system_event WHERE {} ORDER BY created_at DESC, event_id DESC LIMIT {}",
        predicates.join(" AND "),
        parameter(7),
    )
}
