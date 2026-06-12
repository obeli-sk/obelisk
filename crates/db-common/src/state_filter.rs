//! SQL rendering of [`ExecutionStateFilter`], shared by the sqlite and postgres DAOs
//! so that `list_executions` filtering stays consistent with the deployment summary
//! buckets in both implementations.

use chrono::{DateTime, Utc};
use concepts::storage::{
    ExecutionStateFilter, RESULT_KIND_JSON_ERROR, RESULT_KIND_JSON_OK, STATE_BLOCKED_BY_JOIN_SET,
    STATE_FINISHED, STATE_LOCKED, STATE_PENDING_AT,
};

/// Render one filter as a parenthesized SQL condition.
///
/// `now_placeholder` is the SQL placeholder substituted for the timestamp carried by
/// [`ExecutionStateFilter::Pending`] / [`ExecutionStateFilter::Scheduled`].
/// `jsonb_cast` is appended to JSON literals (`"::jsonb"` on postgres, `""` on sqlite).
#[must_use]
pub fn state_filter_to_sql(
    filter: &ExecutionStateFilter,
    now_placeholder: &str,
    jsonb_cast: &str,
) -> String {
    match filter {
        ExecutionStateFilter::Locked => {
            format!("(state = '{STATE_LOCKED}' AND is_paused = false)")
        }
        ExecutionStateFilter::Pending { .. } => format!(
            "(state = '{STATE_PENDING_AT}' AND is_paused = false \
            AND pending_expires_finished <= {now_placeholder})"
        ),
        ExecutionStateFilter::Scheduled { .. } => format!(
            "(state = '{STATE_PENDING_AT}' AND is_paused = false \
            AND pending_expires_finished > {now_placeholder})"
        ),
        ExecutionStateFilter::Blocked => {
            format!("(state = '{STATE_BLOCKED_BY_JOIN_SET}' AND is_paused = false)")
        }
        ExecutionStateFilter::Paused => "(is_paused = true)".to_string(),
        ExecutionStateFilter::Finished => format!("(state = '{STATE_FINISHED}')"),
        ExecutionStateFilter::FinishedOk => format!(
            "(state = '{STATE_FINISHED}' AND result_kind = '{RESULT_KIND_JSON_OK}'{jsonb_cast})"
        ),
        ExecutionStateFilter::FinishedError => format!(
            "(state = '{STATE_FINISHED}' AND result_kind = '{RESULT_KIND_JSON_ERROR}'{jsonb_cast})"
        ),
        ExecutionStateFilter::FinishedExecutionFailure => format!(
            "(state = '{STATE_FINISHED}' AND result_kind IS NOT NULL \
            AND result_kind NOT IN \
            ('{RESULT_KIND_JSON_OK}'{jsonb_cast}, '{RESULT_KIND_JSON_ERROR}'{jsonb_cast}))"
        ),
    }
}

/// Extract the `now` timestamp carried by `Pending`/`Scheduled` filters, if any.
/// All entries must carry the same `now`, see
/// [`concepts::storage::ListExecutionsFilter::state_filters`].
#[must_use]
pub fn state_filters_now(filters: &[ExecutionStateFilter]) -> Option<DateTime<Utc>> {
    filters.iter().find_map(|filter| match filter {
        ExecutionStateFilter::Pending { now } | ExecutionStateFilter::Scheduled { now } => {
            Some(*now)
        }
        _ => None,
    })
}
