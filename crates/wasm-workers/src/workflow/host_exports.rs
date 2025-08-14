use std::ops::Deref as _;
use std::time::Duration;

use assert_matches::assert_matches;
use chrono::DateTime;
use concepts::prefixed_ulid::ExecutionIdDerived;
use concepts::storage::HistoryEventScheduleAt;
use concepts::{ExecutionId, prefixed_ulid::DelayId};
use concepts::{FunctionFqn, JoinSetId};
use indexmap::indexmap;
use val_json::wast_val::WastVal;
use wasmtime::component::Val;

pub(crate) const SUFFIX_FN_SUBMIT: &str = "-submit";
pub(crate) const SUFFIX_FN_AWAIT_NEXT: &str = "-await-next";
pub(crate) const SUFFIX_FN_SCHEDULE: &str = "-schedule";
pub(crate) const SUFFIX_FN_STUB: &str = "-stub";

// Generate `obelisk:workflow:workflow-support@2.0.0`
pub(crate) mod v2_0_0 {
    use chrono::DateTime;
    use chrono::Utc;
    use concepts::storage::HistoryEventScheduleAt;
    use obelisk::types::time::Datetime;
    pub(crate) use obelisk::types::time::Duration as DurationEnum_2_0_0;
    pub(crate) use obelisk::types::time::ScheduleAt as ScheduleAt_2_0_0;
    pub(crate) use obelisk::workflow::workflow_support::ClosingStrategy as ClosingStrategy_2_0_0;
    use std::time::Duration;
    use std::time::UNIX_EPOCH;

    wasmtime::component::bindgen!({
        path: "host-wit-workflow/",
        async: true,
        inline: "package any:any;
                world bindings {
                    import obelisk:workflow/workflow-support@2.0.0;
                }",
        world: "any:any/bindings",
        trappable_imports: true,
        with: {
            "obelisk:types/execution/join-set-id": concepts::JoinSetId,
        }
    });

    impl From<DurationEnum_2_0_0> for Duration {
        fn from(value: DurationEnum_2_0_0) -> Self {
            match value {
                DurationEnum_2_0_0::Milliseconds(millis) => Duration::from_millis(millis),
                DurationEnum_2_0_0::Seconds(secs) => Duration::from_secs(secs),
                DurationEnum_2_0_0::Minutes(mins) => Duration::from_secs(u64::from(mins * 60)),
                DurationEnum_2_0_0::Hours(hours) => Duration::from_secs(u64::from(hours * 60 * 60)),
                DurationEnum_2_0_0::Days(days) => {
                    Duration::from_secs(u64::from(days * 24 * 60 * 60))
                }
            }
        }
    }

    impl From<Datetime> for DateTime<Utc> {
        fn from(
            Datetime {
                seconds,
                nanoseconds,
            }: Datetime,
        ) -> Self {
            let duration = Duration::new(seconds, nanoseconds);
            let systemtime = UNIX_EPOCH + duration;
            DateTime::<Utc>::from(systemtime)
        }
    }

    impl From<ScheduleAt_2_0_0> for HistoryEventScheduleAt {
        fn from(value: ScheduleAt_2_0_0) -> Self {
            match value {
                ScheduleAt_2_0_0::Now => Self::Now,
                ScheduleAt_2_0_0::At(datetime) => Self::At(DateTime::from(datetime)),
                ScheduleAt_2_0_0::In(duration) => Self::In(Duration::from(duration)),
            }
        }
    }
}

pub fn history_event_schedule_at_from_wast_val(
    scheduled_at: &WastVal,
) -> Result<HistoryEventScheduleAt, &'static str> {
    let WastVal::Variant(variant, val) = scheduled_at else {
        return Err("wrong type");
    };
    match (variant.as_str(), val) {
        ("now", None) => Ok(HistoryEventScheduleAt::Now),
        ("in", Some(duration)) => {
            if let &WastVal::Variant(key, value) = &duration.deref() {
                let duration = match (key.as_str(), value.as_deref()) {
                    ("milliseconds", Some(WastVal::U64(value))) => Duration::from_millis(*value),
                    ("seconds", Some(WastVal::U64(value))) => Duration::from_secs(*value),
                    ("minutes", Some(WastVal::U64(value))) => Duration::from_secs(*value * 60),
                    ("hours", Some(WastVal::U64(value))) => Duration::from_secs(*value * 60 * 60),
                    ("days", Some(WastVal::U64(value))) => {
                        Duration::from_secs(*value * 60 * 60 * 24)
                    }
                    _ => {
                        return Err(
                            "cannot convert `scheduled-at`, `in` variant: value must be one of the following keys: `milliseconds`(U64), `seconds`(U64), `minutes`(U32), `hours`(U32), `days`(U32)",
                        );
                    }
                };
                Ok(HistoryEventScheduleAt::In(duration))
            } else {
                Err("cannot convert `scheduled-at`, `in` variant: value must be a variant")
            }
        }
        ("at", Some(date_time)) if matches!(date_time.deref(), WastVal::Record(_)) => {
            let date_time =
                assert_matches!(date_time.deref(), WastVal::Record(keys_vals) => keys_vals)
                    .iter()
                    .map(|(k, v)| (k.as_str(), v))
                    .collect::<std::collections::HashMap<_, _>>();
            let seconds = date_time.get("seconds");
            let nanoseconds = date_time.get("nanoseconds");
            match (date_time.len(), seconds, nanoseconds) {
                (2, Some(WastVal::U64(seconds)), Some(WastVal::U32(nanoseconds))) => {
                    let date_time = v2_0_0::obelisk::types::time::Datetime {
                        seconds: *seconds,
                        nanoseconds: *nanoseconds,
                    };
                    let date_time = DateTime::from(date_time);
                    Ok(HistoryEventScheduleAt::At(date_time))
                }
                _ => Err(
                    "cannot convert `scheduled-at`, `at` variant: record must have exactly two keys: `seconds`(U64), `nanoseconds`(U32)",
                ),
            }
        }
        _ => Err("cannot convert `scheduled-at` variant, expected one of `now`, `in`, `at`"),
    }
}

pub(crate) fn execution_id_into_wast_val(execution_id: &ExecutionId) -> WastVal {
    WastVal::Record(indexmap! {"id".to_string() => WastVal::String(execution_id.to_string())})
}
pub(crate) fn execution_id_into_val(execution_id: &ExecutionId) -> Val {
    Val::Record(vec![(
        "id".to_string(),
        Val::String(execution_id.to_string()),
    )])
}

pub(crate) fn execution_id_derived_into_wast_val(execution_id: &ExecutionIdDerived) -> WastVal {
    WastVal::Record(indexmap! {"id".to_string() => WastVal::String(execution_id.to_string())})
}

pub(crate) fn join_set_id_into_wast_val(join_set_id: &JoinSetId) -> WastVal {
    WastVal::Record(indexmap! {"id".to_string() => WastVal::String(join_set_id.to_string())})
}

pub(crate) fn delay_id_into_wast_val(delay_id: &DelayId) -> WastVal {
    WastVal::Record(indexmap! {"id".to_string() => WastVal::String(delay_id.to_string())})
}

pub(crate) fn ffqn_into_wast_val(ffqn: &FunctionFqn) -> WastVal {
    WastVal::Record(indexmap! {
        "interface-name".to_string() => WastVal::String(ffqn.ifc_fqn.to_string()),
        "function-name".to_string() => WastVal::String(ffqn.function_name.to_string()),
    })
}
