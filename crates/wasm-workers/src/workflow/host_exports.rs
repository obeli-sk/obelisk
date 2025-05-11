use concepts::JoinSetId;
use concepts::prefixed_ulid::ExecutionIdDerived;
use concepts::{ExecutionId, prefixed_ulid::DelayId};
use indexmap::indexmap;
use val_json::wast_val::WastVal;
use wasmtime::component::Val;

pub(crate) const SUFFIX_FN_SUBMIT: &str = "-submit";
pub(crate) const SUFFIX_FN_AWAIT_NEXT: &str = "-await-next";
pub(crate) const SUFFIX_FN_SCHEDULE: &str = "-schedule";

// Generate `obelisk:workflow:workflow-support@1.1.0`
pub(crate) mod v1_1_0 {
    wasmtime::component::bindgen!({
        path: "host-wit/",
        async: true,
        inline: "package any:any;
                world bindings {
                    import obelisk:workflow/workflow-support@1.1.0;
                }",
        world: "any:any/bindings",
        trappable_imports: true,
        with: {
            "obelisk:types/execution/join-set-id": concepts::JoinSetId,
        }
    });

    use obelisk::types::time::Duration as DurationEnum_1_1_0;
    use std::time::Duration;

    impl From<DurationEnum_1_1_0> for Duration {
        fn from(value: DurationEnum_1_1_0) -> Self {
            match value {
                DurationEnum_1_1_0::Milliseconds(millis) => Duration::from_millis(millis),
                DurationEnum_1_1_0::Seconds(secs) => Duration::from_secs(secs),
                DurationEnum_1_1_0::Minutes(mins) => Duration::from_secs(u64::from(mins * 60)),
                DurationEnum_1_1_0::Hours(hours) => Duration::from_secs(u64::from(hours * 60 * 60)),
                DurationEnum_1_1_0::Days(days) => {
                    Duration::from_secs(u64::from(days * 24 * 60 * 60))
                }
            }
        }
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

pub(crate) fn delay_id_into_wast_val(delay_id: DelayId) -> WastVal {
    WastVal::Record(indexmap! {"id".to_string() => WastVal::String(delay_id.to_string())})
}
