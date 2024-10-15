// Generate `obelisk::workflow::host_activities`
wasmtime::component::bindgen!({
    path: "host-wit/",
    async: true,
    interfaces: "import obelisk:workflow/host-activities;",
    trappable_imports: true,
});

use std::time::Duration;

use assert_matches::assert_matches;
use concepts::{
    prefixed_ulid::{DelayId, JoinSetId},
    ExecutionId,
};
use indexmap::indexmap;
pub(crate) use obelisk::types::time::Duration as DurationEnum;
use tracing::error;
use val_json::wast_val::WastVal;
use wasmtime::component::Val;

pub(crate) const SUFFIX_FN_SUBMIT: &str = "-submit";
pub(crate) const SUFFIX_FN_AWAIT_NEXT: &str = "-await-next";
pub(crate) const SUFFIX_FN_SCHEDULE: &str = "-schedule";

impl From<DurationEnum> for Duration {
    fn from(value: DurationEnum) -> Self {
        match value {
            DurationEnum::Milliseconds(millis) => Duration::from_millis(millis),
            DurationEnum::Seconds(secs) => Duration::from_secs(secs),
            DurationEnum::Minutes(mins) => Duration::from_secs(u64::from(mins * 60)),
            DurationEnum::Hours(hours) => Duration::from_secs(u64::from(hours * 60 * 60)),
            DurationEnum::Days(days) => Duration::from_secs(u64::from(days * 24 * 60 * 60)),
        }
    }
}

pub(crate) enum ValToJoinSetIdError {
    ParseError,
    TypeError,
}

pub(crate) fn val_to_join_set_id(join_set_id: &Val) -> Result<JoinSetId, ValToJoinSetIdError> {
    match join_set_id {
        Val::Record(attrs)
            if attrs.len() == 1 || attrs[0].0 != "id" || !matches!(attrs[0].1, Val::String(_)) =>
        {
            let join_set_id = assert_matches!(&attrs[0].1, Val::String(s) => s);
            join_set_id.parse().map_err(|parse_err| {
                error!("Cannot parse JoinSetId `{join_set_id}` - {parse_err:?}");
                ValToJoinSetIdError::ParseError
            })
        }
        _ => {
            error!("Wrong type for JoinSetId, expected join-set-id, got `{join_set_id:?}`");
            Err(ValToJoinSetIdError::TypeError)
        }
    }
}

pub(crate) fn execution_id_into_wast_val(execution_id: ExecutionId) -> WastVal {
    WastVal::Record(indexmap! {"id".to_string() => WastVal::String(execution_id.to_string())})
}
pub(crate) fn execution_id_into_val(execution_id: ExecutionId) -> Val {
    Val::Record(vec![(
        "id".to_string(),
        Val::String(execution_id.to_string()),
    )])
}

pub(crate) fn join_set_id_into_wast_val(join_set_id: JoinSetId) -> WastVal {
    WastVal::Record(indexmap! {"id".to_string() => WastVal::String(join_set_id.to_string())})
}

pub(crate) fn delay_id_into_wast_val(delay_id: DelayId) -> WastVal {
    WastVal::Record(indexmap! {"id".to_string() => WastVal::String(delay_id.to_string())})
}
