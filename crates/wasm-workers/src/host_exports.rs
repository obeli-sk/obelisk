// Generate `obelisk::workflow::host_activities`
wasmtime::component::bindgen!({
    path: "host-wit/",
    async: true,
    // interfaces: "import obelisk:workflow/host-activities;", // Broken in 26.0.0
    inline: "package any:any;
                world bindings {
                    import obelisk:workflow/host-activities;
                }",
    world: "any:any/bindings",
    trappable_imports: true,
    with: {
        "obelisk:types/execution/join-set-id": concepts::prefixed_ulid::JoinSetId,
    }
});

use crate::workflow::workflow_ctx::WorkflowCtx;
use concepts::{
    prefixed_ulid::{DelayId, JoinSetId},
    storage::{DbConnection, DbPool},
    ExecutionId,
};
use indexmap::indexmap;
pub(crate) use obelisk::types::time::Duration as DurationEnum;
use std::time::Duration;
use tracing::error;
use utils::time::ClockFn;
use val_json::wast_val::WastVal;
use wasmtime::component::{Resource, Val};

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

#[derive(Debug, thiserror::Error)]
#[error("cannot get join set from the first parameter")]
pub(crate) struct ValToJoinSetIdError;

pub(crate) fn val_to_join_set_id<C: ClockFn, DB: DbConnection, P: DbPool<DB>>(
    join_set_id: &Val,
    mut store_ctx: &mut wasmtime::StoreContextMut<'_, WorkflowCtx<C, DB, P>>,
) -> Result<JoinSetId, ValToJoinSetIdError> {
    if let Val::Resource(resource) = join_set_id {
        let resource: Resource<JoinSetId> = resource
            .try_into_resource(&mut store_ctx)
            .inspect_err(|err| error!("Cannot turn `ResourceAny` into a `Resource` - {err:?}"))
            .map_err(|_| ValToJoinSetIdError)?;
        let join_set_id = store_ctx
            .data()
            .resource_table
            .get(&resource)
            .inspect_err(|err| error!("Cannot get resource - {err:?}"))
            .map_err(|_| ValToJoinSetIdError)?;
        Ok(*join_set_id)
    } else {
        error!("Wrong type for JoinSetId, expected join-set-id, got `{join_set_id:?}`");
        Err(ValToJoinSetIdError)
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

pub(crate) fn join_set_id_into_wast_val(join_set_id: JoinSetId) -> WastVal {
    WastVal::Record(indexmap! {"id".to_string() => WastVal::String(join_set_id.to_string())})
}

pub(crate) fn delay_id_into_wast_val(delay_id: DelayId) -> WastVal {
    WastVal::Record(indexmap! {"id".to_string() => WastVal::String(delay_id.to_string())})
}
