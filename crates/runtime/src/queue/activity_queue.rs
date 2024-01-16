use crate::{activity::Activity, database::ActivityEventFetcher, ActivityFailed, FunctionFqn};
use std::{collections::HashMap, sync::Arc};

use tracing::{debug, warn};

pub(crate) async fn process(
    activity_event_fetcher: ActivityEventFetcher,
    functions_to_activities: HashMap<Arc<FunctionFqn<'static>>, Arc<Activity>>,
) {
    while let Some((request, resp_tx)) = activity_event_fetcher.fetch_one().await {
        let activity_res = match functions_to_activities.get(&request.fqn) {
            Some(activity) => activity.run(&request).await,
            None => Err(ActivityFailed::NotFound {
                workflow_id: request.workflow_id,
                activity_fqn: request.fqn,
            }),
        };
        if let Err(err) = &activity_res {
            warn!("{err}");
        }
        let _ = resp_tx.send(activity_res);
    }
    debug!("ActivityQueueReceiver::process exiting"); // TODO: add runtime_id
}
