//! Subscription management for pending executions.

use crate::NotifierPendingAt;
use concepts::{FunctionFqn, component_id::InputContentDigest};
use hashbrown::HashMap;
use tokio::sync::mpsc;
use tracing::debug;

/// Manages subscriptions for pending execution notifications.
#[derive(Default)]
pub struct PendingFfqnSubscribersHolder {
    by_ffqns: HashMap<FunctionFqn, (mpsc::Sender<()>, u64)>,
    by_component: HashMap<InputContentDigest /* input digest */, (mpsc::Sender<()>, u64)>,
}

impl PendingFfqnSubscribersHolder {
    /// Notify any subscribers that match the given pending notification.
    pub fn notify(&self, notifier: &NotifierPendingAt) {
        if let Some((subscription, _)) = self.by_ffqns.get(&notifier.ffqn) {
            debug!("Notifying pending subscriber by ffqn");
            // Does not block
            let _ = subscription.try_send(());
        }
        if let Some((subscription, _)) = self.by_component.get(&notifier.component_input_digest) {
            debug!("Notifying pending subscriber by component");
            // Does not block
            let _ = subscription.try_send(());
        }
    }

    /// Insert a subscription by function FQN.
    pub fn insert_ffqn(&mut self, ffqn: FunctionFqn, value: (mpsc::Sender<()>, u64)) {
        self.by_ffqns.insert(ffqn, value);
    }

    /// Remove a subscription by function FQN.
    pub fn remove_ffqn(&mut self, ffqn: &FunctionFqn) -> Option<(mpsc::Sender<()>, u64)> {
        self.by_ffqns.remove(ffqn)
    }

    /// Insert a subscription by component input digest.
    pub fn insert_by_component(
        &mut self,
        input_content_digest: InputContentDigest,
        value: (mpsc::Sender<()>, u64),
    ) {
        self.by_component.insert(input_content_digest, value);
    }

    /// Remove a subscription by component input digest.
    pub fn remove_by_component(
        &mut self,
        input_content_digest: &InputContentDigest,
    ) -> Option<(mpsc::Sender<()>, u64)> {
        self.by_component.remove(input_content_digest)
    }
}
