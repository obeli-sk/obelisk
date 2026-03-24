use crate::webhook::webhook_trigger::WebhookServerState;
use std::sync::Arc;
use tokio::sync::watch;

type WebhookStateSender = watch::Sender<Arc<WebhookServerState>>;
pub type WebhookStateWatcher = watch::Receiver<Arc<WebhookServerState>>;

pub struct WebhookRegistry {
    /// Maps `http_server` name → (webhook server state sender, watcher)
    servers_to_state_channels:
        hashbrown::HashMap<String, (WebhookStateSender, WebhookStateWatcher)>,
}

impl WebhookRegistry {
    /// Returns the registry, per-server webhook state watchers) pair.
    /// Important: Http servers with no routes must be present with state set to `None` in
    /// order to accept routes in later deployments.
    pub fn new(
        http_servers_to_webhooks_and_state: impl IntoIterator<Item = (String, Arc<WebhookServerState>)>,
    ) -> WebhookRegistry {
        let servers_to_state_channels = http_servers_to_webhooks_and_state
            .into_iter()
            .map(|(name, state)| {
                let (tx, watcher) = watch::channel(state);
                (name, (tx, watcher))
            })
            .collect();
        WebhookRegistry {
            servers_to_state_channels,
        }
    }

    #[must_use]
    pub fn get_watcher(&self, name: &str) -> Option<WebhookStateWatcher> {
        self.servers_to_state_channels
            .get(name)
            .map(|(_sender, watcher)| watcher.clone())
    }

    /// Pushes new state to each http server's channel.
    /// Important: Parameter `new_http_servers_to_webhooks_and_state` must contain exactly the same http servers as those passed to the constructor.
    pub fn swap(
        &self,
        new_http_servers_to_webhooks_and_state: impl IntoIterator<
            Item = (String, Arc<WebhookServerState>),
        >,
    ) {
        let mut new_servers_to_states: hashbrown::HashMap<String, Arc<WebhookServerState>> =
            new_http_servers_to_webhooks_and_state.into_iter().collect();
        for (current_server, (tx, _watcher)) in &self.servers_to_state_channels {
            let new_state = new_servers_to_states
                .remove(current_server)
                .unwrap_or_else(|| panic!("server `{current_server}` missing from new state"));
            // Watchers may have been dropped (server loop exited); ignore error.
            let _ = tx.send(new_state);
        }
        assert!(
            new_servers_to_states.is_empty(),
            "new state contains unknown servers: {:?}",
            new_servers_to_states.keys().collect::<Vec<_>>()
        );
    }
}
