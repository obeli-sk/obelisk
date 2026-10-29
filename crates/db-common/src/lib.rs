//! Common types and utilities shared between database implementations.

mod combined_state;
mod join_set_open_tracker;
mod notifiers;
mod state_filter;
mod subscribers;
mod system_events;

pub use combined_state::{CombinedState, CombinedStateDTO};
pub use join_set_open_tracker::{
    JoinSetCancellable, JoinSetCloseCancellations, JoinSetMember, JoinSetOpenTracker,
    JoinSetOpenTrackerError, JoinSetResponseId,
};
pub use notifiers::{AppendNotifier, NotifierExecutionFinished, NotifierPendingAt};
pub use state_filter::{state_filter_to_sql, state_filters_now};
pub use subscribers::PendingFfqnSubscribersHolder;

pub use system_events::system_event_list_sql;

/// Exclusive bytewise upper bound for an execution and its dot-delimited descendants.
#[must_use]
pub fn execution_tree_upper_bound(execution_id: &str) -> String {
    format!("{execution_id}/")
}

/// Exclusive bytewise upper bound for a literal UTF-8 prefix, if one exists.
#[must_use]
pub fn text_prefix_upper_bound(prefix: &str) -> Option<String> {
    prefix.char_indices().rev().find_map(|(offset, ch)| {
        let next = if ch == '\u{d7ff}' {
            Some('\u{e000}')
        } else {
            char::from_u32(u32::from(ch) + 1)
        };
        next.map(|next| format!("{}{next}", &prefix[..offset]))
    })
}
