pub(crate) mod grpc_server;
pub(crate) mod web_api_server;

use concepts::prefixed_ulid::DeploymentId;
use concepts::storage::{DeploymentState, Pagination};

/// Determine if the current deployment should be added to the list of deployment states.
///
/// The current deployment is added when we're at the "newest" edge of the list:
/// - `OlderThan` with no cursor: user requests latest deployments
/// - `OlderThan` with cursor=current and `including_cursor=true`: user clicked Older from empty page
/// - `NewerThan` with fewer results than requested: reached the newest end
///   (but not if cursor is current deployment with `including_cursor=false`)
pub(crate) fn should_add_current_deployment(
    pagination: &Pagination<Option<DeploymentId>>,
    current_deployment_id: DeploymentId,
    states: &[DeploymentState],
) -> bool {
    let current_not_in_results =
        states.first().map(|dep| dep.deployment_id) != Some(current_deployment_id);

    match pagination {
        Pagination::OlderThan { cursor: None, .. } => current_not_in_results,
        Pagination::OlderThan {
            cursor: Some(cursor),
            including_cursor: true,
            ..
        } if *cursor == current_deployment_id => current_not_in_results,
        Pagination::NewerThan {
            length,
            cursor,
            including_cursor,
        } => {
            // Don't add if user explicitly excluded the current deployment via cursor
            let excluded_by_cursor = *cursor == Some(current_deployment_id) && !including_cursor;
            states.len() < usize::from(*length) && current_not_in_results && !excluded_by_cursor
        }
        Pagination::OlderThan { .. } => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn older_than_no_cursor_adds_current_if_missing() {
        let current = DeploymentId::generate();
        let other = DeploymentId::generate();
        let pagination = Pagination::OlderThan {
            length: 20,
            cursor: None,
            including_cursor: false,
        };

        // Current not in results -> should add
        let states = vec![DeploymentState::new(other)];
        assert!(should_add_current_deployment(&pagination, current, &states));

        // Current already first -> should not add
        let states = vec![DeploymentState::new(current)];
        assert!(!should_add_current_deployment(
            &pagination,
            current,
            &states
        ));

        // Empty results -> should add
        let states: Vec<DeploymentState> = vec![];
        assert!(should_add_current_deployment(&pagination, current, &states));
    }

    #[test]
    fn older_than_with_cursor_current_including_adds_if_missing() {
        let current = DeploymentId::generate();
        let other = DeploymentId::generate();

        // Cursor is current deployment, including_cursor=true
        let pagination = Pagination::OlderThan {
            length: 20,
            cursor: Some(current),
            including_cursor: true,
        };

        // Current not in results -> should add
        let states = vec![DeploymentState::new(other)];
        assert!(should_add_current_deployment(&pagination, current, &states));

        // Current already first -> should not add
        let states = vec![DeploymentState::new(current)];
        assert!(!should_add_current_deployment(
            &pagination,
            current,
            &states
        ));
    }

    #[test]
    fn older_than_with_cursor_current_excluding_does_not_add() {
        let current = DeploymentId::generate();

        // Cursor is current deployment, including_cursor=false
        let pagination = Pagination::OlderThan {
            length: 20,
            cursor: Some(current),
            including_cursor: false,
        };

        let states: Vec<DeploymentState> = vec![];
        assert!(!should_add_current_deployment(
            &pagination,
            current,
            &states
        ));
    }

    #[test]
    fn older_than_with_other_cursor_does_not_add() {
        let current = DeploymentId::generate();
        let other = DeploymentId::generate();

        let pagination = Pagination::OlderThan {
            length: 20,
            cursor: Some(other),
            including_cursor: true,
        };

        let states: Vec<DeploymentState> = vec![];
        assert!(!should_add_current_deployment(
            &pagination,
            current,
            &states
        ));
    }

    #[test]
    fn newer_than_fewer_results_adds_current_if_missing() {
        let current = DeploymentId::generate();
        let other = DeploymentId::generate();

        let pagination = Pagination::NewerThan {
            length: 20,
            cursor: Some(other),
            including_cursor: false,
        };

        // Fewer results than length, current not in results -> should add
        let states = vec![DeploymentState::new(other)];
        assert!(should_add_current_deployment(&pagination, current, &states));

        // Empty results -> should add
        let states: Vec<DeploymentState> = vec![];
        assert!(should_add_current_deployment(&pagination, current, &states));
    }

    #[test]
    fn newer_than_full_results_does_not_add() {
        let current = DeploymentId::generate();

        let pagination = Pagination::NewerThan {
            length: 2,
            cursor: None,
            including_cursor: false,
        };

        // Results >= length -> should not add
        let states = vec![
            DeploymentState::new(DeploymentId::generate()),
            DeploymentState::new(DeploymentId::generate()),
        ];
        assert!(!should_add_current_deployment(
            &pagination,
            current,
            &states
        ));
    }

    #[test]
    fn newer_than_cursor_current_excluding_does_not_add() {
        let current = DeploymentId::generate();

        // User clicked Newer with cursor=current, excluding cursor
        let pagination = Pagination::NewerThan {
            length: 20,
            cursor: Some(current),
            including_cursor: false,
        };

        // Even with empty results, should not add (user explicitly excluded)
        let states: Vec<DeploymentState> = vec![];
        assert!(!should_add_current_deployment(
            &pagination,
            current,
            &states
        ));
    }

    #[test]
    fn newer_than_cursor_current_including_adds_if_missing() {
        let current = DeploymentId::generate();

        let pagination = Pagination::NewerThan {
            length: 20,
            cursor: Some(current),
            including_cursor: true,
        };

        // Empty results, including cursor -> should add
        let states: Vec<DeploymentState> = vec![];
        assert!(should_add_current_deployment(&pagination, current, &states));
    }

    #[test]
    fn does_not_add_if_current_already_first() {
        let current = DeploymentId::generate();

        let states = vec![DeploymentState::new(current)];

        // OlderThan no cursor
        let pagination = Pagination::OlderThan {
            length: 20,
            cursor: None,
            including_cursor: false,
        };
        assert!(!should_add_current_deployment(
            &pagination,
            current,
            &states
        ));

        // NewerThan fewer results
        let pagination = Pagination::NewerThan {
            length: 20,
            cursor: None,
            including_cursor: false,
        };
        assert!(!should_add_current_deployment(
            &pagination,
            current,
            &states
        ));
    }
}
