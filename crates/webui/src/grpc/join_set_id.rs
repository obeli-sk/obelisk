use std::hash::Hash;

use super::grpc_client;

impl Hash for grpc_client::JoinSetId {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.execution_id
            .as_ref()
            .inspect(|id| id.to_string().hash(state));
        self.name.hash(state);
    }
}

impl Eq for grpc_client::JoinSetId {}
