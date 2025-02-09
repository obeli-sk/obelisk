use std::hash::Hash;

use super::grpc_client;

impl Hash for grpc_client::JoinSetId {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.kind.hash(state);
        self.name.hash(state);
    }
}

impl Eq for grpc_client::JoinSetId {}
