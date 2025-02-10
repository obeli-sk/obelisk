use std::{fmt::Display, hash::Hash};

use super::grpc_client;

const JOIN_SET_ID_INFIX: char = ':';

impl Hash for grpc_client::JoinSetId {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.kind.hash(state);
        self.name.hash(state);
    }
}

impl Eq for grpc_client::JoinSetId {}

impl Display for grpc_client::JoinSetId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let code = match self.kind() {
            grpc_client::join_set_id::JoinSetKind::OneOff => "o",
            grpc_client::join_set_id::JoinSetKind::Named => "n",
            grpc_client::join_set_id::JoinSetKind::Generated => "g",
        };
        write!(f, "{code}{JOIN_SET_ID_INFIX}{}", self.name)
    }
}
