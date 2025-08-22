use std::fmt::Display;

use super::grpc_client;

const JOIN_SET_ID_INFIX: char = ':';

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
