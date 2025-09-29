use super::grpc_client::{ComponentId, ComponentType};
use std::{fmt::Display, str::FromStr};

impl ComponentId {
    pub fn as_type(&self) -> ComponentType {
        let ty = self
            .id
            .split_once(":")
            .expect("ComponentId must contain `:` infix")
            .0
            .to_uppercase();
        ComponentType::from_str_name(&ty)
            .unwrap_or_else(|| panic!("ComponentType must be found for {ty}"))
    }
}

impl Display for ComponentId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.id)
    }
}

impl FromStr for ComponentId {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(ComponentId { id: s.to_string() })
    }
}
