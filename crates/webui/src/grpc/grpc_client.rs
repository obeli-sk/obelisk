use std::{fmt::Display, hash::Hash, str::FromStr};

use implicit_clone::ImplicitClone;

tonic::include_proto!("obelisk");

impl ImplicitClone for FunctionDetail {}

impl Display for ComponentId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.id)
    }
}

impl Eq for ComponentId {}
impl Hash for ComponentId {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl ImplicitClone for ComponentId {}

impl FromStr for ComponentId {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(ComponentId { id: s.to_string() })
    }
}
