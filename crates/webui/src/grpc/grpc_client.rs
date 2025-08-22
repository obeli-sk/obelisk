use std::{fmt::Display, hash::Hash, str::FromStr};

tonic::include_proto!("obelisk");

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
