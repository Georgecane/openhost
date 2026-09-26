use std::fmt;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Kind {
    Participant,
    LogicalNode,
    Lease,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Identity {
    pub id: String,
    pub kind: Kind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdentityError {
    Empty,
    InvalidUuid,
    NonCanonical,
    WrongVersion,
}

impl fmt::Display for IdentityError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for IdentityError {}

impl Identity {
    pub fn new(kind: Kind) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            kind,
        }
    }

    pub fn parse(id: &str, kind: Kind) -> Result<Self, IdentityError> {
        if id.is_empty() {
            return Err(IdentityError::Empty);
        }
        let uuid = Uuid::parse_str(id).map_err(|_| IdentityError::InvalidUuid)?;
        if uuid.to_string() != id {
            return Err(IdentityError::NonCanonical);
        }
        if uuid.get_version_num() != 4 {
            return Err(IdentityError::WrongVersion);
        }
        Ok(Self {
            id: id.to_owned(),
            kind,
        })
    }

    pub fn validate(&self) -> Result<(), IdentityError> {
        Self::parse(&self.id, self.kind).map(|_| ())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn creates_v4_identity() {
        let identity = Identity::new(Kind::Participant);
        assert_eq!(identity.id.len(), 36);
        identity.validate().unwrap();
    }

    #[test]
    fn rejects_non_canonical_uuid() {
        let upper = "550E8400-E29B-41D4-A716-446655440000";
        assert_eq!(
            Identity::parse(upper, Kind::Participant).unwrap_err(),
            IdentityError::NonCanonical
        );
    }

    #[test]
    fn preserves_kind() {
        let id = "550e8400-e29b-41d4-a716-446655440000";
        let identity = Identity::parse(id, Kind::LogicalNode).unwrap();
        assert_eq!(identity.kind, Kind::LogicalNode);
    }
}
