use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Kind { Participant, LogicalNode, Lease }

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Identity { pub id: String, pub kind: Kind }

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdentityError { Empty, Invalid }

impl std::fmt::Display for IdentityError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "{self:?}") }
}
impl std::error::Error for IdentityError {}

impl Identity {
    pub fn new(kind: Kind) -> Self { Self { id: Uuid::new_v4().to_string(), kind } }

    pub fn parse(id: &str, kind: Kind) -> Result<Self, IdentityError> {
        if id.is_empty() { return Err(IdentityError::Empty); }
        let uuid = Uuid::parse_str(id).map_err(|_| IdentityError::Invalid)?;
        if uuid.to_string() != id || uuid.get_version_num() != 4 { return Err(IdentityError::Invalid); }
        Ok(Self { id: id.to_owned(), kind })
    }

    pub fn validate(&self) -> Result<(), IdentityError> { Self::parse(&self.id, self.kind).map(|_| ()) }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn creates_distinct_v4_identities() {
        let a = Identity::new(Kind::Participant);
        let b = Identity::new(Kind::Participant);
        assert_ne!(a.id, b.id);
        a.validate().unwrap();
        b.validate().unwrap();
    }

    #[test]
    fn parse_preserves_identity() {
        let id = "550e8400-e29b-41d4-a716-446655440000";
        let parsed = Identity::parse(id, Kind::LogicalNode).unwrap();
        assert_eq!(parsed.id, id);
        assert_eq!(parsed.kind, Kind::LogicalNode);
    }

    #[test]
    fn rejects_malformed_identity() {
        assert_eq!(Identity::parse("", Kind::Participant).unwrap_err(), IdentityError::Empty);
        assert_eq!(Identity::parse("not-a-uuid", Kind::Participant).unwrap_err(), IdentityError::Invalid);
        assert_eq!(Identity::parse("550e8400-e29b-11d4-a716-446655440000", Kind::Participant).unwrap_err(), IdentityError::Invalid);
    }
}
