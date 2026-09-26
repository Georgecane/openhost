use crate::identity::{Identity, Kind};
use crate::resource::ResourceFragment;
use std::time::{Duration, SystemTime};

#[derive(Debug, Clone, PartialEq)]
pub struct Lease {
    pub id: Identity,
    pub logical_node_id: Identity,
    pub participant_id: Identity,
    pub resources: ResourceFragment,
    pub created_at: SystemTime,
    pub expires_at: SystemTime,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaseError { InvalidLease, InvalidExpiry, InvalidLifetime, InvalidParticipant, InvalidNode }

impl std::fmt::Display for LeaseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "{self:?}") }
}
impl std::error::Error for LeaseError {}

impl Lease {
    pub fn new(id: Identity, logical_node_id: Identity, participant_id: Identity, resources: ResourceFragment,
        created_at: SystemTime, expires_at: SystemTime) -> Result<Self, LeaseError> {
        let lease = Self { id, logical_node_id, participant_id, resources, created_at, expires_at };
        lease.validate()?;
        Ok(lease)
    }

    pub fn validate(&self) -> Result<(), LeaseError> {
        if self.id.kind != Kind::Lease || self.id.validate().is_err() { return Err(LeaseError::InvalidLease); }
        if self.logical_node_id.kind != Kind::LogicalNode || self.logical_node_id.validate().is_err() { return Err(LeaseError::InvalidNode); }
        if self.participant_id.kind != Kind::Participant || self.participant_id.validate().is_err() { return Err(LeaseError::InvalidParticipant); }
        self.resources.validate_requirement().map_err(|_| LeaseError::InvalidLease)?;
        if self.created_at == SystemTime::UNIX_EPOCH || self.expires_at <= self.created_at { return Err(LeaseError::InvalidExpiry); }
        let duration = self.expires_at.duration_since(self.created_at).map_err(|_| LeaseError::InvalidExpiry)?;
        if self.resources.lifetime.is_some_and(|l| l < duration) { return Err(LeaseError::InvalidLifetime); }
        Ok(())
    }

    pub fn active_at(&self, now: SystemTime) -> bool { now >= self.created_at && now < self.expires_at }

    pub fn remaining_at(&self, now: SystemTime) -> Duration {
        self.expires_at.duration_since(now).unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ids() -> (Identity, Identity, Identity) {
        (Identity::new(Kind::Lease), Identity::new(Kind::LogicalNode), Identity::new(Kind::Participant))
    }

    #[test]
    fn creates_valid_lease() {
        let (lease, node, participant) = ids();
        let created = SystemTime::UNIX_EPOCH + Duration::from_secs(100);
        let got = Lease::new(lease, node, participant, ResourceFragment {
            cpu: crate::resource::CpuCapacity { cores: 2.0 }, ..Default::default()
        }, created, created + Duration::from_secs(3600)).unwrap();
        got.validate().unwrap();
    }

    #[test]
    fn enforces_lifetime() {
        let (lease, node, participant) = ids();
        let created = SystemTime::UNIX_EPOCH + Duration::from_secs(100);
        let err = Lease::new(lease, node, participant, ResourceFragment {
            cpu: crate::resource::CpuCapacity { cores: 1.0 },
            lifetime: Some(Duration::from_secs(1800)), ..Default::default()
        }, created, created + Duration::from_secs(3600)).unwrap_err();
        assert_eq!(err, LeaseError::InvalidLifetime);
    }
}
