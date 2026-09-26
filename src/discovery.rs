use crate::capability::Capability;
use crate::identity::{Identity, Kind};
use std::collections::BTreeMap;
use std::fmt;
use std::sync::RwLock;
use std::time::{Duration, SystemTime};

#[derive(Debug, Clone)]
pub struct Advertisement {
    pub participant: Identity,
    pub capability: Capability,
    pub sequence: u64,
    pub observed_at: SystemTime,
}

#[derive(Debug, Clone)]
pub struct Member {
    pub advertisement: Advertisement,
    pub last_seen: SystemTime,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum State {
    Active,
    Stale,
    Expired,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FreshnessPolicy {
    pub stale_after: Duration,
    pub expire_after: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiscoveryError {
    InvalidAdvertisement,
    InvalidTimestamp,
    InvalidPolicy,
    StaleSequence,
    NotFound,
}

impl fmt::Display for DiscoveryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for DiscoveryError {}

impl FreshnessPolicy {
    pub fn validate(&self) -> Result<(), DiscoveryError> {
        if self.stale_after.is_zero() || self.expire_after <= self.stale_after {
            return Err(DiscoveryError::InvalidPolicy);
        }
        Ok(())
    }
}

impl Advertisement {
    pub fn validate(&self) -> Result<(), DiscoveryError> {
        if self.participant.kind != Kind::Participant
            || self.participant.validate().is_err()
            || self.capability.validate().is_err()
            || self.observed_at == SystemTime::UNIX_EPOCH
        {
            return Err(DiscoveryError::InvalidAdvertisement);
        }
        Ok(())
    }
}

impl Member {
    pub fn state_at(
        &self,
        now: SystemTime,
        policy: FreshnessPolicy,
    ) -> Result<State, DiscoveryError> {
        policy.validate()?;
        if self.last_seen == SystemTime::UNIX_EPOCH || now < self.last_seen {
            return Err(DiscoveryError::InvalidTimestamp);
        }
        let age = now
            .duration_since(self.last_seen)
            .map_err(|_| DiscoveryError::InvalidTimestamp)?;
        if age >= policy.expire_after {
            Ok(State::Expired)
        } else if age >= policy.stale_after {
            Ok(State::Stale)
        } else {
            Ok(State::Active)
        }
    }
}

pub trait Registry: Send + Sync {
    fn upsert(
        &self,
        advertisement: Advertisement,
        received_at: SystemTime,
    ) -> Result<(), DiscoveryError>;
    fn remove(&self, id: &Identity) -> Result<(), DiscoveryError>;
    fn get(&self, id: &Identity) -> Result<Member, DiscoveryError>;
    fn members(&self) -> Vec<Member>;
    fn members_at(&self, now: SystemTime) -> Result<Vec<(Member, State)>, DiscoveryError>;
}

pub struct MemoryRegistry {
    members: RwLock<BTreeMap<String, Member>>,
    policy: FreshnessPolicy,
}

impl MemoryRegistry {
    pub fn new(policy: FreshnessPolicy) -> Result<Self, DiscoveryError> {
        policy.validate()?;
        Ok(Self {
            members: RwLock::new(BTreeMap::new()),
            policy,
        })
    }
}

impl Registry for MemoryRegistry {
    fn upsert(
        &self,
        advertisement: Advertisement,
        received_at: SystemTime,
    ) -> Result<(), DiscoveryError> {
        advertisement.validate()?;
        if received_at < advertisement.observed_at || received_at == SystemTime::UNIX_EPOCH {
            return Err(DiscoveryError::InvalidTimestamp);
        }

        let mut members = self.members.write().expect("discovery lock poisoned");
        if let Some(existing) = members.get(&advertisement.participant.id)
            && advertisement.sequence < existing.advertisement.sequence
        {
            return Err(DiscoveryError::StaleSequence);
        }

        members.insert(
            advertisement.participant.id.clone(),
            Member {
                advertisement,
                last_seen: received_at,
            },
        );
        Ok(())
    }

    fn remove(&self, id: &Identity) -> Result<(), DiscoveryError> {
        if id.kind != Kind::Participant || id.validate().is_err() {
            return Err(DiscoveryError::InvalidAdvertisement);
        }
        self.members
            .write()
            .expect("discovery lock poisoned")
            .remove(&id.id)
            .map(|_| ())
            .ok_or(DiscoveryError::NotFound)
    }

    fn get(&self, id: &Identity) -> Result<Member, DiscoveryError> {
        self.members
            .read()
            .expect("discovery lock poisoned")
            .get(&id.id)
            .cloned()
            .ok_or(DiscoveryError::NotFound)
    }

    fn members(&self) -> Vec<Member> {
        self.members
            .read()
            .expect("discovery lock poisoned")
            .values()
            .cloned()
            .collect()
    }

    fn members_at(&self, now: SystemTime) -> Result<Vec<(Member, State)>, DiscoveryError> {
        self.members()
            .into_iter()
            .map(|member| {
                let state = member.state_at(now, self.policy)?;
                Ok((member, state))
            })
            .collect()
    }
}
