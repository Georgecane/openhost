use crate::discovery::{Advertisement, Member, Registry as DiscoveryRegistry};
use crate::identity::{Identity, Kind};
use crate::lease::{Lease, LeaseError};
use crate::node::LogicalNode;
use crate::participant::Participant;
use crate::registry::Registry;
use crate::resource::ResourceFragment;
use crate::scheduler::{Scheduler, SchedulerError};
use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlError { InvalidTime, InvalidLeaseDuration, LeaseNotFound, Scheduler(SchedulerError) }

impl std::fmt::Display for ControlError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "{self:?}") }
}
impl std::error::Error for ControlError {}

pub struct Plane {
    registry: Arc<Registry>,
    discovery: Arc<dyn DiscoveryRegistry>,
    scheduler: Arc<dyn Scheduler>,
    leases: RwLock<BTreeMap<String, Lease>>,
}

impl Plane {
    pub fn new(registry: Arc<Registry>, discovery: Arc<dyn DiscoveryRegistry>, scheduler: Arc<dyn Scheduler>) -> Self {
        Self { registry, discovery, scheduler, leases: RwLock::new(BTreeMap::new()) }
    }

    pub fn register_participant(&self, participant: Arc<Participant>) -> Result<(), crate::registry::RegistryError> {
        self.registry.register(participant)
    }

    pub fn announce_participant(&self, advertisement: Advertisement, observed_at: SystemTime) -> Result<(), crate::discovery::DiscoveryError> {
        self.discovery.upsert(advertisement, observed_at)
    }

    pub fn remove_discovered_participant(&self, id: &Identity) -> Result<(), crate::discovery::DiscoveryError> {
        self.discovery.remove(id)
    }

    pub fn discovered_participants(&self) -> Vec<Member> { self.discovery.members() }

    pub fn discovered_participants_at(&self, now: SystemTime) -> Result<Vec<Member>, crate::discovery::DiscoveryError> {
        self.discovery.members_at(now)
    }

    pub fn create_logical_node(&self, requirement: ResourceFragment) -> Result<LogicalNode, SchedulerError> {
        self.scheduler.plan(requirement)
    }

    pub fn create_leased_logical_node(&self, requirement: ResourceFragment, now: SystemTime, duration: Duration)
        -> Result<(LogicalNode, Vec<Lease>), ControlError> {
        if now == SystemTime::UNIX_EPOCH { return Err(ControlError::InvalidTime); }
        if duration.is_zero() { return Err(ControlError::InvalidLeaseDuration); }

        let node = self.scheduler.plan(requirement).map_err(ControlError::Scheduler)?;
        let node_id = Identity::parse(&node.id, Kind::LogicalNode).map_err(|_| ControlError::Scheduler(SchedulerError::InsufficientResources))?;
        let expires_at = now + duration;
        let mut leases = Vec::with_capacity(node.resources.allocations.len());

        for allocation in &node.resources.allocations {
            let participant_id = Identity::parse(&allocation.participant_id, Kind::Participant)
                .map_err(|_| ControlError::Scheduler(SchedulerError::InsufficientResources))?;
            let lease_id = Identity::new(Kind::Lease);
            let mut resources = allocation.resources;
            if resources.lifetime.is_none_or(|l| l > duration) { resources.lifetime = Some(duration); }
            let lease = Lease::new(lease_id, node_id.clone(), participant_id, resources, now, expires_at)
                .map_err(|_| ControlError::Scheduler(SchedulerError::InsufficientResources))?;
            leases.push(lease);
        }

        let mut stored = self.leases.write().expect("lease registry lock poisoned");
        for lease in &leases { stored.insert(lease.id.id.clone(), lease.clone()); }
        Ok((node, leases))
    }

    pub fn get_lease(&self, id: &Identity) -> Result<Lease, ControlError> {
        if id.kind != Kind::Lease || id.validate().is_err() { return Err(ControlError::LeaseNotFound); }
        self.leases.read().expect("lease registry lock poisoned").get(&id.id).cloned().ok_or(ControlError::LeaseNotFound)
    }
}
