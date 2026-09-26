use crate::fabric::{Fabric, ResourceOffer};
use crate::identity::{Identity, Kind};
use crate::participant::{Participant, ParticipantError, State};
use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegistryError { Exists, NotFound, Invalid }

impl std::fmt::Display for RegistryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "{self:?}") }
}
impl std::error::Error for RegistryError {}

#[derive(Default)]
pub struct Registry {
    participants: RwLock<BTreeMap<String, Arc<Participant>>>,
}

impl Registry {
    pub fn new() -> Self { Self::default() }

    pub fn register(&self, participant: Arc<Participant>) -> Result<(), RegistryError> {
        let id = participant.snapshot().id;
        if id.kind != Kind::Participant || id.validate().is_err() { return Err(RegistryError::Invalid); }
        let mut map = self.participants.write().expect("registry lock poisoned");
        if map.contains_key(&id.id) { return Err(RegistryError::Exists); }
        map.insert(id.id, participant);
        Ok(())
    }

    pub fn remove(&self, id: &Identity) -> Result<(), RegistryError> {
        if id.kind != Kind::Participant || id.validate().is_err() { return Err(RegistryError::Invalid); }
        if self.participants.write().expect("registry lock poisoned").remove(&id.id).is_none() {
            return Err(RegistryError::NotFound);
        }
        Ok(())
    }

    pub fn get(&self, id: &Identity) -> Result<Arc<Participant>, RegistryError> {
        if id.kind != Kind::Participant || id.validate().is_err() { return Err(RegistryError::Invalid); }
        self.participants.read().expect("registry lock poisoned").get(&id.id).cloned().ok_or(RegistryError::NotFound)
    }
}

impl Fabric for Registry {
    fn offers(&self) -> Vec<ResourceOffer> {
        self.participants.read().expect("registry lock poisoned").values().filter_map(|p| {
            match (p.snapshot().state, p.offer()) {
                (State::Active, Ok(resources)) => Some(ResourceOffer { participant_id: p.snapshot().id.id, resources }),
                _ => None,
            }
        }).collect()
    }
}

impl From<ParticipantError> for RegistryError {
    fn from(_: ParticipantError) -> Self { RegistryError::Invalid }
}
