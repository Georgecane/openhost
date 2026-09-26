use crate::fabric::{Fabric, ResourceOffer};
use crate::identity::{Identity, Kind};
use crate::participant::{Participant, State};
use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, RwLock};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegistryError {
    Exists,
    NotFound,
    Invalid,
}

impl fmt::Display for RegistryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for RegistryError {}

#[derive(Default)]
pub struct Registry {
    participants: RwLock<BTreeMap<String, Arc<Participant>>>,
}

impl Registry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&self, participant: Arc<Participant>) -> Result<(), RegistryError> {
        let id = participant.snapshot().id;
        if id.kind != Kind::Participant || id.validate().is_err() {
            return Err(RegistryError::Invalid);
        }
        let mut participants = self.participants.write().expect("registry lock poisoned");
        if participants.contains_key(&id.id) {
            return Err(RegistryError::Exists);
        }
        participants.insert(id.id, participant);
        Ok(())
    }

    pub fn get(&self, id: &Identity) -> Result<Arc<Participant>, RegistryError> {
        if id.kind != Kind::Participant || id.validate().is_err() {
            return Err(RegistryError::Invalid);
        }
        self.participants
            .read()
            .expect("registry lock poisoned")
            .get(&id.id)
            .cloned()
            .ok_or(RegistryError::NotFound)
    }

    pub fn remove(&self, id: &Identity) -> Result<(), RegistryError> {
        if id.kind != Kind::Participant || id.validate().is_err() {
            return Err(RegistryError::Invalid);
        }
        self.participants
            .write()
            .expect("registry lock poisoned")
            .remove(&id.id)
            .map(|_| ())
            .ok_or(RegistryError::NotFound)
    }
}

impl Fabric for Registry {
    fn offers(&self) -> Vec<ResourceOffer> {
        self.participants
            .read()
            .expect("registry lock poisoned")
            .values()
            .filter_map(|participant| {
                let snapshot = participant.snapshot();
                if snapshot.state != State::Active {
                    return None;
                }
                participant.offer().ok().map(|resources| ResourceOffer {
                    participant_id: snapshot.id.id,
                    resources,
                })
            })
            .collect()
    }
}
