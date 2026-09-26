use crate::resource::ResourceFragment;
use std::collections::BTreeMap;
use std::sync::RwLock;

#[derive(Debug, Clone, PartialEq)]
pub struct ResourceOffer {
    pub participant_id: String,
    pub resources: ResourceFragment,
}

pub trait Fabric: Send + Sync {
    fn offers(&self) -> Vec<ResourceOffer>;
}

#[derive(Debug, Default)]
pub struct MemoryFabric {
    offers: RwLock<BTreeMap<String, ResourceOffer>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FabricError { InvalidParticipant, InvalidResource }

impl std::fmt::Display for FabricError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "{self:?}") }
}
impl std::error::Error for FabricError {}

impl MemoryFabric {
    pub fn new() -> Self { Self::default() }

    pub fn upsert(&self, offer: ResourceOffer) -> Result<(), FabricError> {
        if offer.participant_id.is_empty() { return Err(FabricError::InvalidParticipant); }
        offer.resources.validate().map_err(|_| FabricError::InvalidResource)?;
        self.offers.write().expect("fabric lock poisoned").insert(offer.participant_id.clone(), offer);
        Ok(())
    }

    pub fn remove(&self, participant_id: &str) {
        self.offers.write().expect("fabric lock poisoned").remove(participant_id);
    }
}

impl Fabric for MemoryFabric {
    fn offers(&self) -> Vec<ResourceOffer> {
        self.offers.read().expect("fabric lock poisoned").values().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resource::{CpuCapacity, ResourceFragment};

    #[test]
    fn upsert_and_remove() {
        let f = MemoryFabric::new();
        f.upsert(ResourceOffer {
            participant_id: "a".into(),
            resources: ResourceFragment { cpu: CpuCapacity { cores: 0.5 }, ..Default::default() },
        }).unwrap();
        assert_eq!(f.offers().len(), 1);
        f.remove("a");
        assert!(f.offers().is_empty());
    }
}
