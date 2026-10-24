use crate::fabric::{Fabric, ResourceOffer};
use crate::identity::{Identity, Kind};
use crate::node::{LogicalNode, RuntimeSpec};
use crate::resource::{
    Allocation, CompositeResource, Cpu, Gpu, Memory, Network, ResourceFragment, Storage,
};
use std::collections::HashSet;
use std::fmt;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchedulerError {
    InvalidRequirement,
    InsufficientResources,
    InvalidOffer,
}

impl fmt::Display for SchedulerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for SchedulerError {}

pub trait Scheduler: Send + Sync {
    fn plan(&self, requirement: ResourceFragment) -> Result<LogicalNode, SchedulerError>;
}

pub struct AggregatingScheduler {
    fabric: Arc<dyn Fabric>,
}

impl AggregatingScheduler {
    pub fn new(fabric: Arc<dyn Fabric>) -> Self {
        Self { fabric }
    }

    fn remaining(required: ResourceFragment, allocated: ResourceFragment) -> ResourceFragment {
        ResourceFragment {
            cpu: Cpu {
                cores: (required.cpu.cores - allocated.cpu.cores).max(0.0),
            },
            memory: Memory {
                bytes: required.memory.bytes.saturating_sub(allocated.memory.bytes),
            },
            storage: Storage {
                bytes: required
                    .storage
                    .bytes
                    .saturating_sub(allocated.storage.bytes),
            },
            network: Network {
                bits_per_second: required
                    .network
                    .bits_per_second
                    .saturating_sub(allocated.network.bits_per_second),
            },
            gpu: Gpu {
                units: required.gpu.units.saturating_sub(allocated.gpu.units),
            },
            lifetime: required.lifetime,
        }
    }

    fn allocation_share(
        available: ResourceFragment,
        remaining: ResourceFragment,
    ) -> Option<ResourceFragment> {
        if let (Some(a), Some(r)) = (available.lifetime, remaining.lifetime)
            && a < r
        {
            return None;
        }

        let share = ResourceFragment {
            cpu: Cpu {
                cores: available.cpu.cores.min(remaining.cpu.cores),
            },
            memory: Memory {
                bytes: available.memory.bytes.min(remaining.memory.bytes),
            },
            storage: Storage {
                bytes: available.storage.bytes.min(remaining.storage.bytes),
            },
            network: Network {
                bits_per_second: available
                    .network
                    .bits_per_second
                    .min(remaining.network.bits_per_second),
            },
            gpu: Gpu {
                units: available.gpu.units.min(remaining.gpu.units),
            },
            lifetime: remaining.lifetime,
        };

        (!share.is_empty()).then_some(share)
    }
}

impl Scheduler for AggregatingScheduler {
    fn plan(&self, requirement: ResourceFragment) -> Result<LogicalNode, SchedulerError> {
        requirement
            .validate_requirement()
            .map_err(|_| SchedulerError::InvalidRequirement)?;

        let mut offers: Vec<ResourceOffer> = self.fabric.offers();
        offers.sort_by(|a, b| a.participant_id.cmp(&b.participant_id));

        let mut seen_participants = HashSet::with_capacity(offers.len());
        let mut total = ResourceFragment::default();
        let mut allocations = Vec::new();

        for offer in offers {
            if offer.participant_id.is_empty()
                || offer.resources.validate().is_err()
                || !seen_participants.insert(offer.participant_id.clone())
            {
                // A participant must contribute at most once to a plan. Counting duplicate
                // advertisements would let one machine's resources be allocated repeatedly.
                return Err(SchedulerError::InvalidOffer);
            }

            let remaining = Self::remaining(requirement, total);
            let Some(share) = Self::allocation_share(offer.resources, remaining) else {
                continue;
            };

            total = total + share;
            allocations.push(Allocation {
                participant_id: offer.participant_id,
                resources: share,
            });

            if total.satisfies(&requirement) {
                break;
            }
        }

        if !total.satisfies(&requirement) {
            return Err(SchedulerError::InsufficientResources);
        }

        let resources =
            CompositeResource::compose(allocations).map_err(|_| SchedulerError::InvalidOffer)?;
        resources
            .validate()
            .map_err(|_| SchedulerError::InvalidOffer)?;

        let id = Identity::new(Kind::LogicalNode);
        Ok(LogicalNode {
            id: id.id,
            resources,
            runtime: RuntimeSpec::default(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fabric::ResourceOffer;

    struct StaticFabric(Vec<ResourceOffer>);

    impl Fabric for StaticFabric {
        fn offers(&self) -> Vec<ResourceOffer> {
            self.0.clone()
        }
    }

    fn offer(participant_id: &str, cores: f64) -> ResourceOffer {
        ResourceOffer {
            participant_id: participant_id.to_owned(),
            resources: ResourceFragment {
                cpu: Cpu { cores },
                ..Default::default()
            },
        }
    }

    #[test]
    fn rejects_duplicate_participant_offers_instead_of_double_counting_resources() {
        let fabric = Arc::new(StaticFabric(vec![
            offer("participant-a", 1.0),
            offer("participant-a", 1.0),
        ]));
        let scheduler = AggregatingScheduler::new(fabric);
        let requirement = ResourceFragment {
            cpu: Cpu { cores: 2.0 },
            ..Default::default()
        };

        assert_eq!(
            scheduler.plan(requirement).unwrap_err(),
            SchedulerError::InvalidOffer
        );
    }

    #[test]
    fn aggregates_distinct_participants_to_satisfy_requirement() {
        let fabric = Arc::new(StaticFabric(vec![
            offer("participant-a", 1.0),
            offer("participant-b", 1.0),
        ]));
        let scheduler = AggregatingScheduler::new(fabric);
        let requirement = ResourceFragment {
            cpu: Cpu { cores: 2.0 },
            ..Default::default()
        };

        let node = scheduler.plan(requirement).unwrap();
        assert_eq!(node.resources.allocations.len(), 2);
    }
}
