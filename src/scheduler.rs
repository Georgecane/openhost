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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SchedulingStrategy {
    /// Sort participant IDs lexicographically (deterministic order).
    #[default]
    DeterministicId,
    /// Prefer a single full-fit offer with the least normalized excess capacity; otherwise
    /// prefer balanced coverage across the requested resource dimensions.
    MinFragmentation,
}

pub trait Scheduler: Send + Sync {
    fn plan(&self, requirement: ResourceFragment) -> Result<LogicalNode, SchedulerError>;
}

pub struct AggregatingScheduler {
    fabric: Arc<dyn Fabric>,
    strategy: SchedulingStrategy,
}

impl AggregatingScheduler {
    pub fn new(fabric: Arc<dyn Fabric>) -> Self {
        Self {
            fabric,
            strategy: SchedulingStrategy::DeterministicId,
        }
    }

    pub fn with_strategy(fabric: Arc<dyn Fabric>, strategy: SchedulingStrategy) -> Self {
        Self { fabric, strategy }
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

    /// Rank a candidate by whether it can satisfy the full requirement, then by
    /// normalized waste or balanced coverage across all requested resource dimensions.
    fn fragmentation_score(
        available: ResourceFragment,
        required: ResourceFragment,
    ) -> (bool, f64, f64) {
        let mut ratios = Vec::with_capacity(5);
        if required.cpu.cores > 0.0 {
            ratios.push(available.cpu.cores / required.cpu.cores);
        }
        if required.memory.bytes > 0 {
            ratios.push(available.memory.bytes as f64 / required.memory.bytes as f64);
        }
        if required.storage.bytes > 0 {
            ratios.push(available.storage.bytes as f64 / required.storage.bytes as f64);
        }
        if required.network.bits_per_second > 0 {
            ratios.push(
                available.network.bits_per_second as f64
                    / required.network.bits_per_second as f64,
            );
        }
        if required.gpu.units > 0 {
            ratios.push(available.gpu.units as f64 / required.gpu.units as f64);
        }

        let waste = ratios.iter().map(|ratio| (ratio - 1.0).max(0.0)).sum();
        let coverage = ratios.iter().map(|ratio| ratio.min(1.0)).sum();
        (available.satisfies(&required), waste, coverage)
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

        let lifetime = match (available.lifetime, remaining.lifetime) {
            (Some(available), Some(required)) => Some(required.min(available)),
            (Some(available), None) => Some(available),
            (None, required) => required,
        };

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
            lifetime,
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

        // Validate every offer before strategy-specific sorting. In particular, a NaN CPU
        // value would make the floating-point comparator non-total if it reached sort_by.
        let mut seen_participants = HashSet::with_capacity(offers.len());
        for offer in &offers {
            if Identity::parse(&offer.participant_id, Kind::Participant).is_err()
                || offer.resources.validate().is_err()
                || !seen_participants.insert(offer.participant_id.clone())
            {
                return Err(SchedulerError::InvalidOffer);
            }
        }

        match self.strategy {
            SchedulingStrategy::DeterministicId => {
                offers.sort_by(|a, b| a.participant_id.cmp(&b.participant_id));
            }
            SchedulingStrategy::MinFragmentation => {
                offers.sort_by(|a, b| {
                    let a_score = Self::fragmentation_score(a.resources, requirement);
                    let b_score = Self::fragmentation_score(b.resources, requirement);

                    match (a_score.0, b_score.0) {
                        (true, true) => a_score
                            .1
                            .partial_cmp(&b_score.1)
                            .unwrap_or(std::cmp::Ordering::Equal),
                        (true, false) => std::cmp::Ordering::Less,
                        (false, true) => std::cmp::Ordering::Greater,
                        (false, false) => b_score
                            .2
                            .partial_cmp(&a_score.2)
                            .unwrap_or(std::cmp::Ordering::Equal),
                    }
                    .then_with(|| a.participant_id.cmp(&b.participant_id))
                });
            }
        }

        let mut total = ResourceFragment::default();
        let mut allocations = Vec::new();

        for offer in offers {
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
    use crate::fabric::MemoryFabric;

    #[test]
    fn scheduler_rejects_empty_requirement() {
        let fabric = Arc::new(MemoryFabric::new());
        let scheduler = AggregatingScheduler::new(fabric);
        assert_eq!(
            scheduler.plan(ResourceFragment::default()).unwrap_err(),
            SchedulerError::InvalidRequirement
        );
    }

    #[test]
    fn scheduler_rejects_insufficient_resources() {
        let fabric = Arc::new(MemoryFabric::new());
        let scheduler = AggregatingScheduler::new(fabric);
        let req = ResourceFragment {
            cpu: Cpu { cores: 2.0 },
            ..Default::default()
        };
        assert_eq!(
            scheduler.plan(req).unwrap_err(),
            SchedulerError::InsufficientResources
        );
    }

    #[test]
    fn scheduler_plans_with_deterministic_strategy() {
        let fabric = Arc::new(MemoryFabric::new());
        let p1 = Identity::new(Kind::Participant);
        let p2 = Identity::new(Kind::Participant);

        fabric
            .upsert(ResourceOffer {
                participant_id: p1.id.clone(),
                resources: ResourceFragment {
                    cpu: Cpu { cores: 1.0 },
                    memory: Memory { bytes: 512 << 20 },
                    ..Default::default()
                },
            })
            .unwrap();

        fabric
            .upsert(ResourceOffer {
                participant_id: p2.id.clone(),
                resources: ResourceFragment {
                    cpu: Cpu { cores: 1.0 },
                    memory: Memory { bytes: 512 << 20 },
                    ..Default::default()
                },
            })
            .unwrap();

        let scheduler = AggregatingScheduler::new(fabric);
        let req = ResourceFragment {
            cpu: Cpu { cores: 1.5 },
            memory: Memory { bytes: 768 << 20 },
            ..Default::default()
        };

        let node = scheduler.plan(req).expect("plan succeeds");
        assert_eq!(node.resources.allocations.len(), 2);
        assert!(node.resources.capacity.satisfies(&req));
    }

    #[test]
    fn scheduler_preserves_finite_lifetime_for_unbounded_requests() {
        let fabric = Arc::new(MemoryFabric::new());
        let participant = Identity::new(Kind::Participant);
        let lifetime = std::time::Duration::from_secs(45);
        fabric
            .upsert(ResourceOffer {
                participant_id: participant.id.clone(),
                resources: ResourceFragment {
                    cpu: Cpu { cores: 1.0 },
                    lifetime: Some(lifetime),
                    ..Default::default()
                },
            })
            .unwrap();

        let scheduler = AggregatingScheduler::new(fabric);
        let node = scheduler
            .plan(ResourceFragment {
                cpu: Cpu { cores: 1.0 },
                lifetime: None,
                ..Default::default()
            })
            .unwrap();

        assert_eq!(node.resources.allocations[0].resources.lifetime, Some(lifetime));
        assert_eq!(node.resources.capacity.lifetime, Some(lifetime));
    }

    #[test]
    fn scheduler_uses_an_offer_that_fits_all_dimensions_before_larger_cpu_only_offers() {
        let fabric = Arc::new(MemoryFabric::new());
        let cpu_heavy = Identity::new(Kind::Participant);
        let balanced = Identity::new(Kind::Participant);

        fabric
            .upsert(ResourceOffer {
                participant_id: cpu_heavy.id.clone(),
                resources: ResourceFragment {
                    cpu: Cpu { cores: 8.0 },
                    memory: Memory { bytes: 128 << 20 },
                    ..Default::default()
                },
            })
            .unwrap();
        fabric
            .upsert(ResourceOffer {
                participant_id: balanced.id.clone(),
                resources: ResourceFragment {
                    cpu: Cpu { cores: 2.5 },
                    memory: Memory { bytes: 2048 << 20 },
                    ..Default::default()
                },
            })
            .unwrap();

        let scheduler = AggregatingScheduler::with_strategy(
            fabric,
            SchedulingStrategy::MinFragmentation,
        );
        let requirement = ResourceFragment {
            cpu: Cpu { cores: 2.0 },
            memory: Memory { bytes: 1024 << 20 },
            ..Default::default()
        };
        let node = scheduler.plan(requirement).unwrap();

        assert_eq!(node.resources.allocations.len(), 1);
        assert_eq!(node.resources.allocations[0].participant_id, balanced.id);
    }

    #[test]
    fn scheduler_min_fragmentation_strategy_prefers_largest_offers() {
        let fabric = Arc::new(MemoryFabric::new());
        let small_p = Identity::new(Kind::Participant);
        let large_p = Identity::new(Kind::Participant);

        // small participant has 0.5 cores
        fabric
            .upsert(ResourceOffer {
                participant_id: small_p.id.clone(),
                resources: ResourceFragment {
                    cpu: Cpu { cores: 0.5 },
                    memory: Memory { bytes: 256 << 20 },
                    ..Default::default()
                },
            })
            .unwrap();

        // large participant has 4.0 cores
        fabric
            .upsert(ResourceOffer {
                participant_id: large_p.id.clone(),
                resources: ResourceFragment {
                    cpu: Cpu { cores: 4.0 },
                    memory: Memory { bytes: 2048 << 20 },
                    ..Default::default()
                },
            })
            .unwrap();

        // When requesting 2.0 cores:
        // MinFragmentation should pick the large participant first and satisfy it with 1 allocation!
        let scheduler =
            AggregatingScheduler::with_strategy(fabric, SchedulingStrategy::MinFragmentation);

        let req = ResourceFragment {
            cpu: Cpu { cores: 2.0 },
            memory: Memory { bytes: 1024 << 20 },
            ..Default::default()
        };

        let node = scheduler.plan(req).expect("plan succeeds");
        assert_eq!(node.resources.allocations.len(), 1);
        assert_eq!(node.resources.allocations[0].participant_id, large_p.id);
        assert_eq!(node.resources.allocations[0].resources.cpu.cores, 2.0);
    } 
    #[test]
    fn min_fragmentation_rejects_nan_offers_before_sorting() {
        struct StaticFabric(Vec<ResourceOffer>);

        impl crate::fabric::Fabric for StaticFabric {
            fn offers(&self) -> Vec<ResourceOffer> {
                self.0.clone()
            }
        }

        let fabric = Arc::new(StaticFabric(vec![ResourceOffer {
            participant_id: Identity::new(Kind::Participant).id,
            resources: ResourceFragment {
                cpu: Cpu { cores: f64::NAN },
                ..Default::default()
            },
        }]));
        let scheduler =
            AggregatingScheduler::with_strategy(fabric, SchedulingStrategy::MinFragmentation);
        let requirement = ResourceFragment {
            cpu: Cpu { cores: 1.0 },
            ..Default::default()
        };

        assert_eq!(
            scheduler.plan(requirement).unwrap_err(),
            SchedulerError::InvalidOffer
        );
    }

}
