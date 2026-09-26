use crate::fabric::{Fabric, ResourceOffer};
use crate::identity::{Identity, Kind};
use crate::node::LogicalNode;
use crate::resource::{Allocation, CompositeResource, CpuCapacity, GpuCapacity, MemoryCapacity, NetworkCapacity, ResourceFragment, StorageCapacity};
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchedulerError { InsufficientResources, NilFabric }

impl std::fmt::Display for SchedulerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "{self:?}") }
}
impl std::error::Error for SchedulerError {}

pub trait Scheduler: Send + Sync {
    fn plan(&self, requirement: ResourceFragment) -> Result<LogicalNode, SchedulerError>;
}

pub struct AggregatingScheduler {
    fabric: Arc<dyn Fabric>,
}

impl AggregatingScheduler {
    pub fn new(fabric: Arc<dyn Fabric>) -> Self { Self { fabric } }

    fn remaining(requirement: ResourceFragment, allocated: ResourceFragment) -> ResourceFragment {
        ResourceFragment {
            cpu: CpuCapacity { cores: (requirement.cpu.cores - allocated.cpu.cores).max(0.0) },
            memory: MemoryCapacity { bytes: requirement.memory.bytes.saturating_sub(allocated.memory.bytes) },
            storage: StorageCapacity { bytes: requirement.storage.bytes.saturating_sub(allocated.storage.bytes) },
            network: NetworkCapacity { bits_per_second: requirement.network.bits_per_second.saturating_sub(allocated.network.bits_per_second) },
            gpu: GpuCapacity { units: requirement.gpu.units.saturating_sub(allocated.gpu.units) },
            lifetime: requirement.lifetime,
        }
    }

    fn min_fragment(available: ResourceFragment, required: ResourceFragment) -> ResourceFragment {
        if let (Some(a), Some(r)) = (available.lifetime, required.lifetime) {
            if a < r { return ResourceFragment::default(); }
        }
        let lifetime = match (available.lifetime, required.lifetime) {
            (Some(a), Some(r)) => Some(a.min(r)),
            (Some(a), None) => Some(a),
            (None, Some(r)) => Some(r),
            (None, None) => None,
        };
        ResourceFragment {
            cpu: CpuCapacity { cores: available.cpu.cores.min(required.cpu.cores) },
            memory: MemoryCapacity { bytes: available.memory.bytes.min(required.memory.bytes) },
            storage: StorageCapacity { bytes: available.storage.bytes.min(required.storage.bytes) },
            network: NetworkCapacity { bits_per_second: available.network.bits_per_second.min(required.network.bits_per_second) },
            gpu: GpuCapacity { units: available.gpu.units.min(required.gpu.units) },
            lifetime,
        }
    }
}

impl Scheduler for AggregatingScheduler {
    fn plan(&self, requirement: ResourceFragment) -> Result<LogicalNode, SchedulerError> {
        requirement.validate_requirement().map_err(|_| SchedulerError::InsufficientResources)?;
        let mut offers: Vec<ResourceOffer> = self.fabric.offers();
        offers.sort_by(|a, b| a.participant_id.cmp(&b.participant_id));

        let mut allocations = Vec::new();
        let mut total = ResourceFragment::default();

        for offer in offers {
            if offer.participant_id.is_empty() || offer.resources.is_empty() { continue; }
            let share = Self::min_fragment(offer.resources, Self::remaining(requirement, total));
            if share.is_empty() { continue; }
            allocations.push(Allocation { participant_id: offer.participant_id, resources: share });
            total = total.add(share);
            if total.satisfies(&requirement) { break; }
        }

        if !total.satisfies(&requirement) { return Err(SchedulerError::InsufficientResources); }
        let composed = CompositeResource::compose(allocations).map_err(|_| SchedulerError::InsufficientResources)?;
        if !composed.satisfies(&requirement) { return Err(SchedulerError::InsufficientResources); }

        let id = Identity::new(Kind::LogicalNode);
        Ok(LogicalNode { id: id.id, resources: composed, runtime: Default::default() })
    }
}
