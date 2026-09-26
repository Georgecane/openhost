use std::collections::HashSet;
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CpuCapacity { pub cores: f64 }
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MemoryCapacity { pub bytes: u64 }
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StorageCapacity { pub bytes: u64 }
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NetworkCapacity { pub bits_per_second: u64 }
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GpuCapacity { pub units: u32 }

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ResourceFragment {
    pub cpu: CpuCapacity,
    pub memory: MemoryCapacity,
    pub storage: StorageCapacity,
    pub network: NetworkCapacity,
    pub gpu: GpuCapacity,
    pub lifetime: Option<Duration>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Allocation {
    pub participant_id: String,
    pub resources: ResourceFragment,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CompositeResource {
    pub capacity: ResourceFragment,
    pub allocations: Vec<Allocation>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResourceError {
    NegativeCapacity,
    EmptyRequirement,
    EmptyComposite,
    DuplicateAllocation,
    InvalidAllocation,
    CapacityMismatch,
}

impl std::fmt::Display for ResourceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "{self:?}") }
}
impl std::error::Error for ResourceError {}

impl Default for ResourceFragment {
    fn default() -> Self {
        Self {
            cpu: CpuCapacity { cores: 0.0 },
            memory: MemoryCapacity { bytes: 0 },
            storage: StorageCapacity { bytes: 0 },
            network: NetworkCapacity { bits_per_second: 0 },
            gpu: GpuCapacity { units: 0 },
            lifetime: None,
        }
    }
}

impl ResourceFragment {
    pub fn validate(&self) -> Result<(), ResourceError> {
        if self.cpu.cores < 0.0 || self.cpu.cores.is_nan() { return Err(ResourceError::NegativeCapacity); }
        Ok(())
    }

    pub fn is_empty(&self) -> bool {
        self.cpu.cores == 0.0 && self.memory.bytes == 0 && self.storage.bytes == 0
            && self.network.bits_per_second == 0 && self.gpu.units == 0
    }

    fn combine_lifetime(a: Option<Duration>, b: Option<Duration>) -> Option<Duration> {
        match (a, b) {
            (None, other) | (other, None) => other,
            (Some(a), Some(b)) => Some(a.min(b)),
        }
    }

    pub fn add(self, other: Self) -> Self {
        Self {
            cpu: CpuCapacity { cores: self.cpu.cores + other.cpu.cores },
            memory: MemoryCapacity { bytes: self.memory.bytes.saturating_add(other.memory.bytes) },
            storage: StorageCapacity { bytes: self.storage.bytes.saturating_add(other.storage.bytes) },
            network: NetworkCapacity { bits_per_second: self.network.bits_per_second.saturating_add(other.network.bits_per_second) },
            gpu: GpuCapacity { units: self.gpu.units.saturating_add(other.gpu.units) },
            lifetime: Self::combine_lifetime(self.lifetime, other.lifetime),
        }
    }

    pub fn satisfies(&self, request: &Self) -> bool {
        self.cpu.cores >= request.cpu.cores
            && self.memory.bytes >= request.memory.bytes
            && self.storage.bytes >= request.storage.bytes
            && self.network.bits_per_second >= request.network.bits_per_second
            && self.gpu.units >= request.gpu.units
            && match (self.lifetime, request.lifetime) {
                (None, _) | (_, None) => true,
                (Some(actual), Some(required)) => actual >= required,
            }
    }

    pub fn validate_requirement(&self) -> Result<(), ResourceError> {
        self.validate()?;
        if self.is_empty() { return Err(ResourceError::EmptyRequirement); }
        Ok(())
    }
}

impl CompositeResource {
    pub fn compose(allocations: Vec<Allocation>) -> Result<Self, ResourceError> {
        if allocations.is_empty() { return Err(ResourceError::EmptyComposite); }
        let mut capacity = ResourceFragment::default();
        let mut seen = HashSet::with_capacity(allocations.len());

        for allocation in &allocations {
            if allocation.participant_id.is_empty() { return Err(ResourceError::InvalidAllocation); }
            if !seen.insert(allocation.participant_id.clone()) { return Err(ResourceError::DuplicateAllocation); }
            allocation.resources.validate_requirement().map_err(|_| ResourceError::InvalidAllocation)?;
            capacity = capacity.add(allocation.resources);
        }
        Ok(Self { capacity, allocations })
    }

    pub fn validate(&self) -> Result<(), ResourceError> {
        let rebuilt = Self::compose(self.allocations.clone())?;
        if rebuilt.capacity != self.capacity { return Err(ResourceError::CapacityMismatch); }
        Ok(())
    }

    pub fn satisfies(&self, requirement: &ResourceFragment) -> bool { self.capacity.satisfies(requirement) }

    pub fn allocation_for(&self, participant_id: &str) -> Option<&Allocation> {
        self.allocations.iter().find(|a| a.participant_id == participant_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cpu(cores: f64) -> ResourceFragment {
        ResourceFragment { cpu: CpuCapacity { cores }, ..Default::default() }
    }

    #[test]
    fn shortest_bounded_lifetime_wins() {
        let a = ResourceFragment { lifetime: Some(Duration::from_secs(1800)), ..cpu(1.0) };
        let b = ResourceFragment { lifetime: Some(Duration::from_secs(600)), ..cpu(2.0) };
        let got = a.add(b);
        assert_eq!(got.cpu.cores, 3.0);
        assert_eq!(got.lifetime, Some(Duration::from_secs(600)));
    }

    #[test]
    fn none_lifetime_is_unbounded() {
        let got = ResourceFragment { lifetime: None, ..Default::default() }
            .add(ResourceFragment { lifetime: Some(Duration::from_secs(3600)), ..Default::default() });
        assert_eq!(got.lifetime, Some(Duration::from_secs(3600)));
    }

    #[test]
    fn composition_is_canonical() {
        let composed = CompositeResource::compose(vec![
            Allocation { participant_id: "a".into(), resources: ResourceFragment {
                cpu: CpuCapacity { cores: 0.75 }, memory: MemoryCapacity { bytes: 512 }, ..Default::default()
            }},
            Allocation { participant_id: "b".into(), resources: ResourceFragment {
                cpu: CpuCapacity { cores: 1.25 }, memory: MemoryCapacity { bytes: 1024 }, ..Default::default()
            }},
        ]).unwrap();
        assert!((composed.capacity.cpu.cores - 2.0).abs() < f64::EPSILON);
        assert_eq!(composed.capacity.memory.bytes, 1536);
        assert_eq!(composed.allocations.len(), 2);
        composed.validate().unwrap();
        assert_eq!(composed.allocation_for("b").unwrap().resources.memory.bytes, 1024);
    }

    #[test]
    fn duplicate_participants_are_rejected() {
        let err = CompositeResource::compose(vec![
            Allocation { participant_id: "a".into(), resources: cpu(1.0) },
            Allocation { participant_id: "a".into(), resources: cpu(1.0) },
        ]).unwrap_err();
        assert_eq!(err, ResourceError::DuplicateAllocation);
    }

    #[test]
    fn tampered_capacity_is_rejected() {
        let mut composed = CompositeResource::compose(vec![
            Allocation { participant_id: "a".into(), resources: cpu(1.0) },
        ]).unwrap();
        composed.capacity.cpu.cores = 2.0;
        assert_eq!(composed.validate().unwrap_err(), ResourceError::CapacityMismatch);
    }
}
