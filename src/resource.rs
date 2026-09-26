use std::collections::HashSet;
use std::fmt;
use std::ops::Add;
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Cpu {
    pub cores: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Memory {
    pub bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Storage {
    pub bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Network {
    pub bits_per_second: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Gpu {
    pub units: u32,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ResourceFragment {
    pub cpu: Cpu,
    pub memory: Memory,
    pub storage: Storage,
    pub network: Network,
    pub gpu: Gpu,
    pub lifetime: Option<Duration>,
}

impl Default for ResourceFragment {
    fn default() -> Self {
        Self {
            cpu: Cpu { cores: 0.0 },
            memory: Memory { bytes: 0 },
            storage: Storage { bytes: 0 },
            network: Network { bits_per_second: 0 },
            gpu: Gpu { units: 0 },
            lifetime: None,
        }
    }
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
    InvalidCpu,
    EmptyRequirement,
    EmptyComposite,
    InvalidAllocation,
    DuplicateParticipant,
    CapacityMismatch,
}

impl fmt::Display for ResourceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for ResourceError {}

impl ResourceFragment {
    pub fn validate(&self) -> Result<(), ResourceError> {
        if !self.cpu.cores.is_finite() || self.cpu.cores < 0.0 {
            return Err(ResourceError::InvalidCpu);
        }
        Ok(())
    }

    pub fn is_empty(&self) -> bool {
        self.cpu.cores == 0.0
            && self.memory.bytes == 0
            && self.storage.bytes == 0
            && self.network.bits_per_second == 0
            && self.gpu.units == 0
    }

    pub fn validate_requirement(&self) -> Result<(), ResourceError> {
        self.validate()?;
        if self.is_empty() {
            return Err(ResourceError::EmptyRequirement);
        }
        Ok(())
    }

    fn legacy_add(self, other: Self) -> Self {
        Self {
            cpu: Cpu {
                cores: self.cpu.cores + other.cpu.cores,
            },
            memory: Memory {
                bytes: self.memory.bytes.saturating_add(other.memory.bytes),
            },
            storage: Storage {
                bytes: self.storage.bytes.saturating_add(other.storage.bytes),
            },
            network: Network {
                bits_per_second: self
                    .network
                    .bits_per_second
                    .saturating_add(other.network.bits_per_second),
            },
            gpu: Gpu {
                units: self.gpu.units.saturating_add(other.gpu.units),
            },
            lifetime: match (self.lifetime, other.lifetime) {
                (None, x) | (x, None) => x,
                (Some(a), Some(b)) => Some(a.min(b)),
            },
        }
    }

    pub fn satisfies(&self, requirement: &Self) -> bool {
        self.cpu.cores >= requirement.cpu.cores
            && self.memory.bytes >= requirement.memory.bytes
            && self.storage.bytes >= requirement.storage.bytes
            && self.network.bits_per_second >= requirement.network.bits_per_second
            && self.gpu.units >= requirement.gpu.units
            && match (self.lifetime, requirement.lifetime) {
                (_, None) | (None, Some(_)) => true,
                (Some(actual), Some(required)) => actual >= required,
            }
    }
}

impl Add for ResourceFragment {
    type Output = Self;

    fn add(self, other: Self) -> Self::Output {
        Self {
            cpu: Cpu {
                cores: self.cpu.cores + other.cpu.cores,
            },
            memory: Memory {
                bytes: self.memory.bytes.saturating_add(other.memory.bytes),
            },
            storage: Storage {
                bytes: self.storage.bytes.saturating_add(other.storage.bytes),
            },
            network: Network {
                bits_per_second: self.network.bits_per_second.saturating_add(other.network.bits_per_second),
            },
            gpu: Gpu {
                units: self.gpu.units.saturating_add(other.gpu.units),
            },
            lifetime: match (self.lifetime, other.lifetime) {
                (None, x) | (x, None) => x,
                (Some(a), Some(b)) => Some(a.min(b)),
            },
        }
    }
}

impl CompositeResource {
    pub fn compose(allocations: Vec<Allocation>) -> Result<Self, ResourceError> {
        if allocations.is_empty() {
            return Err(ResourceError::EmptyComposite);
        }

        let mut seen = HashSet::with_capacity(allocations.len());
        let mut capacity = ResourceFragment::default();

        for allocation in &allocations {
            if allocation.participant_id.is_empty() {
                return Err(ResourceError::InvalidAllocation);
            }
            if !seen.insert(&allocation.participant_id) {
                return Err(ResourceError::DuplicateParticipant);
            }
            allocation
                .resources
                .validate_requirement()
                .map_err(|_| ResourceError::InvalidAllocation)?;
            capacity = capacity + allocation.resources;
        }

        Ok(Self {
            capacity,
            allocations,
        })
    }

    pub fn validate(&self) -> Result<(), ResourceError> {
        let rebuilt = Self::compose(self.allocations.clone())?;
        if rebuilt.capacity != self.capacity {
            return Err(ResourceError::CapacityMismatch);
        }
        Ok(())
    }

    pub fn satisfies(&self, requirement: &ResourceFragment) -> bool {
        self.capacity.satisfies(requirement)
    }

    pub fn allocation_for(&self, participant_id: &str) -> Option<&Allocation> {
        self.allocations
            .iter()
            .find(|a| a.participant_id == participant_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cpu(cores: f64) -> ResourceFragment {
        ResourceFragment {
            cpu: Cpu { cores },
            ..Default::default()
        }
    }

    #[test]
    fn bounded_composition_uses_shortest_lifetime() {
        let a = ResourceFragment {
            lifetime: Some(Duration::from_secs(30)),
            ..cpu(1.0)
        };
        let b = ResourceFragment {
            lifetime: Some(Duration::from_secs(10)),
            ..cpu(2.0)
        };
        let total = a + b;
        assert_eq!(total.cpu.cores, 3.0);
        assert_eq!(total.lifetime, Some(Duration::from_secs(10)));
    }

    #[test]
    fn fn_unbounded_plus_bounded_is_bounded() {
        let a = ResourceFragment {
            lifetime: None,
            ..cpu(1.0)
        };
        let b = ResourceFragment {
            lifetime: Some(Duration::from_secs(10)),
            ..cpu(2.0)
        };
        assert_eq!((a + b).lifetime, Some(Duration::from_secs(10)));
    }

    #[test]
    fn composition_is_canonical() {
        let composite = CompositeResource::compose(vec![
            Allocation {
                participant_id: "a".into(),
                resources: cpu(0.75),
            },
            Allocation {
                participant_id: "b".into(),
                resources: cpu(1.25),
            },
        ])
        .unwrap();
        assert_eq!(composite.capacity.cpu.cores, 2.0);
        composite.validate().unwrap();
    }

    #[test]
    fn duplicate_participants_are_rejected() {
        let result = CompositeResource::compose(vec![
            Allocation {
                participant_id: "a".into(),
                resources: cpu(1.0),
            },
            Allocation {
                participant_id: "a".into(),
                resources: cpu(1.0),
            },
        ]);
        assert_eq!(result.unwrap_err(), ResourceError::DuplicateParticipant);
    }

    #[test]
    fn tampered_capacity_is_rejected() {
        let mut composite = CompositeResource::compose(vec![Allocation {
            participant_id: "a".into(),
            resources: cpu(1.0),
        }])
        .unwrap();
        composite.capacity.cpu.cores = 2.0;
        assert_eq!(
            composite.validate().unwrap_err(),
            ResourceError::CapacityMismatch
        );
    }
}
