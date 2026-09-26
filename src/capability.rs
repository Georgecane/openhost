use std::fmt;
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Capability {
    pub compute: ComputeCapability,
    pub memory: MemoryCapability,
    pub storage: StorageCapability,
    pub network: NetworkCapability,
    pub reliability: Reliability,
    pub latency: Latency,
    pub lifetime: Lifetime,
    pub security: Security,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ComputeCapability { pub cpu_cores: f64, pub gpu_units: u32 }
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MemoryCapability { pub bytes: u64 }
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StorageCapability { pub bytes: u64 }
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NetworkCapability { pub bits_per_second: u64 }
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Reliability { pub availability: f64 }
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Latency { pub to_participant: Duration }
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Lifetime { pub duration: Option<Duration> }
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Security { pub trusted: bool }

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CapabilityError { InvalidCpu, InvalidAvailability, Empty, ZeroLifetime }

impl fmt::Display for CapabilityError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { write!(f, "{self:?}") }
}
impl std::error::Error for CapabilityError {}

impl Capability {
    pub fn validate(&self) -> Result<(), CapabilityError> {
        if !self.compute.cpu_cores.is_finite() || self.compute.cpu_cores < 0.0 {
            return Err(CapabilityError::InvalidCpu);
        }
        if !(0.0..=1.0).contains(&self.reliability.availability) {
            return Err(CapabilityError::InvalidAvailability);
        }
        if self.lifetime.duration.is_some_and(|d| d.is_zero()) {
            return Err(CapabilityError::ZeroLifetime);
        }
        if self.compute.cpu_cores == 0.0
            && self.compute.gpu_units == 0
            && self.memory.bytes == 0
            && self.storage.bytes == 0
            && self.network.bits_per_second == 0
        {
            return Err(CapabilityError::Empty);
        }
        Ok(())
    }

    pub fn available_for(&self, requested: Option<Duration>) -> bool {
        match (self.lifetime.duration, requested) {
            (_, None) | (None, Some(_)) => true,
            (Some(actual), Some(required)) => actual >= required,
        }
    }
}
