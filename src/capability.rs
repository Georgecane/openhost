use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Capability {
    pub compute: ComputeCapability,
    pub memory: MemoryCapability,
    pub storage: StorageCapability,
    pub network: NetworkCapability,
    pub reliability: ReliabilityProfile,
    pub latency: LatencyProfile,
    pub lifetime: LifetimeProfile,
    pub security: SecurityProfile,
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
pub struct ReliabilityProfile { pub availability: f64 }
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LatencyProfile { pub to_participant: Duration }
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LifetimeProfile { pub duration: Option<Duration> }
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SecurityProfile { pub trusted: bool }

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CapabilityError { Invalid, Negative, InvalidLatency }

impl std::fmt::Display for CapabilityError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "{self:?}") }
}
impl std::error::Error for CapabilityError {}

impl Capability {
    pub fn validate(&self) -> Result<(), CapabilityError> {
        if self.compute.cpu_cores < 0.0 || self.compute.cpu_cores.is_nan()
            || !(0.0..=1.0).contains(&self.reliability.availability)
            || self.lifetime.duration.is_some_and(|d| d.is_zero()) && self.compute.cpu_cores < 0.0
        {
            return Err(CapabilityError::Negative);
        }
        if self.memory.bytes == 0 && self.storage.bytes == 0 && self.network.bits_per_second == 0
            && self.compute.cpu_cores == 0.0 && self.compute.gpu_units == 0
        {
            return Err(CapabilityError::Invalid);
        }
        if self.latency.to_participant.is_zero() {
            return Ok(());
        }
        Ok(())
    }

    pub fn available_for(&self, duration: Option<Duration>) -> bool {
        match (self.lifetime.duration, duration) {
            (_, None) => true,
            (None, Some(_)) => true,
            (Some(actual), Some(required)) => actual >= required,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid() -> Capability {
        Capability {
            compute: ComputeCapability { cpu_cores: 4.0, gpu_units: 1 },
            memory: MemoryCapability { bytes: 8 << 30 },
            storage: StorageCapability { bytes: 0 },
            network: NetworkCapability { bits_per_second: 1_000_000_000 },
            reliability: ReliabilityProfile { availability: 0.99 },
            latency: LatencyProfile { to_participant: Duration::from_millis(10) },
            lifetime: LifetimeProfile { duration: Some(Duration::from_secs(3600)) },
            security: SecurityProfile { trusted: true },
        }
    }

    #[test]
    fn validates_and_checks_lifetime() {
        let c = valid();
        c.validate().unwrap();
        assert!(c.available_for(Some(Duration::from_secs(3600))));
        assert!(!c.available_for(Some(Duration::from_secs(3601))));
        assert!(c.available_for(None));
    }

    #[test]
    fn rejects_invalid_values() {
        let mut c = valid();
        c.compute.cpu_cores = -1.0;
        assert_eq!(c.validate().unwrap_err(), CapabilityError::Negative);
        c = valid();
        c.reliability.availability = 1.1;
        assert_eq!(c.validate().unwrap_err(), CapabilityError::Negative);
        c = valid();
        c.compute.cpu_cores = 0.0;
        c.memory.bytes = 0;
        c.network.bits_per_second = 0;
        c.compute.gpu_units = 0;
        assert_eq!(c.validate().unwrap_err(), CapabilityError::Invalid);
    }
}
