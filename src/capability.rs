use crate::resource::{Cpu, Gpu, Memory, Network, ResourceFragment, Storage};
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
pub struct ComputeCapability {
    pub cpu_cores: f64,
    pub gpu_units: u32,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MemoryCapability {
    pub bytes: u64,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StorageCapability {
    pub bytes: u64,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NetworkCapability {
    pub bits_per_second: u64,
}
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Reliability {
    pub availability: f64,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Latency {
    pub to_participant: Duration,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Lifetime {
    pub duration: Option<Duration>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Security {
    pub trusted: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CapabilityError {
    InvalidCpu,
    InvalidAvailability,
    Empty,
    ZeroLifetime,
}

impl fmt::Display for CapabilityError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
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

    pub fn to_resource_fragment(&self) -> ResourceFragment {
        (*self).into()
    }
}

impl From<Capability> for ResourceFragment {
    fn from(cap: Capability) -> Self {
        Self {
            cpu: Cpu {
                cores: cap.compute.cpu_cores,
            },
            memory: Memory {
                bytes: cap.memory.bytes,
            },
            storage: Storage {
                bytes: cap.storage.bytes,
            },
            network: Network {
                bits_per_second: cap.network.bits_per_second,
            },
            gpu: Gpu {
                units: cap.compute.gpu_units,
            },
            lifetime: cap.lifetime.duration,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_capability() -> Capability {
        Capability {
            compute: ComputeCapability {
                cpu_cores: 4.0,
                gpu_units: 1,
            },
            memory: MemoryCapability {
                bytes: 8 * 1024 * 1024 * 1024,
            },
            storage: StorageCapability {
                bytes: 100 * 1024 * 1024 * 1024,
            },
            network: NetworkCapability {
                bits_per_second: 1_000_000_000,
            },
            reliability: Reliability {
                availability: 0.999,
            },
            latency: Latency {
                to_participant: Duration::from_millis(5),
            },
            lifetime: Lifetime {
                duration: Some(Duration::from_secs(3600)),
            },
            security: Security { trusted: true },
        }
    }

    #[test]
    fn valid_capability_passes_validation() {
        let cap = sample_capability();
        assert!(cap.validate().is_ok());
    }

    #[test]
    fn invalid_cpu_is_rejected() {
        let mut cap = sample_capability();
        cap.compute.cpu_cores = -1.0;
        assert_eq!(cap.validate().unwrap_err(), CapabilityError::InvalidCpu);

        cap.compute.cpu_cores = f64::NAN;
        assert_eq!(cap.validate().unwrap_err(), CapabilityError::InvalidCpu);
    }

    #[test]
    fn invalid_availability_is_rejected() {
        let mut cap = sample_capability();
        cap.reliability.availability = 1.5;
        assert_eq!(
            cap.validate().unwrap_err(),
            CapabilityError::InvalidAvailability
        );

        cap.reliability.availability = -0.1;
        assert_eq!(
            cap.validate().unwrap_err(),
            CapabilityError::InvalidAvailability
        );
    }

    #[test]
    fn zero_lifetime_is_rejected() {
        let mut cap = sample_capability();
        cap.lifetime.duration = Some(Duration::ZERO);
        assert_eq!(cap.validate().unwrap_err(), CapabilityError::ZeroLifetime);
    }

    #[test]
    fn empty_capability_is_rejected() {
        let cap = Capability {
            compute: ComputeCapability {
                cpu_cores: 0.0,
                gpu_units: 0,
            },
            memory: MemoryCapability { bytes: 0 },
            storage: StorageCapability { bytes: 0 },
            network: NetworkCapability { bits_per_second: 0 },
            reliability: Reliability { availability: 0.95 },
            latency: Latency {
                to_participant: Duration::from_millis(10),
            },
            lifetime: Lifetime { duration: None },
            security: Security { trusted: false },
        };
        assert_eq!(cap.validate().unwrap_err(), CapabilityError::Empty);
    }

    #[test]
    fn availability_for_duration() {
        let cap = sample_capability();
        assert!(cap.available_for(None));
        assert!(cap.available_for(Some(Duration::from_secs(1800))));
        assert!(cap.available_for(Some(Duration::from_secs(3600))));
        assert!(!cap.available_for(Some(Duration::from_secs(7200))));

        let mut unbounded = sample_capability();
        unbounded.lifetime.duration = None;
        assert!(unbounded.available_for(Some(Duration::from_secs(100000))));
    }

    #[test]
    fn capability_converts_to_resource_fragment() {
        let cap = sample_capability();
        let fragment = cap.to_resource_fragment();
        assert_eq!(fragment.cpu.cores, 4.0);
        assert_eq!(fragment.gpu.units, 1);
        assert_eq!(fragment.memory.bytes, 8 * 1024 * 1024 * 1024);
        assert_eq!(fragment.storage.bytes, 100 * 1024 * 1024 * 1024);
        assert_eq!(fragment.network.bits_per_second, 1_000_000_000);
        assert_eq!(fragment.lifetime, Some(Duration::from_secs(3600)));
        assert!(fragment.validate().is_ok());
    }
}
