use crate::capability::Capability;
use crate::identity::{Identity, Kind};
use std::collections::BTreeMap;
use std::fmt;
use std::sync::RwLock;
use std::time::{Duration, SystemTime};

#[derive(Debug, Clone)]
pub struct Advertisement {
    pub participant: Identity,
    pub capability: Capability,
    pub sequence: u64,
    pub observed_at: SystemTime,
}

#[derive(Debug, Clone)]
pub struct Member {
    pub advertisement: Advertisement,
    pub last_seen: SystemTime,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum State {
    Active,
    Stale,
    Expired,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FreshnessPolicy {
    pub stale_after: Duration,
    pub expire_after: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiscoveryError {
    InvalidAdvertisement,
    InvalidTimestamp,
    InvalidPolicy,
    StaleSequence,
    SequenceExhausted,
    NotFound,
    MalformedFrame,
    UnsupportedProtocolVersion,
    NetworkFailure,
}

impl fmt::Display for DiscoveryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for DiscoveryError {}

impl FreshnessPolicy {
    pub fn validate(&self) -> Result<(), DiscoveryError> {
        if self.stale_after.is_zero() || self.expire_after <= self.stale_after {
            return Err(DiscoveryError::InvalidPolicy);
        }
        Ok(())
    }
}

impl Advertisement {
    pub fn validate(&self) -> Result<(), DiscoveryError> {
        if self.participant.kind != Kind::Participant
            || self.participant.validate().is_err()
            || self.capability.validate().is_err()
            || self.observed_at == SystemTime::UNIX_EPOCH
        {
            return Err(DiscoveryError::InvalidAdvertisement);
        }
        Ok(())
    }
}

impl Member {
    pub fn state_at(
        &self,
        now: SystemTime,
        policy: FreshnessPolicy,
    ) -> Result<State, DiscoveryError> {
        policy.validate()?;
        if self.last_seen == SystemTime::UNIX_EPOCH || now < self.last_seen {
            return Err(DiscoveryError::InvalidTimestamp);
        }
        let age = now
            .duration_since(self.last_seen)
            .map_err(|_| DiscoveryError::InvalidTimestamp)?;
        if age >= policy.expire_after {
            Ok(State::Expired)
        } else if age >= policy.stale_after {
            Ok(State::Stale)
        } else {
            Ok(State::Active)
        }
    }
}

pub trait Registry: Send + Sync {
    fn upsert(
        &self,
        advertisement: Advertisement,
        received_at: SystemTime,
    ) -> Result<(), DiscoveryError>;
    fn remove(&self, id: &Identity) -> Result<(), DiscoveryError>;
    fn get(&self, id: &Identity) -> Result<Member, DiscoveryError>;
    fn members(&self) -> Vec<Member>;
    fn members_at(&self, now: SystemTime) -> Result<Vec<(Member, State)>, DiscoveryError>;
}

pub struct MemoryRegistry {
    members: RwLock<BTreeMap<String, Member>>,
    policy: FreshnessPolicy,
}

impl MemoryRegistry {
    pub fn new(policy: FreshnessPolicy) -> Result<Self, DiscoveryError> {
        policy.validate()?;
        Ok(Self {
            members: RwLock::new(BTreeMap::new()),
            policy,
        })
    }
}

impl Registry for MemoryRegistry {
    fn upsert(
        &self,
        advertisement: Advertisement,
        received_at: SystemTime,
    ) -> Result<(), DiscoveryError> {
        advertisement.validate()?;
        if received_at == SystemTime::UNIX_EPOCH {
            return Err(DiscoveryError::InvalidTimestamp);
        }

        let mut members = self.members.write().expect("discovery lock poisoned");
        if let Some(existing) = members.get(&advertisement.participant.id)
            && advertisement.sequence <= existing.advertisement.sequence
        {
            return Err(DiscoveryError::StaleSequence);
        }

        members.insert(
            advertisement.participant.id.clone(),
            Member {
                advertisement,
                last_seen: received_at,
            },
        );
        Ok(())
    }

    fn remove(&self, id: &Identity) -> Result<(), DiscoveryError> {
        if id.kind != Kind::Participant || id.validate().is_err() {
            return Err(DiscoveryError::InvalidAdvertisement);
        }
        self.members
            .write()
            .expect("discovery lock poisoned")
            .remove(&id.id)
            .map(|_| ())
            .ok_or(DiscoveryError::NotFound)
    }

    fn get(&self, id: &Identity) -> Result<Member, DiscoveryError> {
        if id.kind != Kind::Participant || id.validate().is_err() {
            return Err(DiscoveryError::InvalidAdvertisement);
        }
        self.members
            .read()
            .expect("discovery lock poisoned")
            .get(&id.id)
            .cloned()
            .ok_or(DiscoveryError::NotFound)
    }

    fn members(&self) -> Vec<Member> {
        self.members
            .read()
            .expect("discovery lock poisoned")
            .values()
            .cloned()
            .collect()
    }

    fn members_at(&self, now: SystemTime) -> Result<Vec<(Member, State)>, DiscoveryError> {
        self.members()
            .into_iter()
            .map(|member| {
                let state = member.state_at(now, self.policy)?;
                Ok((member, state))
            })
            .collect()
    }
}

pub const DISCOVERY_PROTOCOL_VERSION: u8 = 1;

pub fn encode_advertisement(ad: &Advertisement) -> Result<Vec<u8>, DiscoveryError> {
    ad.validate()?;
    let mut buf = Vec::with_capacity(128);
    buf.push(DISCOVERY_PROTOCOL_VERSION);

    let id_bytes = ad.participant.id.as_bytes();
    let id_len: u16 = id_bytes
        .len()
        .try_into()
        .map_err(|_| DiscoveryError::InvalidAdvertisement)?;
    buf.extend_from_slice(&id_len.to_be_bytes());
    buf.extend_from_slice(id_bytes);

    buf.extend_from_slice(&ad.capability.compute.cpu_cores.to_be_bytes());
    buf.extend_from_slice(&ad.capability.compute.gpu_units.to_be_bytes());
    buf.extend_from_slice(&ad.capability.memory.bytes.to_be_bytes());
    buf.extend_from_slice(&ad.capability.storage.bytes.to_be_bytes());
    buf.extend_from_slice(&ad.capability.network.bits_per_second.to_be_bytes());
    buf.extend_from_slice(&ad.capability.reliability.availability.to_be_bytes());

    let latency_nanos: u64 = ad
        .capability
        .latency
        .to_participant
        .as_nanos()
        .try_into()
        .map_err(|_| DiscoveryError::InvalidAdvertisement)?;
    buf.extend_from_slice(&latency_nanos.to_be_bytes());

    match ad.capability.lifetime.duration {
        Some(dur) => {
            buf.push(1);
            let dur_nanos: u64 = dur
                .as_nanos()
                .try_into()
                .map_err(|_| DiscoveryError::InvalidAdvertisement)?;
            buf.extend_from_slice(&dur_nanos.to_be_bytes());
        }
        None => buf.push(0),
    }

    buf.push(if ad.capability.security.trusted { 1 } else { 0 });
    buf.extend_from_slice(&ad.sequence.to_be_bytes());

    let dur = ad
        .observed_at
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_err(|_| DiscoveryError::InvalidTimestamp)?;
    buf.extend_from_slice(&dur.as_secs().to_be_bytes());
    buf.extend_from_slice(&dur.subsec_nanos().to_be_bytes());

    Ok(buf)
}

pub fn decode_advertisement(data: &[u8]) -> Result<Advertisement, DiscoveryError> {
    if data.is_empty() {
        return Err(DiscoveryError::MalformedFrame);
    }
    let version = data[0];
    if version != DISCOVERY_PROTOCOL_VERSION {
        return Err(DiscoveryError::UnsupportedProtocolVersion);
    }
    let mut offset = 1;

    let take = |offset: &mut usize, len: usize| -> Result<&[u8], DiscoveryError> {
        if *offset + len > data.len() {
            return Err(DiscoveryError::MalformedFrame);
        }
        let slice = &data[*offset..*offset + len];
        *offset += len;
        Ok(slice)
    };

    let id_len = u16::from_be_bytes(
        take(&mut offset, 2)?
            .try_into()
            .map_err(|_| DiscoveryError::MalformedFrame)?,
    ) as usize;
    let id_str = std::str::from_utf8(take(&mut offset, id_len)?)
        .map_err(|_| DiscoveryError::MalformedFrame)?;
    let participant = Identity::parse(id_str, Kind::Participant)
        .map_err(|_| DiscoveryError::InvalidAdvertisement)?;

    let cpu_cores = f64::from_be_bytes(
        take(&mut offset, 8)?
            .try_into()
            .map_err(|_| DiscoveryError::MalformedFrame)?,
    );
    let gpu_units = u32::from_be_bytes(
        take(&mut offset, 4)?
            .try_into()
            .map_err(|_| DiscoveryError::MalformedFrame)?,
    );
    let mem_bytes = u64::from_be_bytes(
        take(&mut offset, 8)?
            .try_into()
            .map_err(|_| DiscoveryError::MalformedFrame)?,
    );
    let storage_bytes = u64::from_be_bytes(
        take(&mut offset, 8)?
            .try_into()
            .map_err(|_| DiscoveryError::MalformedFrame)?,
    );
    let net_bps = u64::from_be_bytes(
        take(&mut offset, 8)?
            .try_into()
            .map_err(|_| DiscoveryError::MalformedFrame)?,
    );
    let availability = f64::from_be_bytes(
        take(&mut offset, 8)?
            .try_into()
            .map_err(|_| DiscoveryError::MalformedFrame)?,
    );
    let latency_nanos = u64::from_be_bytes(
        take(&mut offset, 8)?
            .try_into()
            .map_err(|_| DiscoveryError::MalformedFrame)?,
    );

    let has_lifetime = take(&mut offset, 1)?[0];
    let lifetime_dur = match has_lifetime {
        0 => None,
        1 => {
            let nanos = u64::from_be_bytes(
                take(&mut offset, 8)?
                    .try_into()
                    .map_err(|_| DiscoveryError::MalformedFrame)?,
            );
            Some(Duration::from_nanos(nanos))
        }
        _ => return Err(DiscoveryError::MalformedFrame),
    };

    let trusted_byte = take(&mut offset, 1)?[0];
    let trusted = match trusted_byte {
        0 => false,
        1 => true,
        _ => return Err(DiscoveryError::MalformedFrame),
    };

    let sequence = u64::from_be_bytes(
        take(&mut offset, 8)?
            .try_into()
            .map_err(|_| DiscoveryError::MalformedFrame)?,
    );
    let secs = u64::from_be_bytes(
        take(&mut offset, 8)?
            .try_into()
            .map_err(|_| DiscoveryError::MalformedFrame)?,
    );
    let subsec_nanos = u32::from_be_bytes(
        take(&mut offset, 4)?
            .try_into()
            .map_err(|_| DiscoveryError::MalformedFrame)?,
    );
    if subsec_nanos >= 1_000_000_000 {
        return Err(DiscoveryError::MalformedFrame);
    }
    let observed_at = SystemTime::UNIX_EPOCH
        .checked_add(Duration::new(secs, subsec_nanos))
        .ok_or(DiscoveryError::InvalidTimestamp)?;

    use crate::capability::{
        Capability, ComputeCapability, Latency, Lifetime, MemoryCapability, NetworkCapability,
        Reliability, Security, StorageCapability,
    };
    let capability = Capability {
        compute: ComputeCapability {
            cpu_cores,
            gpu_units,
        },
        memory: MemoryCapability { bytes: mem_bytes },
        storage: StorageCapability {
            bytes: storage_bytes,
        },
        network: NetworkCapability {
            bits_per_second: net_bps,
        },
        reliability: Reliability { availability },
        latency: Latency {
            to_participant: Duration::from_nanos(latency_nanos),
        },
        lifetime: Lifetime {
            duration: lifetime_dur,
        },
        security: Security { trusted },
    };

    if offset != data.len() {
        return Err(DiscoveryError::MalformedFrame);
    }

    let ad = Advertisement {
        participant,
        capability,
        sequence,
        observed_at,
    };
    ad.validate()?;
    Ok(ad)
}

use std::net::UdpSocket;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

pub struct DiscoveryBeacon {
    socket: UdpSocket,
    target_address: String,
    advertisement: Advertisement,
    next_sequence: std::sync::atomic::AtomicU64,
}

impl DiscoveryBeacon {
    pub fn bind(
        bind_address: &str,
        target_address: impl Into<String>,
        advertisement: Advertisement,
    ) -> Result<Self, DiscoveryError> {
        advertisement.validate()?;
        let socket = UdpSocket::bind(bind_address).map_err(|_| DiscoveryError::NetworkFailure)?;
        let _ = socket.set_broadcast(true);
        let next_sequence = advertisement.sequence;
        Ok(Self {
            socket,
            target_address: target_address.into(),
            advertisement,
            next_sequence: std::sync::atomic::AtomicU64::new(next_sequence),
        })
    }

    pub fn local_addr(&self) -> Result<std::net::SocketAddr, DiscoveryError> {
        self.socket
            .local_addr()
            .map_err(|_| DiscoveryError::NetworkFailure)
    }

    pub fn broadcast_once(&self) -> Result<(), DiscoveryError> {
        let sequence = self
            .next_sequence
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                current.checked_add(1)
            })
            .map_err(|_| DiscoveryError::SequenceExhausted)?;
        let mut advertisement = self.advertisement.clone();
        advertisement.sequence = sequence;
        let payload = encode_advertisement(&advertisement)?;
        self.socket
            .send_to(&payload, &self.target_address)
            .map_err(|_| DiscoveryError::NetworkFailure)?;
        Ok(())
    }

    pub fn broadcast_until(
        &self,
        shutdown: &AtomicBool,
        interval: Duration,
    ) -> Result<(), DiscoveryError> {
        if interval.is_zero() {
            return Err(DiscoveryError::InvalidPolicy);
        }
        while !shutdown.load(Ordering::Relaxed) {
            self.broadcast_once()?;
            let start = std::time::Instant::now();
            while start.elapsed() < interval {
                if shutdown.load(Ordering::Relaxed) {
                    return Ok(());
                }
                std::thread::sleep(Duration::from_millis(10).min(interval));
            }
        }
        Ok(())
    }
}

pub struct DiscoveryListener {
    socket: UdpSocket,
    registry: Arc<dyn Registry>,
}

impl DiscoveryListener {
    pub fn bind(bind_address: &str, registry: Arc<dyn Registry>) -> Result<Self, DiscoveryError> {
        let socket = UdpSocket::bind(bind_address).map_err(|_| DiscoveryError::NetworkFailure)?;
        Ok(Self { socket, registry })
    }

    pub fn local_addr(&self) -> Result<std::net::SocketAddr, DiscoveryError> {
        self.socket
            .local_addr()
            .map_err(|_| DiscoveryError::NetworkFailure)
    }

    pub fn receive_once(&self) -> Result<Advertisement, DiscoveryError> {
        let mut buf = [0u8; 2048];
        let (len, _) = self
            .socket
            .recv_from(&mut buf)
            .map_err(|_| DiscoveryError::NetworkFailure)?;
        let advertisement = decode_advertisement(&buf[..len])?;
        self.registry
            .upsert(advertisement.clone(), SystemTime::now())?;
        Ok(advertisement)
    }

    pub fn listen_until(&self, shutdown: &AtomicBool) -> Result<(), DiscoveryError> {
        self.socket
            .set_read_timeout(Some(Duration::from_millis(50)))
            .map_err(|_| DiscoveryError::NetworkFailure)?;
        let mut buf = [0u8; 2048];
        while !shutdown.load(Ordering::Relaxed) {
            match self.socket.recv_from(&mut buf) {
                Ok((len, _)) => {
                    if let Ok(ad) = decode_advertisement(&buf[..len]) {
                        let _ = self.registry.upsert(ad, SystemTime::now());
                    }
                }
                Err(e)
                    if e.kind() == std::io::ErrorKind::WouldBlock
                        || e.kind() == std::io::ErrorKind::TimedOut =>
                {
                    continue;
                }
                Err(_) => return Err(DiscoveryError::NetworkFailure),
            }
        }
        Ok(())
    }
}

/// Projects discovered peers into scheduler offers.
///
/// UDP advertisements are unauthenticated hints. The default constructor therefore fails
/// closed and emits no scheduler offers. The opt-in constructor is only suitable for isolated
/// test networks where every sender is already trusted by the operator.
pub struct DiscoveryFabric {
    registry: Arc<dyn Registry>,
    allow_unverified_advertisements: bool,
}

impl DiscoveryFabric {
    /// Create a fail-closed discovery fabric. Offers remain empty until an authenticated
    /// discovery mechanism is added.
    pub fn new(registry: Arc<dyn Registry>) -> Self {
        Self {
            registry,
            allow_unverified_advertisements: false,
        }
    }

    /// Explicitly allow unsigned UDP advertisements to become resource offers.
    ///
    /// Do not use this option on untrusted networks or for workloads with security impact.
    pub fn with_unverified_advertisements(registry: Arc<dyn Registry>) -> Self {
        Self {
            registry,
            allow_unverified_advertisements: true,
        }
    }
}

impl crate::fabric::Fabric for DiscoveryFabric {
    fn offers(&self) -> Vec<crate::fabric::ResourceOffer> {
        if !self.allow_unverified_advertisements {
            return Vec::new();
        }

        let Ok(members) = self.registry.members_at(SystemTime::now()) else {
            return Vec::new();
        };

        members
            .into_iter()
            .filter_map(|(member, state)| {
                if state != State::Active {
                    return None;
                }
                let resources = member.advertisement.capability.to_resource_fragment();
                if resources.validate().is_err() || resources.is_empty() {
                    return None;
                }
                Some(crate::fabric::ResourceOffer {
                    participant_id: member.advertisement.participant.id,
                    resources,
                })
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::capability::{
        Capability, ComputeCapability, Latency, Lifetime, MemoryCapability, NetworkCapability,
        Reliability, Security, StorageCapability,
    };

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
            reliability: Reliability { availability: 0.99 },
            latency: Latency {
                to_participant: Duration::from_millis(5),
            },
            lifetime: Lifetime {
                duration: Some(Duration::from_secs(3600)),
            },
            security: Security { trusted: true },
        }
    }

    fn sample_advertisement() -> Advertisement {
        Advertisement {
            participant: Identity::new(Kind::Participant),
            capability: sample_capability(),
            sequence: 42,
            observed_at: SystemTime::now(),
        }
    }

    #[test]
    fn advertisement_encoding_roundtrip() {
        let original = sample_advertisement();
        let encoded = encode_advertisement(&original).expect("encode succeeds");
        let decoded = decode_advertisement(&encoded).expect("decode succeeds");

        assert_eq!(original.participant, decoded.participant);
        assert_eq!(original.sequence, decoded.sequence);
        assert_eq!(
            original.capability.compute.cpu_cores,
            decoded.capability.compute.cpu_cores
        );
        assert_eq!(
            original.capability.compute.gpu_units,
            decoded.capability.compute.gpu_units
        );
        assert_eq!(
            original.capability.memory.bytes,
            decoded.capability.memory.bytes
        );
        assert_eq!(
            original.capability.lifetime.duration,
            decoded.capability.lifetime.duration
        );
        assert_eq!(
            original.capability.security.trusted,
            decoded.capability.security.trusted
        );
    }

    #[test]
    fn decode_rejects_empty_and_corrupt_data() {
        assert_eq!(
            decode_advertisement(&[]).unwrap_err(),
            DiscoveryError::MalformedFrame
        );
        assert_eq!(
            decode_advertisement(&[0u8, 1, 2]).unwrap_err(),
            DiscoveryError::UnsupportedProtocolVersion
        );
        assert_eq!(
            decode_advertisement(&[DISCOVERY_PROTOCOL_VERSION, 0, 5, 65]).unwrap_err(),
            DiscoveryError::MalformedFrame
        );
    }

    #[test]
    fn decoder_rejects_invalid_timestamp_components() {
        let mut invalid_subseconds = encode_advertisement(&sample_advertisement()).unwrap();
        let length = invalid_subseconds.len();
        invalid_subseconds[length - 4..].copy_from_slice(&1_000_000_000u32.to_be_bytes());
        assert_eq!(
            decode_advertisement(&invalid_subseconds).unwrap_err(),
            DiscoveryError::MalformedFrame
        );

        let mut overflowing_timestamp = encode_advertisement(&sample_advertisement()).unwrap();
        let length = overflowing_timestamp.len();
        overflowing_timestamp[length - 12..length - 4]
            .copy_from_slice(&u64::MAX.to_be_bytes());
        assert_eq!(
            decode_advertisement(&overflowing_timestamp).unwrap_err(),
            DiscoveryError::InvalidTimestamp
        );
    }

    #[test]
    fn decoder_rejects_trailing_bytes() {
        let mut encoded = encode_advertisement(&sample_advertisement()).unwrap();
        encoded.push(0);
        assert_eq!(
            decode_advertisement(&encoded).unwrap_err(),
            DiscoveryError::MalformedFrame
        );
    }

    #[test]
    fn registry_rejects_replayed_sequence_numbers() {
        let policy = FreshnessPolicy {
            stale_after: Duration::from_secs(5),
            expire_after: Duration::from_secs(10),
        };
        let registry = MemoryRegistry::new(policy).unwrap();
        let advertisement = sample_advertisement();
        let now = SystemTime::now();
        registry.upsert(advertisement.clone(), now).unwrap();

        assert_eq!(
            registry.upsert(advertisement, now + Duration::from_secs(1)).unwrap_err(),
            DiscoveryError::StaleSequence
        );
    }

    #[test]
    fn discovery_beacon_and_listener_exchange_advertisement_over_udp() {
        let policy = FreshnessPolicy {
            stale_after: Duration::from_secs(5),
            expire_after: Duration::from_secs(10),
        };
        let registry = Arc::new(MemoryRegistry::new(policy).unwrap());

        let listener =
            DiscoveryListener::bind("127.0.0.1:0", Arc::clone(&registry) as Arc<dyn Registry>)
                .expect("bind listener");
        let listener_addr = listener.local_addr().expect("local addr");

        let ad = sample_advertisement();
        let beacon = DiscoveryBeacon::bind("127.0.0.1:0", listener_addr.to_string(), ad.clone())
            .expect("bind beacon");

        beacon.broadcast_once().expect("broadcast");
        let received = listener.receive_once().expect("receive once");

        assert_eq!(received.participant, ad.participant);
        assert_eq!(received.sequence, ad.sequence);

        let member = registry.get(&ad.participant).expect("member registered");
        assert_eq!(member.advertisement.participant, ad.participant);
    }

    #[test]
    fn discovery_listener_until_shuts_down_cleanly() {
        let policy = FreshnessPolicy {
            stale_after: Duration::from_secs(5),
            expire_after: Duration::from_secs(10),
        };
        let registry = Arc::new(MemoryRegistry::new(policy).unwrap());
        let listener =
            DiscoveryListener::bind("127.0.0.1:0", Arc::clone(&registry) as Arc<dyn Registry>)
                .unwrap();
        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown_clone = Arc::clone(&shutdown);

        let handle = std::thread::spawn(move || listener.listen_until(&shutdown_clone));

        std::thread::sleep(Duration::from_millis(50));
        shutdown.store(true, Ordering::Relaxed);
        handle.join().unwrap().expect("clean shutdown");
    }

    #[test]
    fn discovery_fabric_fails_closed_for_unsigned_advertisements() {
        use crate::fabric::Fabric;

        let policy = FreshnessPolicy {
            stale_after: Duration::from_secs(60),
            expire_after: Duration::from_secs(120),
        };
        let registry = Arc::new(MemoryRegistry::new(policy).unwrap());
        let ad = sample_advertisement();
        registry.upsert(ad, SystemTime::now()).unwrap();

        assert!(DiscoveryFabric::new(registry).offers().is_empty());
    }

    #[test]
    fn discovery_fabric_yields_active_member_offers() {
        use crate::fabric::Fabric;

        let policy = FreshnessPolicy {
            stale_after: Duration::from_secs(60),
            expire_after: Duration::from_secs(120),
        };
        let registry = Arc::new(MemoryRegistry::new(policy).unwrap());
        let fabric = DiscoveryFabric::with_unverified_advertisements(registry.clone());

        // Empty registry produces no offers
        assert!(fabric.offers().is_empty());

        let ad = sample_advertisement();
        registry.upsert(ad.clone(), SystemTime::now()).unwrap();

        let offers = fabric.offers();
        assert_eq!(offers.len(), 1);
        assert_eq!(offers[0].participant_id, ad.participant.id);
        assert_eq!(offers[0].resources.cpu.cores, 4.0);
        assert_eq!(offers[0].resources.memory.bytes, 8 * 1024 * 1024 * 1024);
    }
    #[test]
    fn registry_rejects_wrong_identity_kind_on_lookup() {
        let policy = FreshnessPolicy {
            stale_after: Duration::from_secs(5),
            expire_after: Duration::from_secs(10),
        };
        let registry = MemoryRegistry::new(policy).unwrap();
        let advertisement = sample_advertisement();
        let participant_id = advertisement.participant.clone();
        registry.upsert(advertisement, SystemTime::now()).unwrap();

        let wrong_kind = Identity {
            id: participant_id.id,
            kind: Kind::LogicalNode,
        };
        assert_eq!(
            registry.get(&wrong_kind).unwrap_err(),
            DiscoveryError::InvalidAdvertisement
        );
    }

}
