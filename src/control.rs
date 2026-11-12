use crate::discovery::{Advertisement, Member, Registry as DiscoveryRegistry};
use crate::execution::{
    ExecutionBackend, ExecutionError, ExecutionPlan, ExecutionReceipt, WorkItem,
};
use crate::fabric::Fabric;
use crate::identity::{Identity, Kind};
use crate::lease::Lease;
use crate::node::{LogicalNode, NodeError, RuntimeSpec};
use crate::participant::Participant;
use crate::registry::Registry;
use crate::resource::ResourceFragment;
use crate::scheduler::{Scheduler, SchedulerError};
use crate::virtual_machine::{VirtualMachine, VirtualMachineError};
use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlError {
    InvalidTime,
    InvalidLeaseDuration,
    LeaseNotFound,
    VirtualMachineNotFound,
    ParticipantNotFound,
    ParticipantNotAllocated,
    LogicalNodeNotFound,
    ConcurrentModification,
    Scheduler(SchedulerError),
    Node(NodeError),
    Execution(ExecutionError),
    VirtualMachine(VirtualMachineError),
}

impl std::fmt::Display for ControlError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for ControlError {}

pub struct Plane {
    registry: Arc<Registry>,
    discovery: Arc<dyn DiscoveryRegistry>,
    scheduler: Arc<dyn Scheduler>,
    leases: RwLock<BTreeMap<String, Lease>>,
    virtual_machines: RwLock<BTreeMap<String, VirtualMachine>>,
    logical_nodes: RwLock<BTreeMap<String, LogicalNode>>,
}

impl Plane {
    pub fn new(
        registry: Arc<Registry>,
        discovery: Arc<dyn DiscoveryRegistry>,
        scheduler: Arc<dyn Scheduler>,
    ) -> Self {
        Self {
            registry,
            discovery,
            scheduler,
            leases: RwLock::new(BTreeMap::new()),
            virtual_machines: RwLock::new(BTreeMap::new()),
            logical_nodes: RwLock::new(BTreeMap::new()),
        }
    }

    pub fn register_participant(
        &self,
        participant: Arc<Participant>,
    ) -> Result<(), crate::registry::RegistryError> {
        self.registry.register(participant)
    }

    pub fn announce_participant(
        &self,
        advertisement: Advertisement,
        observed_at: SystemTime,
    ) -> Result<(), crate::discovery::DiscoveryError> {
        self.discovery.upsert(advertisement, observed_at)
    }

    pub fn remove_discovered_participant(
        &self,
        id: &Identity,
    ) -> Result<(), crate::discovery::DiscoveryError> {
        self.discovery.remove(id)
    }

    pub fn discovered_participants(&self) -> Vec<Member> {
        self.discovery.members()
    }

    pub fn discovered_participants_at(
        &self,
        now: SystemTime,
    ) -> Result<Vec<Member>, crate::discovery::DiscoveryError> {
        self.discovery
            .members_at(now)
            .map(|members| members.into_iter().map(|(member, _)| member).collect())
    }

    pub fn create_logical_node(
        &self,
        requirement: ResourceFragment,
    ) -> Result<LogicalNode, SchedulerError> {
        self.scheduler.plan(requirement)
    }

    pub fn create_logical_node_with_runtime(
        &self,
        requirement: ResourceFragment,
        runtime: RuntimeSpec,
    ) -> Result<LogicalNode, ControlError> {
        self.create_logical_node(requirement)
            .map_err(ControlError::Scheduler)?
            .with_runtime(runtime)
            .map_err(ControlError::Node)
    }

    pub fn create_virtual_machine(
        &self,
        node: &LogicalNode,
        created_at: SystemTime,
    ) -> Result<VirtualMachine, ControlError> {
        let vm =
            VirtualMachine::from_node(node, created_at).map_err(ControlError::VirtualMachine)?;
        self.logical_nodes
            .write()
            .expect("logical node registry lock poisoned")
            .insert(node.id.clone(), node.clone());
        self.virtual_machines
            .write()
            .expect("virtual machine registry lock poisoned")
            .insert(vm.id.id.clone(), vm.clone());
        Ok(vm)
    }

    pub fn create_leased_virtual_machine(
        &self,
        requirement: ResourceFragment,
        runtime: RuntimeSpec,
        created_at: SystemTime,
        duration: Duration,
    ) -> Result<(VirtualMachine, Vec<Lease>), ControlError> {
        let (node, leases) = self.create_leased_logical_node_with_runtime(
            requirement,
            runtime,
            created_at,
            duration,
        )?;

        match VirtualMachine::from_leased_node(&node, leases.clone(), created_at) {
            Ok(vm) => {
                self.logical_nodes
                    .write()
                    .expect("logical node registry lock poisoned")
                    .insert(node.id.clone(), node.clone());
                self.virtual_machines
                    .write()
                    .expect("virtual machine registry lock poisoned")
                    .insert(vm.id.id.clone(), vm.clone());
                Ok((vm, leases))
            }
            Err(error) => {
                self.remove_leases(&leases);
                Err(ControlError::VirtualMachine(error))
            }
        }
    }

    pub fn get_virtual_machine(&self, id: &Identity) -> Result<VirtualMachine, ControlError> {
        if id.kind != Kind::VirtualMachine || id.validate().is_err() {
            return Err(ControlError::VirtualMachineNotFound);
        }

        self.virtual_machines
            .read()
            .expect("virtual machine registry lock poisoned")
            .get(&id.id)
            .cloned()
            .ok_or(ControlError::VirtualMachineNotFound)
    }

    pub fn start_virtual_machine(&self, id: &Identity) -> Result<VirtualMachine, ControlError> {
        let mut vms = self
            .virtual_machines
            .write()
            .expect("virtual machine registry lock poisoned");
        let vm = vms
            .get_mut(&id.id)
            .ok_or(ControlError::VirtualMachineNotFound)?;
        vm.start().map_err(ControlError::VirtualMachine)?;
        Ok(vm.clone())
    }

    pub fn stop_virtual_machine(&self, id: &Identity) -> Result<VirtualMachine, ControlError> {
        let mut vms = self
            .virtual_machines
            .write()
            .expect("virtual machine registry lock poisoned");
        let vm = vms
            .get_mut(&id.id)
            .ok_or(ControlError::VirtualMachineNotFound)?;
        vm.stop().map_err(ControlError::VirtualMachine)?;
        Ok(vm.clone())
    }

    pub fn destroy_virtual_machine(&self, id: &Identity) -> Result<VirtualMachine, ControlError> {
        let vm = {
            let mut vms = self
                .virtual_machines
                .write()
                .expect("virtual machine registry lock poisoned");
            vms.remove(&id.id)
                .ok_or(ControlError::VirtualMachineNotFound)?
        };

        if !vm.leases.is_empty() {
            self.release_leases(&vm.leases)?;
        }

        self.logical_nodes
            .write()
            .expect("logical node registry lock poisoned")
            .remove(&vm.node_id.id);
        Ok(vm)
    }

    pub fn execute_virtual_machine<B: ExecutionBackend>(
        &self,
        id: &Identity,
        workload: &WorkItem,
        backend: &B,
    ) -> Result<ExecutionReceipt, ControlError> {
        let vm_snapshot = self.get_virtual_machine(id)?;

        let node = self
            .logical_nodes
            .read()
            .expect("logical node registry lock poisoned")
            .get(&vm_snapshot.node_id.id)
            .cloned()
            .ok_or(ControlError::VirtualMachineNotFound)?;

        // Execute against a snapshot without holding registry locks across backend I/O.
        // Commit the resulting VM state only if no lifecycle operation changed or removed
        // the VM in the meantime. In particular, never reinsert a VM destroyed mid-execution.
        let mut updated_vm = vm_snapshot.clone();
        let result = updated_vm.execute(&node, workload, backend);

        let mut virtual_machines = self
            .virtual_machines
            .write()
            .expect("virtual machine registry lock poisoned");
        if virtual_machines.get(&vm_snapshot.id.id) != Some(&vm_snapshot) {
            return Err(ControlError::ConcurrentModification);
        }
        virtual_machines.insert(updated_vm.id.id.clone(), updated_vm);

        result.map_err(ControlError::VirtualMachine)
    }

    pub fn audit_virtual_machine_health(
        &self,
        id: &Identity,
        now: SystemTime,
    ) -> Result<Vec<Identity>, ControlError> {
        if now == SystemTime::UNIX_EPOCH {
            return Err(ControlError::InvalidTime);
        }

        let vm = self.get_virtual_machine(id)?;
        let mut unhealthy = Vec::new();

        for lease in &vm.leases {
            if !lease.active_at(now) {
                unhealthy.push(lease.participant_id.clone());
                continue;
            }

            match self.registry.get(&lease.participant_id) {
                Ok(participant) => {
                    if participant.snapshot().state != crate::participant::State::Active {
                        unhealthy.push(lease.participant_id.clone());
                    }
                }
                Err(_) => {
                    // Discovery advertisements are currently unsigned hints. A missing local
                    // participant cannot be considered healthy based on an unauthenticated UDP
                    // advertisement, so fail closed until authenticated liveness is available.
                    unhealthy.push(lease.participant_id.clone());
                }
            }
        }

        Ok(unhealthy)
    }

    pub fn heal_virtual_machine(
        &self,
        vm_id: &Identity,
        failed_participant_id: &Identity,
        now: SystemTime,
        lease_duration: Duration,
    ) -> Result<(VirtualMachine, Lease), ControlError> {
        if now == SystemTime::UNIX_EPOCH {
            return Err(ControlError::InvalidTime);
        }
        if lease_duration.is_zero() {
            return Err(ControlError::InvalidLeaseDuration);
        }
        if failed_participant_id.kind != Kind::Participant
            || failed_participant_id.validate().is_err()
        {
            return Err(ControlError::ParticipantNotAllocated);
        }

        // Build a complete replacement off to the side. No live lease or registry entry is
        // mutated until all validation and composition has succeeded.
        let vm_snapshot = self.get_virtual_machine(vm_id)?;
        let node_snapshot = self
            .logical_nodes
            .read()
            .expect("logical node registry lock poisoned")
            .get(&vm_snapshot.node_id.id)
            .cloned()
            .ok_or(ControlError::VirtualMachineNotFound)?;

        let allocation_index = node_snapshot
            .resources
            .allocations
            .iter()
            .position(|allocation| allocation.participant_id == failed_participant_id.id)
            .ok_or(ControlError::ParticipantNotAllocated)?;
        let old_lease_index = vm_snapshot
            .leases
            .iter()
            .position(|lease| lease.participant_id == *failed_participant_id)
            .ok_or(ControlError::LeaseNotFound)?;
        let old_lease = vm_snapshot.leases[old_lease_index].clone();
        // The maintenance reaper may already have removed an expired lease. Capture the
        // current stored value (including absence) and compare it again at commit time.
        let old_stored_lease = self
            .leases
            .read()
            .expect("lease registry lock poisoned")
            .get(&old_lease.id.id)
            .cloned();
        if old_stored_lease
            .as_ref()
            .is_some_and(|stored| stored != &old_lease)
        {
            return Err(ControlError::ConcurrentModification);
        }

        let needed_resources = node_snapshot.resources.allocations[allocation_index].resources;
        let existing_participants: std::collections::HashSet<_> = node_snapshot
            .resources
            .allocations
            .iter()
            .map(|allocation| allocation.participant_id.clone())
            .collect();

        let replacement_offer = self
            .registry
            .offers()
            .into_iter()
            .filter(|offer| !existing_participants.contains(&offer.participant_id))
            .find(|offer| {
                offer.resources.validate_requirement().is_ok()
                    && offer.resources.satisfies(&needed_resources)
                    && Identity::parse(&offer.participant_id, Kind::Participant).is_ok()
            })
            .ok_or(ControlError::Scheduler(
                SchedulerError::InsufficientResources,
            ))?;
        let replacement_participant_id =
            Identity::parse(&replacement_offer.participant_id, Kind::Participant)
                .map_err(|_| ControlError::Scheduler(SchedulerError::InvalidOffer))?;

        // Preserve the allocation's resource snapshot (including its lifetime) so the
        // recomposed node remains exactly compatible with the VM specification. The lease
        // duration is capped by that snapshot rather than changing the allocation itself.
        let resources = needed_resources;
        let effective_duration = resources
            .lifetime
            .map_or(lease_duration, |maximum| lease_duration.min(maximum));
        if effective_duration.is_zero() {
            return Err(ControlError::InvalidLeaseDuration);
        }
        let expires_at = now
            .checked_add(effective_duration)
            .ok_or(ControlError::InvalidTime)?;
        let new_lease = Lease::new(
            Identity::new(Kind::Lease),
            vm_snapshot.node_id.clone(),
            replacement_participant_id.clone(),
            resources,
            now,
            expires_at,
        )
        .map_err(|_| ControlError::Scheduler(SchedulerError::InvalidOffer))?;

        let mut replacement_node = node_snapshot.clone();
        replacement_node.resources.allocations[allocation_index] =
            crate::resource::Allocation {
                participant_id: replacement_participant_id.id.clone(),
                resources,
            };
        replacement_node.resources =
            crate::resource::CompositeResource::compose(
                replacement_node.resources.allocations.clone(),
            )
            .map_err(|_| ControlError::Scheduler(SchedulerError::InvalidOffer))?;
        replacement_node
            .resources
            .validate()
            .map_err(|_| ControlError::Scheduler(SchedulerError::InvalidOffer))?;
        if replacement_node.resources.capacity != node_snapshot.resources.capacity {
            return Err(ControlError::Scheduler(SchedulerError::InvalidOffer));
        }

        let mut replacement_vm = vm_snapshot.clone();
        replacement_vm.leases[old_lease_index] = new_lease.clone();
        if replacement_vm.state == crate::virtual_machine::VirtualMachineState::Failed
            && replacement_vm.leases.iter().all(|lease| {
                lease.active_at(now)
                    && self.registry.get(&lease.participant_id).is_ok_and(|participant| {
                        participant.snapshot().state == crate::participant::State::Active
                    })
            })
        {
            replacement_vm.state = crate::virtual_machine::VirtualMachineState::Running;
        }

        // Commit under a fixed lock order and reject stale snapshots. This prevents an
        // in-flight heal from restoring an older VM/node snapshot over concurrent lifecycle
        // changes such as stop or destroy.
        let mut leases = self.leases.write().expect("lease registry lock poisoned");
        let mut nodes = self
            .logical_nodes
            .write()
            .expect("logical node registry lock poisoned");
        let mut virtual_machines = self
            .virtual_machines
            .write()
            .expect("virtual machine registry lock poisoned");

        if virtual_machines.get(&vm_snapshot.id.id) != Some(&vm_snapshot)
            || nodes.get(&node_snapshot.id) != Some(&node_snapshot)
            || leases.get(&old_lease.id.id).cloned() != old_stored_lease
        {
            return Err(ControlError::ConcurrentModification);
        }

        leases.remove(&old_lease.id.id);
        leases.insert(new_lease.id.id.clone(), new_lease.clone());
        nodes.insert(replacement_node.id.clone(), replacement_node);
        virtual_machines.insert(replacement_vm.id.id.clone(), replacement_vm.clone());

        Ok((replacement_vm, new_lease))
    }

    pub fn release_lease(&self, id: &Identity) -> Result<Lease, ControlError> {
        if id.kind != Kind::Lease || id.validate().is_err() {
            return Err(ControlError::LeaseNotFound);
        }

        self.leases
            .write()
            .expect("lease registry lock poisoned")
            .remove(&id.id)
            .ok_or(ControlError::LeaseNotFound)
    }

    pub fn release_leases(&self, leases: &[Lease]) -> Result<(), ControlError> {
        for lease in leases {
            self.release_lease(&lease.id)?;
        }
        Ok(())
    }

    fn remove_leases(&self, leases: &[Lease]) {
        let mut stored = self.leases.write().expect("lease registry lock poisoned");
        for lease in leases {
            stored.remove(&lease.id.id);
        }
    }

    pub fn create_execution_plan(
        &self,
        node: &LogicalNode,
        workload: &WorkItem,
    ) -> Result<ExecutionPlan, ControlError> {
        ExecutionPlan::from_node(node, workload).map_err(ControlError::Execution)
    }

    pub fn dispatch_execution_plan<T: crate::transport::Transport>(
        &self,
        plan: &ExecutionPlan,
        workload: &WorkItem,
        endpoints: &[crate::transport::ExecutionEndpoint],
        transport: T,
    ) -> Result<crate::execution::DispatchReceipt, ControlError> {
        crate::execution::ExecutionDispatcher::new(transport, endpoints.to_vec())
            .dispatch(plan, workload)
            .map_err(ControlError::Execution)
    }

    /// Dispatch with an explicit caller identity. Required for mTLS transports with
    /// participant-to-certificate authorization enabled on remote endpoints.
    pub fn dispatch_execution_plan_as<T: crate::transport::Transport>(
        &self,
        source_participant_id: Identity,
        plan: &ExecutionPlan,
        workload: &WorkItem,
        endpoints: &[crate::transport::ExecutionEndpoint],
        transport: T,
    ) -> Result<crate::execution::DispatchReceipt, ControlError> {
        crate::execution::ExecutionDispatcher::with_identity(
            source_participant_id,
            transport,
            endpoints.to_vec(),
        )
        .dispatch(plan, workload)
        .map_err(ControlError::Execution)
    }

    pub fn create_leased_logical_node_with_runtime(
        &self,
        requirement: ResourceFragment,
        runtime: RuntimeSpec,
        now: SystemTime,
        duration: Duration,
    ) -> Result<(LogicalNode, Vec<Lease>), ControlError> {
        runtime.validate().map_err(ControlError::Node)?;
        let (node, leases) = self.create_leased_logical_node(requirement, now, duration)?;
        let node = node.with_runtime(runtime).map_err(ControlError::Node)?;
        Ok((node, leases))
    }

    pub fn create_leased_logical_node(
        &self,
        requirement: ResourceFragment,
        now: SystemTime,
        duration: Duration,
    ) -> Result<(LogicalNode, Vec<Lease>), ControlError> {
        if now == SystemTime::UNIX_EPOCH {
            return Err(ControlError::InvalidTime);
        }
        if duration.is_zero() {
            return Err(ControlError::InvalidLeaseDuration);
        }

        let node = self
            .scheduler
            .plan(requirement)
            .map_err(ControlError::Scheduler)?;
        let node_id = Identity::parse(&node.id, Kind::LogicalNode)
            .map_err(|_| ControlError::Scheduler(SchedulerError::InsufficientResources))?;
        let mut leases = Vec::with_capacity(node.resources.allocations.len());

        for allocation in &node.resources.allocations {
            let participant_id = Identity::parse(&allocation.participant_id, Kind::Participant)
                .map_err(|_| ControlError::Scheduler(SchedulerError::InvalidOffer))?;
            let resources = allocation.resources;
            let allocation_duration = resources
                .lifetime
                .map_or(duration, |maximum| duration.min(maximum));
            if allocation_duration.is_zero() {
                return Err(ControlError::InvalidLeaseDuration);
            }
            let expires_at = now
                .checked_add(allocation_duration)
                .ok_or(ControlError::InvalidTime)?;
            let lease = Lease::new(
                Identity::new(Kind::Lease),
                node_id.clone(),
                participant_id,
                resources,
                now,
                expires_at,
            )
            .map_err(|_| ControlError::Scheduler(SchedulerError::InvalidOffer))?;
            leases.push(lease);
        }

        let mut stored = self.leases.write().expect("lease registry lock poisoned");
        for lease in &leases {
            stored.insert(lease.id.id.clone(), lease.clone());
        }
        Ok((node, leases))
    }

    pub fn get_lease(&self, id: &Identity) -> Result<Lease, ControlError> {
        if id.kind != Kind::Lease || id.validate().is_err() {
            return Err(ControlError::LeaseNotFound);
        }
        self.leases
            .read()
            .expect("lease registry lock poisoned")
            .get(&id.id)
            .cloned()
            .ok_or(ControlError::LeaseNotFound)
    }

    pub fn list_virtual_machines(&self) -> Vec<VirtualMachine> {
        self.virtual_machines
            .read()
            .expect("virtual machine registry lock poisoned")
            .values()
            .cloned()
            .collect()
    }

    pub fn list_logical_nodes(&self) -> Vec<LogicalNode> {
        self.logical_nodes
            .read()
            .expect("logical node registry lock poisoned")
            .values()
            .cloned()
            .collect()
    }

    pub fn get_logical_node(&self, id: &str) -> Result<LogicalNode, ControlError> {
        self.logical_nodes
            .read()
            .expect("logical node registry lock poisoned")
            .get(id)
            .cloned()
            .ok_or(ControlError::LogicalNodeNotFound)
    }

    pub fn list_leases(&self) -> Vec<Lease> {
        self.leases
            .read()
            .expect("lease registry lock poisoned")
            .values()
            .cloned()
            .collect()
    }

    pub fn reap_expired_leases(&self, now: SystemTime) -> Result<Vec<Lease>, ControlError> {
        if now == SystemTime::UNIX_EPOCH {
            return Err(ControlError::InvalidTime);
        }
        let mut leases = self.leases.write().expect("lease registry lock poisoned");
        let mut expired = Vec::new();
        leases.retain(|_, lease| {
            if now >= lease.expires_at {
                expired.push(lease.clone());
                false
            } else {
                true
            }
        });
        Ok(expired)
    }
}

#[cfg(test)]
mod leased_vm_tests {
    use super::*;
    use crate::capability::{
        Capability, ComputeCapability, Latency, Lifetime, MemoryCapability, NetworkCapability,
        Reliability, Security, StorageCapability,
    };
    use crate::discovery::{FreshnessPolicy, MemoryRegistry as DiscoveryMemoryRegistry};
    use crate::fabric::Fabric;
    use crate::participant::Participant;
    use crate::scheduler::AggregatingScheduler;
    use std::sync::Arc;

    fn plane() -> Plane {
        let registry: Arc<Registry> = Arc::new(Registry::default());
        let fabric: Arc<dyn Fabric> = registry.clone();
        let discovery: Arc<dyn DiscoveryRegistry> = Arc::new(
            DiscoveryMemoryRegistry::new(FreshnessPolicy {
                stale_after: Duration::from_secs(60),
                expire_after: Duration::from_secs(120),
            })
            .unwrap(),
        );
        let scheduler: Arc<dyn Scheduler> = Arc::new(AggregatingScheduler::new(fabric));
        Plane::new(registry, discovery, scheduler)
    }

    fn active_participant() -> Arc<Participant> {
        let participant = Arc::new(
            Participant::new(
                Identity::new(Kind::Participant),
                Capability {
                    compute: ComputeCapability {
                        cpu_cores: 2.0,
                        gpu_units: 0,
                    },
                    memory: MemoryCapability { bytes: 0 },
                    storage: StorageCapability { bytes: 0 },
                    network: NetworkCapability { bits_per_second: 0 },
                    reliability: Reliability { availability: 1.0 },
                    latency: Latency {
                        to_participant: Duration::from_millis(1),
                    },
                    lifetime: Lifetime { duration: None },
                    security: Security { trusted: true },
                },
                ResourceFragment {
                    cpu: crate::resource::Cpu { cores: 2.0 },
                    ..Default::default()
                },
                SystemTime::UNIX_EPOCH + Duration::from_secs(1),
            )
            .unwrap(),
        );
        participant
            .activate(SystemTime::UNIX_EPOCH + Duration::from_secs(2))
            .unwrap();
        participant
    }

    #[test]
    fn leased_vm_caps_expiry_to_participant_allocation_lifetime() {
        let plane = plane();
        let lifetime = Duration::from_secs(25);
        let participant = Arc::new(
            Participant::new(
                Identity::new(Kind::Participant),
                Capability {
                    compute: ComputeCapability {
                        cpu_cores: 2.0,
                        gpu_units: 0,
                    },
                    memory: MemoryCapability { bytes: 0 },
                    storage: StorageCapability { bytes: 0 },
                    network: NetworkCapability { bits_per_second: 0 },
                    reliability: Reliability { availability: 1.0 },
                    latency: Latency {
                        to_participant: Duration::from_millis(1),
                    },
                    lifetime: Lifetime {
                        duration: Some(lifetime),
                    },
                    security: Security { trusted: true },
                },
                ResourceFragment {
                    cpu: crate::resource::Cpu { cores: 2.0 },
                    lifetime: Some(lifetime),
                    ..Default::default()
                },
                SystemTime::UNIX_EPOCH + Duration::from_secs(1),
            )
            .unwrap(),
        );
        participant
            .activate(SystemTime::UNIX_EPOCH + Duration::from_secs(2))
            .unwrap();
        plane.register_participant(participant).unwrap();

        let created_at = SystemTime::UNIX_EPOCH + Duration::from_secs(100);
        let (_, leases) = plane
            .create_leased_virtual_machine(
                ResourceFragment {
                    cpu: crate::resource::Cpu { cores: 1.0 },
                    ..Default::default()
                },
                RuntimeSpec::new("wasm", "1"),
                created_at,
                Duration::from_secs(60),
            )
            .unwrap();

        assert_eq!(leases.len(), 1);
        assert_eq!(leases[0].expires_at.duration_since(created_at).unwrap(), lifetime);
        assert_eq!(leases[0].resources.lifetime, Some(lifetime));
    }

    #[test]
    fn invalid_runtime_does_not_create_or_leak_leases() {
        let plane = plane();
        plane.register_participant(active_participant()).unwrap();

        let result = plane.create_leased_logical_node_with_runtime(
            ResourceFragment {
                cpu: crate::resource::Cpu { cores: 1.0 },
                ..Default::default()
            },
            RuntimeSpec::new("", "1"),
            SystemTime::now(),
            Duration::from_secs(60),
        );

        assert_eq!(result.unwrap_err(), ControlError::Node(NodeError::EmptyRuntimeName));
        assert!(plane.list_leases().is_empty());
    }

    #[test]
    fn control_plane_creates_vm_from_owned_leases() {
        let plane = plane();
        let participant = active_participant();
        plane
            .register_participant(Arc::clone(&participant))
            .unwrap();

        let created_at = SystemTime::now();
        let (vm, leases) = plane
            .create_leased_virtual_machine(
                ResourceFragment {
                    cpu: crate::resource::Cpu { cores: 1.0 },
                    ..Default::default()
                },
                RuntimeSpec::new("wasm", "1"),
                created_at,
                Duration::from_secs(60),
            )
            .unwrap();

        assert_eq!(vm.leases, leases);
        assert_eq!(vm.node_id.kind, Kind::LogicalNode);
        for lease in &leases {
            assert_eq!(plane.get_lease(&lease.id).unwrap(), *lease);
        }
    }

    #[test]
    fn control_plane_owns_virtual_machine_lifecycle() {
        let plane = plane();
        plane.register_participant(active_participant()).unwrap();

        let created_at = SystemTime::now();
        let (vm, leases) = plane
            .create_leased_virtual_machine(
                ResourceFragment {
                    cpu: crate::resource::Cpu { cores: 1.0 },
                    ..Default::default()
                },
                RuntimeSpec::new("wasm", "1"),
                created_at,
                Duration::from_secs(60),
            )
            .unwrap();

        assert_eq!(plane.get_virtual_machine(&vm.id).unwrap(), vm);

        let started = plane.start_virtual_machine(&vm.id).unwrap();
        assert_eq!(
            started.state,
            crate::virtual_machine::VirtualMachineState::Running
        );
        assert_eq!(
            plane.get_virtual_machine(&vm.id).unwrap().state,
            crate::virtual_machine::VirtualMachineState::Running
        );

        let stopped = plane.stop_virtual_machine(&vm.id).unwrap();
        assert_eq!(
            stopped.state,
            crate::virtual_machine::VirtualMachineState::Stopped
        );

        let destroyed = plane.destroy_virtual_machine(&vm.id).unwrap();
        assert_eq!(destroyed.id, vm.id);

        assert_eq!(
            plane.get_virtual_machine(&vm.id).unwrap_err(),
            ControlError::VirtualMachineNotFound
        );
        for lease in leases {
            assert_eq!(
                plane.get_lease(&lease.id).unwrap_err(),
                ControlError::LeaseNotFound
            );
        }
    }

    #[test]
    fn control_plane_executes_workload_through_owned_vm() {
        let plane = plane();
        plane.register_participant(active_participant()).unwrap();
        let created_at = SystemTime::now();
        let (vm, _) = plane
            .create_leased_virtual_machine(
                ResourceFragment {
                    cpu: crate::resource::Cpu { cores: 1.0 },
                    ..Default::default()
                },
                RuntimeSpec::new("wasm", "1"),
                created_at,
                Duration::from_secs(60),
            )
            .unwrap();
        plane.start_virtual_machine(&vm.id).unwrap();
        let workload = WorkItem {
            id: "control-plane-workload".into(),
            payload: vec![1, 2, 3],
        };
        let receipt = plane
            .execute_virtual_machine(&vm.id, &workload, &crate::execution::NoopBackend)
            .unwrap();
        assert_eq!(receipt.workload_id, workload.id);
        assert_eq!(receipt.node_id, vm.node_id);
        assert_eq!(receipt.dispatched_units, 1);
        assert_eq!(
            plane.get_virtual_machine(&vm.id).unwrap().state,
            crate::virtual_machine::VirtualMachineState::Running
        );
    }

    #[test]
    fn control_plane_lookup_does_not_wait_for_execution() {
        use std::sync::mpsc;
        use std::thread;

        #[derive(Debug, Clone)]
        struct BlockingBackend {
            started: Arc<std::sync::Barrier>,
            release: Arc<std::sync::Barrier>,
        }

        impl ExecutionBackend for BlockingBackend {
            fn execute(
                &self,
                _: &ExecutionPlan,
                workload: &WorkItem,
            ) -> Result<ExecutionReceipt, ExecutionError> {
                self.started.wait();
                self.release.wait();
                Ok(ExecutionReceipt {
                    workload_id: workload.id.clone(),
                    node_id: Identity::new(Kind::LogicalNode),
                    dispatched_units: 1,
                })
            }
        }

        let plane = Arc::new(plane());
        plane.register_participant(active_participant()).unwrap();
        let created_at = SystemTime::now();
        let (vm, _) = plane
            .create_leased_virtual_machine(
                ResourceFragment {
                    cpu: crate::resource::Cpu { cores: 1.0 },
                    ..Default::default()
                },
                RuntimeSpec::new("wasm", "1"),
                created_at,
                Duration::from_secs(60),
            )
            .unwrap();
        plane.start_virtual_machine(&vm.id).unwrap();

        let started = Arc::new(std::sync::Barrier::new(2));
        let release = Arc::new(std::sync::Barrier::new(2));
        let backend = BlockingBackend {
            started: Arc::clone(&started),
            release: Arc::clone(&release),
        };
        let workload = WorkItem {
            id: "lookup-during-execution".into(),
            payload: Vec::new(),
        };

        let execute_plane = Arc::clone(&plane);
        let execute_id = vm.id.clone();
        let execute_handle = thread::spawn(move || {
            execute_plane.execute_virtual_machine(&execute_id, &workload, &backend)
        });

        started.wait();

        let (tx, rx) = mpsc::channel();
        let lookup_plane = Arc::clone(&plane);
        let lookup_id = vm.id.clone();
        thread::spawn(move || {
            tx.send(lookup_plane.get_virtual_machine(&lookup_id))
                .unwrap();
        });

        let visible_vm = rx
            .recv_timeout(Duration::from_secs(1))
            .expect("VM lookup blocked while execution was running")
            .unwrap();
        assert_eq!(visible_vm.id, vm.id);
        assert_eq!(
            visible_vm.state,
            crate::virtual_machine::VirtualMachineState::Running
        );

        release.wait();
        execute_handle.join().unwrap().unwrap();
    }

    #[test]
    fn control_plane_vm_remains_visible_during_execution() {
        use std::sync::{Arc, Barrier};
        use std::thread;

        #[derive(Debug, Clone)]
        struct BlockingBackend {
            started: Arc<Barrier>,
            release: Arc<Barrier>,
        }

        impl ExecutionBackend for BlockingBackend {
            fn execute(
                &self,
                _: &ExecutionPlan,
                workload: &WorkItem,
            ) -> Result<ExecutionReceipt, ExecutionError> {
                self.started.wait();
                self.release.wait();
                Ok(ExecutionReceipt {
                    workload_id: workload.id.clone(),
                    node_id: Identity::new(Kind::LogicalNode),
                    dispatched_units: 1,
                })
            }
        }

        let plane = Arc::new(plane());
        plane.register_participant(active_participant()).unwrap();

        let created_at = SystemTime::now();
        let (vm, _) = plane
            .create_leased_virtual_machine(
                ResourceFragment {
                    cpu: crate::resource::Cpu { cores: 1.0 },
                    ..Default::default()
                },
                RuntimeSpec::new("wasm", "1"),
                created_at,
                Duration::from_secs(60),
            )
            .unwrap();
        plane.start_virtual_machine(&vm.id).unwrap();

        let started = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        let backend = BlockingBackend {
            started: Arc::clone(&started),
            release: Arc::clone(&release),
        };
        let workload = WorkItem {
            id: "concurrent-workload".into(),
            payload: Vec::new(),
        };

        let execute_plane = Arc::clone(&plane);
        let execute_id = vm.id.clone();
        let execute_handle = thread::spawn(move || {
            execute_plane.execute_virtual_machine(&execute_id, &workload, &backend)
        });

        started.wait();

        let lookup_plane = Arc::clone(&plane);
        let lookup_id = vm.id.clone();
        let lookup_handle = thread::spawn(move || lookup_plane.get_virtual_machine(&lookup_id));

        release.wait();

        let receipt = execute_handle.join().unwrap().unwrap();
        let visible_vm = lookup_handle.join().unwrap().unwrap();

        assert_eq!(receipt.workload_id, "concurrent-workload");
        assert_eq!(visible_vm.id, vm.id);
        assert_eq!(
            visible_vm.state,
            crate::virtual_machine::VirtualMachineState::Running
        );
    }

    #[test]
    fn control_plane_vm_survives_fabric_resource_change() {
        let plane = plane();
        let participant = active_participant();
        let participant_id = participant.snapshot().id;
        plane.register_participant(participant).unwrap();

        let created_at = SystemTime::now();
        let (vm, leases) = plane
            .create_leased_virtual_machine(
                ResourceFragment {
                    cpu: crate::resource::Cpu { cores: 1.0 },
                    ..Default::default()
                },
                RuntimeSpec::new("wasm", "1"),
                created_at,
                Duration::from_secs(60),
            )
            .unwrap();

        plane.start_virtual_machine(&vm.id).unwrap();

        plane.registry.remove(&participant_id).unwrap();

        let workload = WorkItem {
            id: "fabric-changed-workload".into(),
            payload: vec![7, 8, 9],
        };
        let receipt = plane
            .execute_virtual_machine(&vm.id, &workload, &crate::execution::NoopBackend)
            .unwrap();

        assert_eq!(receipt.workload_id, workload.id);
        assert_eq!(receipt.node_id, vm.node_id);
        assert_eq!(receipt.dispatched_units, 1);
        assert_eq!(
            plane.get_virtual_machine(&vm.id).unwrap().state,
            crate::virtual_machine::VirtualMachineState::Running
        );
        assert_eq!(plane.get_lease(&leases[0].id).unwrap(), leases[0]);
    }

    #[test]
    fn control_plane_execution_rejects_stopped_vm() {
        let plane = plane();
        plane.register_participant(active_participant()).unwrap();
        let created_at = SystemTime::UNIX_EPOCH + Duration::from_secs(100);
        let (vm, _) = plane
            .create_leased_virtual_machine(
                ResourceFragment {
                    cpu: crate::resource::Cpu { cores: 1.0 },
                    ..Default::default()
                },
                RuntimeSpec::new("wasm", "1"),
                created_at,
                Duration::from_secs(60),
            )
            .unwrap();
        let workload = WorkItem {
            id: "stopped-workload".into(),
            payload: Vec::new(),
        };
        assert_eq!(
            plane
                .execute_virtual_machine(&vm.id, &workload, &crate::execution::NoopBackend)
                .unwrap_err(),
            ControlError::VirtualMachine(VirtualMachineError::NotRunning)
        );
    }

    #[test]
    fn control_plane_execution_marks_vm_failed_on_backend_error() {
        #[derive(Debug, Clone, Copy)]
        struct FailingBackend;
        impl ExecutionBackend for FailingBackend {
            fn execute(
                &self,
                _: &ExecutionPlan,
                _: &WorkItem,
            ) -> Result<ExecutionReceipt, ExecutionError> {
                Err(ExecutionError::BackendRejected)
            }
        }
        let plane = plane();
        plane.register_participant(active_participant()).unwrap();
        let created_at = SystemTime::now();
        let (vm, _) = plane
            .create_leased_virtual_machine(
                ResourceFragment {
                    cpu: crate::resource::Cpu { cores: 1.0 },
                    ..Default::default()
                },
                RuntimeSpec::new("wasm", "1"),
                created_at,
                Duration::from_secs(60),
            )
            .unwrap();
        plane.start_virtual_machine(&vm.id).unwrap();
        let workload = WorkItem {
            id: "failed-workload".into(),
            payload: Vec::new(),
        };
        assert_eq!(
            plane
                .execute_virtual_machine(&vm.id, &workload, &FailingBackend)
                .unwrap_err(),
            ControlError::VirtualMachine(VirtualMachineError::Execution(
                ExecutionError::BackendRejected
            ))
        );
        assert_eq!(
            plane.get_virtual_machine(&vm.id).unwrap().state,
            crate::virtual_machine::VirtualMachineState::Failed
        );
    }

    #[test]
    fn control_plane_releases_leases() {
        let plane = plane();
        let participant = active_participant();
        plane.register_participant(participant).unwrap();

        let created_at = SystemTime::UNIX_EPOCH + Duration::from_secs(100);
        let (_, leases) = plane
            .create_leased_virtual_machine(
                ResourceFragment {
                    cpu: crate::resource::Cpu { cores: 1.0 },
                    ..Default::default()
                },
                RuntimeSpec::new("wasm", "1"),
                created_at,
                Duration::from_secs(60),
            )
            .unwrap();

        plane.release_leases(&leases).unwrap();

        for lease in &leases {
            assert_eq!(
                plane.get_lease(&lease.id).unwrap_err(),
                ControlError::LeaseNotFound
            );
        }
    }

    #[test]
    fn control_plane_audit_marks_missing_local_participants_unhealthy() {
        let plane = plane();
        let participant = active_participant();
        let participant_id = participant.snapshot().id.clone();
        plane.register_participant(participant).unwrap();

        let created_at = SystemTime::UNIX_EPOCH + Duration::from_secs(500);
        let (vm, _) = plane
            .create_leased_virtual_machine(
                ResourceFragment {
                    cpu: crate::resource::Cpu { cores: 1.0 },
                    ..Default::default()
                },
                RuntimeSpec::new("wasm", "1"),
                created_at,
                Duration::from_secs(60),
            )
            .unwrap();

        plane.registry.remove(&participant_id).unwrap();
        assert_eq!(
            plane
                .audit_virtual_machine_health(&vm.id, created_at + Duration::from_secs(1))
                .unwrap(),
            vec![participant_id]
        );
    }

    #[test]
    fn failed_heal_preserves_vm_node_and_original_lease() {
        let plane = plane();
        let participant = active_participant();
        let participant_id = participant.snapshot().id.clone();
        plane.register_participant(Arc::clone(&participant)).unwrap();

        let created_at = SystemTime::UNIX_EPOCH + Duration::from_secs(700);
        let (vm, leases) = plane
            .create_leased_virtual_machine(
                ResourceFragment {
                    cpu: crate::resource::Cpu { cores: 1.0 },
                    ..Default::default()
                },
                RuntimeSpec::new("wasm", "1"),
                created_at,
                Duration::from_secs(60),
            )
            .unwrap();
        participant
            .drain(created_at + Duration::from_secs(1))
            .unwrap();

        let vm_before = plane.get_virtual_machine(&vm.id).unwrap();
        let node_before = plane.get_logical_node(&vm.node_id.id).unwrap();
        let lease_before = plane.get_lease(&leases[0].id).unwrap();

        assert_eq!(
            plane
                .heal_virtual_machine(
                    &vm.id,
                    &participant_id,
                    created_at + Duration::from_secs(1),
                    Duration::from_secs(30),
                )
                .unwrap_err(),
            ControlError::Scheduler(SchedulerError::InsufficientResources)
        );

        assert_eq!(plane.get_virtual_machine(&vm.id).unwrap(), vm_before);
        assert_eq!(plane.get_logical_node(&vm.node_id.id).unwrap(), node_before);
        assert_eq!(plane.get_lease(&leases[0].id).unwrap(), lease_before);
        assert_eq!(plane.list_leases().len(), 1);
    }

    #[test]
    fn control_plane_audits_unhealthy_participants_and_heals_vm() {
        let plane = plane();
        let p1 = active_participant();
        let p2 = active_participant();
        let p1_id = p1.snapshot().id;
        let p2_id = p2.snapshot().id;

        plane.register_participant(Arc::clone(&p1)).unwrap();

        let created_at = SystemTime::UNIX_EPOCH + Duration::from_secs(100);
        let (vm, leases) = plane
            .create_leased_virtual_machine(
                ResourceFragment {
                    cpu: crate::resource::Cpu { cores: 1.0 },
                    ..Default::default()
                },
                RuntimeSpec::new("wasm", "1"),
                created_at,
                Duration::from_secs(60),
            )
            .unwrap();

        assert_eq!(leases.len(), 1);
        assert_eq!(leases[0].participant_id, p1_id);

        // p1 drains (becomes unhealthy)
        p1.drain(created_at + Duration::from_secs(10)).unwrap();

        // Audit finds p1 unhealthy
        let unhealthy = plane
            .audit_virtual_machine_health(&vm.id, created_at + Duration::from_secs(10))
            .unwrap();
        assert_eq!(unhealthy, vec![p1_id.clone()]);

        // Register replacement participant p2
        plane.register_participant(Arc::clone(&p2)).unwrap();

        // Heal the VM by replacing p1 with p2
        let (healed_vm, new_lease) = plane
            .heal_virtual_machine(
                &vm.id,
                &p1_id,
                created_at + Duration::from_secs(10),
                Duration::from_secs(50),
            )
            .unwrap();

        assert_eq!(new_lease.participant_id, p2_id);
        assert_eq!(healed_vm.leases.len(), 1);
        assert_eq!(healed_vm.leases[0].participant_id, p2_id);

        // Audit now reports healthy
        let unhealthy_after = plane
            .audit_virtual_machine_health(&vm.id, created_at + Duration::from_secs(10))
            .unwrap();
        assert!(unhealthy_after.is_empty());
    }

    #[test]
    fn control_plane_can_heal_after_expired_lease_was_reaped() {
        let plane = plane();
        let failed_participant = active_participant();
        let replacement_participant = active_participant();
        let failed_id = failed_participant.snapshot().id;
        let replacement_id = replacement_participant.snapshot().id.clone();
        plane
            .register_participant(Arc::clone(&failed_participant))
            .unwrap();

        let created_at = SystemTime::UNIX_EPOCH + Duration::from_secs(2_000);
        let (vm, leases) = plane
            .create_leased_virtual_machine(
                ResourceFragment {
                    cpu: crate::resource::Cpu { cores: 1.0 },
                    ..Default::default()
                },
                RuntimeSpec::new("wasm", "1"),
                created_at,
                Duration::from_secs(30),
            )
            .unwrap();
        failed_participant
            .drain(created_at + Duration::from_secs(5))
            .unwrap();

        let reaped = plane
            .reap_expired_leases(created_at + Duration::from_secs(31))
            .unwrap();
        assert_eq!(reaped, leases);
        assert_eq!(
            plane.get_lease(&leases[0].id).unwrap_err(),
            ControlError::LeaseNotFound
        );

        plane.register_participant(replacement_participant).unwrap();
        let (healed_vm, new_lease) = plane
            .heal_virtual_machine(
                &vm.id,
                &failed_id,
                created_at + Duration::from_secs(31),
                Duration::from_secs(60),
            )
            .unwrap();

        assert_eq!(new_lease.participant_id, replacement_id);
        assert_eq!(healed_vm.leases.len(), 1);
        assert_eq!(healed_vm.leases[0], new_lease);
        assert_eq!(plane.get_lease(&new_lease.id).unwrap(), new_lease);
    }

    #[test]
    fn control_plane_query_and_reap_leases() {
        let plane = plane();
        let participant = active_participant();
        plane.register_participant(participant).unwrap();

        let created_at = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000);
        let duration = Duration::from_secs(30);

        let (vm, leases) = plane
            .create_leased_virtual_machine(
                ResourceFragment {
                    cpu: crate::resource::Cpu { cores: 0.5 },
                    ..Default::default()
                },
                RuntimeSpec::new("echo", "1"),
                created_at,
                duration,
            )
            .unwrap();

        // Listing VMs
        let vms = plane.list_virtual_machines();
        assert_eq!(vms.len(), 1);
        assert_eq!(vms[0].id, vm.id);

        // Listing logical nodes
        let nodes = plane.list_logical_nodes();
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0].id, vm.node_id.id);

        // Fetching logical node by ID
        let fetched_node = plane.get_logical_node(&vm.node_id.id).unwrap();
        assert_eq!(fetched_node.id, vm.node_id.id);

        assert_eq!(
            plane.get_logical_node("non-existent").unwrap_err(),
            ControlError::LogicalNodeNotFound
        );

        // Listing leases
        let stored_leases = plane.list_leases();
        assert_eq!(stored_leases.len(), 1);
        assert_eq!(stored_leases[0].id, leases[0].id);

        // Reaping before expiration does nothing
        let reaped = plane
            .reap_expired_leases(created_at + Duration::from_secs(10))
            .unwrap();
        assert!(reaped.is_empty());
        assert_eq!(plane.list_leases().len(), 1);

        // Reaping after expiration removes the lease
        let reaped = plane
            .reap_expired_leases(created_at + Duration::from_secs(35))
            .unwrap();
        assert_eq!(reaped.len(), 1);
        assert_eq!(reaped[0].id, leases[0].id);
        assert!(plane.list_leases().is_empty());
    }    
    #[test]
    fn execution_does_not_resurrect_destroyed_vm_after_backend_finishes() {
        use std::sync::Barrier;

        struct BlockingBackend {
            entered: Arc<Barrier>,
            release: Arc<Barrier>,
        }

        impl ExecutionBackend for BlockingBackend {
            fn execute(
                &self,
                plan: &ExecutionPlan,
                workload: &WorkItem,
            ) -> Result<ExecutionReceipt, ExecutionError> {
                self.entered.wait();
                self.release.wait();
                Ok(ExecutionReceipt {
                    workload_id: workload.id.clone(),
                    node_id: plan.node_id.clone(),
                    dispatched_units: plan.units.len(),
                })
            }
        }

        let plane = Arc::new(plane());
        plane.register_participant(active_participant()).unwrap();

        let (vm, leases) = plane
            .create_leased_virtual_machine(
                ResourceFragment {
                    cpu: crate::resource::Cpu { cores: 1.0 },
                    ..Default::default()
                },
                RuntimeSpec::new("wasm", "1"),
                SystemTime::now(),
                Duration::from_secs(60),
            )
            .unwrap();
        plane.start_virtual_machine(&vm.id).unwrap();

        let entered = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        let backend = BlockingBackend {
            entered: Arc::clone(&entered),
            release: Arc::clone(&release),
        };
        let worker_plane = Arc::clone(&plane);
        let worker_vm_id = vm.id.clone();
        let workload = WorkItem {
            id: "destroy-during-execution".into(),
            payload: Vec::new(),
        };
        let execution = std::thread::spawn(move || {
            worker_plane.execute_virtual_machine(&worker_vm_id, &workload, &backend)
        });

        entered.wait();
        plane.destroy_virtual_machine(&vm.id).unwrap();
        assert!(plane.list_leases().is_empty());
        assert!(plane.list_logical_nodes().is_empty());

        release.wait();
        assert_eq!(
            execution.join().unwrap().unwrap_err(),
            ControlError::ConcurrentModification
        );
        assert_eq!(
            plane.get_virtual_machine(&vm.id).unwrap_err(),
            ControlError::VirtualMachineNotFound
        );
        for lease in leases {
            assert_eq!(
                plane.get_lease(&lease.id).unwrap_err(),
                ControlError::LeaseNotFound
            );
        }
    }

}
