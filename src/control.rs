use crate::discovery::{Advertisement, Member, Registry as DiscoveryRegistry};
use crate::execution::{ExecutionBackend, ExecutionError, ExecutionPlan, ExecutionReceipt, WorkItem};
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
        let vm = VirtualMachine::from_node(node, created_at).map_err(ControlError::VirtualMachine)?;
        self.logical_nodes.write().expect("logical node registry lock poisoned").insert(node.id.clone(), node.clone());
        self.virtual_machines.write().expect("virtual machine registry lock poisoned").insert(vm.id.id.clone(), vm.clone());
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
                self.logical_nodes.write().expect("logical node registry lock poisoned").insert(node.id.clone(), node.clone());
                self.virtual_machines.write().expect("virtual machine registry lock poisoned").insert(vm.id.id.clone(), vm.clone());
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
        let mut vm = self.virtual_machines.write().expect("virtual machine registry lock poisoned")
            .remove(&id.id).ok_or(ControlError::VirtualMachineNotFound)?;
        let node = self.logical_nodes.read().expect("logical node registry lock poisoned")
            .get(&vm.node_id.id).cloned().ok_or(ControlError::VirtualMachineNotFound)?;
        let result = vm.execute(&node, workload, backend);
        self.virtual_machines.write().expect("virtual machine registry lock poisoned")
            .insert(vm.id.id.clone(), vm);
        result.map_err(ControlError::VirtualMachine)
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

    pub fn create_leased_logical_node_with_runtime(
        &self,
        requirement: ResourceFragment,
        runtime: RuntimeSpec,
        now: SystemTime,
        duration: Duration,
    ) -> Result<(LogicalNode, Vec<Lease>), ControlError> {
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
        let expires_at = now + duration;
        let mut leases = Vec::with_capacity(node.resources.allocations.len());

        for allocation in &node.resources.allocations {
            let participant_id = Identity::parse(&allocation.participant_id, Kind::Participant)
                .map_err(|_| ControlError::Scheduler(SchedulerError::InsufficientResources))?;
            let lease_id = Identity::new(Kind::Lease);
            let mut resources = allocation.resources;
            if resources.lifetime.is_none_or(|l| l > duration) {
                resources.lifetime = Some(duration);
            }
            let lease = Lease::new(
                lease_id,
                node_id.clone(),
                participant_id,
                resources,
                now,
                expires_at,
            )
            .map_err(|_| ControlError::Scheduler(SchedulerError::InsufficientResources))?;
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
    fn control_plane_creates_vm_from_owned_leases() {
        let plane = plane();
        let participant = active_participant();
        plane.register_participant(Arc::clone(&participant)).unwrap();

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

        assert_eq!(plane.get_virtual_machine(&vm.id).unwrap(), vm);

        let started = plane.start_virtual_machine(&vm.id).unwrap();
        assert_eq!(started.state, crate::virtual_machine::VirtualMachineState::Running);
        assert_eq!(
            plane.get_virtual_machine(&vm.id).unwrap().state,
            crate::virtual_machine::VirtualMachineState::Running
        );

        let stopped = plane.stop_virtual_machine(&vm.id).unwrap();
        assert_eq!(stopped.state, crate::virtual_machine::VirtualMachineState::Stopped);

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
        let created_at = SystemTime::UNIX_EPOCH + Duration::from_secs(100);
        let (vm, _) = plane.create_leased_virtual_machine(
            ResourceFragment { cpu: crate::resource::Cpu { cores: 1.0 }, ..Default::default() },
            RuntimeSpec::new("wasm", "1"), created_at, Duration::from_secs(60),
        ).unwrap();
        plane.start_virtual_machine(&vm.id).unwrap();
        let workload = WorkItem { id: "control-plane-workload".into(), payload: vec![1,2,3] };
        let receipt = plane.execute_virtual_machine(&vm.id, &workload, &crate::execution::NoopBackend).unwrap();
        assert_eq!(receipt.workload_id, workload.id);
        assert_eq!(receipt.node_id, vm.node_id);
        assert_eq!(receipt.dispatched_units, 1);
        assert_eq!(plane.get_virtual_machine(&vm.id).unwrap().state, crate::virtual_machine::VirtualMachineState::Running);
    }

    #[test]
    fn control_plane_vm_survives_fabric_resource_change() {
        let plane = plane();
        let participant = active_participant();
        let participant_id = participant.id.clone();
        plane.register_participant(participant).unwrap();

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
        let (vm, _) = plane.create_leased_virtual_machine(
            ResourceFragment { cpu: crate::resource::Cpu { cores: 1.0 }, ..Default::default() },
            RuntimeSpec::new("wasm", "1"), created_at, Duration::from_secs(60),
        ).unwrap();
        let workload = WorkItem { id: "stopped-workload".into(), payload: Vec::new() };
        assert_eq!(plane.execute_virtual_machine(&vm.id, &workload, &crate::execution::NoopBackend).unwrap_err(),
            ControlError::VirtualMachine(VirtualMachineError::NotRunning));
    }

    #[test]
    fn control_plane_execution_marks_vm_failed_on_backend_error() {
        #[derive(Debug, Clone, Copy)]
        struct FailingBackend;
        impl ExecutionBackend for FailingBackend {
            fn execute(&self, _: &ExecutionPlan, _: &WorkItem) -> Result<ExecutionReceipt, ExecutionError> {
                Err(ExecutionError::BackendRejected)
            }
        }
        let plane = plane();
        plane.register_participant(active_participant()).unwrap();
        let created_at = SystemTime::UNIX_EPOCH + Duration::from_secs(100);
        let (vm, _) = plane.create_leased_virtual_machine(
            ResourceFragment { cpu: crate::resource::Cpu { cores: 1.0 }, ..Default::default() },
            RuntimeSpec::new("wasm", "1"), created_at, Duration::from_secs(60),
        ).unwrap();
        plane.start_virtual_machine(&vm.id).unwrap();
        let workload = WorkItem { id: "failed-workload".into(), payload: Vec::new() };
        assert_eq!(plane.execute_virtual_machine(&vm.id, &workload, &FailingBackend).unwrap_err(),
            ControlError::VirtualMachine(VirtualMachineError::Execution(ExecutionError::BackendRejected)));
        assert_eq!(plane.get_virtual_machine(&vm.id).unwrap().state, crate::virtual_machine::VirtualMachineState::Failed);
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
}
