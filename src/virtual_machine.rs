use crate::execution::{
    ExecutionBackend, ExecutionError, ExecutionPlan, ExecutionReceipt, WorkItem,
};
use crate::identity::{Identity, Kind};
use crate::lease::Lease;
use crate::node::{LogicalNode, NodeError, RuntimeSpec};
use crate::resource::ResourceFragment;
use std::fmt;
use std::time::SystemTime;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VirtualMachineState {
    Created,
    Running,
    Stopped,
    Failed,
}

#[derive(Debug, Clone, PartialEq)]
pub struct VirtualMachineSpec {
    pub resources: ResourceFragment,
    pub runtime: RuntimeSpec,
}

impl VirtualMachineSpec {
    pub fn new(resources: ResourceFragment, runtime: RuntimeSpec) -> Self {
        Self { resources, runtime }
    }

    pub fn validate(&self) -> Result<(), VirtualMachineError> {
        self.resources
            .validate()
            .map_err(|_| VirtualMachineError::InvalidResources)?;
        self.runtime
            .validate()
            .map_err(VirtualMachineError::InvalidRuntime)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct VirtualMachine {
    pub id: Identity,
    pub node_id: Identity,
    pub spec: VirtualMachineSpec,
    pub leases: Vec<Lease>,
    pub state: VirtualMachineState,
    pub created_at: SystemTime,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VirtualMachineError {
    InvalidVirtualMachineIdentity,
    InvalidLogicalNodeIdentity,
    InvalidResources,
    InvalidRuntime(NodeError),
    EmptyResources,
    NodeMismatch,
    ResourceMismatch,
    RuntimeMismatch,
    InvalidLease,
    LeaseNodeMismatch,
    LeaseParticipantMismatch,
    LeaseResourceMismatch,
    LeaseExpired,
    Execution(ExecutionError),
    NotRunning,
}

impl fmt::Display for VirtualMachineError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for VirtualMachineError {}

fn same_capacity(left: ResourceFragment, right: ResourceFragment) -> bool {
    left.cpu == right.cpu
        && left.memory == right.memory
        && left.storage == right.storage
        && left.network == right.network
        && left.gpu == right.gpu
}

impl VirtualMachine {
    pub fn from_node(
        node: &LogicalNode,
        created_at: SystemTime,
    ) -> Result<Self, VirtualMachineError> {
        Self::from_node_with_leases(node, Vec::new(), created_at)
    }

    pub fn from_leased_node(
        node: &LogicalNode,
        leases: Vec<Lease>,
        created_at: SystemTime,
    ) -> Result<Self, VirtualMachineError> {
        if leases.is_empty() {
            return Err(VirtualMachineError::InvalidLease);
        }

        Self::from_node_with_leases(node, leases, created_at)
    }

    fn from_node_with_leases(
        node: &LogicalNode,
        leases: Vec<Lease>,
        created_at: SystemTime,
    ) -> Result<Self, VirtualMachineError> {
        if created_at == SystemTime::UNIX_EPOCH {
            return Err(VirtualMachineError::InvalidResources);
        }

        let node_id = Identity::parse(&node.id, Kind::LogicalNode)
            .map_err(|_| VirtualMachineError::InvalidLogicalNodeIdentity)?;
        let resources = node.resources.capacity;
        if resources.is_empty() {
            return Err(VirtualMachineError::EmptyResources);
        }

        let spec = VirtualMachineSpec::new(resources, node.runtime.clone());
        spec.validate()?;

        if !leases.is_empty() {
            if leases.len() != node.resources.allocations.len() {
                return Err(VirtualMachineError::InvalidLease);
            }

            for lease in &leases {
                lease
                    .validate()
                    .map_err(|_| VirtualMachineError::InvalidLease)?;

                if lease.logical_node_id != node_id {
                    return Err(VirtualMachineError::LeaseNodeMismatch);
                }

                if !lease.active_at(created_at) {
                    return Err(VirtualMachineError::LeaseExpired);
                }

                let allocation = node
                    .resources
                    .allocations
                    .iter()
                    .find(|allocation| allocation.participant_id == lease.participant_id.id)
                    .ok_or(VirtualMachineError::LeaseParticipantMismatch)?;

                if !same_capacity(allocation.resources, lease.resources) {
                    return Err(VirtualMachineError::LeaseResourceMismatch);
                }
            }

            let unique_participants = leases
                .iter()
                .map(|lease| lease.participant_id.id.as_str())
                .collect::<std::collections::BTreeSet<_>>();

            if unique_participants.len() != leases.len() {
                return Err(VirtualMachineError::InvalidLease);
            }
        }

        Ok(Self {
            id: Identity::new(Kind::VirtualMachine),
            node_id,
            spec,
            leases,
            state: VirtualMachineState::Created,
            created_at,
        })
    }

    pub fn execution_plan(
        &self,
        node: &LogicalNode,
        workload: &WorkItem,
    ) -> Result<ExecutionPlan, VirtualMachineError> {
        let node_id = Identity::parse(&node.id, Kind::LogicalNode)
            .map_err(|_| VirtualMachineError::InvalidLogicalNodeIdentity)?;
        if node_id != self.node_id {
            return Err(VirtualMachineError::NodeMismatch);
        }
        if node.resources.capacity != self.spec.resources {
            return Err(VirtualMachineError::ResourceMismatch);
        }
        if node.runtime != self.spec.runtime {
            return Err(VirtualMachineError::RuntimeMismatch);
        }

        ExecutionPlan::from_node(node, workload).map_err(VirtualMachineError::Execution)
    }

    pub fn execute<B: ExecutionBackend>(
        &mut self,
        node: &LogicalNode,
        workload: &WorkItem,
        backend: &B,
    ) -> Result<ExecutionReceipt, VirtualMachineError> {
        self.execute_at(node, workload, backend, SystemTime::now())
    }

    pub fn execute_at<B: ExecutionBackend>(
        &mut self,
        node: &LogicalNode,
        workload: &WorkItem,
        backend: &B,
        now: SystemTime,
    ) -> Result<ExecutionReceipt, VirtualMachineError> {
        if self.state != VirtualMachineState::Running {
            return Err(VirtualMachineError::NotRunning);
        }

        if self.leases.iter().any(|lease| !lease.active_at(now)) {
            return Err(VirtualMachineError::LeaseExpired);
        }

        let plan = self.execution_plan(node, workload)?;
        match plan.execute(backend, workload) {
            Ok(receipt) => Ok(receipt),
            Err(error) => {
                self.state = VirtualMachineState::Failed;
                Err(VirtualMachineError::Execution(error))
            }
        }
    }

    pub fn start(&mut self) -> Result<(), VirtualMachineError> {
        if self.state != VirtualMachineState::Created {
            return Err(VirtualMachineError::InvalidVirtualMachineIdentity);
        }
        self.state = VirtualMachineState::Running;
        Ok(())
    }

    pub fn stop(&mut self) -> Result<(), VirtualMachineError> {
        if self.state != VirtualMachineState::Running {
            return Err(VirtualMachineError::InvalidVirtualMachineIdentity);
        }
        self.state = VirtualMachineState::Stopped;
        Ok(())
    }

    pub fn fail(&mut self) {
        self.state = VirtualMachineState::Failed;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::Kind;
    use crate::resource::{Allocation, CompositeResource, Cpu};

    fn node() -> LogicalNode {
        let participant = Identity::new(Kind::Participant);
        let resources = CompositeResource::compose(vec![Allocation {
            participant_id: participant.id,
            resources: ResourceFragment {
                cpu: Cpu { cores: 2.0 },
                ..Default::default()
            },
        }])
        .unwrap();

        LogicalNode {
            id: Identity::new(Kind::LogicalNode).id,
            resources,
            runtime: RuntimeSpec::new("wasm", "1"),
        }
    }

    #[test]
    fn vm_binds_to_matching_leases() {
        let node = node();
        let created_at = SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(100);
        let participant_id =
            Identity::parse(&node.resources.allocations[0].participant_id, Kind::Participant)
                .unwrap();
        let node_id = Identity::parse(&node.id, Kind::LogicalNode).unwrap();
        let lease = Lease::new(
            Identity::new(Kind::Lease),
            node_id,
            participant_id,
            node.resources.allocations[0].resources,
            created_at,
            created_at + std::time::Duration::from_secs(60),
        )
        .unwrap();

        let vm = VirtualMachine::from_leased_node(&node, vec![lease], created_at).unwrap();

        assert_eq!(vm.leases.len(), 1);
        assert_eq!(vm.state, VirtualMachineState::Created);
    }

    #[test]
    fn vm_rejects_lease_for_different_node() {
        let node = node();
        let created_at = SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(100);
        let lease = Lease::new(
            Identity::new(Kind::Lease),
            Identity::new(Kind::LogicalNode),
            Identity::parse(&node.resources.allocations[0].participant_id, Kind::Participant)
                .unwrap(),
            node.resources.allocations[0].resources,
            created_at,
            created_at + std::time::Duration::from_secs(60),
        )
        .unwrap();

        assert_eq!(
            VirtualMachine::from_leased_node(&node, vec![lease], created_at).unwrap_err(),
            VirtualMachineError::LeaseNodeMismatch
        );
    }

    #[test]
    fn vm_rejects_expired_lease_at_execution_time() {
        let node = node();
        let created_at = SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(100);
        let participant_id =
            Identity::parse(&node.resources.allocations[0].participant_id, Kind::Participant)
                .unwrap();
        let node_id = Identity::parse(&node.id, Kind::LogicalNode).unwrap();
        let lease = Lease::new(
            Identity::new(Kind::Lease),
            node_id,
            participant_id,
            node.resources.allocations[0].resources,
            created_at,
            created_at + std::time::Duration::from_secs(60),
        )
        .unwrap();
        let mut vm = VirtualMachine::from_leased_node(&node, vec![lease], created_at).unwrap();
        vm.start().unwrap();
        let workload = WorkItem {
            id: "work-lease-expired".into(),
            payload: Vec::new(),
        };

        assert_eq!(
            vm.execute_at(
                &node,
                &workload,
                &crate::execution::NoopBackend,
                created_at + std::time::Duration::from_secs(60),
            )
            .unwrap_err(),
            VirtualMachineError::LeaseExpired
        );
        assert_eq!(vm.state, VirtualMachineState::Running);
    }

    #[test]
    fn vm_maps_logical_node_resources_and_runtime() {
        let created_at = SystemTime::now();
        let vm = VirtualMachine::from_node(&node(), created_at).unwrap();

        assert_eq!(vm.node_id.kind, Kind::LogicalNode);
        assert_eq!(vm.spec.resources.cpu.cores, 2.0);
        assert_eq!(vm.spec.runtime, RuntimeSpec::new("wasm", "1"));
        assert_eq!(vm.state, VirtualMachineState::Created);
        assert_eq!(vm.created_at, created_at);
    }

    #[test]
    fn vm_has_distinct_identity_from_logical_node() {
        let node = node();
        let vm = VirtualMachine::from_node(&node, SystemTime::now()).unwrap();

        assert_ne!(vm.id.id, node.id);
        assert_eq!(vm.id.kind, Kind::VirtualMachine);
    }

    #[test]
    fn vm_rejects_invalid_runtime() {
        let mut node = node();
        node.runtime = RuntimeSpec::default();

        assert_eq!(
            VirtualMachine::from_node(&node, SystemTime::now()).unwrap_err(),
            VirtualMachineError::InvalidRuntime(NodeError::EmptyRuntimeName)
        );
    }

    #[test]
    fn running_vm_executes_workload_through_existing_backend() {
        let node = node();
        let mut vm = VirtualMachine::from_node(&node, SystemTime::now()).unwrap();
        vm.start().unwrap();
        let workload = WorkItem {
            id: "work-1".into(),
            payload: vec![1, 2, 3],
        };

        let receipt = vm
            .execute(&node, &workload, &crate::execution::NoopBackend)
            .unwrap();

        assert_eq!(receipt.workload_id, workload.id);
        assert_eq!(receipt.node_id, vm.node_id);
        assert_eq!(receipt.dispatched_units, 1);
        assert_eq!(vm.state, VirtualMachineState::Running);
    }

    #[test]
    fn running_vm_dispatches_through_endpoint_transport_and_runtime_registry() {
        use crate::endpoint::{ParticipantExecutionEndpoint, RegistryRuntimeAdapter};
        use crate::execution::ExecutionDispatcher;
        use crate::runtime::{Runtime, RuntimeContext, RuntimeError, RuntimeRegistry};
        use crate::transport::{EndpointTransport, ExecutionEndpoint};
        use std::sync::Arc;

        #[derive(Debug, Default)]
        struct RecordingRuntime;

        impl Runtime for RecordingRuntime {
            fn name(&self) -> &str {
                "wasm"
            }

            fn run(
                &self,
                _context: &RuntimeContext,
                _workload: WorkItem,
            ) -> Result<(), RuntimeError> {
                Ok(())
            }
        }

        let node = node();
        let participant_id = Identity::parse(
            &node.resources.allocations[0].participant_id,
            Kind::Participant,
        )
        .unwrap();
        let mut registry = RuntimeRegistry::default();
        registry
            .register(RuntimeSpec::new("wasm", "1"), Arc::new(RecordingRuntime))
            .unwrap();

        let endpoint = Arc::new(
            ParticipantExecutionEndpoint::new(
                participant_id.clone(),
                RegistryRuntimeAdapter::new(Arc::new(registry)),
            )
            .unwrap(),
        );
        let transport = EndpointTransport::new();
        transport.register(endpoint).unwrap();

        let endpoints =
            vec![ExecutionEndpoint::new(participant_id, "loopback://participant-1").unwrap()];
        let dispatcher = ExecutionDispatcher::new(transport, endpoints);
        let workload = WorkItem {
            id: "work-1".into(),
            payload: vec![1, 2, 3],
        };
        let mut vm = VirtualMachine::from_node(&node, SystemTime::now()).unwrap();
        vm.start().unwrap();

        let receipt = vm.execute(&node, &workload, &dispatcher).unwrap();

        assert_eq!(receipt.workload_id, workload.id);
        assert_eq!(receipt.dispatched_units, 1);
        assert_eq!(vm.state, VirtualMachineState::Running);
    }

    #[test]
    fn running_vm_dispatches_to_all_participants() {
        use crate::endpoint::{ParticipantExecutionEndpoint, RegistryRuntimeAdapter};
        use crate::execution::ExecutionDispatcher;
        use crate::runtime::{Runtime, RuntimeContext, RuntimeError, RuntimeRegistry};
        use crate::transport::{EndpointTransport, ExecutionEndpoint};
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;

        #[derive(Debug)]
        struct CountingRuntime {
            calls: Arc<AtomicUsize>,
        }

        impl Runtime for CountingRuntime {
            fn name(&self) -> &str {
                "wasm"
            }

            fn run(
                &self,
                _context: &RuntimeContext,
                _workload: WorkItem,
            ) -> Result<(), RuntimeError> {
                self.calls.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        }

        let participant_a = Identity::new(Kind::Participant);
        let participant_b = Identity::new(Kind::Participant);
        let resources = CompositeResource::compose(vec![
            Allocation {
                participant_id: participant_a.id.clone(),
                resources: ResourceFragment {
                    cpu: Cpu { cores: 1.0 },
                    ..Default::default()
                },
            },
            Allocation {
                participant_id: participant_b.id.clone(),
                resources: ResourceFragment {
                    cpu: Cpu { cores: 2.0 },
                    ..Default::default()
                },
            },
        ])
        .unwrap();
        let node = LogicalNode {
            id: Identity::new(Kind::LogicalNode).id,
            resources,
            runtime: RuntimeSpec::new("wasm", "1"),
        };

        let calls = Arc::new(AtomicUsize::new(0));
        let mut registry = RuntimeRegistry::default();
        registry
            .register(
                RuntimeSpec::new("wasm", "1"),
                Arc::new(CountingRuntime {
                    calls: Arc::clone(&calls),
                }),
            )
            .unwrap();
        let registry = Arc::new(registry);

        let endpoint_a = Arc::new(
            ParticipantExecutionEndpoint::new(
                participant_a.clone(),
                RegistryRuntimeAdapter::new(Arc::clone(&registry)),
            )
            .unwrap(),
        );
        let endpoint_b = Arc::new(
            ParticipantExecutionEndpoint::new(
                participant_b.clone(),
                RegistryRuntimeAdapter::new(registry),
            )
            .unwrap(),
        );

        let transport = EndpointTransport::new();
        transport.register(endpoint_a).unwrap();
        transport.register(endpoint_b).unwrap();

        let endpoints = vec![
            ExecutionEndpoint::new(participant_a, "loopback://participant-a").unwrap(),
            ExecutionEndpoint::new(participant_b, "loopback://participant-b").unwrap(),
        ];
        let dispatcher = ExecutionDispatcher::new(transport, endpoints);
        let workload = WorkItem {
            id: "work-distributed".into(),
            payload: vec![1, 2, 3],
        };
        let mut vm = VirtualMachine::from_node(&node, SystemTime::now()).unwrap();
        vm.start().unwrap();

        let receipt = vm.execute(&node, &workload, &dispatcher).unwrap();

        assert_eq!(receipt.dispatched_units, 2);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(vm.state, VirtualMachineState::Running);
    }

    #[test]
    fn vm_fails_when_one_participant_execution_fails() {
        use crate::endpoint::{ParticipantExecutionEndpoint, RegistryRuntimeAdapter};
        use crate::execution::ExecutionDispatcher;
        use crate::runtime::{Runtime, RuntimeContext, RuntimeError, RuntimeRegistry};
        use crate::transport::{EndpointTransport, ExecutionEndpoint};
        use std::sync::Arc;

        #[derive(Debug, Default)]
        struct FailingRuntime;

        impl Runtime for FailingRuntime {
            fn name(&self) -> &str {
                "wasm"
            }

            fn run(
                &self,
                _context: &RuntimeContext,
                _workload: WorkItem,
            ) -> Result<(), RuntimeError> {
                Err(RuntimeError::ExecutionFailed)
            }
        }

        let participant_a = Identity::new(Kind::Participant);
        let participant_b = Identity::new(Kind::Participant);
        let resources = CompositeResource::compose(vec![
            Allocation {
                participant_id: participant_a.id.clone(),
                resources: ResourceFragment {
                    cpu: Cpu { cores: 1.0 },
                    ..Default::default()
                },
            },
            Allocation {
                participant_id: participant_b.id.clone(),
                resources: ResourceFragment {
                    cpu: Cpu { cores: 1.0 },
                    ..Default::default()
                },
            },
        ])
        .unwrap();
        let node = LogicalNode {
            id: Identity::new(Kind::LogicalNode).id,
            resources,
            runtime: RuntimeSpec::new("wasm", "1"),
        };

        let mut registry = RuntimeRegistry::default();
        registry
            .register(
                RuntimeSpec::new("wasm", "1"),
                Arc::new(FailingRuntime),
            )
            .unwrap();
        let registry = Arc::new(registry);

        let endpoint_a = Arc::new(
            ParticipantExecutionEndpoint::new(
                participant_a.clone(),
                RegistryRuntimeAdapter::new(Arc::clone(&registry)),
            )
            .unwrap(),
        );
        let endpoint_b = Arc::new(
            ParticipantExecutionEndpoint::new(
                participant_b.clone(),
                RegistryRuntimeAdapter::new(registry),
            )
            .unwrap(),
        );

        let transport = EndpointTransport::new();
        transport.register(endpoint_a).unwrap();
        transport.register(endpoint_b).unwrap();

        let endpoints = vec![
            ExecutionEndpoint::new(participant_a, "loopback://participant-a").unwrap(),
            ExecutionEndpoint::new(participant_b, "loopback://participant-b").unwrap(),
        ];
        let dispatcher = ExecutionDispatcher::new(transport, endpoints);
        let workload = WorkItem {
            id: "work-failing".into(),
            payload: vec![1, 2, 3],
        };
        let mut vm = VirtualMachine::from_node(&node, SystemTime::now()).unwrap();
        vm.start().unwrap();

        let error = vm.execute(&node, &workload, &dispatcher).unwrap_err();

        assert_eq!(
            error,
            VirtualMachineError::Execution(ExecutionError::BackendRejected)
        );
        assert_eq!(vm.state, VirtualMachineState::Failed);
    }

    #[test]
    fn created_vm_cannot_execute() {
        let node = node();
        let mut vm = VirtualMachine::from_node(&node, SystemTime::now()).unwrap();
        let workload = WorkItem {
            id: "work-1".into(),
            payload: Vec::new(),
        };

        assert_eq!(
            vm.execute(&node, &workload, &crate::execution::NoopBackend)
                .unwrap_err(),
            VirtualMachineError::NotRunning
        );
    }

    #[test]
    fn vm_lifecycle_is_explicit() {
        let mut vm = VirtualMachine::from_node(&node(), SystemTime::now()).unwrap();

        vm.start().unwrap();
        assert_eq!(vm.state, VirtualMachineState::Running);

        vm.stop().unwrap();
        assert_eq!(vm.state, VirtualMachineState::Stopped);

        assert!(vm.start().is_err());
    }

    #[test]
    fn vm_rejects_empty_resources() {
        let mut node = node();
        node.resources.capacity = ResourceFragment::default();

        assert_eq!(
            VirtualMachine::from_node(&node, SystemTime::now()).unwrap_err(),
            VirtualMachineError::EmptyResources
        );
    }
}
