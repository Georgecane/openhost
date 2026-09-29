use crate::execution::{
    ExecutionBackend, ExecutionError, ExecutionPlan, ExecutionReceipt, WorkItem,
};
use crate::identity::{Identity, Kind};
use crate::node::{LogicalNode, NodeError, RuntimeSpec};
use crate::resource::ResourceFragment;
use crate::transport::{EndpointTransport, ExecutionEndpoint};
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
    Execution(ExecutionError),
    NotRunning,
}

impl fmt::Display for VirtualMachineError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for VirtualMachineError {}

impl VirtualMachine {
    pub fn from_node(
        node: &LogicalNode,
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

        Ok(Self {
            id: Identity::new(Kind::VirtualMachine),
            node_id,
            spec,
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
        if self.state != VirtualMachineState::Running {
            return Err(VirtualMachineError::NotRunning);
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

        let endpoints = vec![
            ExecutionEndpoint::new(participant_id, "loopback://participant-1").unwrap(),
        ];
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
