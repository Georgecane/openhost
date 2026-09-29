use crate::identity::{Identity, Kind};
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
            id: Identity::new(Kind::LogicalNode),
            node_id,
            spec,
            state: VirtualMachineState::Created,
            created_at,
        })
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
        assert_eq!(vm.id.kind, Kind::LogicalNode);
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
        node.resources = CompositeResource::default();

        assert_eq!(
            VirtualMachine::from_node(&node, SystemTime::now()).unwrap_err(),
            VirtualMachineError::EmptyResources
        );
    }
}
