use crate::identity::{Identity, Kind};
use crate::node::{LogicalNode, RuntimeSpec};
use crate::resource::ResourceFragment;
pub use crate::runtime::Workload as WorkItem;
use std::fmt;

#[derive(Debug, Clone, PartialEq)]
pub struct ExecutionUnit {
    pub participant_id: Identity,
    pub resources: ResourceFragment,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ExecutionPlan {
    pub workload_id: String,
    pub node_id: Identity,
    pub runtime: RuntimeSpec,
    pub units: Vec<ExecutionUnit>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionReceipt {
    pub workload_id: String,
    pub node_id: Identity,
    pub dispatched_units: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionError {
    EmptyWorkloadId,
    InvalidNodeIdentity,
    InvalidParticipantIdentity,
    EmptyPlan,
    InvalidResources,
    InvalidRuntimeSpec,
    WorkloadMismatch,
    BackendRejected,
}

impl fmt::Display for ExecutionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for ExecutionError {}

pub trait ExecutionBackend: Send + Sync {
    fn execute(
        &self,
        plan: &ExecutionPlan,
        workload: &WorkItem,
    ) -> Result<ExecutionReceipt, ExecutionError>;
}

#[derive(Debug, Default, Clone, Copy)]
pub struct NoopBackend;

impl ExecutionBackend for NoopBackend {
    fn execute(
        &self,
        plan: &ExecutionPlan,
        workload: &WorkItem,
    ) -> Result<ExecutionReceipt, ExecutionError> {
        if plan.workload_id != workload.id {
            return Err(ExecutionError::WorkloadMismatch);
        }

        if plan.units.is_empty() {
            return Err(ExecutionError::EmptyPlan);
        }

        Ok(ExecutionReceipt {
            workload_id: workload.id.clone(),
            node_id: plan.node_id.clone(),
            dispatched_units: plan.units.len(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DispatchReceipt {
    pub workload_id: String,
    pub node_id: Identity,
    pub dispatched_units: usize,
}

impl DispatchReceipt {
    fn from_responses(
        workload_id: String,
        node_id: Identity,
        responses: usize,
    ) -> Self {
        Self {
            workload_id,
            node_id,
            dispatched_units: responses,
        }
    }
}

pub struct ExecutionDispatcher<T> {
    transport: T,
}

impl<T> ExecutionDispatcher<T> {
    pub fn new(transport: T) -> Self {
        Self { transport }
    }

    pub fn transport(&self) -> &T {
        &self.transport
    }
}

impl<T: crate::transport::Transport> ExecutionDispatcher<T> {
    pub fn dispatch(
        &self,
        plan: &ExecutionPlan,
        workload: &WorkItem,
        endpoints: &[crate::transport::ExecutionEndpoint],
    ) -> Result<DispatchReceipt, ExecutionError> {
        if plan.workload_id != workload.id {
            return Err(ExecutionError::WorkloadMismatch);
        }
        if plan.units.is_empty() {
            return Err(ExecutionError::EmptyPlan);
        }

        let mut dispatched = 0;
        for unit in &plan.units {
            let endpoint = endpoints
                .iter()
                .find(|endpoint| endpoint.participant_id == unit.participant_id)
                .ok_or(ExecutionError::BackendRejected)?;

            let request = crate::transport::ExecutionRequest {
                request_id: format!("{}:{}", workload.id, unit.participant_id.id),
                workload: workload.clone(),
                node_id: plan.node_id.clone(),
                runtime: plan.runtime.clone(),
                unit: unit.clone(),
                endpoint: endpoint.clone(),
            };

            self.transport
                .dispatch(request)
                .map_err(|_| ExecutionError::BackendRejected)?;
            dispatched += 1;
        }

        Ok(DispatchReceipt::from_responses(
            workload.id.clone(),
            plan.node_id.clone(),
            dispatched,
        ))
    }
}

impl ExecutionPlan {
    pub fn from_node(node: &LogicalNode, workload: &WorkItem) -> Result<Self, ExecutionError> {
        if workload.id.is_empty() {
            return Err(ExecutionError::EmptyWorkloadId);
        }

        let node_id = Identity::parse(&node.id, Kind::LogicalNode)
            .map_err(|_| ExecutionError::InvalidNodeIdentity)?;

        if node.resources.allocations.is_empty() {
            return Err(ExecutionError::EmptyPlan);
        }

        let mut units = Vec::with_capacity(node.resources.allocations.len());
        for allocation in &node.resources.allocations {
            let participant_id = Identity::parse(&allocation.participant_id, Kind::Participant)
                .map_err(|_| ExecutionError::InvalidParticipantIdentity)?;

            allocation
                .resources
                .validate_requirement()
                .map_err(|_| ExecutionError::InvalidResources)?;

            units.push(ExecutionUnit {
                participant_id,
                resources: allocation.resources,
            });
        }

        if node.runtime.name.is_empty() || node.runtime.version.is_empty() {
            return Err(ExecutionError::InvalidRuntimeSpec);
        }

        Ok(Self {
            workload_id: workload.id.clone(),
            node_id,
            runtime: node.runtime.clone(),
            units,
        })
    }

    pub fn total_resources(&self) -> ResourceFragment {
        self.units
            .iter()
            .fold(ResourceFragment::default(), |total, unit| {
                total + unit.resources
            })
    }

    pub fn execute<B: ExecutionBackend>(
        &self,
        backend: &B,
        workload: &WorkItem,
    ) -> Result<ExecutionReceipt, ExecutionError> {
        backend.execute(self, workload)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::{LogicalNode, RuntimeSpec};
    use crate::resource::{Allocation, CompositeResource, Cpu};

    fn node() -> LogicalNode {
        let a = Identity::new(Kind::Participant);
        let b = Identity::new(Kind::Participant);
        let resources = CompositeResource::compose(vec![
            Allocation {
                participant_id: a.id,
                resources: ResourceFragment {
                    cpu: Cpu { cores: 1.0 },
                    ..Default::default()
                },
            },
            Allocation {
                participant_id: b.id,
                resources: ResourceFragment {
                    cpu: Cpu { cores: 2.0 },
                    ..Default::default()
                },
            },
        ])
        .unwrap();

        LogicalNode {
            id: Identity::new(Kind::LogicalNode).id,
            resources,
            runtime: RuntimeSpec {
                name: "recording".into(),
                version: "1".into(),
            },
        }
    }

    #[test]
    fn plan_preserves_distribution() {
        let workload = WorkItem {
            id: "work-1".into(),
            payload: vec![1, 2, 3],
        };
        let plan = ExecutionPlan::from_node(&node(), &workload).unwrap();

        assert_eq!(plan.workload_id, "work-1");
        assert_eq!(
            plan.runtime,
            RuntimeSpec {
                name: "recording".into(),
                version: "1".into(),
            }
        );
        assert_eq!(plan.units.len(), 2);
        assert_eq!(plan.total_resources().cpu.cores, 3.0);
    }

    #[test]
    fn empty_runtime_spec_is_rejected() {
        let mut node = node();
        node.runtime = RuntimeSpec::default();
        let workload = WorkItem {
            id: "work-1".into(),
            payload: Vec::new(),
        };

        assert_eq!(
            ExecutionPlan::from_node(&node, &workload).unwrap_err(),
            ExecutionError::InvalidRuntimeSpec
        );
    }

    #[test]
    fn empty_workload_id_is_rejected() {
        let workload = WorkItem {
            id: String::new(),
            payload: Vec::new(),
        };
        assert_eq!(
            ExecutionPlan::from_node(&node(), &workload).unwrap_err(),
            ExecutionError::EmptyWorkloadId
        );
    }

    #[test]
    fn invalid_participant_identity_is_rejected() {
        let node = LogicalNode {
            id: Identity::new(Kind::LogicalNode).id,
            resources: CompositeResource::compose(vec![Allocation {
                participant_id: "not-a-uuid".into(),
                resources: ResourceFragment {
                    cpu: Cpu { cores: 1.0 },
                    ..Default::default()
                },
            }])
            .unwrap(),
            runtime: RuntimeSpec::default(),
        };
        let workload = WorkItem {
            id: "work-1".into(),
            payload: Vec::new(),
        };

        assert_eq!(
            ExecutionPlan::from_node(&node, &workload).unwrap_err(),
            ExecutionError::InvalidParticipantIdentity
        );
    }

    #[test]
    fn backend_receives_plan_without_changing_distribution() {
        let workload = WorkItem {
            id: "work-1".into(),
            payload: vec![1, 2, 3],
        };
        let plan = ExecutionPlan::from_node(&node(), &workload).unwrap();
        let receipt = plan.execute(&NoopBackend, &workload).unwrap();

        assert_eq!(receipt.workload_id, "work-1");
        assert_eq!(receipt.node_id, plan.node_id);
        assert_eq!(receipt.dispatched_units, 2);
    }

    #[test]
    fn backend_rejects_mismatched_workload() {
        let workload = WorkItem {
            id: "work-1".into(),
            payload: Vec::new(),
        };
        let other = WorkItem {
            id: "work-2".into(),
            payload: Vec::new(),
        };
        let plan = ExecutionPlan::from_node(&node(), &workload).unwrap();

        assert_eq!(
            plan.execute(&NoopBackend, &other).unwrap_err(),
            ExecutionError::WorkloadMismatch
        );
    }
}
