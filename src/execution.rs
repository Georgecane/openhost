use crate::identity::{Identity, Kind};
use crate::node::LogicalNode;
use crate::resource::ResourceFragment;
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkItem {
    pub id: String,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ExecutionUnit {
    pub participant_id: Identity,
    pub resources: ResourceFragment,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ExecutionPlan {
    pub workload_id: String,
    pub node_id: Identity,
    pub units: Vec<ExecutionUnit>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionError {
    EmptyWorkloadId,
    InvalidNodeIdentity,
    InvalidParticipantIdentity,
    EmptyPlan,
    InvalidResources,
}

impl fmt::Display for ExecutionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for ExecutionError {}

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

        Ok(Self {
            workload_id: workload.id.clone(),
            node_id,
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
            runtime: RuntimeSpec::default(),
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
        assert_eq!(plan.units.len(), 2);
        assert_eq!(plan.total_resources().cpu.cores, 3.0);
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
}
