use crate::identity::{Identity, Kind};
use crate::resource::ResourceFragment;
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Workload {
    pub id: String,
    pub payload: Vec<u8>,
}

impl Workload {
    pub fn validate(&self) -> Result<(), RuntimeError> {
        if self.id.is_empty() {
            return Err(RuntimeError::EmptyWorkloadId);
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct RuntimeContext {
    pub participant_id: Identity,
    pub node_id: Identity,
    pub resources: ResourceFragment,
}

impl RuntimeContext {
    pub fn new(
        participant_id: Identity,
        node_id: Identity,
        resources: ResourceFragment,
    ) -> Result<Self, RuntimeError> {
        if participant_id.kind != Kind::Participant {
            return Err(RuntimeError::InvalidParticipantIdentity);
        }

        if node_id.kind != Kind::LogicalNode {
            return Err(RuntimeError::InvalidNodeIdentity);
        }

        resources
            .validate_requirement()
            .map_err(|_| RuntimeError::InvalidResources)?;

        Ok(Self {
            participant_id,
            node_id,
            resources,
        })
    }
}

pub trait Runtime: Send + Sync {
    fn name(&self) -> &str;

    fn run(
        &self,
        context: &RuntimeContext,
        workload: Workload,
    ) -> Result<(), RuntimeError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeError {
    EmptyWorkloadId,
    InvalidParticipantIdentity,
    InvalidNodeIdentity,
    InvalidResources,
    Unsupported,
    ExecutionFailed,
}

impl fmt::Display for RuntimeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for RuntimeError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::Kind;
    use crate::resource::{Cpu, ResourceFragment};

    #[derive(Debug, Default, Clone, Copy)]
    struct RecordingRuntime;

    impl Runtime for RecordingRuntime {
        fn name(&self) -> &str {
            "recording"
        }

        fn run(
            &self,
            _context: &RuntimeContext,
            _workload: Workload,
        ) -> Result<(), RuntimeError> {
            Ok(())
        }
    }

    #[test]
    fn context_rejects_invalid_participant() {
        let result = RuntimeContext::new(
            Identity::new(Kind::LogicalNode),
            Identity::new(Kind::LogicalNode),
            ResourceFragment::default(),
        );

        assert_eq!(
            result.unwrap_err(),
            RuntimeError::InvalidParticipantIdentity
        );
    }

    #[test]
    fn context_rejects_invalid_node() {
        let result = RuntimeContext::new(
            Identity::new(Kind::Participant),
            Identity::new(Kind::Participant),
            ResourceFragment {
                cpu: Cpu { cores: 1.0 },
                ..Default::default()
            },
        );

        assert_eq!(result.unwrap_err(), RuntimeError::InvalidNodeIdentity);
    }

    #[test]
    fn workload_requires_identity() {
        let workload = Workload {
            id: String::new(),
            payload: Vec::new(),
        };

        assert_eq!(
            workload.validate().unwrap_err(),
            RuntimeError::EmptyWorkloadId
        );
    }

    #[test]
    fn runtime_accepts_valid_context() {
        let context = RuntimeContext::new(
            Identity::new(Kind::Participant),
            Identity::new(Kind::LogicalNode),
            ResourceFragment {
                cpu: Cpu { cores: 1.0 },
                ..Default::default()
            },
        )
        .unwrap();

        let workload = Workload {
            id: "work-1".into(),
            payload: vec![1, 2, 3],
        };

        assert_eq!(RecordingRuntime.name(), "recording");
        RecordingRuntime.run(&context, workload).unwrap();
    }
}
