use crate::resource::CompositeResource;

#[derive(Debug, Clone, PartialEq)]
pub struct LogicalNode {
    pub id: String,
    pub resources: CompositeResource,
    pub runtime: RuntimeSpec,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RuntimeSpec {
    pub name: String,
    pub version: String,
}

impl RuntimeSpec {
    pub fn new(name: impl Into<String>, version: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            version: version.into(),
        }
    }

    pub fn validate(&self) -> Result<(), NodeError> {
        if self.name.is_empty() {
            return Err(NodeError::EmptyRuntimeName);
        }

        if self.version.is_empty() {
            return Err(NodeError::EmptyRuntimeVersion);
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeError {
    EmptyRuntimeName,
    EmptyRuntimeVersion,
}

impl std::fmt::Display for NodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for NodeError {}

impl LogicalNode {
    pub fn assign_runtime(&mut self, runtime: RuntimeSpec) -> Result<(), NodeError> {
        runtime.validate()?;
        self.runtime = runtime;
        Ok(())
    }

    pub fn with_runtime(mut self, runtime: RuntimeSpec) -> Result<Self, NodeError> {
        self.assign_runtime(runtime)?;
        Ok(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::{Identity, Kind};
    use crate::resource::{Allocation, Cpu, ResourceFragment};

    fn node() -> LogicalNode {
        let participant = Identity::new(Kind::Participant);
        let resources = CompositeResource::compose(vec![Allocation {
            participant_id: participant.id,
            resources: ResourceFragment {
                cpu: Cpu { cores: 1.0 },
                ..Default::default()
            },
        }])
        .unwrap();

        LogicalNode {
            id: Identity::new(Kind::LogicalNode).id,
            resources,
            runtime: RuntimeSpec::default(),
        }
    }

    #[test]
    fn runtime_spec_requires_name_and_version() {
        assert_eq!(
            RuntimeSpec::default().validate().unwrap_err(),
            NodeError::EmptyRuntimeName
        );
        assert_eq!(
            RuntimeSpec::new("recording", "").validate().unwrap_err(),
            NodeError::EmptyRuntimeVersion
        );
    }

    #[test]
    fn logical_node_can_assign_runtime() {
        let mut node = node();
        let runtime = RuntimeSpec::new("recording", "1");

        node.assign_runtime(runtime.clone()).unwrap();

        assert_eq!(node.runtime, runtime);
    }

    #[test]
    fn logical_node_rejects_invalid_runtime_assignment() {
        let mut node = node();

        assert_eq!(
            node.assign_runtime(RuntimeSpec::default()).unwrap_err(),
            NodeError::EmptyRuntimeName
        );
        assert!(node.runtime.name.is_empty());
    }

    #[test]
    fn with_runtime_returns_bound_node() {
        let node = node()
            .with_runtime(RuntimeSpec::new("recording", "1"))
            .unwrap();

        assert_eq!(node.runtime, RuntimeSpec::new("recording", "1"));
    }
}
