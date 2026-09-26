use crate::identity::{Identity, Kind};
use crate::node::RuntimeSpec;
use crate::resource::ResourceFragment;
use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

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

    fn run(&self, context: &RuntimeContext, workload: Workload) -> Result<(), RuntimeError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeRegistryError {
    EmptyName,
    EmptyVersion,
    DuplicateRuntime,
    RuntimeNotFound,
}

impl fmt::Display for RuntimeRegistryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for RuntimeRegistryError {}

#[derive(Default)]
pub struct RuntimeRegistry {
    runtimes: BTreeMap<(String, String), Arc<dyn Runtime>>,
}

impl RuntimeRegistry {
    pub fn register(
        &mut self,
        spec: RuntimeSpec,
        runtime: Arc<dyn Runtime>,
    ) -> Result<(), RuntimeRegistryError> {
        validate_spec(&spec)?;

        let key = (spec.name, spec.version);
        if self.runtimes.contains_key(&key) {
            return Err(RuntimeRegistryError::DuplicateRuntime);
        }

        self.runtimes.insert(key, runtime);
        Ok(())
    }

    pub fn resolve(&self, spec: &RuntimeSpec) -> Result<Arc<dyn Runtime>, RuntimeRegistryError> {
        validate_spec(spec)?;

        self.runtimes
            .get(&(spec.name.clone(), spec.version.clone()))
            .cloned()
            .ok_or(RuntimeRegistryError::RuntimeNotFound)
    }

    pub fn contains(&self, spec: &RuntimeSpec) -> Result<bool, RuntimeRegistryError> {
        validate_spec(spec)?;

        Ok(self
            .runtimes
            .contains_key(&(spec.name.clone(), spec.version.clone())))
    }

    pub fn len(&self) -> usize {
        self.runtimes.len()
    }
}

fn validate_spec(spec: &RuntimeSpec) -> Result<(), RuntimeRegistryError> {
    if spec.name.is_empty() {
        return Err(RuntimeRegistryError::EmptyName);
    }

    if spec.version.is_empty() {
        return Err(RuntimeRegistryError::EmptyVersion);
    }

    Ok(())
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

        fn run(&self, _context: &RuntimeContext, _workload: Workload) -> Result<(), RuntimeError> {
            Ok(())
        }
    }

    fn spec(name: &str, version: &str) -> RuntimeSpec {
        RuntimeSpec {
            name: name.into(),
            version: version.into(),
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

    #[test]
    fn registry_resolves_runtime_by_spec() {
        let mut registry = RuntimeRegistry::default();
        let runtime = Arc::new(RecordingRuntime);
        let runtime_ref: Arc<dyn Runtime> = runtime.clone();

        registry
            .register(spec("recording", "1"), runtime_ref)
            .unwrap();

        let resolved = registry.resolve(&spec("recording", "1")).unwrap();
        assert_eq!(resolved.name(), "recording");
        assert_eq!(registry.len(), 1);
    }

    #[test]
    fn registry_rejects_duplicate_runtime() {
        let mut registry = RuntimeRegistry::default();
        let runtime: Arc<dyn Runtime> = Arc::new(RecordingRuntime);

        registry
            .register(spec("recording", "1"), runtime.clone())
            .unwrap();
        assert_eq!(
            registry
                .register(spec("recording", "1"), runtime)
                .unwrap_err(),
            RuntimeRegistryError::DuplicateRuntime
        );
    }

    #[test]
    fn registry_rejects_unknown_runtime() {
        let registry = RuntimeRegistry::default();

        let result = registry.resolve(&spec("missing", "1"));
        assert!(matches!(
            result,
            Err(RuntimeRegistryError::RuntimeNotFound)
        ));
    }

    #[test]
    fn registry_rejects_invalid_spec() {
        let mut registry = RuntimeRegistry::default();
        let runtime: Arc<dyn Runtime> = Arc::new(RecordingRuntime);

        assert_eq!(
            registry.register(spec("", "1"), runtime).unwrap_err(),
            RuntimeRegistryError::EmptyName
        );
    }
}
