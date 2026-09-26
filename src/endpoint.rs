use crate::execution::WorkItem;
use crate::identity::{Identity, Kind};
use crate::resource::ResourceFragment;
use crate::runtime::{Runtime, RuntimeContext, RuntimeError};
use crate::transport::{ExecutionRequest, ExecutionResponse, ExecutionStatus};

pub trait ParticipantExecutor: Send + Sync {
    fn execute(
        &self,
        participant_id: &Identity,
        node_id: &Identity,
        resources: ResourceFragment,
        workload: WorkItem,
    ) -> Result<(), EndpointExecutionError>;
}

pub struct RuntimeAdapter<R> {
    runtime: R,
}

impl<R> RuntimeAdapter<R> {
    pub fn new(runtime: R) -> Self {
        Self { runtime }
    }

    pub fn runtime(&self) -> &R {
        &self.runtime
    }
}

impl<R: Runtime> ParticipantExecutor for RuntimeAdapter<R> {
    fn execute(
        &self,
        participant_id: &Identity,
        node_id: &Identity,
        resources: ResourceFragment,
        workload: WorkItem,
    ) -> Result<(), EndpointExecutionError> {
        let context = RuntimeContext::new(participant_id.clone(), node_id.clone(), resources)
            .map_err(EndpointExecutionError::Runtime)?;

        workload
            .validate()
            .map_err(EndpointExecutionError::Runtime)?;

        self.runtime
            .run(&context, workload)
            .map_err(EndpointExecutionError::Runtime)
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct NoopParticipantExecutor;

impl ParticipantExecutor for NoopParticipantExecutor {
    fn execute(
        &self,
        _participant_id: &Identity,
        _node_id: &Identity,
        _resources: ResourceFragment,
        _workload: WorkItem,
    ) -> Result<(), EndpointExecutionError> {
        Ok(())
    }
}

pub struct ParticipantExecutionEndpoint<E> {
    participant_id: Identity,
    executor: E,
}

impl<E> ParticipantExecutionEndpoint<E> {
    pub fn new(participant_id: Identity, executor: E) -> Result<Self, EndpointError> {
        if participant_id.kind != Kind::Participant {
            return Err(EndpointError::InvalidParticipantIdentity);
        }

        Ok(Self {
            participant_id,
            executor,
        })
    }

    pub fn participant_id(&self) -> &Identity {
        &self.participant_id
    }
}

impl<E: ParticipantExecutor> ParticipantExecutionEndpoint<E> {
    pub fn handle(&self, request: ExecutionRequest) -> Result<ExecutionResponse, EndpointError> {
        validate_request(&self.participant_id, &request)?;

        let request_id = request.request_id.clone();
        let workload = request.workload;
        let node_id = request.node_id;

        self.executor
            .execute(
                &self.participant_id,
                &node_id,
                request.unit.resources,
                workload,
            )
            .map_err(|error| EndpointError::ExecutionFailed {
                request_id: request_id.clone(),
                error,
            })?;

        Ok(ExecutionResponse {
            request_id,
            status: ExecutionStatus::Completed,
            error: None,
        })
    }
}

fn validate_request(
    participant_id: &Identity,
    request: &ExecutionRequest,
) -> Result<(), EndpointError> {
    if request.request_id.is_empty() {
        return Err(EndpointError::EmptyRequestId);
    }

    if request.workload.id.is_empty() {
        return Err(EndpointError::EmptyWorkloadId);
    }

    if request.node_id.kind != Kind::LogicalNode {
        return Err(EndpointError::InvalidNodeIdentity);
    }

    if request.unit.participant_id.kind != Kind::Participant {
        return Err(EndpointError::InvalidParticipantIdentity);
    }

    if &request.unit.participant_id != participant_id
        || &request.endpoint.participant_id != participant_id
        || request.endpoint.participant_id != request.unit.participant_id
    {
        return Err(EndpointError::ParticipantMismatch);
    }

    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EndpointError {
    EmptyRequestId,
    EmptyWorkloadId,
    InvalidParticipantIdentity,
    InvalidNodeIdentity,
    ParticipantMismatch,
    ExecutionFailed {
        request_id: String,
        error: EndpointExecutionError,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EndpointExecutionError {
    Rejected,
    ExecutionFailed,
    Runtime(RuntimeError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::ExecutionUnit;
    use crate::identity::Kind;
    use crate::resource::{Cpu, ResourceFragment};
    use crate::transport::ExecutionEndpoint;

    #[derive(Debug, Default, Clone, Copy)]
    struct FailingExecutor;

    impl ParticipantExecutor for FailingExecutor {
        fn execute(
            &self,
            _participant_id: &Identity,
            _node_id: &Identity,
            _resources: ResourceFragment,
            _workload: WorkItem,
        ) -> Result<(), EndpointExecutionError> {
            Err(EndpointExecutionError::Rejected)
        }
    }

    fn request(participant_id: Identity) -> ExecutionRequest {
        ExecutionRequest {
            request_id: "request-1".into(),
            workload: WorkItem {
                id: "work-1".into(),
                payload: vec![1, 2, 3],
            },
            node_id: Identity::new(Kind::LogicalNode),
            unit: ExecutionUnit {
                participant_id: participant_id.clone(),
                resources: ResourceFragment {
                    cpu: Cpu { cores: 1.0 },
                    ..Default::default()
                },
            },
            endpoint: ExecutionEndpoint::new(participant_id, "loopback://participant-1").unwrap(),
        }
    }

    #[test]
    fn endpoint_rejects_non_participant_identity() {
        let result = ParticipantExecutionEndpoint::new(
            Identity::new(Kind::LogicalNode),
            NoopParticipantExecutor,
        );

        assert!(matches!(
            result,
            Err(EndpointError::InvalidParticipantIdentity)
        ));
    }

    #[test]
    fn endpoint_completes_valid_request() {
        let participant_id = Identity::new(Kind::Participant);
        let endpoint =
            ParticipantExecutionEndpoint::new(participant_id.clone(), NoopParticipantExecutor)
                .unwrap();

        let response = endpoint.handle(request(participant_id)).unwrap();

        assert_eq!(response.request_id, "request-1");
        assert_eq!(response.status, ExecutionStatus::Completed);
        assert_eq!(response.error, None);
    }

    #[test]
    fn endpoint_rejects_participant_mismatch() {
        let participant_id = Identity::new(Kind::Participant);
        let other = Identity::new(Kind::Participant);
        let endpoint =
            ParticipantExecutionEndpoint::new(participant_id, NoopParticipantExecutor).unwrap();

        let result = endpoint.handle(request(other));

        assert_eq!(result.unwrap_err(), EndpointError::ParticipantMismatch);
    }

    #[test]
    fn endpoint_reports_executor_failure() {
        let participant_id = Identity::new(Kind::Participant);
        let endpoint =
            ParticipantExecutionEndpoint::new(participant_id.clone(), FailingExecutor).unwrap();

        let result = endpoint.handle(request(participant_id));

        assert_eq!(
            result.unwrap_err(),
            EndpointError::ExecutionFailed {
                request_id: "request-1".into(),
                error: EndpointExecutionError::Rejected,
            }
        );
    }
}
