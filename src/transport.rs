use crate::execution::{ExecutionUnit, WorkItem};
use crate::identity::{Identity, Kind};
use crate::node::RuntimeSpec;
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionEndpoint {
    pub participant_id: Identity,
    pub locator: String,
}

impl ExecutionEndpoint {
    pub fn new(
        participant_id: Identity,
        locator: impl Into<String>,
    ) -> Result<Self, TransportError> {
        if participant_id.kind != Kind::Participant {
            return Err(TransportError::InvalidEndpointIdentity);
        }

        let locator = locator.into();
        if locator.is_empty() {
            return Err(TransportError::EmptyLocator);
        }

        Ok(Self {
            participant_id,
            locator,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ExecutionRequest {
    pub request_id: String,
    pub workload: WorkItem,
    pub node_id: Identity,
    pub runtime: RuntimeSpec,
    pub unit: ExecutionUnit,
    pub endpoint: ExecutionEndpoint,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionStatus {
    Pending,
    Dispatched,
    Running,
    Completed,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionResponse {
    pub request_id: String,
    pub status: ExecutionStatus,
    pub error: Option<String>,
}

impl ExecutionResponse {
    pub fn completed(request_id: impl Into<String>) -> Self {
        Self {
            request_id: request_id.into(),
            status: ExecutionStatus::Completed,
            error: None,
        }
    }

    pub fn failed(request_id: impl Into<String>, error: impl Into<String>) -> Self {
        Self {
            request_id: request_id.into(),
            status: ExecutionStatus::Failed,
            error: Some(error.into()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportError {
    EmptyRequestId,
    EmptyWorkloadId,
    InvalidEndpointIdentity,
    EmptyLocator,
    InvalidNodeIdentity,
    InvalidParticipantIdentity,
    InvalidRuntimeSpec,
    EndpointUnavailable,
    RequestRejected,
}

impl fmt::Display for TransportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for TransportError {}

pub trait Transport: Send + Sync {
    fn dispatch(&self, request: ExecutionRequest) -> Result<ExecutionResponse, TransportError>;
}

#[derive(Debug, Default, Clone, Copy)]
pub struct LoopbackTransport;

impl Transport for LoopbackTransport {
    fn dispatch(&self, request: ExecutionRequest) -> Result<ExecutionResponse, TransportError> {
        if request.request_id.is_empty() {
            return Err(TransportError::EmptyRequestId);
        }

        if request.workload.id.is_empty() {
            return Err(TransportError::EmptyWorkloadId);
        }

        if request.node_id.kind != Kind::LogicalNode {
            return Err(TransportError::InvalidNodeIdentity);
        }

        if request.runtime.name.is_empty() {
            return Err(TransportError::InvalidRuntimeSpec);
        }

        if request.runtime.version.is_empty() {
            return Err(TransportError::InvalidRuntimeSpec);
        }

        if request.unit.participant_id.kind != Kind::Participant {
            return Err(TransportError::InvalidParticipantIdentity);
        }

        if request.endpoint.participant_id != request.unit.participant_id {
            return Err(TransportError::InvalidParticipantIdentity);
        }

        Ok(ExecutionResponse {
            request_id: request.request_id,
            status: ExecutionStatus::Dispatched,
            error: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::ExecutionUnit;
    use crate::resource::{Cpu, ResourceFragment};

    fn request() -> ExecutionRequest {
        let participant_id = Identity::new(Kind::Participant);
        ExecutionRequest {
            request_id: "request-1".into(),
            workload: WorkItem {
                id: "work-1".into(),
                payload: vec![1, 2, 3],
            },
            node_id: Identity::new(Kind::LogicalNode),
            runtime: RuntimeSpec {
                name: "recording".into(),
                version: "1".into(),
            },
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
        let result = ExecutionEndpoint::new(Identity::new(Kind::LogicalNode), "loopback://node");
        assert_eq!(result.unwrap_err(), TransportError::InvalidEndpointIdentity);
    }

    #[test]
    fn loopback_dispatch_preserves_request_identity() {
        let request = request();
        let response = LoopbackTransport.dispatch(request.clone()).unwrap();

        assert_eq!(response.request_id, request.request_id);
        assert_eq!(response.status, ExecutionStatus::Dispatched);
        assert_eq!(response.error, None);
    }

    #[test]
    fn loopback_rejects_endpoint_participant_mismatch() {
        let mut request = request();
        request.endpoint.participant_id = Identity::new(Kind::Participant);

        assert_eq!(
            LoopbackTransport.dispatch(request).unwrap_err(),
            TransportError::InvalidParticipantIdentity
        );
    }

    #[test]
    fn loopback_rejects_invalid_runtime_spec() {
        let mut request = request();
        request.runtime.version.clear();

        assert_eq!(
            LoopbackTransport.dispatch(request).unwrap_err(),
            TransportError::InvalidRuntimeSpec
        );
    }

    #[test]
    fn response_helpers_encode_terminal_states() {
        let completed = ExecutionResponse::completed("request-1");
        assert_eq!(completed.status, ExecutionStatus::Completed);
        assert_eq!(completed.error, None);

        let failed = ExecutionResponse::failed("request-2", "execution failed");
        assert_eq!(failed.status, ExecutionStatus::Failed);
        assert_eq!(failed.error.as_deref(), Some("execution failed"));
    }
}
