use crate::endpoint::ExecutionHandler;
use crate::execution::{ExecutionUnit, WorkItem};
use crate::identity::{Identity, Kind};
use crate::node::RuntimeSpec;
use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, RwLock};

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

#[derive(Default)]
pub struct EndpointTransport {
    endpoints: RwLock<BTreeMap<String, Arc<dyn ExecutionHandler>>>,
}

impl EndpointTransport {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&self, endpoint: Arc<dyn ExecutionHandler>) -> Result<(), TransportError> {
        let participant_id = endpoint.participant_id();
        if participant_id.kind != Kind::Participant {
            return Err(TransportError::InvalidParticipantIdentity);
        }
        let mut endpoints = self
            .endpoints
            .write()
            .expect("endpoint transport lock poisoned");
        if endpoints.contains_key(&participant_id.id) {
            return Err(TransportError::EndpointAlreadyRegistered);
        }
        endpoints.insert(participant_id.id.clone(), endpoint);
        Ok(())
    }

    pub fn unregister(&self, participant_id: &Identity) -> Result<(), TransportError> {
        if participant_id.kind != Kind::Participant {
            return Err(TransportError::InvalidParticipantIdentity);
        }
        self.endpoints
            .write()
            .expect("endpoint transport lock poisoned")
            .remove(&participant_id.id);
        Ok(())
    }

    pub fn len(&self) -> usize {
        self.endpoints
            .read()
            .expect("endpoint transport lock poisoned")
            .len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Transport for EndpointTransport {
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
        if request.runtime.name.is_empty() || request.runtime.version.is_empty() {
            return Err(TransportError::InvalidRuntimeSpec);
        }
        if request.unit.participant_id.kind != Kind::Participant {
            return Err(TransportError::InvalidParticipantIdentity);
        }
        if request.endpoint.participant_id != request.unit.participant_id {
            return Err(TransportError::InvalidParticipantIdentity);
        }
        let endpoint = self
            .endpoints
            .read()
            .expect("endpoint transport lock poisoned")
            .get(&request.unit.participant_id.id)
            .cloned()
            .ok_or(TransportError::EndpointUnavailable)?;
        endpoint
            .handle_request(request)
            .map_err(|_| TransportError::RequestRejected)
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
    EndpointAlreadyRegistered,
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
    fn endpoint_transport_dispatches_to_registered_endpoint() {
        let participant_id = Identity::new(Kind::Participant);
        let endpoint = Arc::new(
            crate::endpoint::ParticipantExecutionEndpoint::new(
                participant_id.clone(),
                crate::endpoint::NoopParticipantExecutor,
            )
            .unwrap(),
        );
        let transport = EndpointTransport::new();
        transport.register(endpoint).unwrap();

        let mut request = request();
        request.unit.participant_id = participant_id.clone();
        request.endpoint.participant_id = participant_id;

        let response = transport.dispatch(request).unwrap();
        assert_eq!(response.status, ExecutionStatus::Completed);
        assert_eq!(transport.len(), 1);
    }

    #[test]
    fn endpoint_transport_rejects_missing_endpoint() {
        let transport = EndpointTransport::new();
        assert_eq!(
            transport.dispatch(request()).unwrap_err(),
            TransportError::EndpointUnavailable
        );
    }

    #[test]
    fn endpoint_transport_rejects_duplicate_registration() {
        let participant_id = Identity::new(Kind::Participant);
        let first = Arc::new(
            crate::endpoint::ParticipantExecutionEndpoint::new(
                participant_id.clone(),
                crate::endpoint::NoopParticipantExecutor,
            )
            .unwrap(),
        );
        let second = Arc::new(
            crate::endpoint::ParticipantExecutionEndpoint::new(
                participant_id,
                crate::endpoint::NoopParticipantExecutor,
            )
            .unwrap(),
        );
        let transport = EndpointTransport::new();
        transport.register(first).unwrap();
        assert_eq!(
            transport.register(second).unwrap_err(),
            TransportError::EndpointAlreadyRegistered
        );
    }

    #[test]
    fn endpoint_transport_unregisters_endpoint() {
        let participant_id = Identity::new(Kind::Participant);
        let endpoint = Arc::new(
            crate::endpoint::ParticipantExecutionEndpoint::new(
                participant_id.clone(),
                crate::endpoint::NoopParticipantExecutor,
            )
            .unwrap(),
        );
        let transport = EndpointTransport::new();
        transport.register(endpoint).unwrap();
        transport.unregister(&participant_id).unwrap();
        assert!(transport.is_empty());
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
