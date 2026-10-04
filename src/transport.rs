use crate::endpoint::ExecutionHandler;
use crate::execution::{ExecutionUnit, WorkItem};
use crate::identity::{Identity, Kind};
use crate::node::RuntimeSpec;
use crate::resource::ResourceFragment;
use std::collections::BTreeMap;
use std::fmt;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
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

#[derive(Clone)]
pub struct TcpEndpointServer {
    listener: Arc<TcpListener>,
    endpoint: Arc<dyn ExecutionHandler>,
}

impl TcpEndpointServer {
    pub fn bind(
        locator: &str,
        endpoint: Arc<dyn ExecutionHandler>,
    ) -> Result<Self, TransportError> {
        let listener = TcpListener::bind(locator).map_err(|_| TransportError::NetworkFailure)?;
        Ok(Self {
            listener: Arc::new(listener),
            endpoint,
        })
    }

    pub fn local_addr(&self) -> Result<std::net::SocketAddr, TransportError> {
        self.listener
            .local_addr()
            .map_err(|_| TransportError::NetworkFailure)
    }

    pub fn serve_once(&self) -> Result<(), TransportError> {
        let (mut stream, _) = self
            .listener
            .accept()
            .map_err(|_| TransportError::NetworkFailure)?;
        let request = read_request(&mut stream)?;
        let request_id = request.request_id.clone();
        let response = match self.endpoint.handle_request(request) {
            Ok(response) => response,
            Err(error) => ExecutionResponse::failed(request_id, format!("{error:?}")),
        };
        write_response(&mut stream, &response)
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct TcpTransport;

impl TcpTransport {
    pub fn new() -> Self {
        Self
    }
}

impl Transport for TcpTransport {
    fn dispatch(&self, request: ExecutionRequest) -> Result<ExecutionResponse, TransportError> {
        validate_transport_request(&request)?;
        let mut stream = TcpStream::connect(&request.endpoint.locator)
            .map_err(|_| TransportError::NetworkFailure)?;
        write_request(&mut stream, &request)?;
        read_response(&mut stream)
    }
}

fn validate_transport_request(request: &ExecutionRequest) -> Result<(), TransportError> {
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
    request
        .unit
        .resources
        .validate_requirement()
        .map_err(|_| TransportError::InvalidResources)?;
    Ok(())
}

const MAX_FRAME_SIZE: u64 = 64 * 1024 * 1024;

fn write_request(stream: &mut TcpStream, request: &ExecutionRequest) -> Result<(), TransportError> {
    let mut payload = Vec::new();
    write_string(&mut payload, &request.request_id)?;
    write_string(&mut payload, &request.workload.id)?;
    write_bytes(&mut payload, &request.workload.payload)?;
    write_identity(&mut payload, &request.node_id)?;
    write_string(&mut payload, &request.runtime.name)?;
    write_string(&mut payload, &request.runtime.version)?;
    write_identity(&mut payload, &request.unit.participant_id)?;
    write_resources(&mut payload, &request.unit.resources)?;
    write_identity(&mut payload, &request.endpoint.participant_id)?;
    write_string(&mut payload, &request.endpoint.locator)?;
    write_frame(stream, &payload)
}

fn read_request(stream: &mut TcpStream) -> Result<ExecutionRequest, TransportError> {
    let payload = read_frame(stream)?;
    let mut reader = Reader::new(&payload);
    let request = ExecutionRequest {
        request_id: reader.string()?,
        workload: WorkItem {
            id: reader.string()?,
            payload: reader.bytes()?,
        },
        node_id: reader.identity()?,
        runtime: RuntimeSpec {
            name: reader.string()?,
            version: reader.string()?,
        },
        unit: ExecutionUnit {
            participant_id: reader.identity()?,
            resources: reader.resources()?,
        },
        endpoint: ExecutionEndpoint {
            participant_id: reader.identity()?,
            locator: reader.string()?,
        },
    };
    reader.finish()?;
    validate_transport_request(&request)?;
    Ok(request)
}

fn write_response(
    stream: &mut TcpStream,
    response: &ExecutionResponse,
) -> Result<(), TransportError> {
    let mut payload = Vec::new();
    write_string(&mut payload, &response.request_id)?;
    payload.push(status_to_u8(response.status));
    match &response.error {
        Some(error) => {
            payload.push(1);
            write_string(&mut payload, error)?;
        }
        None => payload.push(0),
    }
    write_frame(stream, &payload)
}

fn read_response(stream: &mut TcpStream) -> Result<ExecutionResponse, TransportError> {
    let payload = read_frame(stream)?;
    let mut reader = Reader::new(&payload);
    let request_id = reader.string()?;
    let status = u8_to_status(reader.byte()?)?;
    let error = if reader.byte()? == 1 {
        Some(reader.string()?)
    } else {
        None
    };
    reader.finish()?;
    Ok(ExecutionResponse {
        request_id,
        status,
        error,
    })
}

fn write_frame(stream: &mut TcpStream, payload: &[u8]) -> Result<(), TransportError> {
    if payload.len() as u64 > MAX_FRAME_SIZE {
        return Err(TransportError::FrameTooLarge);
    }
    stream
        .write_all(&(payload.len() as u64).to_be_bytes())
        .and_then(|_| stream.write_all(payload))
        .map_err(|_| TransportError::NetworkFailure)
}

fn read_frame(stream: &mut TcpStream) -> Result<Vec<u8>, TransportError> {
    let mut header = [0_u8; 8];
    stream
        .read_exact(&mut header)
        .map_err(|_| TransportError::NetworkFailure)?;
    let length = u64::from_be_bytes(header);
    if length > MAX_FRAME_SIZE {
        return Err(TransportError::FrameTooLarge);
    }
    let mut payload = vec![0_u8; length as usize];
    stream
        .read_exact(&mut payload)
        .map_err(|_| TransportError::NetworkFailure)?;
    Ok(payload)
}

fn write_string(buffer: &mut Vec<u8>, value: &str) -> Result<(), TransportError> {
    let bytes = value.as_bytes();
    if bytes.len() > u32::MAX as usize {
        return Err(TransportError::FrameTooLarge);
    }
    buffer.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
    buffer.extend_from_slice(bytes);
    Ok(())
}

fn write_bytes(buffer: &mut Vec<u8>, value: &[u8]) -> Result<(), TransportError> {
    if value.len() > u32::MAX as usize {
        return Err(TransportError::FrameTooLarge);
    }
    buffer.extend_from_slice(&(value.len() as u32).to_be_bytes());
    buffer.extend_from_slice(value);
    Ok(())
}

fn write_identity(buffer: &mut Vec<u8>, identity: &Identity) -> Result<(), TransportError> {
    write_string(buffer, &identity.id)?;
    buffer.push(kind_to_u8(identity.kind));
    Ok(())
}

fn write_resources(
    buffer: &mut Vec<u8>,
    resources: ResourceFragment,
) -> Result<(), TransportError> {
    buffer.extend_from_slice(&resources.cpu.cores.to_be_bytes());
    buffer.extend_from_slice(&resources.memory.bytes.to_be_bytes());
    buffer.extend_from_slice(&resources.storage.bytes.to_be_bytes());
    buffer.extend_from_slice(&resources.network.bits_per_second.to_be_bytes());
    buffer.extend_from_slice(&resources.gpu.units.to_be_bytes());
    match resources.lifetime {
        Some(duration) => {
            buffer.push(1);
            buffer.extend_from_slice(&duration.as_secs().to_be_bytes());
            buffer.extend_from_slice(&duration.subsec_nanos().to_be_bytes());
        }
        None => buffer.push(0),
    }
    Ok(())
}

fn kind_to_u8(kind: Kind) -> u8 {
    match kind {
        Kind::Participant => 0,
        Kind::LogicalNode => 1,
        Kind::Lease => 2,
        Kind::VirtualMachine => 3,
    }
}

fn status_to_u8(status: ExecutionStatus) -> u8 {
    match status {
        ExecutionStatus::Pending => 0,
        ExecutionStatus::Dispatched => 1,
        ExecutionStatus::Running => 2,
        ExecutionStatus::Completed => 3,
        ExecutionStatus::Failed => 4,
        ExecutionStatus::Cancelled => 5,
    }
}

struct Reader<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> Reader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8], TransportError> {
        let end = self
            .position
            .checked_add(length)
            .ok_or(TransportError::MalformedFrame)?;
        if end > self.bytes.len() {
            return Err(TransportError::MalformedFrame);
        }
        let value = &self.bytes[self.position..end];
        self.position = end;
        Ok(value)
    }

    fn byte(&mut self) -> Result<u8, TransportError> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> Result<u32, TransportError> {
        Ok(u32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }

    fn u64(&mut self) -> Result<u64, TransportError> {
        Ok(u64::from_be_bytes(self.take(8)?.try_into().unwrap()))
    }

    fn f64(&mut self) -> Result<f64, TransportError> {
        Ok(f64::from_be_bytes(self.take(8)?.try_into().unwrap()))
    }

    fn string(&mut self) -> Result<String, TransportError> {
        let length = self.u32()? as usize;
        String::from_utf8(self.take(length)?.to_vec())
            .map_err(|_| TransportError::MalformedFrame)
    }

    fn bytes(&mut self) -> Result<Vec<u8>, TransportError> {
        let length = self.u32()? as usize;
        Ok(self.take(length)?.to_vec())
    }

    fn identity(&mut self) -> Result<Identity, TransportError> {
        let id = self.string()?;
        let kind = match self.byte()? {
            0 => Kind::Participant,
            1 => Kind::LogicalNode,
            2 => Kind::Lease,
            3 => Kind::VirtualMachine,
            _ => return Err(TransportError::MalformedFrame),
        };
        Identity::parse(&id, kind).map_err(|_| TransportError::MalformedFrame)
    }

    fn resources(&mut self) -> Result<ResourceFragment, TransportError> {
        let cpu = crate::resource::Cpu { cores: self.f64()? };
        let memory = crate::resource::Memory { bytes: self.u64()? };
        let storage = crate::resource::Storage { bytes: self.u64()? };
        let network = crate::resource::Network {
            bits_per_second: self.u64()?,
        };
        let gpu = crate::resource::Gpu { units: self.u32()? };
        let lifetime = match self.byte()? {
            0 => None,
            1 => Some(std::time::Duration::new(self.u64()?, self.u32()?)),
            _ => return Err(TransportError::MalformedFrame),
        };
        let resources = ResourceFragment {
            cpu,
            memory,
            storage,
            network,
            gpu,
            lifetime,
        };
        resources
            .validate_requirement()
            .map_err(|_| TransportError::InvalidResources)?;
        Ok(resources)
    }

    fn finish(&self) -> Result<(), TransportError> {
        if self.position == self.bytes.len() {
            Ok(())
        } else {
            Err(TransportError::MalformedFrame)
        }
    }
}

fn u8_to_status(value: u8) -> Result<ExecutionStatus, TransportError> {
    match value {
        0 => Ok(ExecutionStatus::Pending),
        1 => Ok(ExecutionStatus::Dispatched),
        2 => Ok(ExecutionStatus::Running),
        3 => Ok(ExecutionStatus::Completed),
        4 => Ok(ExecutionStatus::Failed),
        5 => Ok(ExecutionStatus::Cancelled),
        _ => Err(TransportError::MalformedFrame),
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
    InvalidResources,
    EndpointUnavailable,
    EndpointAlreadyRegistered,
    RequestRejected,
    NetworkFailure,
    FrameTooLarge,
    MalformedFrame,
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
    fn tcp_transport_executes_through_real_socket() {
        let participant_id = Identity::new(Kind::Participant);
        let endpoint = Arc::new(
            crate::endpoint::ParticipantExecutionEndpoint::new(
                participant_id.clone(),
                crate::endpoint::NoopParticipantExecutor,
            )
            .unwrap(),
        );
        let server = TcpEndpointServer::bind("127.0.0.1:0", endpoint).unwrap();
        let address = server.local_addr().unwrap();

        let server_thread = std::thread::spawn(move || server.serve_once());
        let mut request = request();
        request.unit.participant_id = participant_id.clone();
        request.endpoint = ExecutionEndpoint::new(
            participant_id,
            address.to_string(),
        )
        .unwrap();

        let response = TcpTransport::new().dispatch(request).unwrap();
        assert_eq!(response.status, ExecutionStatus::Completed);
        assert!(response.error.is_none());
        server_thread.join().unwrap().unwrap();
    }

    #[test]
    fn tcp_transport_rejects_unreachable_endpoint() {
        let mut request = request();
        request.endpoint.locator = "127.0.0.1:1".into();

        assert_eq!(
            TcpTransport::new().dispatch(request).unwrap_err(),
            TransportError::NetworkFailure
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
