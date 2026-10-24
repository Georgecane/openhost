use crate::endpoint::ExecutionHandler;
use crate::execution::{ExecutionUnit, WorkItem};
use crate::identity::{Identity, Kind};
use crate::node::RuntimeSpec;
use crate::resource::ResourceFragment;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use rustls::{
    ClientConfig, ClientConnection, RootCertStore, ServerConfig, ServerConnection, StreamOwned,
};
use std::collections::BTreeMap;
use std::fmt;
use std::fs::File;
use std::io::{BufReader, Read, Write};
use std::net::{IpAddr, SocketAddr, TcpListener, TcpStream, ToSocketAddrs};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

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
    /// Authenticated identity expected from the caller when using TLS.
    pub source_participant_id: Option<Identity>,
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

const IO_TIMEOUT: Duration = Duration::from_secs(10);
type ClientAllowlist = BTreeMap<String, Vec<Vec<u8>>>;
type ClientAuthorization<'a> = Option<(&'a ClientAllowlist, Option<&'a [u8]>)>;

/// A participant identity pinned to one or more exact client leaf certificates.
#[derive(Debug, Clone)]
pub struct AuthorizedClientCertificate {
    participant_id: Identity,
    certificate_der: Vec<u8>,
}

impl AuthorizedClientCertificate {
    /// Load the first certificate in a PEM file and bind it to a participant identity.
    ///
    /// Multiple certificates may be supplied for the same identity to support rotation.
    pub fn from_pem<P: AsRef<Path>>(
        participant_id: Identity,
        certificate_pem: P,
    ) -> Result<Self, TransportError> {
        if participant_id.kind != Kind::Participant || participant_id.validate().is_err() {
            return Err(TransportError::InvalidSourceIdentity);
        }

        let certificate = load_certificates(certificate_pem.as_ref())?
            .into_iter()
            .next()
            .ok_or(TransportError::TlsConfigurationFailure)?;

        Ok(Self {
            participant_id,
            certificate_der: certificate.as_ref().to_vec(),
        })
    }

    pub fn participant_id(&self) -> &Identity {
        &self.participant_id
    }
}

#[derive(Clone)]
pub struct TcpEndpointServer {
    listener: Arc<TcpListener>,
    endpoint: Arc<dyn ExecutionHandler>,
    tls_config: Option<Arc<ServerConfig>>,
    authorized_clients: Option<Arc<ClientAllowlist>>,
}

impl TcpEndpointServer {
    /// Bind an unencrypted TCP endpoint for trusted local networks or tests.
    pub fn bind(
        locator: &str,
        endpoint: Arc<dyn ExecutionHandler>,
    ) -> Result<Self, TransportError> {
        Self::bind_inner(locator, endpoint, None, None)
    }

    /// Bind an endpoint using mutual TLS and explicit participant-to-certificate authorization.
    ///
    /// In addition to validating the client's certificate chain, the server checks that the
    /// request's source participant ID is mapped to the exact leaf certificate presented by
    /// that client. Use a dedicated client CA for this endpoint's trust domain.
    pub fn bind_mtls<C: AsRef<Path>, K: AsRef<Path>, A: AsRef<Path>>(
        locator: &str,
        endpoint: Arc<dyn ExecutionHandler>,
        server_certificate_chain_pem: C,
        server_private_key_pem: K,
        trusted_client_ca_pem: A,
        authorized_clients: Vec<AuthorizedClientCertificate>,
    ) -> Result<Self, TransportError> {
        let config = build_server_tls_config(
            server_certificate_chain_pem.as_ref(),
            server_private_key_pem.as_ref(),
            trusted_client_ca_pem.as_ref(),
        )?;
        let allowed = build_client_allowlist(authorized_clients)?;
        Self::bind_inner(
            locator,
            endpoint,
            Some(Arc::new(config)),
            Some(Arc::new(allowed)),
        )
    }

    fn bind_inner(
        locator: &str,
        endpoint: Arc<dyn ExecutionHandler>,
        tls_config: Option<Arc<ServerConfig>>,
        authorized_clients: Option<Arc<ClientAllowlist>>,
    ) -> Result<Self, TransportError> {
        let listener = TcpListener::bind(locator).map_err(|_| TransportError::NetworkFailure)?;
        Ok(Self {
            listener: Arc::new(listener),
            endpoint,
            tls_config,
            authorized_clients,
        })
    }

    pub fn local_addr(&self) -> Result<std::net::SocketAddr, TransportError> {
        self.listener
            .local_addr()
            .map_err(|_| TransportError::NetworkFailure)
    }

    /// Accept and handle one connection. This is useful for simple embedding and tests.
    pub fn serve_once(&self) -> Result<(), TransportError> {
        let (stream, _) = self
            .listener
            .accept()
            .map_err(|_| TransportError::NetworkFailure)?;
        handle_connection(
            stream,
            Arc::clone(&self.endpoint),
            self.tls_config.clone(),
            self.authorized_clients.clone(),
        )
    }

    /// Accept exactly the requested number of connections, handling them concurrently.
    /// Returns after all accepted connections have finished.
    pub fn serve_n(&self, connection_count: usize) -> Result<(), TransportError> {
        let mut workers = Vec::with_capacity(connection_count);
        for _ in 0..connection_count {
            let (stream, _) = self
                .listener
                .accept()
                .map_err(|_| TransportError::NetworkFailure)?;
            let endpoint = Arc::clone(&self.endpoint);
            let tls_config = self.tls_config.clone();
            let authorized_clients = self.authorized_clients.clone();
            workers.push(std::thread::spawn(move || {
                handle_connection(stream, endpoint, tls_config, authorized_clients)
            }));
        }

        for worker in workers {
            worker
                .join()
                .map_err(|_| TransportError::NetworkFailure)??;
        }
        Ok(())
    }

    /// Run continuously with a bounded worker pool and bounded pending-connection queue.
    ///
    /// This method runs until the listener fails. Use `serve_until` when graceful shutdown
    /// needs to be controlled by the caller.
    pub fn serve(&self) -> Result<(), TransportError> {
        let shutdown = AtomicBool::new(false);
        self.serve_until(&shutdown)
    }

    /// Serve until `shutdown` becomes true, then drain accepted connections and join workers.
    ///
    /// The listener is polled every 10 milliseconds so shutdown does not require a wake-up
    /// connection. An execution handler that never returns can delay graceful shutdown.
    pub fn serve_until(&self, shutdown: &AtomicBool) -> Result<(), TransportError> {
        self.listener
            .set_nonblocking(true)
            .map_err(|_| TransportError::NetworkFailure)?;

        let result = self.run_until_shutdown(shutdown);
        let restore_result = self
            .listener
            .set_nonblocking(false)
            .map_err(|_| TransportError::NetworkFailure);

        result.and(restore_result)
    }

    fn run_until_shutdown(&self, shutdown: &AtomicBool) -> Result<(), TransportError> {
        const ACCEPT_POLL_INTERVAL: Duration = Duration::from_millis(10);

        let worker_count = std::thread::available_parallelism()
            .map(usize::from)
            .unwrap_or(4)
            .clamp(2, 32);
        let queue_capacity = worker_count.saturating_mul(2).max(16);
        let (sender, receiver) = std::sync::mpsc::sync_channel::<TcpStream>(queue_capacity);
        let receiver = Arc::new(std::sync::Mutex::new(receiver));
        let mut workers = Vec::with_capacity(worker_count);

        for _ in 0..worker_count {
            let receiver = Arc::clone(&receiver);
            let endpoint = Arc::clone(&self.endpoint);
            let tls_config = self.tls_config.clone();
            let authorized_clients = self.authorized_clients.clone();
            workers.push(std::thread::spawn(move || loop {
                let stream = {
                    let Ok(receiver) = receiver.lock() else {
                        return;
                    };
                    match receiver.recv() {
                        Ok(stream) => stream,
                        Err(_) => return,
                    }
                };
                let _ = handle_connection(
                    stream,
                    Arc::clone(&endpoint),
                    tls_config.clone(),
                    authorized_clients.clone(),
                );
            }));
        }

        let accept_result = loop {
            if shutdown.load(Ordering::Acquire) {
                break Ok(());
            }

            match self.listener.accept() {
                Ok((stream, _)) => {
                    if sender.send(stream).is_err() {
                        break Err(TransportError::NetworkFailure);
                    }
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(ACCEPT_POLL_INTERVAL);
                }
                Err(_) => break Err(TransportError::NetworkFailure),
            }
        };

        drop(sender);
        let mut worker_panicked = false;
        for worker in workers {
            worker_panicked |= worker.join().is_err();
        }

        if worker_panicked {
            return Err(TransportError::NetworkFailure);
        }

        accept_result
    }
}

fn handle_connection(
    mut socket: TcpStream,
    endpoint: Arc<dyn ExecutionHandler>,
    tls_config: Option<Arc<ServerConfig>>,
    authorized_clients: Option<Arc<ClientAllowlist>>,
) -> Result<(), TransportError> {
    configure_stream(&socket)?;
    if let Some(config) = tls_config {
        let mut connection =
            ServerConnection::new(config).map_err(|_| TransportError::TlsConfigurationFailure)?;
        while connection.is_handshaking() {
            connection
                .complete_io(&mut socket)
                .map_err(|_| TransportError::TlsHandshakeFailure)?;
        }
        let peer_certificate = connection
            .peer_certificates()
            .and_then(|chain| chain.first())
            .map(|certificate| certificate.as_ref().to_vec());
        let mut stream = StreamOwned::new(connection, socket);
        let authorization = authorized_clients
            .as_deref()
            .map(|allowed| (allowed, peer_certificate.as_deref()));
        handle_request_stream(&mut stream, endpoint, authorization)
    } else {
        handle_request_stream(&mut socket, endpoint, None)
    }
}

fn handle_request_stream<S: Read + Write>(
    stream: &mut S,
    endpoint: Arc<dyn ExecutionHandler>,
    authorization: ClientAuthorization<'_>,
) -> Result<(), TransportError> {
    let request = read_request(stream)?;

    if let Some((allowed_clients, peer_certificate)) = authorization {
        let source = request
            .source_participant_id
            .as_ref()
            .ok_or(TransportError::MissingSourceIdentity)?;
        let authorized_certificates = allowed_clients
            .get(&source.id)
            .ok_or(TransportError::UnauthorizedClient)?;
        let peer_certificate = peer_certificate.ok_or(TransportError::UnauthorizedClient)?;
        if !authorized_certificates
            .iter()
            .any(|certificate| certificate.as_slice() == peer_certificate)
        {
            return Err(TransportError::UnauthorizedClient);
        }
    }

    let request_id = request.request_id.clone();
    let response = match endpoint.handle_request(request) {
        Ok(response) => response,
        Err(error) => ExecutionResponse::failed(request_id, format!("{error:?}")),
    };
    write_response(stream, &response)
}

fn build_client_allowlist(
    authorized_clients: Vec<AuthorizedClientCertificate>,
) -> Result<ClientAllowlist, TransportError> {
    if authorized_clients.is_empty() {
        return Err(TransportError::EmptyClientAllowlist);
    }

    let mut allowlist = BTreeMap::<String, Vec<Vec<u8>>>::new();
    let mut certificate_owners = BTreeMap::<Vec<u8>, String>::new();
    for client in authorized_clients {
        if let Some(owner) = certificate_owners.get(&client.certificate_der) {
            if owner != &client.participant_id.id {
                return Err(TransportError::CertificateAssignedToMultipleIdentities);
            }
            return Err(TransportError::DuplicateAuthorizedClientCertificate);
        }
        certificate_owners.insert(
            client.certificate_der.clone(),
            client.participant_id.id.clone(),
        );
        allowlist
            .entry(client.participant_id.id)
            .or_default()
            .push(client.certificate_der);
    }
    Ok(allowlist)
}

fn configure_stream(stream: &TcpStream) -> Result<(), TransportError> {
    stream
        .set_read_timeout(Some(IO_TIMEOUT))
        .and_then(|_| stream.set_write_timeout(Some(IO_TIMEOUT)))
        .map_err(|_| TransportError::NetworkFailure)
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
        let mut stream = connect_with_timeout(&request.endpoint.locator)?;
        configure_stream(&stream)?;
        write_request(&mut stream, &request)?;
        let response = read_response(&mut stream)?;
        if response.request_id != request.request_id {
            return Err(TransportError::ResponseIdMismatch);
        }
        Ok(response)
    }
}

/// A client transport that encrypts requests and authenticates both peers with TLS.
///
/// Connections use the hostname or IP address from each endpoint locator for server-name
/// verification. The client configuration must include a client certificate for mTLS.
#[derive(Clone)]
pub struct TlsTcpTransport {
    config: Arc<ClientConfig>,
}

impl TlsTcpTransport {
    pub fn new(config: Arc<ClientConfig>) -> Self {
        Self { config }
    }

    /// Build an mTLS client from a trusted server CA, a client certificate chain, and its key.
    pub fn from_pem_files<C: AsRef<Path>, K: AsRef<Path>, A: AsRef<Path>>(
        trusted_server_ca_pem: A,
        client_certificate_chain_pem: C,
        client_private_key_pem: K,
    ) -> Result<Self, TransportError> {
        let roots = load_root_certificates(trusted_server_ca_pem.as_ref())?;
        let certificates = load_certificates(client_certificate_chain_pem.as_ref())?;
        let private_key = load_private_key(client_private_key_pem.as_ref())?;
        let config = ClientConfig::builder()
            .with_root_certificates(roots)
            .with_client_auth_cert(certificates, private_key)
            .map_err(|_| TransportError::TlsConfigurationFailure)?;
        Ok(Self::new(Arc::new(config)))
    }
}

impl Transport for TlsTcpTransport {
    fn dispatch(&self, request: ExecutionRequest) -> Result<ExecutionResponse, TransportError> {
        validate_transport_request(&request)?;
        if request.source_participant_id.is_none() {
            return Err(TransportError::MissingSourceIdentity);
        }
        let server_name = server_name_for_locator(&request.endpoint.locator)?;
        let mut socket = connect_with_timeout(&request.endpoint.locator)?;
        configure_stream(&socket)?;

        let mut connection = ClientConnection::new(Arc::clone(&self.config), server_name)
            .map_err(|_| TransportError::TlsConfigurationFailure)?;
        while connection.is_handshaking() {
            connection
                .complete_io(&mut socket)
                .map_err(|_| TransportError::TlsHandshakeFailure)?;
        }

        let mut stream = StreamOwned::new(connection, socket);
        write_request(&mut stream, &request)?;
        let response = read_response(&mut stream)?;
        if response.request_id != request.request_id {
            return Err(TransportError::ResponseIdMismatch);
        }
        Ok(response)
    }
}

fn server_name_for_locator(locator: &str) -> Result<ServerName<'static>, TransportError> {
    if let Ok(address) = locator.parse::<SocketAddr>() {
        return Ok(ServerName::IpAddress(address.ip().into()));
    }

    let (host, port) = locator
        .rsplit_once(':')
        .ok_or(TransportError::InvalidLocator)?;
    if port.parse::<u16>().is_err() {
        return Err(TransportError::InvalidLocator);
    }
    let host = host
        .strip_prefix('[')
        .and_then(|host| host.strip_suffix(']'))
        .unwrap_or(host);
    if host.is_empty() {
        return Err(TransportError::InvalidLocator);
    }
    if let Ok(address) = host.parse::<IpAddr>() {
        return Ok(ServerName::IpAddress(address.into()));
    }
    ServerName::try_from(host.to_owned()).map_err(|_| TransportError::TlsConfigurationFailure)
}

fn load_certificates(path: &Path) -> Result<Vec<CertificateDer<'static>>, TransportError> {
    let file = File::open(path).map_err(|_| TransportError::TlsConfigurationFailure)?;
    let mut reader = BufReader::new(file);
    let certificates = rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| TransportError::TlsConfigurationFailure)?;
    if certificates.is_empty() {
        return Err(TransportError::TlsConfigurationFailure);
    }
    Ok(certificates)
}

fn load_private_key(path: &Path) -> Result<PrivateKeyDer<'static>, TransportError> {
    let file = File::open(path).map_err(|_| TransportError::TlsConfigurationFailure)?;
    rustls_pemfile::private_key(&mut BufReader::new(file))
        .map_err(|_| TransportError::TlsConfigurationFailure)?
        .ok_or(TransportError::TlsConfigurationFailure)
}

fn load_root_certificates(path: &Path) -> Result<RootCertStore, TransportError> {
    let mut roots = RootCertStore::empty();
    for certificate in load_certificates(path)? {
        roots
            .add(certificate)
            .map_err(|_| TransportError::TlsConfigurationFailure)?;
    }
    Ok(roots)
}

fn build_server_tls_config<C: AsRef<Path>, K: AsRef<Path>, A: AsRef<Path>>(
    server_certificate_chain_pem: C,
    server_private_key_pem: K,
    trusted_client_ca_pem: A,
) -> Result<ServerConfig, TransportError> {
    let client_roots = load_root_certificates(trusted_client_ca_pem.as_ref())?;
    let client_verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(client_roots))
        .build()
        .map_err(|_| TransportError::TlsConfigurationFailure)?;
    let certificates = load_certificates(server_certificate_chain_pem.as_ref())?;
    let private_key = load_private_key(server_private_key_pem.as_ref())?;
    ServerConfig::builder()
        .with_client_cert_verifier(client_verifier)
        .with_single_cert(certificates, private_key)
        .map_err(|_| TransportError::TlsConfigurationFailure)
}

fn connect_with_timeout(locator: &str) -> Result<TcpStream, TransportError> {
    let addresses = locator
        .to_socket_addrs()
        .map_err(|_| TransportError::NetworkFailure)?;
    for address in addresses {
        if let Ok(stream) = TcpStream::connect_timeout(&address, IO_TIMEOUT) {
            return Ok(stream);
        }
    }
    Err(TransportError::NetworkFailure)
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
    if let Some(source) = &request.source_participant_id
        && (source.kind != Kind::Participant || source.validate().is_err())
    {
        return Err(TransportError::InvalidSourceIdentity);
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
const PROTOCOL_VERSION: u8 = 1;

fn write_request<S: Write>(stream: &mut S, request: &ExecutionRequest) -> Result<(), TransportError> {
    let mut payload = Vec::new();
    payload.push(PROTOCOL_VERSION);
    write_string(&mut payload, &request.request_id)?;
    match &request.source_participant_id {
        Some(identity) => {
            payload.push(1);
            write_identity(&mut payload, identity)?;
        }
        None => payload.push(0),
    }
    write_string(&mut payload, &request.workload.id)?;
    write_bytes(&mut payload, &request.workload.payload)?;
    write_identity(&mut payload, &request.node_id)?;
    write_string(&mut payload, &request.runtime.name)?;
    write_string(&mut payload, &request.runtime.version)?;
    write_identity(&mut payload, &request.unit.participant_id)?;
    write_resources(&mut payload, request.unit.resources)?;
    write_identity(&mut payload, &request.endpoint.participant_id)?;
    write_string(&mut payload, &request.endpoint.locator)?;
    write_frame(stream, &payload)
}

fn read_request<S: Read>(stream: &mut S) -> Result<ExecutionRequest, TransportError> {
    let payload = read_frame(stream)?;
    let mut reader = Reader::new(&payload);
    let version = reader.byte()?;
    if version != PROTOCOL_VERSION {
        return Err(TransportError::UnsupportedProtocolVersion);
    }
    let request_id = reader.string()?;
    let source_participant_id = match reader.byte()? {
        0 => None,
        1 => Some(reader.identity()?),
        _ => return Err(TransportError::MalformedFrame),
    };
    let request = ExecutionRequest {
        request_id,
        source_participant_id,
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

fn write_response<S: Write>(
    stream: &mut S,
    response: &ExecutionResponse,
) -> Result<(), TransportError> {
    let mut payload = Vec::new();
    payload.push(PROTOCOL_VERSION);
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

fn read_response<S: Read>(stream: &mut S) -> Result<ExecutionResponse, TransportError> {
    let payload = read_frame(stream)?;
    let mut reader = Reader::new(&payload);
    let version = reader.byte()?;
    if version != PROTOCOL_VERSION {
        return Err(TransportError::UnsupportedProtocolVersion);
    }
    let request_id = reader.string()?;
    let status = u8_to_status(reader.byte()?)?;
    let error = match reader.byte()? {
        0 => None,
        1 => Some(reader.string()?),
        _ => return Err(TransportError::MalformedFrame),
    };
    reader.finish()?;
    Ok(ExecutionResponse {
        request_id,
        status,
        error,
    })
}

fn write_frame<S: Write>(stream: &mut S, payload: &[u8]) -> Result<(), TransportError> {
    if payload.len() as u64 > MAX_FRAME_SIZE {
        return Err(TransportError::FrameTooLarge);
    }
    stream
        .write_all(&(payload.len() as u64).to_be_bytes())
        .and_then(|_| stream.write_all(payload))
        .map_err(|_| TransportError::NetworkFailure)
}

fn read_frame<S: Read>(stream: &mut S) -> Result<Vec<u8>, TransportError> {
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
    InvalidLocator,
    InvalidSourceIdentity,
    MissingSourceIdentity,
    UnauthorizedClient,
    EmptyClientAllowlist,
    DuplicateAuthorizedClientCertificate,
    CertificateAssignedToMultipleIdentities,
    InvalidNodeIdentity,
    InvalidParticipantIdentity,
    InvalidRuntimeSpec,
    InvalidResources,
    EndpointUnavailable,
    EndpointAlreadyRegistered,
    RequestRejected,
    NetworkFailure,
    UnsupportedProtocolVersion,
    TlsConfigurationFailure,
    TlsHandshakeFailure,
    ResponseIdMismatch,
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
            source_participant_id: None,
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
    fn tcp_server_shuts_down_cleanly_and_restores_listener_mode() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::time::Duration;

        let participant_id = Identity::new(Kind::Participant);
        let endpoint = Arc::new(
            crate::endpoint::ParticipantExecutionEndpoint::new(
                participant_id.clone(),
                crate::endpoint::NoopParticipantExecutor,
            )
            .unwrap(),
        );
        let server = TcpEndpointServer::bind("127.0.0.1:0", endpoint).unwrap();
        let shutdown = Arc::new(AtomicBool::new(false));
        let server_for_thread = server.clone();
        let shutdown_for_thread = Arc::clone(&shutdown);

        let server_thread = std::thread::spawn(move || {
            server_for_thread.serve_until(&shutdown_for_thread)
        });

        std::thread::sleep(Duration::from_millis(30));
        shutdown.store(true, Ordering::Release);
        server_thread.join().unwrap().unwrap();

        // A subsequent blocking accept proves the listener mode was restored.
        let address = server.local_addr().unwrap();
        let server_for_request = server.clone();
        let request_thread = std::thread::spawn(move || server_for_request.serve_once());
        let mut request = request();
        request.unit.participant_id = participant_id.clone();
        request.endpoint =
            ExecutionEndpoint::new(participant_id, address.to_string()).unwrap();
        let response = TcpTransport::new().dispatch(request).unwrap();

        assert_eq!(response.status, ExecutionStatus::Completed);
        request_thread.join().unwrap().unwrap();
    }

    struct TlsTestFiles {
        directory: std::path::PathBuf,
        server_certificate: std::path::PathBuf,
        server_key: std::path::PathBuf,
        client_certificate: std::path::PathBuf,
        client_key: std::path::PathBuf,
        untrusted_client_certificate: std::path::PathBuf,
        untrusted_client_key: std::path::PathBuf,
    }

    impl TlsTestFiles {
        fn new() -> Self {
            use std::fs;
            let directory =
                std::env::temp_dir().join(format!("openhost-tls-test-{}", uuid::Uuid::new_v4()));
            fs::create_dir_all(&directory).unwrap();

            let server = rcgen::generate_simple_self_signed(vec!["127.0.0.1".to_owned()])
                .unwrap();
            let client =
                rcgen::generate_simple_self_signed(vec!["openhost-client".to_owned()]).unwrap();
            let untrusted_client =
                rcgen::generate_simple_self_signed(vec!["untrusted-client".to_owned()]).unwrap();

            let server_certificate = directory.join("server-cert.pem");
            let server_key = directory.join("server-key.pem");
            let client_certificate = directory.join("client-cert.pem");
            let client_key = directory.join("client-key.pem");
            let untrusted_client_certificate = directory.join("untrusted-client-cert.pem");
            let untrusted_client_key = directory.join("untrusted-client-key.pem");

            fs::write(&server_certificate, server.cert.pem()).unwrap();
            fs::write(&server_key, server.signing_key.serialize_pem()).unwrap();
            fs::write(&client_certificate, client.cert.pem()).unwrap();
            fs::write(&client_key, client.signing_key.serialize_pem()).unwrap();
            fs::write(&untrusted_client_certificate, untrusted_client.cert.pem()).unwrap();
            fs::write(
                &untrusted_client_key,
                untrusted_client.signing_key.serialize_pem(),
            )
            .unwrap();

            Self {
                directory,
                server_certificate,
                server_key,
                client_certificate,
                client_key,
                untrusted_client_certificate,
                untrusted_client_key,
            }
        }
    }

    impl Drop for TlsTestFiles {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.directory);
        }
    }

    #[test]
    fn mtls_transport_executes_over_authenticated_tls() {
        let files = TlsTestFiles::new();
        let participant_id = Identity::new(Kind::Participant);
        let endpoint = Arc::new(
            crate::endpoint::ParticipantExecutionEndpoint::new(
                participant_id.clone(),
                crate::endpoint::NoopParticipantExecutor,
            )
            .unwrap(),
        );
        let source_participant_id = Identity::new(Kind::Participant);
        let grant = AuthorizedClientCertificate::from_pem(
            source_participant_id.clone(),
            &files.client_certificate,
        )
        .unwrap();
        let server = TcpEndpointServer::bind_mtls(
            "127.0.0.1:0",
            endpoint,
            &files.server_certificate,
            &files.server_key,
            &files.client_certificate,
            vec![grant],
        )
        .unwrap();
        let address = server.local_addr().unwrap();
        let server_thread = std::thread::spawn(move || server.serve_once());

        let transport = TlsTcpTransport::from_pem_files(
            &files.server_certificate,
            &files.client_certificate,
            &files.client_key,
        )
        .unwrap();
        let mut request = request();
        request.source_participant_id = Some(source_participant_id);
        request.unit.participant_id = participant_id.clone();
        request.endpoint =
            ExecutionEndpoint::new(participant_id, address.to_string()).unwrap();

        let response = transport.dispatch(request).unwrap();
        assert_eq!(response.status, ExecutionStatus::Completed);
        assert!(response.error.is_none());
        server_thread.join().unwrap().unwrap();
    }

    #[test]
    fn mtls_server_rejects_source_identity_not_bound_to_client_certificate() {
        let files = TlsTestFiles::new();
        let target_participant_id = Identity::new(Kind::Participant);
        let authorized_source_id = Identity::new(Kind::Participant);
        let spoofed_source_id = Identity::new(Kind::Participant);
        let endpoint = Arc::new(
            crate::endpoint::ParticipantExecutionEndpoint::new(
                target_participant_id.clone(),
                crate::endpoint::NoopParticipantExecutor,
            )
            .unwrap(),
        );
        let grant = AuthorizedClientCertificate::from_pem(
            authorized_source_id,
            &files.client_certificate,
        )
        .unwrap();
        let server = TcpEndpointServer::bind_mtls(
            "127.0.0.1:0",
            endpoint,
            &files.server_certificate,
            &files.server_key,
            &files.client_certificate,
            vec![grant],
        )
        .unwrap();
        let address = server.local_addr().unwrap();
        let server_thread = std::thread::spawn(move || server.serve_once());

        let transport = TlsTcpTransport::from_pem_files(
            &files.server_certificate,
            &files.client_certificate,
            &files.client_key,
        )
        .unwrap();
        let mut request = request();
        request.source_participant_id = Some(spoofed_source_id);
        request.unit.participant_id = target_participant_id.clone();
        request.endpoint =
            ExecutionEndpoint::new(target_participant_id, address.to_string()).unwrap();

        assert_eq!(
            transport.dispatch(request).unwrap_err(),
            TransportError::NetworkFailure
        );
        assert_eq!(
            server_thread.join().unwrap().unwrap_err(),
            TransportError::UnauthorizedClient
        );
    }

    #[test]
    fn mtls_server_rejects_untrusted_client_certificate() {
        let files = TlsTestFiles::new();
        let participant_id = Identity::new(Kind::Participant);
        let endpoint = Arc::new(
            crate::endpoint::ParticipantExecutionEndpoint::new(
                participant_id.clone(),
                crate::endpoint::NoopParticipantExecutor,
            )
            .unwrap(),
        );
        let source_participant_id = Identity::new(Kind::Participant);
        let grant = AuthorizedClientCertificate::from_pem(
            source_participant_id.clone(),
            &files.client_certificate,
        )
        .unwrap();
        let server = TcpEndpointServer::bind_mtls(
            "127.0.0.1:0",
            endpoint,
            &files.server_certificate,
            &files.server_key,
            &files.client_certificate,
            vec![grant],
        )
        .unwrap();
        let address = server.local_addr().unwrap();
        let server_thread = std::thread::spawn(move || server.serve_once());

        let transport = TlsTcpTransport::from_pem_files(
            &files.server_certificate,
            &files.untrusted_client_certificate,
            &files.untrusted_client_key,
        )
        .unwrap();
        let mut request = request();
        request.source_participant_id = Some(source_participant_id);
        request.unit.participant_id = participant_id.clone();
        request.endpoint =
            ExecutionEndpoint::new(participant_id, address.to_string()).unwrap();

        let client_result = transport.dispatch(request);
        assert!(matches!(
            client_result,
            Err(TransportError::TlsHandshakeFailure | TransportError::NetworkFailure)
        ));

        let server_result = server_thread.join().unwrap();
        assert!(matches!(
            server_result,
            Err(TransportError::TlsHandshakeFailure | TransportError::NetworkFailure)
        ));
    }

    #[test]
    fn tcp_server_handles_concurrent_connections() {
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
        let server_thread = std::thread::spawn(move || server.serve_n(2));

        let mut first = request();
        first.request_id = "concurrent-1".into();
        first.unit.participant_id = participant_id.clone();
        first.endpoint = ExecutionEndpoint::new(participant_id.clone(), address.to_string()).unwrap();

        let mut second = first.clone();
        second.request_id = "concurrent-2".into();

        let first_thread = std::thread::spawn(move || TcpTransport::new().dispatch(first));
        let second_thread = std::thread::spawn(move || TcpTransport::new().dispatch(second));

        let first_response = first_thread.join().unwrap().unwrap();
        let second_response = second_thread.join().unwrap().unwrap();
        server_thread.join().unwrap().unwrap();

        assert_eq!(first_response.request_id, "concurrent-1");
        assert_eq!(second_response.request_id, "concurrent-2");
        assert_eq!(first_response.status, ExecutionStatus::Completed);
        assert_eq!(second_response.status, ExecutionStatus::Completed);
    }

    #[test]
    fn tcp_transport_rejects_response_id_mismatch() {
        struct MismatchedHandler {
            participant_id: Identity,
        }

        impl crate::endpoint::ExecutionHandler for MismatchedHandler {
            fn participant_id(&self) -> &Identity {
                &self.participant_id
            }

            fn handle_request(
                &self,
                request: ExecutionRequest,
            ) -> Result<ExecutionResponse, crate::endpoint::EndpointError> {
                Ok(ExecutionResponse::completed(format!("{}-wrong", request.request_id)))
            }
        }

        let participant_id = Identity::new(Kind::Participant);
        let endpoint = Arc::new(MismatchedHandler {
            participant_id: participant_id.clone(),
        });
        let server = TcpEndpointServer::bind("127.0.0.1:0", endpoint).unwrap();
        let address = server.local_addr().unwrap();
        let server_thread = std::thread::spawn(move || server.serve_once());

        let mut request = request();
        request.unit.participant_id = participant_id.clone();
        request.endpoint =
            ExecutionEndpoint::new(participant_id, address.to_string()).unwrap();

        assert_eq!(
            TcpTransport::new().dispatch(request).unwrap_err(),
            TransportError::ResponseIdMismatch
        );
        server_thread.join().unwrap().unwrap();
    }

    #[test]
    fn tcp_server_rejects_unsupported_request_protocol_version() {
        let participant_id = Identity::new(Kind::Participant);
        let endpoint = Arc::new(
            crate::endpoint::ParticipantExecutionEndpoint::new(
                participant_id,
                crate::endpoint::NoopParticipantExecutor,
            )
            .unwrap(),
        );
        let server = TcpEndpointServer::bind("127.0.0.1:0", endpoint).unwrap();
        let address = server.local_addr().unwrap();
        let server_thread = std::thread::spawn(move || server.serve_once());

        let mut stream = TcpStream::connect(address).unwrap();
        configure_stream(&stream).unwrap();
        write_frame(&mut stream, &[PROTOCOL_VERSION + 1]).unwrap();

        assert_eq!(
            server_thread.join().unwrap().unwrap_err(),
            TransportError::UnsupportedProtocolVersion
        );
    }

    #[test]
    fn tcp_transport_rejects_unsupported_response_protocol_version() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server_thread = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            write_frame(&mut stream, &[PROTOCOL_VERSION + 1]).unwrap();
        });

        let mut stream = TcpStream::connect(address).unwrap();
        configure_stream(&stream).unwrap();
        assert_eq!(
            read_response(&mut stream).unwrap_err(),
            TransportError::UnsupportedProtocolVersion
        );
        server_thread.join().unwrap();
    }

    #[test]
    fn tcp_transport_rejects_invalid_response_error_flag() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server_thread = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut payload = Vec::new();
            payload.push(PROTOCOL_VERSION);
            write_string(&mut payload, "request-1").unwrap();
            payload.push(status_to_u8(ExecutionStatus::Completed));
            payload.push(2);
            write_frame(&mut stream, &payload).unwrap();
        });

        let mut stream = TcpStream::connect(address).unwrap();
        configure_stream(&stream).unwrap();
        assert_eq!(read_response(&mut stream).unwrap_err(), TransportError::MalformedFrame);
        server_thread.join().unwrap();
    }

    #[test]
    fn tcp_dispatcher_executes_plan_across_multiple_remote_endpoints() {
        use crate::execution::{ExecutionDispatcher, ExecutionPlan};
        use crate::node::LogicalNode;
        use crate::resource::{Allocation, CompositeResource};

        let participant_a = Identity::new(Kind::Participant);
        let participant_b = Identity::new(Kind::Participant);
        let endpoint_a = Arc::new(
            crate::endpoint::ParticipantExecutionEndpoint::new(
                participant_a.clone(),
                crate::endpoint::NoopParticipantExecutor,
            )
            .unwrap(),
        );
        let endpoint_b = Arc::new(
            crate::endpoint::ParticipantExecutionEndpoint::new(
                participant_b.clone(),
                crate::endpoint::NoopParticipantExecutor,
            )
            .unwrap(),
        );
        let server_a = TcpEndpointServer::bind("127.0.0.1:0", endpoint_a).unwrap();
        let server_b = TcpEndpointServer::bind("127.0.0.1:0", endpoint_b).unwrap();
        let address_a = server_a.local_addr().unwrap();
        let address_b = server_b.local_addr().unwrap();

        let server_thread_a = std::thread::spawn(move || server_a.serve_once());
        let server_thread_b = std::thread::spawn(move || server_b.serve_once());

        let node = LogicalNode {
            id: Identity::new(Kind::LogicalNode).id,
            resources: CompositeResource::compose(vec![
                Allocation {
                    participant_id: participant_a.id.clone(),
                    resources: ResourceFragment {
                        cpu: Cpu { cores: 1.0 },
                        ..Default::default()
                    },
                },
                Allocation {
                    participant_id: participant_b.id.clone(),
                    resources: ResourceFragment {
                        cpu: Cpu { cores: 1.0 },
                        ..Default::default()
                    },
                },
            ])
            .unwrap(),
            runtime: RuntimeSpec::new("recording", "1"),
        };
        let workload = WorkItem {
            id: "tcp-multi-participant".into(),
            payload: vec![9, 8, 7],
        };
        let plan = ExecutionPlan::from_node(&node, &workload).unwrap();
        let endpoints = vec![
            ExecutionEndpoint::new(participant_a, address_a.to_string()).unwrap(),
            ExecutionEndpoint::new(participant_b, address_b.to_string()).unwrap(),
        ];
        let dispatcher = ExecutionDispatcher::new(TcpTransport::new(), endpoints);

        let receipt = dispatcher.dispatch(&plan, &workload).unwrap();

        assert_eq!(receipt.workload_id, workload.id);
        assert_eq!(receipt.node_id.id, node.id);
        assert_eq!(receipt.dispatched_units, 2);
        server_thread_a.join().unwrap().unwrap();
        server_thread_b.join().unwrap().unwrap();
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
