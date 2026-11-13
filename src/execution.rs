use crate::identity::{Identity, Kind};
use crate::node::{LogicalNode, RuntimeSpec};
use crate::resource::ResourceFragment;
pub use crate::runtime::Workload as WorkItem;
use std::collections::HashSet;
use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};

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
    InvalidConcurrencyLimit,
    DuplicateWorkloadId,
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
    fn from_responses(workload_id: String, node_id: Identity, responses: usize) -> Self {
        Self {
            workload_id,
            node_id,
            dispatched_units: responses,
        }
    }
}

pub struct ExecutionDispatcher<T> {
    source_participant_id: Option<Identity>,
    transport: T,
    endpoints: Vec<crate::transport::ExecutionEndpoint>,
    max_concurrency: usize,
}

const DEFAULT_MAX_CONCURRENCY: usize = 8;

#[derive(Debug, Clone, PartialEq)]
pub struct ExecutionJob {
    pub plan: ExecutionPlan,
    pub workload: WorkItem,
}

impl ExecutionJob {
    pub fn new(plan: ExecutionPlan, workload: WorkItem) -> Self {
        Self { plan, workload }
    }
}

impl<T> ExecutionDispatcher<T> {
    /// Create a dispatcher for transports that do not require caller identity, such as local
    /// in-process execution or explicitly trusted plain TCP networks.
    pub fn new(transport: T, endpoints: Vec<crate::transport::ExecutionEndpoint>) -> Self {
        Self {
            source_participant_id: None,
            transport,
            endpoints,
            max_concurrency: DEFAULT_MAX_CONCURRENCY,
        }
    }

    /// Create a dispatcher that identifies the participant issuing execution requests.
    ///
    /// TLS transports require this identity and the server must authorize its client certificate
    /// for the declared identity.
    pub fn with_identity(
        source_participant_id: Identity,
        transport: T,
        endpoints: Vec<crate::transport::ExecutionEndpoint>,
    ) -> Self {
        Self {
            source_participant_id: Some(source_participant_id),
            transport,
            endpoints,
            max_concurrency: DEFAULT_MAX_CONCURRENCY,
        }
    }

    pub fn transport(&self) -> &T {
        &self.transport
    }

    /// Set the maximum number of participant requests dispatched simultaneously.
    pub fn with_max_concurrency(mut self, max_concurrency: usize) -> Result<Self, ExecutionError> {
        if max_concurrency == 0 {
            return Err(ExecutionError::InvalidConcurrencyLimit);
        }
        self.max_concurrency = max_concurrency;
        Ok(self)
    }

    pub fn max_concurrency(&self) -> usize {
        self.max_concurrency
    }
}

impl<T: crate::transport::Transport> ExecutionDispatcher<T> {
    pub fn dispatch(
        &self,
        plan: &ExecutionPlan,
        workload: &WorkItem,
    ) -> Result<DispatchReceipt, ExecutionError> {
        let requests = requests_for_plan(
            &self.source_participant_id,
            &self.endpoints,
            plan,
            workload,
        )?;
        let responses = dispatch_bounded(&self.transport, &requests, self.max_concurrency)?;
        let dispatched = count_dispatched(&requests, &responses)?;
        Ok(DispatchReceipt::from_responses(
            workload.id.clone(),
            plan.node_id.clone(),
            dispatched,
        ))
    }
}

impl<T: crate::transport::Transport> ExecutionBackend for ExecutionDispatcher<T> {
    fn execute(
        &self,
        plan: &ExecutionPlan,
        workload: &WorkItem,
    ) -> Result<ExecutionReceipt, ExecutionError> {
        let receipt = self.dispatch(plan, workload)?;
        Ok(ExecutionReceipt {
            workload_id: receipt.workload_id,
            node_id: receipt.node_id,
            dispatched_units: receipt.dispatched_units,
        })
    }
}



fn requests_for_plan(
    source_participant_id: &Option<Identity>,
    endpoints: &[crate::transport::ExecutionEndpoint],
    plan: &ExecutionPlan,
    workload: &WorkItem,
) -> Result<Vec<crate::transport::ExecutionRequest>, ExecutionError> {
    if plan.workload_id != workload.id {
        return Err(ExecutionError::WorkloadMismatch);
    }
    if workload.id.is_empty() {
        return Err(ExecutionError::EmptyWorkloadId);
    }
    if plan.units.is_empty() {
        return Err(ExecutionError::EmptyPlan);
    }

    plan.units
        .iter()
        .map(|unit| {
            let endpoint = endpoints
                .iter()
                .find(|endpoint| endpoint.participant_id == unit.participant_id)
                .ok_or(ExecutionError::BackendRejected)?;

            Ok(crate::transport::ExecutionRequest {
                request_id: format!("{}:{}", workload.id, unit.participant_id.id),
                source_participant_id: source_participant_id.clone(),
                workload: workload.clone(),
                node_id: plan.node_id.clone(),
                runtime: plan.runtime.clone(),
                unit: unit.clone(),
                endpoint: endpoint.clone(),
            })
        })
        .collect()
}

/// Dispatch requests through a fixed-size worker set. Results are restored to request order,
/// regardless of the order in which remote participants respond.
fn dispatch_bounded<T: crate::transport::Transport>(
    transport: &T,
    requests: &[crate::transport::ExecutionRequest],
    max_concurrency: usize,
) -> Result<Vec<crate::transport::ExecutionResponse>, ExecutionError> {
    if max_concurrency == 0 {
        return Err(ExecutionError::InvalidConcurrencyLimit);
    }
    if requests.is_empty() {
        return Ok(Vec::new());
    }

    let worker_count = max_concurrency.min(requests.len());
    let next_request = AtomicUsize::new(0);
    let (sender, receiver) = std::sync::mpsc::channel();
    let worker_panicked = std::thread::scope(|scope| {
        let mut workers = Vec::with_capacity(worker_count);
        for _ in 0..worker_count {
            let sender = sender.clone();
            let next_request = &next_request;
            workers.push(scope.spawn(move || loop {
                let index = next_request.fetch_add(1, Ordering::Relaxed);
                if index >= requests.len() {
                    break;
                }

                let response = transport.dispatch(requests[index].clone());
                if sender.send((index, response)).is_err() {
                    break;
                }
            }));
        }
        drop(sender);

        let mut panicked = false;
        for worker in workers {
            panicked |= worker.join().is_err();
        }
        panicked
    });

    if worker_panicked {
        return Err(ExecutionError::BackendRejected);
    }

    let mut ordered: Vec<Option<Result<crate::transport::ExecutionResponse, crate::transport::TransportError>>> =
        (0..requests.len()).map(|_| None).collect();
    for _ in 0..requests.len() {
        let (index, response) = receiver
            .recv()
            .map_err(|_| ExecutionError::BackendRejected)?;
        let slot = ordered
            .get_mut(index)
            .ok_or(ExecutionError::BackendRejected)?;
        if slot.replace(response).is_some() {
            return Err(ExecutionError::BackendRejected);
        }
    }

    ordered
        .into_iter()
        .map(|response| {
            response
                .ok_or(ExecutionError::BackendRejected)?
                .map_err(|_| ExecutionError::BackendRejected)
        })
        .collect()
}

fn count_dispatched(
    requests: &[crate::transport::ExecutionRequest],
    responses: &[crate::transport::ExecutionResponse],
) -> Result<usize, ExecutionError> {
    if requests.len() != responses.len() {
        return Err(ExecutionError::BackendRejected);
    }

    let mut dispatched = 0;
    for (request, response) in requests.iter().zip(responses) {
        if response.request_id != request.request_id {
            return Err(ExecutionError::BackendRejected);
        }
        match response.status {
            crate::transport::ExecutionStatus::Dispatched
            | crate::transport::ExecutionStatus::Running
            | crate::transport::ExecutionStatus::Completed => dispatched += 1,
            crate::transport::ExecutionStatus::Pending
            | crate::transport::ExecutionStatus::Failed
            | crate::transport::ExecutionStatus::Cancelled => {
                return Err(ExecutionError::BackendRejected);
            }
        }
    }
    Ok(dispatched)
}

/// Executes multiple independent workloads across participant allocations using one shared,
/// bounded parallel dispatch pool. Each workload is fanned out to every unit in its plan.
pub struct ExecutionEngine<T> {
    dispatcher: ExecutionDispatcher<T>,
}

impl<T> ExecutionEngine<T> {
    pub fn new(
        transport: T,
        endpoints: Vec<crate::transport::ExecutionEndpoint>,
    ) -> Self {
        Self {
            dispatcher: ExecutionDispatcher::new(transport, endpoints),
        }
    }

    pub fn with_identity(
        source_participant_id: Identity,
        transport: T,
        endpoints: Vec<crate::transport::ExecutionEndpoint>,
    ) -> Self {
        Self {
            dispatcher: ExecutionDispatcher::with_identity(
                source_participant_id,
                transport,
                endpoints,
            ),
        }
    }

    pub fn with_max_concurrency(mut self, max_concurrency: usize) -> Result<Self, ExecutionError> {
        self.dispatcher = self.dispatcher.with_max_concurrency(max_concurrency)?;
        Ok(self)
    }

    pub fn max_concurrency(&self) -> usize {
        self.dispatcher.max_concurrency()
    }

    pub fn transport(&self) -> &T {
        self.dispatcher.transport()
    }
}

impl<T: crate::transport::Transport> ExecutionEngine<T> {
    /// Execute a batch of independent workloads. Every job is dispatched to all participant
    /// units in its execution plan, with a shared batch concurrency limit.
    ///
    /// The returned receipts preserve input job order. A failed or mismatched participant
    /// response rejects the batch; successful partial results are not reported as complete.
    pub fn execute_batch(
        &self,
        jobs: &[ExecutionJob],
    ) -> Result<Vec<ExecutionReceipt>, ExecutionError> {
        if jobs.is_empty() {
            return Err(ExecutionError::EmptyPlan);
        }

        let mut seen_workloads = HashSet::with_capacity(jobs.len());
        let mut requests = Vec::new();
        let mut owners = Vec::new();

        for (job_index, job) in jobs.iter().enumerate() {
            if !seen_workloads.insert(job.workload.id.as_str()) {
                return Err(ExecutionError::DuplicateWorkloadId);
            }

            let job_requests = requests_for_plan(
                &self.dispatcher.source_participant_id,
                &self.dispatcher.endpoints,
                &job.plan,
                &job.workload,
            )?;
            owners.extend(std::iter::repeat_n(job_index, job_requests.len()));
            requests.extend(job_requests);
        }

        let responses = dispatch_bounded(
            &self.dispatcher.transport,
            &requests,
            self.dispatcher.max_concurrency,
        )?;

        let mut dispatched_by_job = vec![0; jobs.len()];
        for ((request, response), owner) in requests.iter().zip(&responses).zip(owners) {
            if response.request_id != request.request_id {
                return Err(ExecutionError::BackendRejected);
            }
            match response.status {
                crate::transport::ExecutionStatus::Dispatched
                | crate::transport::ExecutionStatus::Running
                | crate::transport::ExecutionStatus::Completed => {
                    dispatched_by_job[owner] += 1;
                }
                crate::transport::ExecutionStatus::Pending
                | crate::transport::ExecutionStatus::Failed
                | crate::transport::ExecutionStatus::Cancelled => {
                    return Err(ExecutionError::BackendRejected);
                }
            }
        }

        Ok(jobs
            .iter()
            .zip(dispatched_by_job)
            .map(|(job, dispatched_units)| ExecutionReceipt {
                workload_id: job.workload.id.clone(),
                node_id: job.plan.node_id.clone(),
                dispatched_units,
            })
            .collect())
    }
}

impl<T: crate::transport::Transport> ExecutionBackend for ExecutionEngine<T> {
    fn execute(
        &self,
        plan: &ExecutionPlan,
        workload: &WorkItem,
    ) -> Result<ExecutionReceipt, ExecutionError> {
        let job = ExecutionJob::new(plan.clone(), workload.clone());
        self.execute_batch(&[job])?
            .into_iter()
            .next()
            .ok_or(ExecutionError::BackendRejected)
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

    #[derive(Clone, Copy)]
    struct StatusTransport(crate::transport::ExecutionStatus);

    impl crate::transport::Transport for StatusTransport {
        fn dispatch(
            &self,
            request: crate::transport::ExecutionRequest,
        ) -> Result<crate::transport::ExecutionResponse, crate::transport::TransportError> {
            Ok(crate::transport::ExecutionResponse {
                request_id: request.request_id,
                status: self.0,
                error: None,
            })
        }
    }

    fn dispatcher_with_status(
        node: &LogicalNode,
        status: crate::transport::ExecutionStatus,
    ) -> ExecutionDispatcher<StatusTransport> {
        let endpoints = node
            .resources
            .allocations
            .iter()
            .map(|allocation| {
                let participant_id =
                    Identity::parse(&allocation.participant_id, Kind::Participant).unwrap();
                crate::transport::ExecutionEndpoint::new(participant_id, "loopback://participant")
                    .unwrap()
            })
            .collect();
        ExecutionDispatcher::new(StatusTransport(status), endpoints)
    }

    #[test]
    fn dispatcher_propagates_source_participant_identity() {
        struct IdentityCheckingTransport(Identity);

        impl crate::transport::Transport for IdentityCheckingTransport {
            fn dispatch(
                &self,
                request: crate::transport::ExecutionRequest,
            ) -> Result<crate::transport::ExecutionResponse, crate::transport::TransportError>
            {
                assert_eq!(
                    request.source_participant_id.as_ref(),
                    Some(&self.0),
                    "dispatcher did not attach its caller identity"
                );
                Ok(crate::transport::ExecutionResponse::completed(
                    request.request_id,
                ))
            }
        }

        let workload = WorkItem {
            id: "identity-propagation".into(),
            payload: Vec::new(),
        };
        let plan_node = node();
        let plan = ExecutionPlan::from_node(&plan_node, &workload).unwrap();
        let endpoints = plan
            .units
            .iter()
            .map(|unit| {
                crate::transport::ExecutionEndpoint::new(
                    unit.participant_id.clone(),
                    "loopback://participant",
                )
                .unwrap()
            })
            .collect();
        let caller = Identity::new(Kind::Participant);
        let dispatcher = ExecutionDispatcher::with_identity(
            caller.clone(),
            IdentityCheckingTransport(caller),
            endpoints,
        );

        let receipt = dispatcher.dispatch(&plan, &workload).unwrap();
        assert_eq!(receipt.dispatched_units, plan.units.len());
    }

    #[test]
    fn dispatcher_accepts_non_terminal_execution_statuses() {
        let workload = WorkItem {
            id: "work-1".into(),
            payload: Vec::new(),
        };
        let plan_node = node();
        let plan = ExecutionPlan::from_node(&plan_node, &workload).unwrap();

        for status in [
            crate::transport::ExecutionStatus::Dispatched,
            crate::transport::ExecutionStatus::Running,
            crate::transport::ExecutionStatus::Completed,
        ] {
            let dispatcher = dispatcher_with_status(&plan_node, status);
            let receipt = dispatcher.dispatch(&plan, &workload).unwrap();
            assert_eq!(receipt.dispatched_units, 2);
        }
    }

    #[test]
    fn dispatcher_dispatches_units_concurrently() {
        use std::sync::{Arc, Barrier};
        use std::time::Duration;

        struct BlockingTransport {
            barrier: Arc<Barrier>,
        }

        impl crate::transport::Transport for BlockingTransport {
            fn dispatch(
                &self,
                request: crate::transport::ExecutionRequest,
            ) -> Result<crate::transport::ExecutionResponse, crate::transport::TransportError>
            {
                self.barrier.wait();
                Ok(crate::transport::ExecutionResponse::completed(
                    request.request_id,
                ))
            }
        }

        let workload = WorkItem {
            id: "work-concurrent".into(),
            payload: Vec::new(),
        };
        let plan_node = node();
        let plan = ExecutionPlan::from_node(&plan_node, &workload).unwrap();
        let dispatcher = ExecutionDispatcher::new(
            BlockingTransport {
                barrier: Arc::new(Barrier::new(plan.units.len())),
            },
            dispatcher_with_status(&plan_node, crate::transport::ExecutionStatus::Completed)
                .endpoints
                .clone(),
        );

        let result = std::thread::spawn(move || dispatcher.dispatch(&plan, &workload)).join();

        assert!(
            result.is_ok(),
            "dispatcher did not complete concurrent dispatch"
        );
        assert_eq!(result.unwrap().unwrap().dispatched_units, 2);
        let _ = Duration::from_secs(0);
    }

    #[test]
    fn dispatcher_rejects_non_success_execution_statuses() {
        let workload = WorkItem {
            id: "work-1".into(),
            payload: Vec::new(),
        };
        let plan_node = node();
        let plan = ExecutionPlan::from_node(&plan_node, &workload).unwrap();

        for status in [
            crate::transport::ExecutionStatus::Pending,
            crate::transport::ExecutionStatus::Failed,
            crate::transport::ExecutionStatus::Cancelled,
        ] {
            let dispatcher = dispatcher_with_status(&plan_node, status);
            assert_eq!(
                dispatcher.dispatch(&plan, &workload).unwrap_err(),
                ExecutionError::BackendRejected
            );
        }
    }

    #[test]
    fn dispatcher_rejects_response_id_mismatch() {
        #[derive(Clone, Copy)]
        struct MismatchedResponseTransport;

        impl crate::transport::Transport for MismatchedResponseTransport {
            fn dispatch(
                &self,
                request: crate::transport::ExecutionRequest,
            ) -> Result<crate::transport::ExecutionResponse, crate::transport::TransportError>
            {
                Ok(crate::transport::ExecutionResponse {
                    request_id: format!("{}:unexpected", request.workload.id),
                    status: crate::transport::ExecutionStatus::Completed,
                    error: None,
                })
            }
        }

        let workload = WorkItem {
            id: "work-1".into(),
            payload: Vec::new(),
        };
        let plan_node = node();
        let plan = ExecutionPlan::from_node(&plan_node, &workload).unwrap();
        let dispatcher = ExecutionDispatcher::new(
            MismatchedResponseTransport,
            dispatcher_with_status(&plan_node, crate::transport::ExecutionStatus::Completed)
                .endpoints
                .clone(),
        );

        assert_eq!(
            dispatcher.dispatch(&plan, &workload).unwrap_err(),
            ExecutionError::BackendRejected
        );
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

    fn node_with_units(count: usize) -> LogicalNode {
        let allocations = (0..count)
            .map(|_| Allocation {
                participant_id: Identity::new(Kind::Participant).id,
                resources: ResourceFragment {
                    cpu: Cpu { cores: 1.0 },
                    ..Default::default()
                },
            })
            .collect();
        LogicalNode {
            id: Identity::new(Kind::LogicalNode).id,
            resources: CompositeResource::compose(allocations).unwrap(),
            runtime: RuntimeSpec::new("recording", "1"),
        }
    }


    #[test]
    fn execution_engine_batches_workloads_with_a_shared_concurrency_limit() {
        use crate::transport::{ExecutionResponse, ExecutionRequest, Transport, TransportError};
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;
        use std::time::Duration;

        struct TrackingTransport {
            active: Arc<AtomicUsize>,
            peak: Arc<AtomicUsize>,
        }

        impl Transport for TrackingTransport {
            fn dispatch(
                &self,
                request: ExecutionRequest,
            ) -> Result<ExecutionResponse, TransportError> {
                let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
                self.peak.fetch_max(active, Ordering::SeqCst);
                std::thread::sleep(Duration::from_millis(10));
                self.active.fetch_sub(1, Ordering::SeqCst);
                Ok(ExecutionResponse::completed(request.request_id))
            }
        }

        let node = node_with_units(4);
        let first_workload = WorkItem {
            id: "batch-first".into(),
            payload: vec![1],
        };
        let second_workload = WorkItem {
            id: "batch-second".into(),
            payload: vec![2],
        };
        let first_plan = ExecutionPlan::from_node(&node, &first_workload).unwrap();
        let second_plan = ExecutionPlan::from_node(&node, &second_workload).unwrap();
        let endpoints = first_plan
            .units
            .iter()
            .map(|unit| {
                crate::transport::ExecutionEndpoint::new(
                    unit.participant_id.clone(),
                    format!("loopback://{}", unit.participant_id.id),
                )
                .unwrap()
            })
            .collect();
        let active = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let engine = ExecutionEngine::new(
            TrackingTransport {
                active: Arc::clone(&active),
                peak: Arc::clone(&peak),
            },
            endpoints,
        )
        .with_max_concurrency(2)
        .unwrap();

        let receipts = engine
            .execute_batch(&[
                ExecutionJob::new(first_plan, first_workload),
                ExecutionJob::new(second_plan, second_workload),
            ])
            .unwrap();

        assert_eq!(receipts.len(), 2);
        assert_eq!(receipts[0].workload_id, "batch-first");
        assert_eq!(receipts[1].workload_id, "batch-second");
        assert_eq!(receipts[0].dispatched_units, 4);
        assert_eq!(receipts[1].dispatched_units, 4);
        assert_eq!(peak.load(Ordering::SeqCst), 2);
        assert_eq!(active.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn execution_engine_rejects_duplicate_workload_ids_in_a_batch() {
        let node = node();
        let workload = WorkItem {
            id: "duplicate-id".into(),
            payload: Vec::new(),
        };
        let plan = ExecutionPlan::from_node(&node, &workload).unwrap();
        let endpoints = plan
            .units
            .iter()
            .map(|unit| {
                crate::transport::ExecutionEndpoint::new(
                    unit.participant_id.clone(),
                    "loopback://participant",
                )
                .unwrap()
            })
            .collect();
        let engine = ExecutionEngine::new(crate::transport::LoopbackTransport, endpoints);
        let jobs = [
            ExecutionJob::new(plan.clone(), workload.clone()),
            ExecutionJob::new(plan, workload),
        ];

        assert_eq!(
            engine.execute_batch(&jobs).unwrap_err(),
            ExecutionError::DuplicateWorkloadId
        );
    }

}
