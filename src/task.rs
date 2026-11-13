use crate::execution::{
    ExecutionEngine, ExecutionError, ExecutionJob, ExecutionPlan, ExecutionReceipt,
    PartitionedExecutionReceipt,
};
use crate::runtime::WorkloadPartition;
use crate::transport::{ExecutionEndpoint, Transport};
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::Mutex;
use std::time::SystemTime;
use uuid::Uuid;

/// Globally unique identifier for one submitted task.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TaskId(String);

impl TaskId {
    pub fn new() -> Self {
        Self(Uuid::new_v4().to_string())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Default for TaskId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for TaskId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskState {
    Pending,
    Dispatched,
    Running,
    Completed,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PartitionedTask {
    pub plan: ExecutionPlan,
    pub workload_id: String,
    pub partitions: Vec<WorkloadPartition>,
}

impl PartitionedTask {
    pub fn new(
        plan: ExecutionPlan,
        workload_id: impl Into<String>,
        partitions: Vec<WorkloadPartition>,
    ) -> Self {
        Self {
            plan,
            workload_id: workload_id.into(),
            partitions,
        }
    }
}

/// The immutable input accepted by the task registry.
///
/// An idempotency key can only be reused when the complete submission is identical.
#[derive(Debug, Clone, PartialEq)]
pub enum TaskSubmission {
    Workload(ExecutionJob),
    Partitioned(PartitionedTask),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskResult {
    Workload(ExecutionReceipt),
    Partitioned(PartitionedExecutionReceipt),
}

#[derive(Debug, Clone, PartialEq)]
pub struct TaskRecord {
    pub id: TaskId,
    pub idempotency_key: String,
    pub submission: TaskSubmission,
    pub state: TaskState,
    /// The first execution is attempt 1. Each explicit retry increments this value.
    pub attempt: u32,
    pub max_attempts: u32,
    pub result: Option<TaskResult>,
    pub last_error: Option<String>,
    pub created_at: SystemTime,
    pub updated_at: SystemTime,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskError {
    EmptyIdempotencyKey,
    InvalidAttemptLimit,
    TaskNotFound,
    IdempotencyConflict,
    InvalidTransition,
    RetryLimitReached,
    RegistryUnavailable,
    EngineConfiguration(ExecutionError),
}

impl fmt::Display for TaskError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for TaskError {}

#[derive(Default)]
struct TaskStore {
    tasks: BTreeMap<TaskId, TaskRecord>,
    idempotency: HashMap<String, TaskId>,
}

/// Thread-safe, in-memory task metadata and idempotency registry.
///
/// This registry is process-local and is not crash durable. A future persistent backend can
/// implement the same state machine without changing the execution engine contract.
#[derive(Default)]
pub struct TaskRegistry {
    store: Mutex<TaskStore>,
}

impl TaskRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn submit(
        &self,
        idempotency_key: impl Into<String>,
        submission: TaskSubmission,
        max_attempts: u32,
    ) -> Result<TaskRecord, TaskError> {
        let idempotency_key = idempotency_key.into();
        if idempotency_key.trim().is_empty() {
            return Err(TaskError::EmptyIdempotencyKey);
        }
        if max_attempts == 0 {
            return Err(TaskError::InvalidAttemptLimit);
        }

        let mut store = self
            .store
            .lock()
            .map_err(|_| TaskError::RegistryUnavailable)?;

        if let Some(existing_id) = store.idempotency.get(&idempotency_key) {
            let existing = store
                .tasks
                .get(existing_id)
                .ok_or(TaskError::RegistryUnavailable)?;
            if existing.submission != submission {
                return Err(TaskError::IdempotencyConflict);
            }
            return Ok(existing.clone());
        }

        let id = loop {
            let candidate = TaskId::new();
            if !store.tasks.contains_key(&candidate) {
                break candidate;
            }
        };
        let now = SystemTime::now();
        let record = TaskRecord {
            id: id.clone(),
            idempotency_key: idempotency_key.clone(),
            submission,
            state: TaskState::Pending,
            attempt: 1,
            max_attempts,
            result: None,
            last_error: None,
            created_at: now,
            updated_at: now,
        };

        store.idempotency.insert(idempotency_key, id.clone());
        store.tasks.insert(id, record.clone());
        Ok(record)
    }

    pub fn get(&self, id: &TaskId) -> Result<TaskRecord, TaskError> {
        let store = self
            .store
            .lock()
            .map_err(|_| TaskError::RegistryUnavailable)?;
        store
            .tasks
            .get(id)
            .cloned()
            .ok_or(TaskError::TaskNotFound)
    }

    pub fn list(&self) -> Result<Vec<TaskRecord>, TaskError> {
        let store = self
            .store
            .lock()
            .map_err(|_| TaskError::RegistryUnavailable)?;
        Ok(store.tasks.values().cloned().collect())
    }

    fn transition(
        &self,
        id: &TaskId,
        next: TaskState,
    ) -> Result<TaskRecord, TaskError> {
        let mut store = self
            .store
            .lock()
            .map_err(|_| TaskError::RegistryUnavailable)?;
        let record = store.tasks.get_mut(id).ok_or(TaskError::TaskNotFound)?;

        let allowed = matches!(
            (record.state, next),
            (TaskState::Pending, TaskState::Dispatched)
                | (TaskState::Pending, TaskState::Cancelled)
                | (TaskState::Dispatched, TaskState::Running)
        );
        if !allowed {
            return Err(TaskError::InvalidTransition);
        }

        record.state = next;
        record.updated_at = SystemTime::now();
        Ok(record.clone())
    }

    fn complete(
        &self,
        id: &TaskId,
        result: TaskResult,
    ) -> Result<TaskRecord, TaskError> {
        let mut store = self
            .store
            .lock()
            .map_err(|_| TaskError::RegistryUnavailable)?;
        let record = store.tasks.get_mut(id).ok_or(TaskError::TaskNotFound)?;
        if record.state != TaskState::Running {
            return Err(TaskError::InvalidTransition);
        }

        record.state = TaskState::Completed;
        record.result = Some(result);
        record.last_error = None;
        record.updated_at = SystemTime::now();
        Ok(record.clone())
    }

    fn fail(
        &self,
        id: &TaskId,
        error: String,
    ) -> Result<TaskRecord, TaskError> {
        let mut store = self
            .store
            .lock()
            .map_err(|_| TaskError::RegistryUnavailable)?;
        let record = store.tasks.get_mut(id).ok_or(TaskError::TaskNotFound)?;
        if record.state != TaskState::Running {
            return Err(TaskError::InvalidTransition);
        }

        record.state = TaskState::Failed;
        record.result = None;
        record.last_error = Some(error);
        record.updated_at = SystemTime::now();
        Ok(record.clone())
    }

    fn retry_failed(&self, id: &TaskId) -> Result<TaskRecord, TaskError> {
        let mut store = self
            .store
            .lock()
            .map_err(|_| TaskError::RegistryUnavailable)?;
        let record = store.tasks.get_mut(id).ok_or(TaskError::TaskNotFound)?;
        if record.state != TaskState::Failed {
            return Err(TaskError::InvalidTransition);
        }
        if record.attempt >= record.max_attempts {
            return Err(TaskError::RetryLimitReached);
        }

        record.attempt += 1;
        record.state = TaskState::Pending;
        record.result = None;
        record.last_error = None;
        record.updated_at = SystemTime::now();
        Ok(record.clone())
    }
}

/// Owns one execution engine and coordinates task submission, state transitions and retries.
///
/// Retries are explicit and may execute work again if a remote participant completed the work
/// but its response was lost. Side-effecting runtimes must therefore be idempotent themselves;
/// this manager does not claim exactly-once remote execution.
pub struct TaskManager<T> {
    engine: ExecutionEngine<T>,
    registry: TaskRegistry,
}

impl<T: Transport + 'static> TaskManager<T> {
    pub fn new(
        transport: T,
        endpoints: Vec<ExecutionEndpoint>,
    ) -> Self {
        Self {
            engine: ExecutionEngine::new(transport, endpoints),
            registry: TaskRegistry::new(),
        }
    }

    pub fn with_max_concurrency(
        mut self,
        max_concurrency: usize,
    ) -> Result<Self, TaskError> {
        self.engine = self
            .engine
            .with_max_concurrency(max_concurrency)
            .map_err(TaskError::EngineConfiguration)?;
        Ok(self)
    }

    pub fn submit(
        &self,
        idempotency_key: impl Into<String>,
        submission: TaskSubmission,
        max_attempts: u32,
    ) -> Result<TaskRecord, TaskError> {
        self.registry.submit(idempotency_key, submission, max_attempts)
    }

    pub fn submit_workload(
        &self,
        idempotency_key: impl Into<String>,
        job: ExecutionJob,
        max_attempts: u32,
    ) -> Result<TaskRecord, TaskError> {
        self.submit(
            idempotency_key,
            TaskSubmission::Workload(job),
            max_attempts,
        )
    }

    pub fn submit_partitioned(
        &self,
        idempotency_key: impl Into<String>,
        task: PartitionedTask,
        max_attempts: u32,
    ) -> Result<TaskRecord, TaskError> {
        self.submit(
            idempotency_key,
            TaskSubmission::Partitioned(task),
            max_attempts,
        )
    }

    pub fn get(&self, id: &TaskId) -> Result<TaskRecord, TaskError> {
        self.registry.get(id)
    }

    pub fn list(&self) -> Result<Vec<TaskRecord>, TaskError> {
        self.registry.list()
    }

    /// Execute a pending task and return its final record.
    ///
    /// A second concurrent caller cannot execute the same pending record: only the caller
    /// that successfully claims the Pending → Dispatched transition proceeds.
    pub fn run_task(&self, id: &TaskId) -> Result<TaskRecord, TaskError> {
        let claimed = self.registry.transition(id, TaskState::Dispatched)?;
        let running = self.registry.transition(&claimed.id, TaskState::Running)?;

        let result = match &running.submission {
            TaskSubmission::Workload(job) => self
                .engine
                .execute_batch(std::slice::from_ref(job))
                .and_then(|mut receipts| {
                    receipts
                        .pop()
                        .ok_or(ExecutionError::BackendRejected)
                })
                .map(TaskResult::Workload),
            TaskSubmission::Partitioned(task) => self
                .engine
                .execute_partitioned(
                    &task.plan,
                    &task.workload_id,
                    &task.partitions,
                )
                .map(TaskResult::Partitioned),
        };

        match result {
            Ok(result) => self.registry.complete(id, result),
            Err(error) => self.registry.fail(id, error.to_string()),
        }
    }

    /// Retry a failed task by returning it to Pending and incrementing its attempt counter.
    /// The caller must invoke run_task separately to execute that attempt.
    pub fn retry_failed(&self, id: &TaskId) -> Result<TaskRecord, TaskError> {
        self.registry.retry_failed(id)
    }

    /// Cancel only tasks that are still Pending. Running work is not forcibly interrupted.
    pub fn cancel_pending(&self, id: &TaskId) -> Result<TaskRecord, TaskError> {
        self.registry.transition(id, TaskState::Cancelled)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::{ExecutionPlan, WorkItem};
    use crate::identity::{Identity, Kind};
    use crate::node::{LogicalNode, RuntimeSpec};
    use crate::resource::{Allocation, CompositeResource, Cpu, ResourceFragment};
    use crate::transport::{ExecutionRequest, ExecutionResponse, LoopbackTransport, TransportError};
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn job(workload_id: &str) -> (ExecutionJob, Vec<ExecutionEndpoint>) {
        let participant_id = Identity::new(Kind::Participant);
        let node = LogicalNode {
            id: Identity::new(Kind::LogicalNode).id,
            resources: CompositeResource::compose(vec![Allocation {
                participant_id: participant_id.id.clone(),
                resources: ResourceFragment {
                    cpu: Cpu { cores: 1.0 },
                    ..Default::default()
                },
            }])
            .unwrap(),
            runtime: RuntimeSpec::new("task-test", "1"),
        };
        let workload = WorkItem {
            id: workload_id.into(),
            payload: vec![1, 2, 3],
        };
        let plan = ExecutionPlan::from_node(&node, &workload).unwrap();
        let endpoint = ExecutionEndpoint::new(participant_id, "loopback://task-test").unwrap();
        (ExecutionJob::new(plan, workload), vec![endpoint])
    }

    #[test]
    fn idempotent_submission_returns_existing_task_and_rejects_conflicts() {
        let (first_job, endpoints) = job("task-one");
        let (different_job, _) = job("task-two");
        let manager = TaskManager::new(LoopbackTransport, endpoints);

        let first = manager
            .submit_workload("client-request-42", first_job.clone(), 3)
            .unwrap();
        let duplicate = manager
            .submit_workload("client-request-42", first_job, 3)
            .unwrap();

        assert_eq!(first.id, duplicate.id);
        assert_eq!(duplicate.state, TaskState::Pending);
        assert_eq!(
            manager
                .submit_workload("client-request-42", different_job, 3)
                .unwrap_err(),
            TaskError::IdempotencyConflict
        );
    }

    #[test]
    fn completed_task_records_result_and_rejects_second_execution() {
        let (job, endpoints) = job("task-complete");
        let manager = TaskManager::new(LoopbackTransport, endpoints);
        let submitted = manager
            .submit_workload("complete-key", job, 1)
            .unwrap();

        let completed = manager.run_task(&submitted.id).unwrap();
        assert_eq!(completed.state, TaskState::Completed);
        assert_eq!(completed.attempt, 1);
        assert!(matches!(completed.result, Some(TaskResult::Workload(_))));
        assert_eq!(
            manager.run_task(&submitted.id).unwrap_err(),
            TaskError::InvalidTransition
        );
    }

    struct FailOnceTransport {
        calls: AtomicUsize,
    }

    impl Transport for FailOnceTransport {
        fn dispatch(
            &self,
            request: ExecutionRequest,
        ) -> Result<ExecutionResponse, TransportError> {
            if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                return Err(TransportError::NetworkFailure);
            }
            Ok(ExecutionResponse::completed(request.request_id))
        }
    }

    #[test]
    fn failed_task_can_be_retried_with_an_incremented_attempt() {
        let (job, endpoints) = job("task-retry");
        let manager = TaskManager::new(
            FailOnceTransport {
                calls: AtomicUsize::new(0),
            },
            endpoints,
        );
        let submitted = manager
            .submit_workload("retry-key", job, 2)
            .unwrap();

        let failed = manager.run_task(&submitted.id).unwrap();
        assert_eq!(failed.state, TaskState::Failed);
        assert_eq!(failed.attempt, 1);
        assert!(failed.last_error.is_some());

        let pending = manager.retry_failed(&submitted.id).unwrap();
        assert_eq!(pending.state, TaskState::Pending);
        assert_eq!(pending.attempt, 2);
        assert!(pending.last_error.is_none());

        let completed = manager.run_task(&submitted.id).unwrap();
        assert_eq!(completed.state, TaskState::Completed);
        assert_eq!(completed.attempt, 2);
        assert!(completed.result.is_some());
    }

    #[test]
    fn retry_limit_and_pending_cancellation_are_enforced() {
        let (retry_job, endpoints) = job("task-retry-limit");
        let manager = TaskManager::new(
            FailOnceTransport {
                calls: AtomicUsize::new(0),
            },
            endpoints,
        );
        let retry_task = manager
            .submit_workload("retry-limit-key", retry_job, 1)
            .unwrap();
        let failed = manager.run_task(&retry_task.id).unwrap();
        assert_eq!(failed.state, TaskState::Failed);
        assert_eq!(
            manager.retry_failed(&retry_task.id).unwrap_err(),
            TaskError::RetryLimitReached
        );

        let (cancel_job, _) = job("task-cancel");
        let cancelled = manager
            .submit_workload("cancel-key", cancel_job, 1)
            .unwrap();
        let cancelled = manager.cancel_pending(&cancelled.id).unwrap();
        assert_eq!(cancelled.state, TaskState::Cancelled);
        assert_eq!(
            manager.run_task(&cancelled.id).unwrap_err(),
            TaskError::InvalidTransition
        );
    }
}
