use crate::execution::{
    AdmissionController, ExecutionEngine, ExecutionError, ExecutionJob, ExecutionPlan,
    ExecutionReceipt, PartitionedExecutionReceipt,
};
use crate::runtime::WorkloadPartition;
use crate::transport::{ExecutionEndpoint, Transport};
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use uuid::Uuid;

/// Globally unique identifier for one submitted task.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize)]
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum TaskState {
    Pending,
    Dispatched,
    Running,
    Completed,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
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
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum TaskSubmission {
    Workload(ExecutionJob),
    Partitioned(PartitionedTask),
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum TaskResult {
    Workload(ExecutionReceipt),
    Partitioned(PartitionedExecutionReceipt),
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
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
    #[serde(with = "system_time_serde")]
    pub created_at: SystemTime,
    #[serde(with = "system_time_serde")]
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
    PersistenceUnavailable,
    RegistryAlreadyOpen,
    CorruptRegistry,
    UnsupportedRegistryVersion,
}

impl fmt::Display for TaskError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for TaskError {}

#[derive(Clone, Default)]
struct TaskStore {
    tasks: BTreeMap<TaskId, TaskRecord>,
    idempotency: HashMap<String, TaskId>,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct PersistedRegistrySnapshot {
    schema_version: u32,
    tasks: Vec<TaskRecord>,
}

impl PersistedRegistrySnapshot {
    fn from_store(store: &TaskStore) -> Self {
        Self {
            schema_version: 1,
            tasks: store.tasks.values().cloned().collect(),
        }
    }

    fn into_store(self) -> Result<TaskStore, TaskError> {
        if self.schema_version != 1 {
            return Err(TaskError::UnsupportedRegistryVersion);
        }

        let mut store = TaskStore::default();
        for record in self.tasks {
            if store.tasks.contains_key(&record.id)
                || store.idempotency.contains_key(&record.idempotency_key)
            {
                return Err(TaskError::CorruptRegistry);
            }
            store
                .idempotency
                .insert(record.idempotency_key.clone(), record.id.clone());
            store.tasks.insert(record.id.clone(), record);
        }
        Ok(store)
    }
}

struct RegistryPersistence {
    file: File,
    // Keeping this handle alive holds the exclusive process-level registry lock.
    _lock_file: File,
}

impl RegistryPersistence {
    fn lock_path(path: &Path) -> PathBuf {
        let mut value = path.as_os_str().to_os_string();
        value.push(".lock");
        PathBuf::from(value)
    }

    fn open(path: &Path) -> Result<(TaskStore, Self), TaskError> {
        if let Some(parent) = path.parent().filter(|parent| !parent.as_os_str().is_empty()) {
            std::fs::create_dir_all(parent)
                .map_err(|_| TaskError::PersistenceUnavailable)?;
        }

        let lock_path = Self::lock_path(path);
        let lock_file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(lock_path)
            .map_err(|_| TaskError::PersistenceUnavailable)?;
        lock_file
            .try_lock()
            .map_err(|_| TaskError::RegistryAlreadyOpen)?;

        let mut file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path)
            .map_err(|_| TaskError::PersistenceUnavailable)?;

        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)
            .map_err(|_| TaskError::PersistenceUnavailable)?;

        // Every committed event ends with a newline. A final partial line is an interrupted
        // append and is discarded; malformed complete lines are treated as corruption.
        let valid_len = bytes
            .iter()
            .rposition(|byte| *byte == b'\\n')
            .map_or(0, |index| index + 1);
        if valid_len != bytes.len() {
            file.set_len(valid_len as u64)
                .map_err(|_| TaskError::PersistenceUnavailable)?;
            file.sync_data()
                .map_err(|_| TaskError::PersistenceUnavailable)?;
            bytes.truncate(valid_len);
        }

        let mut store = TaskStore::default();
        for line in bytes.split(|byte| *byte == b'\\n').filter(|line| !line.is_empty()) {
            let snapshot: PersistedRegistrySnapshot =
                serde_json::from_slice(line).map_err(|_| TaskError::CorruptRegistry)?;
            store = snapshot.into_store()?;
        }

        file.seek(SeekFrom::End(0))
            .map_err(|_| TaskError::PersistenceUnavailable)?;

        Ok((
            store,
            Self {
                file,
                _lock_file: lock_file,
            },
        ))
    }

    fn append(&mut self, store: &TaskStore) -> Result<(), TaskError> {
        let snapshot = PersistedRegistrySnapshot::from_store(store);
        let bytes =
            serde_json::to_vec(&snapshot).map_err(|_| TaskError::PersistenceUnavailable)?;
        let previous_len = self
            .file
            .metadata()
            .map_err(|_| TaskError::PersistenceUnavailable)?
            .len();

        let append_result = (|| -> std::io::Result<()> {
            self.file.seek(SeekFrom::End(0))?;
            self.file.write_all(&bytes)?;
            self.file.write_all(b"\\n")?;
            self.file.sync_data()
        })();

        if append_result.is_err() {
            // Do not leave a partial event that could be mistaken for a committed state later.
            let _ = self.file.set_len(previous_len);
            let _ = self.file.seek(SeekFrom::End(0));
            let _ = self.file.sync_data();
            return Err(TaskError::PersistenceUnavailable);
        }

        Ok(())
    }
}

/// Thread-safe task metadata and idempotency registry.
///
/// The default constructor is memory-only. Use `open` for an append-only, crash-recoverable
/// registry. A durable registry takes an exclusive process lock for its lifetime; multiple
/// TaskManagers in one process should share a clone of the same TaskRegistry handle.
#[derive(Clone, Default)]
pub struct TaskRegistry {
    store: Arc<Mutex<TaskStore>>,
    persistence: Option<Arc<Mutex<RegistryPersistence>>>,
}

impl TaskRegistry {
    /// Create a process-local registry that is not crash durable.
    pub fn new() -> Self {
        Self::default()
    }

    /// Open or create a durable registry journal.
    ///
    /// Every state transition is appended as a versioned full-state snapshot and fsynced before
    /// becoming visible to callers. An unfinished trailing journal record is discarded on
    /// recovery. The lock file prevents two independent registry owners from writing the same
    /// journal concurrently.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, TaskError> {
        let (store, persistence) = RegistryPersistence::open(path.as_ref())?;
        let registry = Self {
            store: Arc::new(Mutex::new(store)),
            persistence: Some(Arc::new(Mutex::new(persistence))),
        };
        registry.recover_interrupted()?;
        Ok(registry)
    }

    fn transact<R>(
        &self,
        operation: impl FnOnce(&mut TaskStore) -> Result<(R, bool), TaskError>,
    ) -> Result<R, TaskError> {
        let mut current = self
            .store
            .lock()
            .map_err(|_| TaskError::RegistryUnavailable)?;
        let mut next = current.clone();
        let (result, changed) = operation(&mut next)?;

        if changed {
            if let Some(persistence) = &self.persistence {
                persistence
                    .lock()
                    .map_err(|_| TaskError::RegistryUnavailable)?
                    .append(&next)?;
            }
            *current = next;
        }

        Ok(result)
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

        self.transact(move |store| {
            if let Some(existing_id) = store.idempotency.get(&idempotency_key) {
                let existing = store
                    .tasks
                    .get(existing_id)
                    .ok_or(TaskError::RegistryUnavailable)?;
                if existing.submission != submission || existing.max_attempts != max_attempts {
                    return Err(TaskError::IdempotencyConflict);
                }
                return Ok((existing.clone(), false));
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
            Ok((record, true))
        })
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
        self.transact(|store| {
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
            Ok((record.clone(), true))
        })
    }

    fn complete(
        &self,
        id: &TaskId,
        result: TaskResult,
    ) -> Result<TaskRecord, TaskError> {
        self.transact(|store| {
            let record = store.tasks.get_mut(id).ok_or(TaskError::TaskNotFound)?;
            if record.state != TaskState::Running {
                return Err(TaskError::InvalidTransition);
            }

            record.state = TaskState::Completed;
            record.result = Some(result);
            record.last_error = None;
            record.updated_at = SystemTime::now();
            Ok((record.clone(), true))
        })
    }

    fn fail(
        &self,
        id: &TaskId,
        error: String,
    ) -> Result<TaskRecord, TaskError> {
        self.transact(|store| {
            let record = store.tasks.get_mut(id).ok_or(TaskError::TaskNotFound)?;
            if record.state != TaskState::Running {
                return Err(TaskError::InvalidTransition);
            }

            record.state = TaskState::Failed;
            record.result = None;
            record.last_error = Some(error);
            record.updated_at = SystemTime::now();
            Ok((record.clone(), true))
        })
    }

    fn retry_failed(&self, id: &TaskId) -> Result<TaskRecord, TaskError> {
        self.transact(|store| {
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
            Ok((record.clone(), true))
        })
    }

    /// Convert tasks left Dispatched or Running by a previous coordinator process into Failed.
    ///
    /// The remote outcome is unknown, so recovery never replays these tasks automatically.
    /// The caller may inspect the failure and explicitly retry under the existing at-least-once
    /// contract.
    pub fn recover_interrupted(&self) -> Result<usize, TaskError> {
        self.transact(|store| {
            let mut recovered = 0;
            for record in store.tasks.values_mut() {
                if matches!(record.state, TaskState::Dispatched | TaskState::Running) {
                    let previous = record.state;
                    record.state = TaskState::Failed;
                    record.last_error = Some(format!(
                        "Coordinator restarted while task was {previous:?}; remote outcome is unknown"
                    ));
                    record.result = None;
                    record.updated_at = SystemTime::now();
                    recovered += 1;
                }
            }
            Ok((recovered, recovered > 0))
        })
    }
}

mod system_time_serde {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    pub fn serialize<S>(value: &SystemTime, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let elapsed = value.duration_since(UNIX_EPOCH).unwrap_or_default();
        (elapsed.as_secs(), elapsed.subsec_nanos()).serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<SystemTime, D::Error>
    where
        D: Deserializer<'de>,
    {
        let (seconds, nanos) = <(u64, u32)>::deserialize(deserializer)?;
        if nanos >= 1_000_000_000 {
            return Err(serde::de::Error::custom("invalid system time nanoseconds"));
        }
        Ok(UNIX_EPOCH + Duration::new(seconds, nanos))
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
        Self::with_registry(transport, endpoints, TaskRegistry::new())
    }

    /// Create a manager that shares an existing registry with other managers.
    pub fn with_registry(
        transport: T,
        endpoints: Vec<ExecutionEndpoint>,
        registry: TaskRegistry,
    ) -> Self {
        Self {
            engine: ExecutionEngine::new(transport, endpoints),
            registry,
        }
    }

    /// Create a manager backed by a durable task registry journal.
    pub fn open(
        transport: T,
        endpoints: Vec<ExecutionEndpoint>,
        registry_path: impl AsRef<Path>,
    ) -> Result<Self, TaskError> {
        let registry = TaskRegistry::open(registry_path)?;
        Ok(Self::with_registry(transport, endpoints, registry))
    }


    /// Configure this manager to share admission capacity with other engines or managers.
    pub fn with_admission_controller(mut self, controller: AdmissionController) -> Self {
        self.engine = self.engine.with_admission_controller(controller);
        self
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
            .submit_workload("client-request-42", first_job.clone(), 3)
            .unwrap();

        assert_eq!(first.id, duplicate.id);
        assert_eq!(duplicate.state, TaskState::Pending);
        assert_eq!(
            manager
                .submit_workload("client-request-42", first_job, 2)
                .unwrap_err(),
            TaskError::IdempotencyConflict
        );
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

    #[test]
    fn task_manager_executes_partitioned_task_through_tcp_and_records_result() {
        use crate::endpoint::{
            ExecutionHandler, ParticipantExecutionEndpoint, RuntimeAdapter,
        };
        use crate::node::RuntimeSpec;
        use crate::resource::{Allocation, CompositeResource, Cpu, ResourceFragment};
        use crate::runtime::{Runtime, RuntimeContext, RuntimeError, WorkloadPartition};
        use crate::transport::{TcpEndpointServer, TcpTransport};
        use crate::node::LogicalNode;
        use std::sync::Arc;

        struct TcpTaskRuntime;

        impl Runtime for TcpTaskRuntime {
            fn name(&self) -> &str {
                "tcp-task-runtime"
            }

            fn run(
                &self,
                _context: &RuntimeContext,
                _workload: WorkItem,
            ) -> Result<(), RuntimeError> {
                Ok(())
            }

            fn run_partition(
                &self,
                _context: &RuntimeContext,
                partition: WorkloadPartition,
            ) -> Result<Vec<u8>, RuntimeError> {
                Ok(partition.payload.into_iter().map(|byte| byte * 2).collect())
            }
        }

        let participant_id = Identity::new(Kind::Participant);
        let handler: Arc<dyn ExecutionHandler> = Arc::new(
            ParticipantExecutionEndpoint::new(
                participant_id.clone(),
                RuntimeAdapter::new(TcpTaskRuntime),
            )
            .unwrap(),
        );
        let server = TcpEndpointServer::bind("127.0.0.1:0", handler).unwrap();
        let address = server.local_addr().unwrap();
        let server_thread = std::thread::spawn(move || server.serve_n(1));

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
            runtime: RuntimeSpec::new("tcp-task-runtime", "1"),
        };
        let workload = WorkItem {
            id: "task-manager-tcp-workload".into(),
            payload: Vec::new(),
        };
        let plan = ExecutionPlan::from_node(&node, &workload).unwrap();
        let endpoint = ExecutionEndpoint::new(participant_id, address.to_string()).unwrap();
        let manager = TaskManager::new(TcpTransport::new(), vec![endpoint])
            .with_max_concurrency(1)
            .unwrap();
        let submission = PartitionedTask::new(
            plan,
            "task-manager-tcp-workload",
            vec![WorkloadPartition::new(
                "task-manager-tcp-workload",
                "only-partition",
                0,
                1,
                vec![4, 5, 6],
            )],
        );

        let submitted = manager
            .submit_partitioned("tcp-task-idempotency-key", submission, 2)
            .unwrap();
        let completed = manager.run_task(&submitted.id).unwrap();

        assert_eq!(completed.state, TaskState::Completed);
        assert_eq!(completed.attempt, 1);
        let TaskResult::Partitioned(receipt) = completed.result.unwrap() else {
            panic!("task should contain a partitioned execution result");
        };
        assert_eq!(receipt.results.len(), 1);
        assert_eq!(receipt.results[0].output, vec![8, 10, 12]);

        server_thread.join().unwrap().unwrap();
    }

    fn temp_registry_path(label: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "openhost-{label}-{}.journal",
            Uuid::new_v4()
        ))
    }

    fn remove_registry_files(path: &Path) {
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(RegistryPersistence::lock_path(path));
    }

    #[test]
    fn durable_registry_restores_completed_task_and_idempotency_after_restart() {
        let path = temp_registry_path("durable-complete");
        let (task, endpoints) = job("durable-workload");

        let registry = TaskRegistry::open(&path).unwrap();
        let manager =
            TaskManager::with_registry(LoopbackTransport, endpoints, registry.clone());
        let submitted = manager
            .submit_workload("durable-idempotency-key", task.clone(), 3)
            .unwrap();
        let completed = manager.run_task(&submitted.id).unwrap();

        assert_eq!(completed.state, TaskState::Completed);
        assert!(completed.result.is_some());
        drop(manager);
        drop(registry);

        let reopened = TaskRegistry::open(&path).unwrap();
        let restored = reopened.get(&submitted.id).unwrap();
        assert_eq!(restored.state, TaskState::Completed);
        assert_eq!(restored.result, completed.result);
        assert_eq!(restored.attempt, 1);

        let duplicate = reopened
            .submit(
                "durable-idempotency-key",
                TaskSubmission::Workload(task),
                3,
            )
            .unwrap();
        assert_eq!(duplicate.id, submitted.id);
        assert_eq!(reopened.list().unwrap().len(), 1);

        drop(reopened);
        remove_registry_files(&path);
    }

    #[test]
    fn durable_registry_recovers_in_flight_tasks_as_failed_without_replaying() {
        let path = temp_registry_path("durable-recovery");
        let (task, _) = job("interrupted-workload");

        let registry = TaskRegistry::open(&path).unwrap();
        let submitted = registry
            .submit("interrupted-key", TaskSubmission::Workload(task), 2)
            .unwrap();
        registry
            .transition(&submitted.id, TaskState::Dispatched)
            .unwrap();
        registry
            .transition(&submitted.id, TaskState::Running)
            .unwrap();
        drop(registry);

        let reopened = TaskRegistry::open(&path).unwrap();
        let recovered = reopened.get(&submitted.id).unwrap();
        assert_eq!(recovered.state, TaskState::Failed);
        assert_eq!(recovered.attempt, 1);
        assert!(
            recovered
                .last_error
                .as_deref()
                .is_some_and(|error| error.contains("remote outcome is unknown"))
        );

        let retry = reopened.retry_failed(&submitted.id).unwrap();
        assert_eq!(retry.state, TaskState::Pending);
        assert_eq!(retry.attempt, 2);

        drop(reopened);
        remove_registry_files(&path);
    }

    #[test]
    fn durable_registry_rejects_a_second_independent_writer() {
        let path = temp_registry_path("durable-lock");
        let registry = TaskRegistry::open(&path).unwrap();

        assert!(matches!(
            TaskRegistry::open(&path),
            Err(TaskError::RegistryAlreadyOpen)
        ));

        drop(registry);
        let reopened = TaskRegistry::open(&path).unwrap();
        drop(reopened);
        remove_registry_files(&path);
    }

    #[test]
    fn durable_registry_discards_only_an_incomplete_trailing_record() {
        use std::io::Write;

        let path = temp_registry_path("durable-tail");
        let (task, _) = job("tail-workload");
        let registry = TaskRegistry::open(&path).unwrap();
        let submitted = registry
            .submit("tail-key", TaskSubmission::Workload(task), 1)
            .unwrap();
        drop(registry);

        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap();
        file.write_all(b"{\\"schema_version\\":")
            .unwrap();
        file.sync_all().unwrap();
        drop(file);

        let reopened = TaskRegistry::open(&path).unwrap();
        assert_eq!(
            reopened.get(&submitted.id).unwrap().state,
            TaskState::Pending
        );
        drop(reopened);
        remove_registry_files(&path);
    }

    #[test]
    fn task_registry_clones_share_the_same_durable_store() {
        let path = temp_registry_path("durable-clone");
        let first = TaskRegistry::open(&path).unwrap();
        let second = first.clone();
        let (task, _) = job("clone-shared-workload");

        let submitted = first
            .submit("clone-shared-key", TaskSubmission::Workload(task), 1)
            .unwrap();
        assert_eq!(second.get(&submitted.id).unwrap(), submitted);

        drop(first);
        drop(second);
        let reopened = TaskRegistry::open(&path).unwrap();
        assert_eq!(reopened.list().unwrap().len(), 1);
        drop(reopened);
        remove_registry_files(&path);
    }

}
