use crate::identity::{Identity, Kind};
use crate::node::RuntimeSpec;
use crate::resource::ResourceFragment;
use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

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

    pub fn is_empty(&self) -> bool {
        self.runtimes.is_empty()
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
    Timeout,
}

impl fmt::Display for RuntimeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for RuntimeError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessCommand {
    pub program: String,
    pub args: Vec<String>,
    pub env: Vec<(String, String)>,
}

impl ProcessCommand {
    pub fn new(program: impl Into<String>) -> Self {
        Self {
            program: program.into(),
            args: Vec::new(),
            env: Vec::new(),
        }
    }

    pub fn arg(mut self, arg: impl Into<String>) -> Self {
        self.args.push(arg.into());
        self
    }

    pub fn args<I, S>(mut self, args: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        for arg in args {
            self.args.push(arg.into());
        }
        self
    }

    pub fn env(mut self, key: impl Into<String>, val: impl Into<String>) -> Self {
        self.env.push((key.into(), val.into()));
        self
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut lines = Vec::new();
        lines.push(self.program.clone());
        lines.push(self.args.len().to_string());
        for arg in &self.args {
            lines.push(arg.clone());
        }
        lines.push(self.env.len().to_string());
        for (k, v) in &self.env {
            lines.push(format!("{}={}", k, v));
        }
        lines.join("\n").into_bytes()
    }

    pub fn decode(payload: &[u8]) -> Result<Self, RuntimeError> {
        let text = std::str::from_utf8(payload).map_err(|_| RuntimeError::Unsupported)?;
        let mut lines = text.lines();
        let program = lines.next().ok_or(RuntimeError::Unsupported)?;
        if program.is_empty() {
            return Err(RuntimeError::Unsupported);
        }

        let args_count_str = lines.next().ok_or(RuntimeError::Unsupported)?;
        let args_count: usize = args_count_str
            .parse()
            .map_err(|_| RuntimeError::Unsupported)?;
        let mut args = Vec::with_capacity(args_count);
        for _ in 0..args_count {
            args.push(lines.next().ok_or(RuntimeError::Unsupported)?.to_string());
        }

        let env_count_str = lines.next().ok_or(RuntimeError::Unsupported)?;
        let env_count: usize = env_count_str
            .parse()
            .map_err(|_| RuntimeError::Unsupported)?;
        let mut env = Vec::with_capacity(env_count);
        for _ in 0..env_count {
            let line = lines.next().ok_or(RuntimeError::Unsupported)?;
            let (k, v) = line.split_once('=').ok_or(RuntimeError::Unsupported)?;
            env.push((k.to_string(), v.to_string()));
        }

        Ok(Self {
            program: program.to_string(),
            args,
            env,
        })
    }
}

#[derive(Debug, Clone)]
pub struct ProcessRuntime {
    name: String,
    allowed_programs: Option<Vec<String>>,
    timeout: Option<Duration>,
}

impl ProcessRuntime {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            allowed_programs: None,
            timeout: None,
        }
    }

    pub fn with_allowlist<I, S>(name: impl Into<String>, allowed: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            name: name.into(),
            allowed_programs: Some(allowed.into_iter().map(Into::into).collect()),
            timeout: None,
        }
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }
}

impl Runtime for ProcessRuntime {
    fn name(&self) -> &str {
        &self.name
    }

    fn run(&self, context: &RuntimeContext, workload: Workload) -> Result<(), RuntimeError> {
        workload.validate()?;
        context
            .resources
            .validate_requirement()
            .map_err(|_| RuntimeError::InvalidResources)?;

        let command = ProcessCommand::decode(&workload.payload)?;
        if let Some(allowed) = &self.allowed_programs
            && !allowed.iter().any(|p| p == &command.program)
        {
            return Err(RuntimeError::Unsupported);
        }

        let mut cmd = std::process::Command::new(&command.program);
        cmd.args(&command.args);
        for (k, v) in &command.env {
            cmd.env(k, v);
        }
        cmd.env("OPENHOST_PARTICIPANT_ID", &context.participant_id.id);
        cmd.env("OPENHOST_NODE_ID", &context.node_id.id);

        let mut child = cmd.spawn().map_err(|_| RuntimeError::ExecutionFailed)?;

        let status = match self.timeout {
            None => child.wait().map_err(|_| RuntimeError::ExecutionFailed)?,
            Some(timeout) => {
                let start = std::time::Instant::now();
                let poll_interval = Duration::from_millis(5);
                loop {
                    match child.try_wait() {
                        Ok(Some(status)) => break status,
                        Ok(None) => {
                            if start.elapsed() >= timeout {
                                let _ = child.kill();
                                let _ = child.wait();
                                return Err(RuntimeError::Timeout);
                            }
                            std::thread::sleep(poll_interval);
                        }
                        Err(_) => {
                            let _ = child.kill();
                            return Err(RuntimeError::ExecutionFailed);
                        }
                    }
                }
            }
        };

        if !status.success() {
            return Err(RuntimeError::ExecutionFailed);
        }
        Ok(())
    }
}

pub type FunctionHandler =
    Arc<dyn Fn(&RuntimeContext, &Workload) -> Result<(), RuntimeError> + Send + Sync>;

#[derive(Clone)]
pub struct FunctionRuntime {
    name: String,
    handler: FunctionHandler,
}

impl FunctionRuntime {
    pub fn new<F>(name: impl Into<String>, handler: F) -> Self
    where
        F: Fn(&RuntimeContext, &Workload) -> Result<(), RuntimeError> + Send + Sync + 'static,
    {
        Self {
            name: name.into(),
            handler: Arc::new(handler),
        }
    }
}

impl Runtime for FunctionRuntime {
    fn name(&self) -> &str {
        &self.name
    }

    fn run(&self, context: &RuntimeContext, workload: Workload) -> Result<(), RuntimeError> {
        workload.validate()?;
        context
            .resources
            .validate_requirement()
            .map_err(|_| RuntimeError::InvalidResources)?;
        (self.handler)(context, &workload)
    }
}

#[derive(Debug, Default, Clone)]
pub struct EchoRuntime {
    name: String,
}

impl EchoRuntime {
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }
}

impl Runtime for EchoRuntime {
    fn name(&self) -> &str {
        if self.name.is_empty() {
            "echo"
        } else {
            &self.name
        }
    }

    fn run(&self, context: &RuntimeContext, workload: Workload) -> Result<(), RuntimeError> {
        workload.validate()?;
        context
            .resources
            .validate_requirement()
            .map_err(|_| RuntimeError::InvalidResources)?;
        Ok(())
    }
}

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
        assert!(matches!(result, Err(RuntimeRegistryError::RuntimeNotFound)));
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

    #[test]
    fn process_command_encode_decode_roundtrip() {
        let cmd = ProcessCommand::new("true")
            .arg("--version")
            .env("VAR", "123");
        let encoded = cmd.encode();
        let decoded = ProcessCommand::decode(&encoded).expect("decode succeeds");
        assert_eq!(cmd, decoded);
    }

    #[test]
    fn process_runtime_executes_successful_command() {
        let runtime = ProcessRuntime::with_allowlist("process", vec!["true"]);
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
            payload: ProcessCommand::new("true").encode(),
        };

        assert!(runtime.run(&context, workload).is_ok());
    }

    #[test]
    fn process_runtime_rejects_unauthorized_command() {
        let runtime = ProcessRuntime::with_allowlist("process", vec!["true"]);
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
            payload: ProcessCommand::new("rm").encode(),
        };

        assert_eq!(
            runtime.run(&context, workload).unwrap_err(),
            RuntimeError::Unsupported
        );
    }

    #[test]
    fn echo_runtime_executes_valid_workload() {
        let runtime = EchoRuntime::new("echo");
        let context = RuntimeContext::new(
            Identity::new(Kind::Participant),
            Identity::new(Kind::LogicalNode),
            ResourceFragment {
                cpu: Cpu { cores: 0.5 },
                ..Default::default()
            },
        )
        .unwrap();

        let workload = Workload {
            id: "echo-1".into(),
            payload: b"hello world".to_vec(),
        };

        assert!(runtime.run(&context, workload).is_ok());
    }

    #[test]
    fn process_runtime_enforces_timeout() {
        let runtime = ProcessRuntime::with_allowlist("process", vec!["sleep"])
            .with_timeout(Duration::from_millis(50));
        let context = RuntimeContext::new(
            Identity::new(Kind::Participant),
            Identity::new(Kind::LogicalNode),
            ResourceFragment {
                cpu: Cpu { cores: 0.5 },
                ..Default::default()
            },
        )
        .unwrap();

        let workload = Workload {
            id: "work-sleep".into(),
            payload: ProcessCommand::new("sleep").arg("5").encode(),
        };

        let result = runtime.run(&context, workload);
        assert_eq!(result.unwrap_err(), RuntimeError::Timeout);
    }

    #[test]
    fn function_runtime_executes_closure() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = Arc::clone(&counter);

        let runtime = FunctionRuntime::new("fn-counter", move |_ctx, workload| {
            if workload.payload == b"increment" {
                counter_clone.fetch_add(1, Ordering::SeqCst);
                Ok(())
            } else {
                Err(RuntimeError::ExecutionFailed)
            }
        });

        assert_eq!(runtime.name(), "fn-counter");

        let context = RuntimeContext::new(
            Identity::new(Kind::Participant),
            Identity::new(Kind::LogicalNode),
            ResourceFragment {
                cpu: Cpu { cores: 0.5 },
                ..Default::default()
            },
        )
        .unwrap();

        let workload = Workload {
            id: "work-fn".into(),
            payload: b"increment".to_vec(),
        };

        assert!(runtime.run(&context, workload).is_ok());
        assert_eq!(counter.load(Ordering::SeqCst), 1);

        let bad_workload = Workload {
            id: "work-bad".into(),
            payload: b"invalid".to_vec(),
        };
        assert_eq!(
            runtime.run(&context, bad_workload).unwrap_err(),
            RuntimeError::ExecutionFailed
        );
    }
}
