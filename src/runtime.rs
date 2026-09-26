use crate::node::LogicalNode;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Workload {
    pub id: String,
    pub payload: Vec<u8>,
}

pub trait Runtime: Send + Sync {
    fn name(&self) -> &str;
    fn run(&self, node: &LogicalNode, workload: Workload) -> Result<(), RuntimeError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeError { Unsupported, ExecutionFailed }

impl std::fmt::Display for RuntimeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "{self:?}") }
}
impl std::error::Error for RuntimeError {}
