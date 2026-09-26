use crate::resource::CompositeResource;

#[derive(Debug, Clone, PartialEq)]
pub struct LogicalNode {
    pub id: String,
    pub resources: CompositeResource,
    pub runtime: RuntimeSpec,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RuntimeSpec {
    pub name: String,
    pub version: String,
}
