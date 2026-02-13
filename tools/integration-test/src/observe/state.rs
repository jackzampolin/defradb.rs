/// Runtime state of a managed node.
pub enum NodeState {
    Starting,
    Ready,
    Error(String),
    Stopped,
}
