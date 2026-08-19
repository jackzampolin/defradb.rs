//! Transaction management for query execution.
//!
//! Plan-layer primitives (`TransactionContext`, `TransactionHandle`,
//! `TransactionRegistry`, `check_doc_access_with_overlay`, and the deferred-ACP
//! overlay) live in the sibling modules here. `TransactionGuard` lives in
//! `guard`; it is generic over the `QueryExecutor` trait defined in the runner
//! layer, which is why it is a separate module rather than a plan-layer type.

mod context;
mod guard;
mod handle;
mod read_access;
mod registry;
mod result;

#[cfg(test)]
mod tests;

// Re-export all public types
pub use context::{
    check_doc_access_with_overlay, current_deferred_acp_mutations, is_doc_registered_with_overlay,
    scope_deferred_acp_mutations, DeferredAcpMutations, TransactionContext,
};
pub use guard::TransactionGuard;
pub use handle::TransactionHandle;
pub use read_access::OverlayChecker;
pub use registry::{NoOpTransactionRegistry, TransactionRegistry};
pub use result::GetTransactionResult;
