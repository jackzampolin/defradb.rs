use crate::{EmbeddedNode, QueryExecutor, QueryRequest, QueryResponse, TransactionHandle};
use query::{TransactionError, TransactionGuard};

/// An owning transaction that uses the node's signing and identity context.
///
/// Dropping it abandons uncommitted writes, including during task cancellation
/// or runtime shutdown. Cloned handles are non-owning. Cancellation after a
/// durable commit cannot undo it; see [`TransactionGuard`].
#[must_use = "dropping the transaction abandons its uncommitted writes"]
pub struct EmbeddedTransaction<'a> {
    node: &'a EmbeddedNode,
    guard: TransactionGuard<'a, dyn QueryExecutor>,
}

impl EmbeddedNode {
    /// Begin a transaction that is abandoned automatically unless finalized.
    ///
    /// ```no_run
    /// # async fn example(node: &defra_node::EmbeddedNode) -> Result<(), query::TransactionError> {
    /// let txn = node.begin_transaction_guard(false).await?;
    /// let response = txn.execute("mutation { add_Users(input: {name: \"Alice\"}) { _docID } }").await;
    /// if response.has_errors() {
    ///     txn.rollback().await
    /// } else {
    ///     txn.commit().await
    /// }
    /// # }
    /// ```
    pub async fn begin_transaction_guard(
        &self,
        readonly: bool,
    ) -> Result<EmbeddedTransaction<'_>, TransactionError> {
        let guard = TransactionGuard::begin(self.runner().as_ref(), readonly).await?;
        Ok(EmbeddedTransaction { node: self, guard })
    }
}

impl EmbeddedTransaction<'_> {
    /// Borrow the non-owning ID for APIs that accept a transaction handle.
    pub fn handle(&self) -> &TransactionHandle {
        self.guard.handle().expect("active transaction")
    }

    /// Execute GraphQL with the node's signing and default identity context.
    pub async fn execute(&self, query: &str) -> QueryResponse {
        self.execute_request(QueryRequest::new(query)).await
    }

    /// Execute a prepared request, preserving any explicitly supplied identity.
    pub async fn execute_request(&self, request: QueryRequest) -> QueryResponse {
        self.node
            .execute_request_in_txn(request, self.handle())
            .await
    }

    /// Commit the writes and consume the owning transaction.
    pub async fn commit(self) -> Result<(), TransactionError> {
        self.guard.commit().await
    }

    /// Roll back the writes and consume the owning transaction.
    pub async fn rollback(self) -> Result<(), TransactionError> {
        self.guard.rollback().await
    }
}
