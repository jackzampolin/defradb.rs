use std::sync::Arc;

use async_trait::async_trait;

use defra_http::router::DumpOperations;
use storage::corekv::Store;

/// Adapter that implements DumpOperations using database.
pub struct DumpAdapter<S: Store> {
    database: Arc<db::DB<S>>,
}

impl<S: Store + 'static> DumpAdapter<S> {
    /// Create an Arc-wrapped adapter.
    pub fn new_arc(database: Arc<db::DB<S>>) -> Arc<dyn DumpOperations> {
        Arc::new(Self { database })
    }
}

#[async_trait]
impl<S: Store + 'static> DumpOperations for DumpAdapter<S> {
    async fn print_dump(&self) -> Result<Vec<String>, String> {
        self.database.print_dump().await
    }
}
