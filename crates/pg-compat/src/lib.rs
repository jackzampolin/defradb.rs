mod bridge;
mod encode;
pub mod error;
mod handler;
mod metadata;
mod types;

use std::net::SocketAddr;
use std::sync::Arc;

use query::{CollectionProvider, QueryExecutor};
use tokio::net::TcpListener;
use tracing::{error, info};

use handler::{DefraHandlerFactory, DefraQueryHandler};

pub use handler::SchemaManager;

/// Postgres wire protocol compatibility server for DefraDB.
///
/// Accepts psql connections and translates SQL queries to GraphQL,
/// executing them through the existing query engine.
pub struct PgServer {
    address: SocketAddr,
    factory: Arc<DefraHandlerFactory>,
}

impl PgServer {
    pub fn new(
        address: SocketAddr,
        executor: Arc<dyn QueryExecutor>,
        collections: Arc<dyn CollectionProvider>,
        schema_manager: Option<Arc<dyn SchemaManager>>,
    ) -> Self {
        let handler = Arc::new(DefraQueryHandler::new(
            executor,
            collections,
            schema_manager,
        ));
        let factory = Arc::new(DefraHandlerFactory::new(handler));
        Self { address, factory }
    }

    pub fn address(&self) -> SocketAddr {
        self.address
    }

    /// Run the PG wire protocol server, accepting connections in a loop.
    pub async fn run(self) -> Result<(), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind(self.address).await?;
        info!(
            "Postgres wire protocol server listening on {}",
            self.address
        );

        loop {
            let (socket, peer) = listener.accept().await?;
            info!("PG connection from {}", peer);

            let factory = self.factory.clone();
            tokio::spawn(async move {
                if let Err(e) = pgwire::tokio::process_socket(socket, None, factory).await {
                    error!("PG connection error from {}: {}", peer, e);
                }
            });
        }
    }
}
