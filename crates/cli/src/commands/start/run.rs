//! Node run loop and shutdown coordination

use tokio::task::JoinHandle;
use tracing::{error, info, warn};

use super::node::Node;
use crate::error::{Error, Result};

impl Node {
    /// Run the node until shutdown
    #[doc(hidden)]
    pub async fn run(mut self) -> Result<()> {
        info!("DefraDB node started");
        info!("API endpoint: http://{}", self.config.api.address);

        // Start HTTP server
        let http_task: Option<JoinHandle<()>> = if let Some(server) = self.http_server.take() {
            info!("Starting HTTP server on {}", self.config.api.address);
            Some(tokio::spawn(async move {
                if let Err(e) = server.run().await {
                    error!("HTTP server error: {}", e);
                }
            }))
        } else {
            None
        };

        // Start PG wire protocol server
        let pg_task: Option<JoinHandle<()>> = if let Some(server) = self.pg_server.take() {
            info!(
                "Starting Postgres wire protocol server on {}",
                server.address()
            );
            Some(tokio::spawn(async move {
                if let Err(e) = server.run().await {
                    error!("PG server error: {}", e);
                }
            }))
        } else {
            None
        };

        // Set up signal handling
        let shutdown_tx = self.shutdown_tx.clone();

        #[cfg(unix)]
        {
            use tokio::signal::unix::{signal, SignalKind};

            let mut sigint =
                signal(SignalKind::interrupt()).map_err(|e| Error::Signal(e.to_string()))?;
            let mut sigterm =
                signal(SignalKind::terminate()).map_err(|e| Error::Signal(e.to_string()))?;

            tokio::spawn(async move {
                tokio::select! {
                    _ = sigint.recv() => {
                        info!("Received SIGINT");
                    }
                    _ = sigterm.recv() => {
                        info!("Received SIGTERM");
                    }
                }
                if let Err(e) = shutdown_tx.send(()).await {
                    error!("Failed to send shutdown signal: {}", e);
                }
            });
        }

        #[cfg(not(unix))]
        {
            tokio::spawn(async move {
                match tokio::signal::ctrl_c().await {
                    Ok(()) => {
                        info!("Received Ctrl+C");
                        if let Err(e) = shutdown_tx.send(()).await {
                            error!("Failed to send shutdown signal: {}", e);
                        }
                    }
                    Err(e) => {
                        error!(
                            "Failed to listen for Ctrl+C signal: {}. Node may not respond to interrupt signals.",
                            e
                        );
                    }
                }
            });
        }

        // Wait for shutdown signal OR HTTP server crash
        let mut http_task = http_task;
        let http_crashed = match &mut http_task {
            Some(task) => {
                tokio::select! {
                    _ = self.shutdown_rx.recv() => false,
                    result = task => {
                        match result {
                            Ok(()) => {
                                error!("HTTP server exited unexpectedly");
                            }
                            Err(e) if e.is_panic() => {
                                error!("HTTP server panicked: {}", e);
                            }
                            Err(e) => {
                                error!("HTTP server task failed: {}", e);
                            }
                        }
                        true
                    }
                }
            }
            None => {
                self.shutdown_rx.recv().await;
                false
            }
        };

        if http_crashed {
            info!("Initiating shutdown due to HTTP server failure...");
        } else {
            info!("Shutting down DefraDB node...");
            // Only abort if we're shutting down normally (not due to crash)
            if let Some(task) = http_task {
                info!("Stopping HTTP server...");
                task.abort();
                match tokio::time::timeout(std::time::Duration::from_secs(1), task).await {
                    Ok(_) => info!("HTTP server stopped"),
                    Err(_) => warn!(
                        timeout_secs = 1,
                        "HTTP server shutdown timed out - server was forcefully terminated. \
                         This may occur if requests were still in flight."
                    ),
                }
            }
            if let Some(task) = pg_task {
                info!("Stopping PG server...");
                task.abort();
                let _ = tokio::time::timeout(std::time::Duration::from_secs(1), task).await;
                info!("PG server stopped");
            }
        }

        if let Some(task) = self.downsample_task.take() {
            info!("Stopping downsample worker...");
            task.abort();
            let _ = tokio::time::timeout(std::time::Duration::from_secs(1), task).await;
        }

        // Shutdown P2P tasks first, then the handle
        if let Some(tasks) = self.p2p_tasks.take() {
            info!("Stopping P2P background tasks...");

            tasks.coordinator.shutdown().await;

            // Abort all tasks - they will stop when the channel closes
            tasks.replication_task.abort();
            tasks.host_task.abort();
            tasks.failure_recorder_task.abort();
            tasks.retry_loop_task.abort();
            if let Some(event_task) = tasks.event_handler_task {
                event_task.abort();
            }

            // Wait briefly for tasks to complete with timeout
            let timeout = std::time::Duration::from_secs(2);
            let _ = tokio::time::timeout(timeout, tasks.replication_task).await;
            let _ = tokio::time::timeout(timeout, tasks.host_task).await;

            info!("P2P background tasks stopped");
        }

        if let Some(handle) = &self.p2p_handle {
            if let Err(e) = handle.shutdown().await {
                warn!("P2P shutdown encountered an issue: {}", e);
            }
        }

        defra_core::signing::clear_identity_store();
        info!("DefraDB node shutdown complete");
        Ok(())
    }
}
