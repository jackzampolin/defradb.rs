use std::sync::Arc;

use events::EventName;
use storage::corekv::Store;
use tokio::task::JoinHandle;
use tokio::time::{self, MissedTickBehavior};

impl<S: Store + 'static> crate::database::DB<S> {
    #[cfg(not(target_arch = "wasm32"))]
    pub fn start_downsample_task(self: Arc<Self>) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut ticker = time::interval(std::time::Duration::from_millis(250));
            ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

            if let Err(error) = self.bootstrap_downsamples(None).await {
                tracing::warn!(error = %error, "Failed to bootstrap downsample collections");
            }

            let Some(bus) = self.event_bus().cloned() else {
                loop {
                    ticker.tick().await;
                    if let Err(error) = self.bootstrap_downsamples(None).await {
                        tracing::warn!(error = %error, "Failed to refresh downsample collections");
                    }
                }
            };

            let mut subscription = bus.subscribe(&[EventName::Update]);

            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        if let Err(error) = self.bootstrap_downsamples(None).await {
                            tracing::warn!(error = %error, "Failed to refresh downsample collections");
                        }
                    }
                    message = subscription.recv() => {
                        let Some(message) = message else {
                            continue;
                        };

                        if subscription.check_and_reset_dropped() > 0 {
                            tracing::warn!(
                                "Downsample event subscription dropped messages; rebuilding all downsample collections"
                            );
                            if let Err(error) = self.bootstrap_downsamples(None).await {
                                tracing::warn!(
                                    error = %error,
                                    "Failed to rebuild downsample collections after dropped events"
                                );
                            }
                        }

                        let Some(update) = message.as_update() else {
                            continue;
                        };

                        if let Err(error) = self
                            .process_downsample_update(&update.collection_id, &update.doc_id)
                            .await
                        {
                            tracing::warn!(
                                collection_id = %update.collection_id,
                                doc_id = %update.doc_id,
                                error = %error,
                                "Failed to process downsample update"
                            );
                        }
                    }
                }
            }
        })
    }
}
