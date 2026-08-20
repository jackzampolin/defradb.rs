//! The stale-transaction cleanup sweep. Subject of proofs/tla/TxnRegistry.tla.

use super::*;

impl<S: Store + 'static> DbTransactionRegistry<S> {
    /// Cleanup transactions idle for longer than the given duration.
    ///
    /// This method finds all transactions whose last observed request was more
    /// than `max_idle_age` ago and rolls them back, freeing resources. This
    /// should be called periodically by a background task to prevent resource
    /// leaks from dropped `TransactionGuard`s or orphaned HTTP transaction
    /// handles.
    ///
    /// Returns a `CleanupResult` containing both successfully cleaned transactions
    /// and any failures. Check `result.is_complete()` to verify all cleanups succeeded.
    ///
    /// # Errors
    ///
    /// Returns an error if the lock is poisoned, indicating a panic elsewhere.
    pub async fn cleanup_stale_transactions(
        &self,
        max_idle_age: Duration,
    ) -> Result<CleanupResult> {
        let now = Instant::now();

        // Collect stale transaction candidates while holding the read lock briefly.
        // Each candidate is re-checked after acquiring the transaction action lock
        // so a request that arrives during cleanup can refresh the idle clock.
        let stale_candidates: Vec<(String, Arc<DbTransactionContext<S>>)> = {
            let guard = self.transactions.read().map_err(|_| {
                Error::LockPoisoned("failed to acquire read lock during cleanup".to_string())
            })?;

            guard
                .iter()
                .filter(|(_, ctx)| ctx.idle_for(now) > max_idle_age)
                .map(|(id, ctx)| (id.clone(), ctx.clone()))
                .collect()
        };

        let mut result = CleanupResult::default();
        for (txn_id, candidate_ctx) in stale_candidates {
            let action_lock = candidate_ctx.action_lock();
            let _action_guard = action_lock.lock().await;

            let now = Instant::now();
            if candidate_ctx.idle_for(now) <= max_idle_age {
                continue;
            }

            // Remove and rollback each stale transaction. Re-check while holding
            // the write lock so a concurrent request that touched the context
            // after candidate collection cannot lose its transaction.
            let ctx = {
                let mut guard = self.transactions.write().map_err(|_| {
                    Error::LockPoisoned("failed to acquire write lock during cleanup".to_string())
                })?;

                // Holding the registry write lock blocks new get()/get_ctx()
                // touches while we do the final idle re-check and remove.
                // The idle timestamp itself is protected by the context mutex,
                // so a request cannot refresh it between this check and removal.
                match guard.get(&txn_id) {
                    Some(current)
                        if Arc::ptr_eq(current, &candidate_ctx)
                            && current.idle_for(Instant::now()) > max_idle_age =>
                    {
                        guard.remove(&txn_id)
                    }
                    _ => None,
                }
            };

            if let Some(ctx) = ctx {
                let idle_secs = ctx.idle_for(Instant::now()).as_secs();
                warn!(
                    txn_id = %txn_id,
                    idle_secs,
                    "Cleaning up idle transaction (orphaned HTTP handle or leaked TransactionGuard?)"
                );

                // Try to take and discard the transaction
                if let Some(txn) = ctx.take_txn().await {
                    if let Err(e) = txn.force_discard() {
                        error!(
                            txn_id = %txn_id,
                            error = %e,
                            "Failed to discard stale transaction during cleanup"
                        );
                        result.failed.push((txn_id.clone(), e.to_string()));
                    } else {
                        result.cleaned += 1;
                    }
                } else {
                    // Transaction was already consumed (committed/rolled back)
                    result.cleaned += 1;
                }
            }
        }

        Ok(result)
    }

    /// Start a periodic task that cleans up transactions idle for longer than
    /// `max_idle_age`.
    ///
    /// # Panics
    ///
    /// Panics if `sweep_interval` is zero.
    #[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
    pub fn start_stale_transaction_cleanup(
        self: &Arc<Self>,
        max_idle_age: Duration,
        sweep_interval: Duration,
    ) -> tokio::task::JoinHandle<()> {
        assert!(
            !sweep_interval.is_zero(),
            "transaction cleanup sweep interval must be non-zero"
        );

        let registry = Arc::clone(self);

        tokio::spawn(async move {
            loop {
                tokio::time::sleep(sweep_interval).await;

                match registry.cleanup_stale_transactions(max_idle_age).await {
                    Ok(result) if result.attempted() > 0 && result.is_complete() => {
                        tracing::info!(
                            cleaned = result.cleaned,
                            max_idle_age_secs = max_idle_age.as_secs(),
                            "Cleaned up idle transactions"
                        );
                    }
                    Ok(result) if result.attempted() > 0 => {
                        tracing::warn!(
                            cleaned = result.cleaned,
                            failed = result.failed.len(),
                            max_idle_age_secs = max_idle_age.as_secs(),
                            "Idle transaction cleanup completed with failures"
                        );
                    }
                    Ok(_) => {}
                    Err(error) => {
                        tracing::warn!(
                            error = %error,
                            max_idle_age_secs = max_idle_age.as_secs(),
                            "Idle transaction cleanup failed"
                        );
                    }
                }
            }
        })
    }
}
