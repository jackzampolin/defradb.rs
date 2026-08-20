//! Transaction begin, commit, rollback and counter finalization.

use super::*;

impl<S: Store + 'static> DbTransactionRegistry<S> {
    /// Apply the txn's recorded counter ops (#1044) then durably commit.
    ///
    /// LOCK LIFECYCLE — conforms to `proofs/tla/InteractiveTxnCounter.tla` GREEN
    /// (`InteractiveGate="AtCommitOnly"`, `GateMode="On"`): the process-wide batch
    /// gate is taken ONLY here (the bounded `IBeginFinalize`/`IFinalizeCommit`
    /// action), never while the interactive txn is active/idle. We briefly hold
    /// the gate while acquiring the touched-counter-doc per-doc guards in SORTED
    /// order (`IAcquire`), then DROP the gate (`IFinalizeCommit` releases the gate
    /// after acquiring the guards) and do the RMW under the held per-doc guards.
    /// The per-doc guards live on the `DbTxn` and release only AFTER the durable
    /// commit (`IExitCrit`), preserving the #1021 invariant that a concurrent
    /// merge never observes a partial RMW.
    ///
    /// GUARD SCOPE — the per-doc guard prevents a concurrent merge from
    /// interleaving its RMW with this finalize's RMW DURING the held window, but
    /// it does NOT serialize against a merge that COMMITTED before this finalize
    /// took the guard. In that case the interactive txn's `begin()` snapshot is
    /// stale and the storage SSI/OCC conflict tracker aborts the commit with a
    /// `TxnConflict` (the client retries the whole txn). No data loss, no
    /// double-apply — this matches Go's storage-txn isolation. So the guard's role
    /// is partial-RMW exclusion, not merge serialization; cross-merge convergence
    /// rests on OCC (`proofs/tla/Ssi.tla`), which is why
    /// `InteractiveTxnCounter.tla` models only the guard lifecycle, not store
    /// values / OCC.
    async fn finalize_and_commit(
        &self,
        handle: &TransactionHandle,
        mut txn: crate::txn::DbTxn<S>,
    ) -> std::result::Result<(), TransactionError> {
        let ops = txn.take_counter_ops();
        if ops.is_empty() {
            return txn.force_commit().await.map_err(|e| {
                TransactionError::execution(format!(
                    "commit error for transaction '{}': {}",
                    handle, e
                ))
            });
        }

        // Distinct touched doc ids, sorted for a deterministic (defensive)
        // acquire order under the gate.
        let mut sorted_doc_ids: Vec<String> = ops.iter().map(|op| op.doc_id.clone()).collect();
        sorted_doc_ids.sort();
        sorted_doc_ids.dedup();

        // IBeginFinalize / IAcquire: take the gate, acquire per-doc guards in
        // sorted order, then IFinalizeCommit drops the gate (the bounded hold).
        {
            let _gate = self.db.doc_write_queue().acquire_batch_gate().await;
            for id in &sorted_doc_ids {
                let guard = self.db.doc_write_queue().acquire(id).await;
                txn.insert_doc_guard(id.clone(), guard);
            }
        } // gate dropped here; per-doc guards remain on the txn until durable commit

        // RMW under the held per-doc guards, into the txn's datastore. The store
        // handles (NamespaceView clones) write through to this txn; they MUST be
        // dropped before `force_commit` or the backend reports "transaction still
        // has references", so they live only inside this block.
        let finalize_result = {
            let datastore = txn.datastore();
            let systemstore = txn.systemstore();
            match (datastore, systemstore) {
                (Ok(ds), Ok(ss)) => Self::apply_counter_ops_at_finalize(&ds, &ss, &ops).await,
                (Err(e), _) | (_, Err(e)) => {
                    Err(query::error::QueryError::execution(e.to_string()))
                }
            }
        };
        if let Err(e) = finalize_result {
            let _ = txn.force_discard();
            return Err(TransactionError::execution(format!(
                "counter finalize error for transaction '{}': {}",
                handle, e
            )));
        }

        // IExitCrit: force_commit commits durably, then release_doc_guards drops
        // the per-doc guards AFTER the durable commit.
        txn.force_commit().await.map_err(|e| {
            TransactionError::execution(format!("commit error for transaction '{}': {}", handle, e))
        })
    }

    /// Perform the recorded counter RMWs into the txn datastore and correct the
    /// materialized blob for update ops (the blob-mirror-at-commit). Called with
    /// the per-doc guards already held by the caller (the finalize driver).
    async fn apply_counter_ops_at_finalize(
        datastore: &datastore::NamespaceView,
        systemstore: &datastore::NamespaceView,
        ops: &[crate::txn::PendingCounterOp],
    ) -> query::error::Result<()> {
        use crate::collection::loader::load_collection_from_systemstore;
        use crate::write::autocommit::helpers::apply_pending_counter_op;
        use std::collections::HashMap;

        // Load each touched collection once (keyed by name) and build its index manager.
        let mut collections: HashMap<String, (Collection, crate::index_manager::IndexManager)> =
            HashMap::new();
        for op in ops {
            if collections.contains_key(&op.collection_name) {
                continue;
            }
            let collection = load_collection_from_systemstore(systemstore, &op.collection_name)
                .await?
                .ok_or_else(|| {
                    query::error::QueryError::collection_not_found(&op.collection_name)
                })?;
            let short_id = collection.resolved_root_id();
            let index_manager = crate::index_manager::IndexManager::from_indexes(
                short_id,
                collection.schema(),
                collection.write_indexes(),
            )
            .map_err(|e| {
                query::error::QueryError::execution(format!(
                    "failed to create index manager for collection '{}': {}",
                    op.collection_name, e
                ))
            })?;
            collections.insert(op.collection_name.clone(), (collection, index_manager));
        }

        // Apply each op's RMW (or create-seed) to the authoritative store,
        // collecting the post-RMW values to mirror into the blob (updates only).
        // DocIDs are content-derived, NOT collection-scoped, so two collections can
        // share a doc_id — key the corrections by (collection_name, doc_id) so each
        // (collection, doc) is corrected against its own collection.
        // (collection_name, doc_id) -> (field -> post-RMW value)
        let mut corrections: HashMap<(String, String), Vec<(String, document::NormalValue)>> =
            HashMap::new();
        for op in ops {
            let (collection, _) = collections
                .get(&op.collection_name)
                .expect("collection loaded above");
            let post = apply_pending_counter_op(
                datastore,
                collection,
                &op.schema_version_id,
                &op.doc_id,
                &op.field,
                op.base.as_ref(),
                &op.delta,
                op.is_create,
            )
            .await?;
            if let Some(value) = post {
                corrections
                    .entry((op.collection_name.clone(), op.doc_id.clone()))
                    .or_default()
                    .push((op.field.clone(), value));
            }
        }

        // Blob correction: for update ops, re-read the doc and set each counter
        // field to its authoritative post-RMW store value, then re-write the blob
        // so the materialized document mirrors the accumulation store.
        for ((collection_name, doc_id), fields) in corrections {
            let (collection, index_manager) = collections
                .get(&collection_name)
                .expect("collection loaded above");
            let doc_id_typed = document::DocID::from_string(&doc_id)
                .map_err(|e| query::error::QueryError::execution(e.to_string()))?;
            let Some(doc_short_id) = collection
                .resolve_doc_short_id(systemstore, &doc_id_typed)
                .await
                .map_err(|e| query::error::QueryError::execution(e.to_string()))?
            else {
                continue;
            };
            let Some(mut doc) = collection
                .get_with_datastore(datastore, doc_short_id, &doc_id_typed)
                .await
                .map_err(|e| query::error::QueryError::execution(e.to_string()))?
            else {
                continue;
            };
            doc.set_id(doc_id_typed);
            for (field, value) in fields {
                doc.set(field, value);
            }
            collection
                .update_with_indexes(datastore, &doc, doc_short_id, index_manager)
                .await
                .map_err(|e| match e {
                    crate::error::Error::DocumentNotFound(id) => {
                        query::error::QueryError::document_not_found(id)
                    }
                    other => crate::error::index_write_query_error("update", other),
                })?;
        }

        Ok(())
    }

    async fn begin_with_options(
        &self,
        readonly: bool,
        defer_readonly_write_back: bool,
    ) -> std::result::Result<TransactionHandle, TransactionError> {
        let txn_id = self.id_counter.fetch_add(1, Ordering::SeqCst).to_string();

        let mut db_txn = self
            .db
            .new_txn(readonly)
            .await
            .map_err(|e| TransactionError::execution(format!("storage error: {}", e)))?;
        // Non-Send on wasm32, where the callbacks it holds drop their `Send`
        // bounds for the single-threaded browser runtime.
        #[cfg_attr(target_arch = "wasm32", allow(clippy::arc_with_non_send_sync))]
        let deferred_acp_mutations = Arc::new(DeferredAcpMutations::new());
        let deferred_acp_mutations_for_commit = deferred_acp_mutations.clone();
        db_txn
            .on_success_async(Box::new(move || {
                Box::pin(async move {
                    deferred_acp_mutations_for_commit.run_all_logged().await;
                })
            }))
            .map_err(|e| {
                TransactionError::execution(format!(
                    "failed to register deferred ACP commit hook: {}",
                    e
                ))
            })?;

        // Transaction-scoped collection caching: collections are loaded lazily
        // from the SystemStore on first access within the transaction. Once loaded,
        // the collection metadata is cached for the transaction's duration.
        let lens_store = Arc::new(TxnLensStore::new(self.db.lens_store().clone()).map_err(
            |e| {
                TransactionError::execution(format!(
                    "failed to create transaction lens store: {}",
                    e
                ))
            },
        )?);
        let fetcher = Arc::new(LensedDocFetcher::new(
            self.db.clone(),
            db_txn,
            lens_store,
            defer_readonly_write_back,
        ));
        let ctx = Arc::new(DbTransactionContext::new_with_broadcaster(
            self.db.clone(),
            txn_id.clone(),
            readonly,
            fetcher,
            deferred_acp_mutations,
            self.broadcaster.clone(),
        ));

        self.transactions
            .write()
            .map_err(|_| TransactionError::lock_poisoned("failed to acquire write lock for begin"))?
            .insert(txn_id.clone(), ctx);

        Ok(TransactionHandle::new(txn_id))
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<S: Store + 'static> TransactionRegistry for DbTransactionRegistry<S> {
    async fn begin(
        &self,
        readonly: bool,
    ) -> std::result::Result<TransactionHandle, TransactionError> {
        self.begin_with_options(readonly, false).await
    }

    async fn begin_implicit_read(
        &self,
    ) -> std::result::Result<TransactionHandle, TransactionError> {
        self.begin_with_options(true, true).await
    }

    fn get(&self, handle: &TransactionHandle) -> GetTransactionResult {
        match self.transactions.read() {
            Ok(guard) => match guard.get(handle.as_str()).cloned() {
                Some(ctx) => {
                    ctx.touch();
                    GetTransactionResult::Found(ctx as Arc<dyn TransactionContext>)
                }
                None => GetTransactionResult::NotFound,
            },
            Err(poisoned) => {
                error!(
                    txn_id = %handle,
                    error = ?poisoned,
                    "Transaction registry lock poisoned - system may be in corrupted state"
                );
                GetTransactionResult::LockPoisoned
            }
        }
    }

    async fn commit(
        &self,
        handle: &TransactionHandle,
    ) -> std::result::Result<(), TransactionError> {
        let ctx = self
            .transactions
            .write()
            .map_err(|_| {
                TransactionError::lock_poisoned(format!(
                    "failed to acquire write lock during commit of '{}'",
                    handle
                ))
            })?
            .remove(handle.as_str())
            .ok_or_else(|| {
                TransactionError::not_found(format!("transaction '{}' not found", handle))
            })?;
        let action_lock = ctx.action_lock();
        let _action_guard = action_lock.lock().await;

        let txn = ctx.take_txn().await.ok_or_else(|| {
            TransactionError::already_finalized(format!(
                "transaction '{}' was already consumed (double commit/rollback?)",
                handle
            ))
        })?;

        self.finalize_and_commit(handle, txn).await
    }

    async fn rollback(
        &self,
        handle: &TransactionHandle,
    ) -> std::result::Result<(), TransactionError> {
        let ctx = self
            .transactions
            .write()
            .map_err(|_| {
                TransactionError::lock_poisoned(format!(
                    "failed to acquire write lock during rollback of '{}'",
                    handle
                ))
            })?
            .remove(handle.as_str())
            .ok_or_else(|| {
                TransactionError::not_found(format!("transaction '{}' not found", handle))
            })?;
        let action_lock = ctx.action_lock();
        let _action_guard = action_lock.lock().await;

        let txn = ctx.take_txn().await.ok_or_else(|| {
            TransactionError::already_finalized(format!(
                "transaction '{}' was already consumed (double commit/rollback?)",
                handle
            ))
        })?;

        txn.force_discard().map_err(|e| {
            TransactionError::execution(format!(
                "rollback error for transaction '{}': {}",
                handle, e
            ))
        })
    }

    async fn finish_implicit_read(
        &self,
        handle: &TransactionHandle,
        apply_read_effects: bool,
    ) -> std::result::Result<(), TransactionError> {
        let ctx = self
            .transactions
            .write()
            .map_err(|_| {
                TransactionError::lock_poisoned(format!(
                    "failed to acquire write lock while finalizing implicit read '{}'",
                    handle
                ))
            })?
            .remove(handle.as_str())
            .ok_or_else(|| {
                TransactionError::not_found(format!("transaction '{}' not found", handle))
            })?;
        let action_lock = ctx.action_lock();
        let _action_guard = action_lock.lock().await;

        let txn = ctx.take_txn().await.ok_or_else(|| {
            TransactionError::already_finalized(format!(
                "transaction '{}' was already consumed",
                handle
            ))
        })?;
        txn.force_discard().map_err(|e| {
            TransactionError::execution(format!(
                "failed to close implicit read transaction '{}': {}",
                handle, e
            ))
        })?;

        if apply_read_effects {
            ctx.persist_pending_migrations().await.map_err(|error| {
                TransactionError::execution(format!(
                    "deferred lens write-back failed for transaction '{}': {}",
                    handle, error
                ))
            })?;
        }

        Ok(())
    }
}
