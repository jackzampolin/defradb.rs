//! Collection and document subscription management.

use blockstore::Blockstore;
use cid::Cid;

use super::SyncCoordinator;
use crate::error::Result;
use crate::transport::P2PTransport;

impl<B: Blockstore + 'static, T: P2PTransport> SyncCoordinator<B, T> {
    /// Subscribe to a collection for sync.
    ///
    /// Uses a write lock for the entire operation to prevent concurrent
    /// subscribe calls from racing past the contains-check.
    pub async fn subscribe_collection(&self, collection_id: &str) -> Result<bool> {
        let mut subscribed_collections = self.subscriptions.subscribed_collections.write().await;

        if subscribed_collections.contains(collection_id) {
            return Ok(false);
        }

        self.subscriptions
            .collection_store
            .add_collection(collection_id)
            .await?;

        let result = self
            .runtime
            .broadcaster
            .subscribe_collection(collection_id)
            .await;

        match result {
            Ok(subscribed) => {
                subscribed_collections.insert(collection_id.to_string());

                if subscribed {
                    tracing::debug!(collection_id = %collection_id, "Subscribed to collection (persisted)");
                }
                Ok(subscribed)
            }
            Err(e) => {
                if let Err(remove_err) = self
                    .subscriptions
                    .collection_store
                    .remove_collection(collection_id)
                    .await
                {
                    tracing::error!(
                        collection_id = %collection_id,
                        subscribe_error = %e,
                        remove_error = %remove_err,
                        "Failed to rollback storage after GossipSub subscription failure"
                    );
                }
                Err(e)
            }
        }
    }

    /// Subscribe to a specific document for sync.
    pub async fn subscribe_document(&self, doc_id: &str) -> Result<bool> {
        self.runtime.broadcaster.subscribe_document(doc_id).await
    }

    /// Unsubscribe from a collection.
    pub async fn unsubscribe_collection(&self, collection_id: &str) -> Result<bool> {
        let mut subscribed_collections = self.subscriptions.subscribed_collections.write().await;

        // Persist the desired unsubscribe intent first. If the live transport
        // leave fails, the in-process topic may linger until retry or restart,
        // but durable restore must not re-install it.
        self.subscriptions
            .collection_store
            .remove_collection(collection_id)
            .await?;

        let result = self
            .runtime
            .broadcaster
            .unsubscribe_collection(collection_id)
            .await;
        subscribed_collections.remove(collection_id);

        let result = result?;

        if result {
            tracing::debug!(collection_id = %collection_id, "Unsubscribed from collection (persisted)");
        }

        Ok(result)
    }

    /// Unsubscribe from a document.
    pub async fn unsubscribe_document(&self, doc_id: &str) -> Result<bool> {
        self.runtime.broadcaster.unsubscribe_document(doc_id).await
    }

    /// Get the list of subscribed collection IDs.
    pub async fn get_subscribed_collections(&self) -> Result<Vec<String>> {
        let collections = self.subscriptions.subscribed_collections.read().await;
        Ok(collections.iter().cloned().collect())
    }

    /// Load and subscribe to all persisted P2P collections.
    ///
    /// This can run before pubsub_rpc services start: collection topic
    /// subscription is a transport-level operation independent of the base
    /// doc-sync / sync-branchable service topics.
    pub async fn load_p2p_collections(&self) -> Result<usize> {
        let collections = self
            .subscriptions
            .collection_store
            .get_all_collections()
            .await?;
        let count = collections.len();

        if count == 0 {
            tracing::debug!("No persisted P2P collections to load");
            return Ok(0);
        }

        tracing::info!(count = count, "Loading persisted P2P collections");

        let mut loaded = 0;
        for collection_id in collections {
            match self
                .runtime
                .broadcaster
                .subscribe_collection(&collection_id)
                .await
            {
                Ok(true) => {
                    self.subscriptions
                        .subscribed_collections
                        .write()
                        .await
                        .insert(collection_id.clone());
                    loaded += 1;
                    tracing::debug!(collection_id = %collection_id, "Loaded P2P collection subscription");
                }
                Ok(false) => {
                    self.subscriptions
                        .subscribed_collections
                        .write()
                        .await
                        .insert(collection_id.clone());
                    loaded += 1;
                    tracing::debug!(collection_id = %collection_id, "P2P collection already subscribed");
                }
                Err(e) => {
                    tracing::warn!(
                        collection_id = %collection_id,
                        error = %e,
                        "Failed to subscribe to persisted P2P collection"
                    );
                }
            }
        }

        tracing::info!(loaded = loaded, "Finished loading P2P collections");
        Ok(loaded)
    }

    /// Mark a block as merged.
    pub async fn mark_as_merged(&self, cid: &Cid) -> Result<()> {
        self.manager.mark_as_merged(cid).await
    }

    /// Mark multiple blocks as merged in a single transaction.
    pub async fn mark_batch_as_merged(&self, cids: &[Cid]) -> Result<()> {
        self.manager.mark_batch_as_merged(cids).await
    }

    /// Check if a block is merged.
    pub async fn is_merged(&self, cid: &Cid) -> Result<bool> {
        self.manager.is_merged(cid).await
    }

    /// Get all unmerged block CIDs.
    pub async fn get_unmerged(&self) -> Result<Vec<Cid>> {
        self.manager.get_unmerged().await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use async_trait::async_trait;
    use blockstore::DefraBlockstore;
    use cid::Cid;
    use storage::backends::MemoryStore;

    use crate::bitswap::AccessMode;
    use crate::message::{
        BranchableSyncReply, BranchableSyncRequest, DocSyncReply, DocSyncRequest, PushLogBroadcast,
        PushLogReply, PushLogRequest, PushSEArtifactsRequest, QuerySEArtifactsReply,
        QuerySEArtifactsRequest,
    };
    use crate::sync::collection_store::P2PCollectionStore;
    use crate::sync::manager::SyncConfig;
    use crate::topics::DefraTopic;
    use crate::transport::{MessageId, P2PTransport, PeerAddr, PeerId};
    use crate::{QueryId, ReplicatorInfo};

    use super::SyncCoordinator;

    type TestBlockstore = DefraBlockstore<MemoryStore>;

    #[derive(Clone)]
    struct RecordingTransport {
        peer_id: PeerId,
        pubkey: Vec<u8>,
        subscribed: Arc<Mutex<HashSet<String>>>,
        fail_subscribe: Arc<Mutex<HashSet<String>>>,
        replicators: Arc<Mutex<HashMap<String, Vec<String>>>>,
    }

    impl RecordingTransport {
        fn new(peer_id: &str) -> Self {
            Self {
                peer_id: PeerId::new(peer_id.to_string()),
                pubkey: vec![1, 2, 3],
                subscribed: Arc::new(Mutex::new(HashSet::new())),
                fail_subscribe: Arc::new(Mutex::new(HashSet::new())),
                replicators: Arc::new(Mutex::new(HashMap::new())),
            }
        }

        fn fail_subscribe(self, topic: &str) -> Self {
            self.fail_subscribe
                .lock()
                .unwrap()
                .insert(topic.to_string());
            self
        }

        fn subscribed_topics(&self) -> Vec<String> {
            let mut topics: Vec<_> = self.subscribed.lock().unwrap().iter().cloned().collect();
            topics.sort();
            topics
        }
    }

    #[async_trait]
    impl P2PTransport for RecordingTransport {
        type ResponseToken = ();

        fn local_peer_id(&self) -> &PeerId {
            &self.peer_id
        }

        fn local_public_key_proto(&self) -> &[u8] {
            &self.pubkey
        }

        fn sign(&self, _data: &[u8]) -> crate::Result<Vec<u8>> {
            Ok(vec![0])
        }

        async fn dial(&self, _peer_id: &PeerId, _addrs: Vec<PeerAddr>) -> crate::Result<()> {
            Ok(())
        }

        async fn listen(&self, _addr: PeerAddr) -> crate::Result<()> {
            Ok(())
        }

        async fn connected_peers(&self) -> crate::Result<Vec<PeerId>> {
            Ok(Vec::new())
        }

        async fn listen_addresses(&self) -> crate::Result<Vec<PeerAddr>> {
            Ok(Vec::new())
        }

        async fn poll_until_connected(
            &self,
            _peer_id: &PeerId,
            _timeout: Duration,
        ) -> crate::Result<()> {
            Ok(())
        }

        async fn peer_addresses(&self) -> crate::Result<Vec<String>> {
            Ok(Vec::new())
        }

        async fn subscribe(&self, topic: DefraTopic) -> crate::Result<bool> {
            let topic = topic.topic_string();
            if self.fail_subscribe.lock().unwrap().contains(&topic) {
                return Err(crate::error::Error::Transport(format!(
                    "injected subscribe failure for {topic}"
                )));
            }
            Ok(self.subscribed.lock().unwrap().insert(topic))
        }

        async fn unsubscribe(&self, topic: DefraTopic) -> crate::Result<bool> {
            Ok(self
                .subscribed
                .lock()
                .unwrap()
                .remove(&topic.topic_string()))
        }

        async fn publish(
            &self,
            _topic: DefraTopic,
            _msg: PushLogBroadcast,
        ) -> crate::Result<MessageId> {
            Ok(MessageId::new("message".to_string()))
        }

        async fn topic_peers(&self, _topic: DefraTopic) -> crate::Result<Vec<PeerId>> {
            Ok(Vec::new())
        }

        async fn send_pushlog_response(
            &self,
            _token: Self::ResponseToken,
            _reply: PushLogReply,
        ) -> crate::Result<()> {
            Ok(())
        }

        async fn send_two_stream_request(
            &self,
            _peer_id: &PeerId,
            _req: PushLogRequest,
        ) -> crate::Result<PushLogReply> {
            Ok(PushLogReply::success("ok"))
        }

        async fn send_two_stream_response(
            &self,
            _peer_id: &PeerId,
            _reply: PushLogReply,
        ) -> crate::Result<()> {
            Ok(())
        }

        async fn send_doc_sync_request(
            &self,
            _peer_id: &PeerId,
            _req: DocSyncRequest,
        ) -> crate::Result<()> {
            Ok(())
        }

        async fn send_doc_sync_response(
            &self,
            _peer_id: &PeerId,
            _reply: DocSyncReply,
        ) -> crate::Result<()> {
            Ok(())
        }

        async fn send_branchable_sync_request(
            &self,
            _peer_id: &PeerId,
            _req: BranchableSyncRequest,
        ) -> crate::Result<()> {
            Ok(())
        }

        async fn send_branchable_sync_response(
            &self,
            _peer_id: &PeerId,
            _reply: BranchableSyncReply,
        ) -> crate::Result<()> {
            Ok(())
        }

        async fn send_car_request(&self, _peer_id: &PeerId, _root_cid: Cid) -> crate::Result<()> {
            Ok(())
        }

        async fn send_car_response(
            &self,
            _peer_id: &PeerId,
            _car_data: Vec<u8>,
        ) -> crate::Result<()> {
            Ok(())
        }

        async fn send_car_response_token(
            &self,
            _token: Self::ResponseToken,
            _car_data: Vec<u8>,
        ) -> crate::Result<()> {
            Ok(())
        }

        async fn send_doc_sync_response_token(
            &self,
            _token: Self::ResponseToken,
            _reply: DocSyncReply,
        ) -> crate::Result<()> {
            Ok(())
        }

        async fn send_branchable_sync_response_token(
            &self,
            _token: Self::ResponseToken,
            _reply: BranchableSyncReply,
        ) -> crate::Result<()> {
            Ok(())
        }

        async fn send_se_artifacts(
            &self,
            _peer_id: &PeerId,
            _req: PushSEArtifactsRequest,
        ) -> crate::Result<()> {
            Ok(())
        }

        async fn send_se_query_request(
            &self,
            _peer_id: &PeerId,
            _req: QuerySEArtifactsRequest,
        ) -> crate::Result<()> {
            Ok(())
        }

        async fn send_se_query_response(
            &self,
            _peer_id: &PeerId,
            _reply: QuerySEArtifactsReply,
        ) -> crate::Result<()> {
            Ok(())
        }

        async fn sync_blocks(
            &self,
            _root: Cid,
            _providers: Vec<PeerId>,
            _missing: Vec<Cid>,
        ) -> crate::Result<QueryId> {
            Ok(QueryId(1))
        }

        async fn cancel_sync(&self, _query_id: QueryId) -> crate::Result<bool> {
            Ok(true)
        }

        async fn create_replicator(
            &self,
            peer_id: &PeerId,
            collections: Vec<String>,
        ) -> crate::Result<()> {
            self.replicators
                .lock()
                .unwrap()
                .insert(peer_id.to_string(), collections);
            Ok(())
        }

        async fn delete_replicator(&self, peer_id: &PeerId) -> crate::Result<()> {
            self.replicators.lock().unwrap().remove(peer_id.as_str());
            Ok(())
        }

        async fn list_replicators(&self) -> crate::Result<Vec<ReplicatorInfo>> {
            Ok(Vec::new())
        }

        async fn get_replicator(&self, _peer_id: &PeerId) -> crate::Result<Option<ReplicatorInfo>> {
            Ok(None)
        }

        async fn remove_replicator_collections(
            &self,
            _peer_id: &PeerId,
            _collections: Vec<String>,
        ) -> crate::Result<bool> {
            Ok(false)
        }

        async fn shutdown(&self) -> crate::Result<()> {
            Ok(())
        }
    }

    async fn new_test_coordinator(
        store: Arc<MemoryStore>,
        transport: RecordingTransport,
    ) -> SyncCoordinator<TestBlockstore, RecordingTransport> {
        let blockstore = Arc::new(DefraBlockstore::new(store.clone(), true));
        let collection_store = Arc::new(P2PCollectionStore::new(store));
        let (coordinator, _events) = SyncCoordinator::with_collection_store(
            transport,
            blockstore,
            SyncConfig::default(),
            AccessMode::Controlled,
            collection_store,
        )
        .await
        .unwrap();
        coordinator
    }

    #[tokio::test]
    async fn load_p2p_collections_reinstalls_subscriptions_after_restart() {
        let store = Arc::new(MemoryStore::new());
        let initial_transport = RecordingTransport::new("initial-peer");
        let initial = new_test_coordinator(store.clone(), initial_transport.clone()).await;

        assert!(initial.subscribe_collection("users").await.unwrap());
        assert_eq!(initial_transport.subscribed_topics(), vec!["users"]);

        let restarted_transport = RecordingTransport::new("restarted-peer");
        let restarted = new_test_coordinator(store, restarted_transport.clone()).await;
        assert!(
            restarted
                .get_subscribed_collections()
                .await
                .unwrap()
                .is_empty(),
            "a fresh coordinator starts with an empty in-memory subscription cache"
        );

        let restored = restarted.load_p2p_collections().await.unwrap();

        assert_eq!(restored, 1);
        assert_eq!(restarted_transport.subscribed_topics(), vec!["users"]);
        assert_eq!(
            restarted.get_subscribed_collections().await.unwrap(),
            vec!["users".to_string()]
        );
    }

    #[tokio::test]
    async fn subscribe_collection_rolls_back_persistence_when_transport_subscribe_fails() {
        let store = Arc::new(MemoryStore::new());
        let failing_transport = RecordingTransport::new("failing-peer").fail_subscribe("users");
        let failing = new_test_coordinator(store.clone(), failing_transport.clone()).await;

        let error = failing
            .subscribe_collection("users")
            .await
            .expect_err("transport subscribe failure should be returned");

        assert!(
            error.to_string().contains("injected subscribe failure"),
            "unexpected error: {error}"
        );
        assert!(
            failing
                .get_subscribed_collections()
                .await
                .unwrap()
                .is_empty(),
            "failed subscription should not remain in memory"
        );
        assert!(
            failing_transport.subscribed_topics().is_empty(),
            "failing transport should not record a subscription"
        );

        let restarted_transport = RecordingTransport::new("restarted-peer");
        let restarted = new_test_coordinator(store, restarted_transport.clone()).await;
        let restored = restarted.load_p2p_collections().await.unwrap();

        assert_eq!(restored, 0, "rolled-back subscription must not reload");
        assert!(
            restarted_transport.subscribed_topics().is_empty(),
            "rolled-back subscription must not persist"
        );
    }
}
