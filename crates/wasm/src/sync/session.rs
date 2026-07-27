use std::collections::BTreeSet;
use std::rc::Rc;
use std::sync::Arc;

use defra_core::browser_sync::{
    BrowserSyncDocument, BrowserSyncPull, BrowserSyncRequest, BrowserSyncResponse,
    MAX_SYNC_BODY_BYTES, MAX_SYNC_DOCUMENTS_PER_REQUEST, MAX_SYNC_PAGE_SIZE,
};
use events::Bus;
use futures::channel::oneshot;
use futures::future::{AbortHandle, Abortable};
use futures::lock::Mutex;
use storage::LevelDbStore;
use wasm_bindgen_futures::spawn_local;

use crate::error::{Result, WasmError};

use super::http::SyncHttpClient;
use super::sse::SseStream;

const INITIAL_RECONNECT_DELAY_MS: u32 = 1_000;
const MAX_RECONNECT_DELAY_MS: u32 = 30_000;
const EMPTY_PUSH_REQUEST_BYTES: usize = b"{\"documents\":[]}".len();

pub(crate) struct SyncTask {
    abort: AbortHandle,
    finished: oneshot::Receiver<()>,
}

impl SyncTask {
    pub(crate) async fn stop(mut self) {
        self.abort.abort();
        let _ = (&mut self.finished).await;
    }
}

impl Drop for SyncTask {
    fn drop(&mut self) {
        self.abort.abort();
    }
}

pub(crate) async fn start(
    database: Arc<db::DB<LevelDbStore>>,
    event_bus: &Arc<events::ChannelBus>,
    server_url: &str,
    auth_token: Option<String>,
) -> Result<SyncTask> {
    let session = Rc::new(SyncSession {
        engine: db_merge::BrowserSyncEngine::new(database),
        http: SyncHttpClient::new(server_url, auth_token)?,
        exchange_lock: Mutex::new(()),
        full_sync_lock: Mutex::new(()),
    });
    let subscription = event_bus.subscribe(&[events::EventName::Update]);
    let subscription_id = subscription.id();
    let events = match session.http.events().await {
        Ok(events) => events,
        Err(error) => {
            event_bus.unsubscribe(subscription_id);
            return Err(error);
        }
    };
    if let Err(error) = session.full_sync().await {
        event_bus.unsubscribe(subscription_id);
        return Err(error);
    }

    let (abort, registration) = AbortHandle::new_pair();
    let (finished_tx, finished) = oneshot::channel();
    let event_bus = Arc::clone(event_bus);
    spawn_local(async move {
        let future = async move {
            futures::future::join(
                Rc::clone(&session).run_local(subscription),
                session.run_remote(events),
            )
            .await;
        };
        let _ = Abortable::new(future, registration).await;
        event_bus.unsubscribe(subscription_id);
        let _ = finished_tx.send(());
    });
    Ok(SyncTask { abort, finished })
}

struct SyncSession {
    engine: db_merge::BrowserSyncEngine<LevelDbStore>,
    http: SyncHttpClient,
    exchange_lock: Mutex<()>,
    full_sync_lock: Mutex<()>,
}

impl SyncSession {
    async fn full_sync(&self) -> Result<()> {
        let _guard = self.full_sync_lock.lock().await;
        self.push_all_documents().await?;
        self.pull_all_documents().await
    }

    async fn push_all_documents(&self) -> Result<()> {
        let refs = self.engine.document_refs().await.map_err(engine_error)?;
        let mut documents = Vec::new();
        let mut serialized_size = EMPTY_PUSH_REQUEST_BYTES;
        for document_ref in refs {
            let loaded = match self.engine.load_document(&document_ref).await {
                Ok(loaded) => loaded,
                // Cannot be represented as a sync payload — too large, or too
                // many blocks or roots — so it can never be pushed. Failing
                // here would abort the whole push and leave recover_full_sync
                // retrying forever; skip it like the request-size check below.
                Err(error @ db_merge::browser_sync::BrowserSyncError::TooLarge(_)) => {
                    warn(&format!(
                        "browser sync skipped document {} because it cannot be represented as a sync payload: {error}",
                        document_ref.doc_id
                    ));
                    continue;
                }
                Err(error) => return Err(engine_error(error)),
            };
            let Some(document) = loaded else {
                continue;
            };

            let document_size = serde_json::to_vec(&document)?.len();
            let single_document_size = EMPTY_PUSH_REQUEST_BYTES + document_size;
            if single_document_size > MAX_SYNC_BODY_BYTES {
                warn(&format!(
                    "browser sync skipped document {} because it exceeds the sync request limit",
                    document.doc_id
                ));
                continue;
            }

            let next_size = serialized_size + document_size + usize::from(!documents.is_empty());
            if next_size > MAX_SYNC_BODY_BYTES {
                self.push_documents(std::mem::take(&mut documents)).await?;
                serialized_size = EMPTY_PUSH_REQUEST_BYTES;
            }

            serialized_size += document_size + usize::from(!documents.is_empty());
            documents.push(document);
            if documents.len() == MAX_SYNC_DOCUMENTS_PER_REQUEST {
                self.push_documents(std::mem::take(&mut documents)).await?;
                serialized_size = EMPTY_PUSH_REQUEST_BYTES;
            }
        }
        if !documents.is_empty() {
            self.push_documents(documents).await?;
        }
        Ok(())
    }

    async fn push_documents(&self, documents: Vec<BrowserSyncDocument>) -> Result<()> {
        self.exchange(BrowserSyncRequest {
            documents,
            pull: None,
        })
        .await?;
        Ok(())
    }

    async fn pull_all_documents(&self) -> Result<()> {
        let mut cursor = None;
        loop {
            let response = self
                .exchange(BrowserSyncRequest {
                    documents: Vec::new(),
                    pull: Some(BrowserSyncPull {
                        doc_ids: Vec::new(),
                        cursor: cursor.clone(),
                        limit: Some(MAX_SYNC_PAGE_SIZE as u16),
                    }),
                })
                .await?;
            match response.next_cursor {
                Some(next) if cursor.as_deref() != Some(next.as_str()) => cursor = Some(next),
                Some(_) => {
                    return Err(WasmError::Sync(
                        "server returned a non-advancing sync cursor".into(),
                    ))
                }
                None => return Ok(()),
            }
        }
    }

    async fn sync_document(&self, doc_id: &str) -> Result<()> {
        let mut documents = Vec::new();
        if let Some(document_ref) = self
            .engine
            .document_ref(doc_id)
            .await
            .map_err(engine_error)?
        {
            // Nothing to push for an over-large document, but the pull below
            // still applies. Failing here would fall back to a full sync on
            // every single update touching this document.
            match self.engine.load_document(&document_ref).await {
                Ok(Some(document)) => documents.push(document),
                Ok(None) => {}
                Err(error @ db_merge::browser_sync::BrowserSyncError::TooLarge(_)) => {
                    warn(&format!(
                        "browser sync skipped document {doc_id} because it cannot be represented as a sync payload: {error}"
                    ));
                }
                Err(error) => return Err(engine_error(error)),
            }
        }
        self.exchange(BrowserSyncRequest {
            documents,
            pull: Some(BrowserSyncPull {
                doc_ids: vec![doc_id.to_string()],
                cursor: None,
                limit: Some(1),
            }),
        })
        .await?;
        Ok(())
    }

    async fn exchange(&self, request: BrowserSyncRequest) -> Result<BrowserSyncResponse> {
        let _guard = self.exchange_lock.lock().await;
        let response = self.http.sync(&request).await?;
        for document in &response.documents {
            self.engine
                .apply_document(document, "server")
                .await
                .map_err(engine_error)?;
        }
        Ok(response)
    }

    async fn run_local(self: Rc<Self>, mut subscription: events::Subscription) {
        while let Some(message) = subscription.recv().await {
            if subscription.check_and_reset_dropped() > 0 {
                if let Err(error) = self.full_sync().await {
                    self.recover_full_sync("recovering dropped local events", error)
                        .await;
                }
                continue;
            }

            let mut doc_ids = BTreeSet::new();
            collect_local_update(&message, &mut doc_ids);
            while let Ok(message) = subscription.try_recv() {
                collect_local_update(&message, &mut doc_ids);
            }
            for doc_id in doc_ids {
                if let Err(error) = self.sync_document(&doc_id).await {
                    self.recover_full_sync(&format!("syncing local document {doc_id}"), error)
                        .await;
                    break;
                }
            }
        }
    }

    async fn run_remote(self: Rc<Self>, mut events: SseStream) {
        loop {
            loop {
                match events.next_document_id().await {
                    Ok(Some(doc_id)) => {
                        if let Err(error) = self.sync_document(&doc_id).await {
                            self.recover_full_sync(
                                &format!("syncing remote document {doc_id}"),
                                error,
                            )
                            .await;
                        }
                    }
                    Ok(None) => break,
                    Err(error) => {
                        warn(&format!("browser sync event stream closed: {error}"));
                        break;
                    }
                }
            }
            events = self.reconnect().await;
        }
    }

    async fn recover_full_sync(&self, context: &str, mut error: WasmError) {
        let mut delay = INITIAL_RECONNECT_DELAY_MS;
        loop {
            warn(&format!("browser sync failed while {context}: {error}"));
            gloo_timers::future::TimeoutFuture::new(delay).await;
            match self.full_sync().await {
                Ok(()) => return,
                Err(next_error) => error = next_error,
            }
            delay = delay.saturating_mul(2).min(MAX_RECONNECT_DELAY_MS);
        }
    }

    async fn reconnect(&self) -> SseStream {
        let mut delay = INITIAL_RECONNECT_DELAY_MS;
        loop {
            gloo_timers::future::TimeoutFuture::new(delay).await;
            match self.http.events().await {
                Ok(events) => match self.full_sync().await {
                    Ok(()) => return events,
                    Err(error) => warn(&format!("browser sync reconnect failed: {error}")),
                },
                Err(error) => warn(&format!("browser sync reconnect failed: {error}")),
            }
            delay = delay.saturating_mul(2).min(MAX_RECONNECT_DELAY_MS);
        }
    }
}

fn collect_local_update(message: &events::Message, doc_ids: &mut BTreeSet<String>) {
    if let Some(update) = message.as_update() {
        if !update.is_relay && !update.doc_id.is_empty() {
            doc_ids.insert(update.doc_id.clone());
        }
    }
}

fn engine_error(error: db_merge::BrowserSyncError) -> WasmError {
    WasmError::Sync(error.to_string())
}

fn warn(message: &str) {
    web_sys::console::warn_1(&message.into());
}

#[cfg(test)]
mod tests {
    use defra_core::browser_sync::{BrowserSyncBlock, BrowserSyncDocument, BrowserSyncRequest};

    use super::EMPTY_PUSH_REQUEST_BYTES;

    #[test]
    fn incremental_push_size_matches_serialized_request() {
        let documents = vec![
            BrowserSyncDocument {
                doc_id: "doc-one".into(),
                collection_id: "collection".into(),
                roots: vec!["root-one".into()],
                blocks: vec![BrowserSyncBlock {
                    cid: "block-one".into(),
                    data: "data-one".into(),
                }],
            },
            BrowserSyncDocument {
                doc_id: "doc-two".into(),
                collection_id: "collection".into(),
                roots: vec![],
                blocks: vec![],
            },
        ];
        let incremental_size = documents.iter().enumerate().fold(
            EMPTY_PUSH_REQUEST_BYTES,
            |size, (index, document)| {
                size + serde_json::to_vec(document).unwrap().len() + usize::from(index > 0)
            },
        );

        let request = BrowserSyncRequest {
            documents,
            pull: None,
        };
        assert_eq!(
            incremental_size,
            serde_json::to_vec(&request).unwrap().len()
        );
    }
}
