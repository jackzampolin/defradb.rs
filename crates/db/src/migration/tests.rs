use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use futures::StreamExt;
use lens::{
    LensConfig, LensDocResultStream, LensDocStream, LensModule, TransformId, TransformStore,
};
use query::{DocFetcher, DocMutator};
use schema::{CollectionVersion, FieldDescription, FieldKind};
use storage::corekv::IterOptions;
use storage::MemoryStore;

use crate::{AutoCommitMutator, DB};

#[derive(Default)]
struct SetVerifiedStore {
    transforms: RwLock<HashSet<TransformId>>,
    transform_calls: AtomicUsize,
}

#[async_trait]
impl TransformStore for SetVerifiedStore {
    async fn add(&self, config: LensConfig) -> lens::Result<TransformId> {
        let id = TransformId::new(format!("test-transform-{}", config.lenses.len()));
        self.transforms.write().unwrap().insert(id.clone());
        Ok(id)
    }

    async fn add_with_id(&self, id: TransformId, _config: LensConfig) -> lens::Result<()> {
        self.transforms.write().unwrap().insert(id);
        Ok(())
    }

    async fn list(&self) -> lens::Result<HashMap<String, LensModule>> {
        Ok(HashMap::new())
    }

    fn transform(
        &self,
        id: &TransformId,
        docs: LensDocStream,
    ) -> lens::Result<LensDocResultStream> {
        if !self.has_transform(id) {
            return Err(lens::Error::TransformNotFound(id.to_string()));
        }
        self.transform_calls.fetch_add(1, Ordering::SeqCst);
        Ok(Box::pin(docs.map(|mut doc| {
            doc.insert("verified".to_string(), serde_json::Value::Bool(true));
            Ok(doc)
        })))
    }

    fn inverse(&self, id: &TransformId, docs: LensDocStream) -> lens::Result<LensDocResultStream> {
        self.transform(id, docs)
    }

    fn has_transform(&self, id: &TransformId) -> bool {
        self.transforms.read().unwrap().contains(id)
    }

    async fn remove(&self, id: &TransformId) -> lens::Result<()> {
        self.transforms.write().unwrap().remove(id);
        Ok(())
    }
}

fn users_schema() -> CollectionVersion {
    let mut schema = CollectionVersion::new(
        "Users",
        "users-v1",
        "users-collection",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "name", FieldKind::string()),
        ],
    );
    schema.is_materialized = true;
    schema
}

async fn seed_user(db: &Arc<DB<MemoryStore>>) -> document::DocID {
    let mut doc = document::Document::new();
    doc.set("name", document::NormalValue::String("Alice".to_string()));
    AutoCommitMutator::new(db.clone())
        .create("Users", doc)
        .await
        .unwrap()
        .doc_id
}

async fn add_verified_version(db: &Arc<DB<MemoryStore>>) -> String {
    db.patch_collection(
        "Users",
        r#"[{"op":"add","path":"/Users/Fields/-","value":{"Name":"verified","Kind":"Boolean"}}]"#,
        None,
    )
    .await
    .unwrap()
    .version_id
}

async fn load_user(db: &Arc<DB<MemoryStore>>, doc_id: &document::DocID) -> document::Document {
    let collection = db.get_collection("Users").unwrap().unwrap();
    let txn = db.new_txn(true).await.unwrap();
    let doc = collection
        .get_by_doc_id(
            &txn.datastore().unwrap(),
            &txn.systemstore().unwrap(),
            doc_id,
        )
        .await
        .unwrap()
        .unwrap();
    txn.discard().unwrap();
    doc
}

async fn load_user_blob(db: &Arc<DB<MemoryStore>>, doc_id: &document::DocID) -> Vec<u8> {
    let collection = db.get_collection("Users").unwrap().unwrap();
    let txn = db.new_txn(true).await.unwrap();
    let datastore = txn.datastore().unwrap();
    let systemstore = txn.systemstore().unwrap();
    let doc_short_id = collection
        .resolve_doc_short_id(&systemstore, doc_id)
        .await
        .unwrap()
        .unwrap();
    let blob = datastore
        .get(&collection.doc_key(doc_short_id))
        .await
        .unwrap()
        .unwrap();
    drop(datastore);
    drop(systemstore);
    txn.discard().unwrap();
    blob
}

async fn namespace_counts(db: &Arc<DB<MemoryStore>>) -> (usize, usize, usize) {
    async fn count(view: datastore::NamespaceView) -> usize {
        let mut iter = view.iterator(IterOptions::new()).await.unwrap();
        let mut count = 0;
        while iter.next().await.unwrap().is_some() {
            count += 1;
        }
        iter.close().await.unwrap();
        count
    }

    let txn = db.new_txn(true).await.unwrap();
    let counts = (
        count(txn.blockstore().unwrap()).await,
        count(txn.headstore().unwrap()).await,
        count(txn.peerstore().unwrap()).await,
    );
    txn.discard().unwrap();
    counts
}

#[tokio::test]
async fn lazy_lensed_read_writes_back_without_new_commits() {
    let store = Arc::new(MemoryStore::new());
    let transform_store = Arc::new(SetVerifiedStore::default());
    let mut raw_db = DB::from_arc(store.clone()).unwrap();
    raw_db.lens_store = transform_store.clone();
    let db = Arc::new(raw_db);

    db.create_collection(users_schema()).await.unwrap();
    let v1 = db
        .get_collection("Users")
        .unwrap()
        .unwrap()
        .version_id()
        .to_string();
    let doc_id = seed_user(&db).await;
    let v2 = add_verified_version(&db).await;
    let fetcher = crate::LensedAutoCommitFetcher::new(db.clone());
    let before_migration = fetcher.get_all("Users").await.unwrap();
    assert_eq!(before_migration[0].get("verified"), None);

    db.set_migration(
        LensConfig::new(
            &v1,
            &v2,
            LensModule::from_bytes(b"\0asm\x01\0\0\0".to_vec()),
        ),
        None,
    )
    .await
    .unwrap();

    let before_counts = namespace_counts(&db).await;
    let docs = fetcher.get_all("Users").await.unwrap();
    assert_eq!(docs.len(), 1);
    assert_eq!(
        docs[0].get("verified"),
        Some(&document::NormalValue::Bool(true))
    );
    assert_eq!(docs[0].schema_version_id(), Some(v2.as_str()));
    assert_eq!(transform_store.transform_calls.load(Ordering::SeqCst), 1);

    let persisted = load_user(&db, &doc_id).await;
    assert_eq!(
        persisted.get("verified"),
        Some(&document::NormalValue::Bool(true))
    );
    assert_eq!(persisted.schema_version_id(), Some(v2.as_str()));
    assert_eq!(namespace_counts(&db).await, before_counts);

    fetcher.get_all("Users").await.unwrap();
    assert_eq!(
        transform_store.transform_calls.load(Ordering::SeqCst),
        1,
        "the cached version must bypass the lens on subsequent reads"
    );

    drop(fetcher);
    drop(db);
    let reopened = Arc::new(DB::open_from_arc(store).await.unwrap());
    let persisted_after_restart = load_user(&reopened, &doc_id).await;
    assert_eq!(
        persisted_after_restart.get("verified"),
        Some(&document::NormalValue::Bool(true))
    );
    assert_eq!(
        persisted_after_restart.schema_version_id(),
        Some(v2.as_str())
    );
}

#[tokio::test]
async fn eager_materialize_restamps_transformless_paths() {
    let store = Arc::new(MemoryStore::new());
    let db = Arc::new(DB::from_arc(store).unwrap());

    db.create_collection(users_schema()).await.unwrap();
    let doc_id = seed_user(&db).await;
    let v2 = add_verified_version(&db).await;
    assert_ne!(
        load_user(&db, &doc_id).await.schema_version_id(),
        Some(v2.as_str())
    );

    let before_counts = namespace_counts(&db).await;
    let before_blob = load_user_blob(&db, &doc_id).await;
    assert_eq!(db.materialize_collection("Users").await.unwrap(), 1);
    assert_eq!(
        load_user(&db, &doc_id).await.schema_version_id(),
        Some(v2.as_str())
    );
    assert_eq!(
        load_user_blob(&db, &doc_id).await,
        before_blob,
        "identity materialization must update only the version key"
    );
    assert_eq!(db.materialize_collection("Users").await.unwrap(), 0);
    assert_eq!(namespace_counts(&db).await, before_counts);
}
