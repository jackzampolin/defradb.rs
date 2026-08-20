use crate::common::schema::test_schema;
use db::BasicTxn;
use db::DbDocMutator;
use db::DbTxn;
use db::DB;
use document::Document;
use document::NormalValue;
use events::Bus;
use events::ChannelBus;
use query::mutator::DocMutator;
use std::sync::Arc;
use storage::backends::MemoryStore;

pub fn new_txn(basic_txn: BasicTxn) -> DbTxn<MemoryStore> {
    DbTxn::<MemoryStore>::new(basic_txn)
}

pub fn next_test_doc_short_id() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(1);
    NEXT.fetch_add(1, Ordering::Relaxed)
}

pub async fn fixture_with_docs(n: usize) -> Arc<DB<MemoryStore>> {
    let db = Arc::new(DB::new(MemoryStore::new()).unwrap());
    db.create_collection(test_schema()).await.unwrap();

    for i in 0..n {
        let txn = db.new_txn(false).await.unwrap();
        let mutator = DbDocMutator::new(db.clone(), txn);
        let mut doc = Document::new();
        doc.set("name", NormalValue::String(format!("user-{i}")));
        mutator.create("Users", doc).await.unwrap();
        let txn = mutator.take_txn().await.unwrap();
        txn.commit().await.unwrap();
    }

    db
}

pub async fn make_test_db_with_bus() -> (Arc<DB<MemoryStore>>, Arc<dyn Bus>) {
    let bus: Arc<dyn Bus> = Arc::new(ChannelBus::new());
    let mut db = DB::new(MemoryStore::new()).expect("create db");
    db.set_event_bus(Arc::clone(&bus));
    (Arc::new(db), bus)
}
