use super::*;
use acp::{DocumentACP, LocalDocumentACP, MemoryAcpStore};
use document::NormalValue;
use events::{Bus, ChannelBus, EventName};
use query::mutator::DocMutator;
use query::runner::DocFetcher;
use schema::{CType, CollectionVersion, FieldDescription, FieldKind, PolicyDescription};
use storage::backends::MemoryStore;

async fn make_test_db_with_bus() -> (Arc<DB<MemoryStore>>, Arc<dyn Bus>) {
    let bus: Arc<dyn Bus> = Arc::new(ChannelBus::new());
    let mut db = DB::new(MemoryStore::new()).expect("create db");
    db.set_event_bus(Arc::clone(&bus));
    (Arc::new(db), bus)
}

fn test_collection() -> CollectionVersion {
    CollectionVersion::new(
        "TestDoc",
        "v1",
        "col-test-doc",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "x", FieldKind::int()),
        ],
    )
}

fn branchable_collection() -> CollectionVersion {
    CollectionVersion::new(
        "Branchable",
        "branchable-v1",
        "col-branchable",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "x", FieldKind::int()),
        ],
    )
    .as_branchable()
}

struct FailingCollectionSigner;

impl defra_core::signing::RemoteSigner for FailingCollectionSigner {
    fn sign_sync(
        &self,
        data: &[u8],
        _authorization: Option<&defra_core::signing::SigningAuthorization>,
    ) -> Result<Vec<u8>, String> {
        let block =
            defra_core::block::Block::from_dag_cbor(data).map_err(|error| error.to_string())?;
        if matches!(block.delta, defra_core::block::CrdtDelta::Collection(_)) {
            return Err("injected collection signing failure".to_string());
        }
        Ok(vec![0; 64])
    }
}

fn failing_collection_signing_config() -> defra_core::signing::SigningConfig {
    defra_core::signing::SigningConfig {
        key_type: defra_core::signing::SigningKeyType::Ed25519,
        private_key_bytes: Vec::new(),
        public_key_bytes: Vec::new(),
        public_key_hex: "test".to_string(),
        remote_signer: Some(Arc::new(FailingCollectionSigner)),
        signing_authorization: None,
    }
}

#[tokio::test]
async fn branchable_create_rolls_back_when_collection_block_fails() {
    let (db, _bus) = make_test_db_with_bus().await;
    db.create_collection(branchable_collection()).await.unwrap();
    let mutator = crate::AutoCommitMutator::new(Arc::clone(&db));

    defra_core::signing::set_signing_config(Some(failing_collection_signing_config()));
    let result = mutator
        .create(
            "Branchable",
            Document::from_json_str(r#"{"x": 1}"#).unwrap(),
        )
        .await;
    defra_core::signing::set_signing_config(None);

    let error = result.expect_err("collection block failure must fail create");
    assert!(error
        .to_string()
        .contains("injected collection signing failure"));

    let fetcher = crate::LensedAutoCommitFetcher::new(Arc::clone(&db));
    assert!(fetcher.get_all("Branchable").await.unwrap().is_empty());
}

#[tokio::test]
async fn branchable_update_rolls_back_when_collection_block_fails() {
    let (db, _bus) = make_test_db_with_bus().await;
    db.create_collection(branchable_collection()).await.unwrap();
    let mutator = crate::AutoCommitMutator::new(Arc::clone(&db));
    let created = mutator
        .create(
            "Branchable",
            Document::from_json_str(r#"{"x": 1}"#).unwrap(),
        )
        .await
        .unwrap();
    let mut doc = mutator
        .get_for_update("Branchable", &created.doc_id)
        .await
        .unwrap()
        .unwrap();
    doc.set("x", NormalValue::Int(2));

    defra_core::signing::set_signing_config(Some(failing_collection_signing_config()));
    let result = mutator
        .update(
            "Branchable",
            doc,
            std::iter::once("x".to_string()).collect(),
        )
        .await;
    defra_core::signing::set_signing_config(None);

    let error = result.expect_err("collection block failure must fail update");
    assert!(error
        .to_string()
        .contains("injected collection signing failure"));

    let persisted = mutator
        .get_for_update("Branchable", &created.doc_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(persisted.get("x"), Some(&NormalValue::Int(1)));
}

fn counter_collection() -> CollectionVersion {
    CollectionVersion::new(
        "Counters",
        "cv1",
        "col-counters",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "count", FieldKind::int()).with_crdt_type(CType::PnCounter),
        ],
    )
}

fn float32_counter_collection() -> CollectionVersion {
    CollectionVersion::new(
        "FloatCounters",
        "fcv1",
        "col-float-counters",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "points", FieldKind::float32())
                .with_crdt_type(CType::PnCounter),
        ],
    )
}

/// Read the PNCounter accumulation store value for a doc/field from the
/// committed store (a fresh read txn), proving the authoritative store — not
/// just the materialized blob — advanced.
async fn read_counter_store(
    db: &Arc<DB<MemoryStore>>,
    schema_version_id: &str,
    doc_id: &str,
    field: &str,
) -> i64 {
    use crdt::traits::ValueReader;
    use crdt::{Counter, NumericKind};

    let txn = db.new_txn(true).await.expect("read txn");
    let datastore = txn.datastore().expect("datastore");
    let counter = Counter::new(
        schema_version_id.to_string(),
        doc_id.as_bytes(),
        field.to_string(),
        true,
        NumericKind::Int64,
    )
    .expect("counter");
    let bytes = ValueReader::value(&counter, &datastore)
        .await
        .expect("counter value");
    assert_eq!(bytes.len(), 8, "int64 counter store value is 8 bytes");
    i64::from_be_bytes(bytes.try_into().unwrap())
}

#[tokio::test]
async fn auto_commit_float32_counter_accumulates_with_float32_precision() {
    let (db, _bus) = make_test_db_with_bus().await;
    db.create_collection(float32_counter_collection())
        .await
        .unwrap();
    let mutator = crate::AutoCommitMutator::new(Arc::clone(&db));

    let mut doc = Document::new();
    doc.set("points", NormalValue::Float32(0.0));
    let doc_id = mutator.create("FloatCounters", doc).await.unwrap().doc_id;

    for (value, delta) in [(10.1f32, 10.1f32), (10.1f32 + 10.2f32, 10.2f32)] {
        let mut doc = mutator
            .get_for_update("FloatCounters", &doc_id)
            .await
            .unwrap()
            .unwrap();
        doc.set("points", NormalValue::Float32(value));
        doc.set_counter_delta("points".into(), NormalValue::Float32(delta));
        mutator
            .update(
                "FloatCounters",
                doc,
                std::iter::once("points".to_string()).collect(),
            )
            .await
            .unwrap();
    }

    let doc = mutator
        .get_for_update("FloatCounters", &doc_id)
        .await
        .unwrap()
        .unwrap();
    let expected = 10.1f32 + 10.2f32;
    assert_eq!(
        doc.get("points"),
        Some(&NormalValue::Float64(f64::from(expected)))
    );

    use crdt::traits::ValueReader;
    let txn = db.new_txn(true).await.unwrap();
    let datastore = txn.datastore().unwrap();
    let counter = crdt::Counter::new(
        "fcv1".into(),
        doc_id.to_string().as_bytes(),
        "points".into(),
        true,
        crdt::NumericKind::Float32,
    )
    .unwrap();
    let bytes = ValueReader::value(&counter, &datastore).await.unwrap();
    assert_eq!(bytes.len(), 4);
    assert_eq!(f32::from_be_bytes(bytes.try_into().unwrap()), expected);
}

#[tokio::test]
async fn alias_reads_and_mutations_use_the_canonical_doc_id() {
    let (db, _bus) = make_test_db_with_bus().await;
    db.create_collection(test_collection()).await.unwrap();
    let mutator = crate::AutoCommitMutator::new(db.clone());
    let mut doc = Document::new();
    doc.set("x", NormalValue::Int(1));
    let created = mutator.create("TestDoc", doc).await.unwrap();
    let canonical_doc_id = created.doc_id;
    let alias =
        DocID::new_v0(defra_core::block::generate_cid_from_bytes(b"imported-alias").unwrap());

    let txn = db.new_txn(false).await.unwrap();
    {
        let systemstore = txn.systemstore().unwrap();
        let doc_ref = crate::docid::map::get_doc_ref(&systemstore, &canonical_doc_id.to_string())
            .await
            .unwrap()
            .unwrap();
        crate::docid::map::set_doc_id_alias(
            &systemstore,
            doc_ref.collection_short_id,
            doc_ref.doc_short_id,
            &alias.to_string(),
        )
        .await
        .unwrap();
    }
    txn.commit().await.unwrap();

    let mut fetched = mutator
        .get_for_update("TestDoc", &alias)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(fetched.id(), Some(&canonical_doc_id));
    fetched.set_id(alias.clone());
    fetched.set("x", NormalValue::Int(2));
    let updated = mutator
        .update(
            "TestDoc",
            fetched,
            std::iter::once("x".to_string()).collect(),
        )
        .await
        .unwrap();
    assert_eq!(updated.document.id(), Some(&canonical_doc_id));

    let deleted = mutator.delete("TestDoc", &alias).await.unwrap();
    assert!(deleted.existed);
    assert_eq!(deleted.doc_id, canonical_doc_id);
    let txn = db.new_txn(true).await.unwrap();
    let systemstore = txn.systemstore().unwrap();
    let owners = crate::docid::map::get_doc_ids_for_block(
        &systemstore,
        &deleted.commit_cid.unwrap().to_string(),
    )
    .await
    .unwrap();
    assert_eq!(owners, vec![canonical_doc_id.to_string()]);
}

#[tokio::test]
async fn explicit_txn_counter_increment_advances_accumulation_store() {
    // Regression for #1021 (now #1044 record-then-finalize): an
    // explicit-transaction counter increment must read-modify-write the
    // authoritative CRDT accumulation store (not only the materialized blob).
    // The RMW is now deferred to the registry commit-time finalize, so the
    // test drives commit through the registry (not a bare force_commit, which
    // would skip the finalize) — keeping the original intent: after commit the
    // authoritative store reflects the increment.
    use crate::txn::registry::DbTransactionRegistry;
    use query::txn::TransactionRegistry;

    let (db, _bus) = make_test_db_with_bus().await;
    db.create_collection(counter_collection())
        .await
        .expect("schema");
    let registry = DbTransactionRegistry::new(Arc::clone(&db));

    // Create the doc (count = 5) in an explicit txn, commit via the registry.
    let handle = registry.begin(false).await.expect("begin");
    let ctx = registry.get(&handle).into_result().unwrap().unwrap();
    let mutator = ctx.doc_mutator().expect("mutator");
    let create_doc = Document::from_json_str(r#"{"count": 5}"#).expect("doc");
    let created = mutator
        .create("Counters", create_doc)
        .await
        .expect("create");
    let doc_id = created.doc_id.to_string();
    drop(mutator);
    drop(ctx);
    registry.commit(&handle).await.expect("commit");

    assert_eq!(
        read_counter_store(&db, "cv1", &doc_id, "count").await,
        5,
        "create must seed the accumulation store at finalize"
    );

    // Increment by 3 in a fresh explicit txn, commit via the registry.
    let handle = registry.begin(false).await.expect("begin");
    let ctx = registry.get(&handle).into_result().unwrap().unwrap();
    let mutator = ctx.doc_mutator().expect("mutator");
    let mut update_doc = Document::from_json_str(r#"{"count": 8}"#).expect("doc");
    update_doc.set_id(document::DocID::from_string(&doc_id).expect("doc id"));
    update_doc.set_counter_delta("count".to_string(), document::NormalValue::Int(3));
    let mut modified = std::collections::HashSet::new();
    modified.insert("count".to_string());
    mutator
        .update("Counters", update_doc, modified)
        .await
        .expect("update");
    drop(mutator);
    drop(ctx);
    registry.commit(&handle).await.expect("commit");

    assert_eq!(
        read_counter_store(&db, "cv1", &doc_id, "count").await,
        8,
        "explicit-txn increment must advance the accumulation store at finalize (#1044)"
    );
}

/// PCounter (increment-only) collection: reconcile MIGRATES a present store
/// upward via max, so the finalize must NOT re-read the provisional blob as the
/// reconcile base (that would double-apply the delta). See #1044 BUG 1.
fn pcounter_collection() -> CollectionVersion {
    CollectionVersion::new(
        "PCounters",
        "pcv1",
        "col-pcounters",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "count", FieldKind::int()).with_crdt_type(CType::PCounter),
        ],
    )
}

/// Regression for #1044 BUG 1 (PCounter explicit-txn double-apply): create a
/// PCounter doc at 5, then increment +3 in a separate explicit txn. The
/// authoritative store must end at 8, NOT 11. Before the fix the finalize
/// re-read the provisional blob (8) as the reconcile base, migrated the present
/// store (5) UPWARD to 8 via PCounter max, then applied +3 → 11. Capturing the
/// pre-write committed value (5) as the reconcile base fixes it.
#[tokio::test]
async fn explicit_txn_pcounter_increment_no_double_apply() {
    use crate::txn::registry::DbTransactionRegistry;
    use query::txn::TransactionRegistry;

    let (db, _bus) = make_test_db_with_bus().await;
    db.create_collection(pcounter_collection())
        .await
        .expect("schema");
    let registry = DbTransactionRegistry::new(Arc::clone(&db));

    // Create the doc (count = 5) in an explicit txn, commit via the registry.
    let handle = registry.begin(false).await.expect("begin");
    let ctx = registry.get(&handle).into_result().unwrap().unwrap();
    let mutator = ctx.doc_mutator().expect("mutator");
    let create_doc = Document::from_json_str(r#"{"count": 5}"#).expect("doc");
    let created = mutator
        .create("PCounters", create_doc)
        .await
        .expect("create");
    let doc_id = created.doc_id.to_string();
    drop(mutator);
    drop(ctx);
    registry.commit(&handle).await.expect("commit");

    assert_eq!(
        read_counter_store(&db, "pcv1", &doc_id, "count").await,
        5,
        "create must seed the PCounter accumulation store at 5"
    );

    // Increment by 3 in a fresh explicit txn (provisional blob = 8).
    let handle = registry.begin(false).await.expect("begin");
    let ctx = registry.get(&handle).into_result().unwrap().unwrap();
    let mutator = ctx.doc_mutator().expect("mutator");
    let mut update_doc = Document::from_json_str(r#"{"count": 8}"#).expect("doc");
    update_doc.set_id(document::DocID::from_string(&doc_id).expect("doc id"));
    update_doc.set_counter_delta("count".to_string(), document::NormalValue::Int(3));
    let mut modified = std::collections::HashSet::new();
    modified.insert("count".to_string());
    mutator
        .update("PCounters", update_doc, modified)
        .await
        .expect("update");
    drop(mutator);
    drop(ctx);
    registry.commit(&handle).await.expect("commit");

    assert_eq!(
        read_counter_store(&db, "pcv1", &doc_id, "count").await,
        8,
        "PCounter increment must NOT double-apply: store == 8 (not 11) (#1044)"
    );
}

/// Multi-doc finalize: an explicit txn that increments counters on TWO
/// different docs must, after commit, leave BOTH accumulation stores
/// advanced — exercising the sorted multi-doc acquire in the finalize driver.
#[tokio::test]
async fn explicit_txn_multi_doc_counter_finalize_advances_both_stores() {
    use crate::txn::registry::DbTransactionRegistry;
    use query::txn::TransactionRegistry;

    let (db, _bus) = make_test_db_with_bus().await;
    db.create_collection(counter_collection())
        .await
        .expect("schema");
    let registry = DbTransactionRegistry::new(Arc::clone(&db));

    // Seed two docs (count = 10 and count = 20) in one explicit txn.
    let handle = registry.begin(false).await.expect("begin");
    let ctx = registry.get(&handle).into_result().unwrap().unwrap();
    let mutator = ctx.doc_mutator().expect("mutator");
    let doc_a = mutator
        .create(
            "Counters",
            Document::from_json_str(r#"{"count": 10}"#).unwrap(),
        )
        .await
        .expect("create a")
        .doc_id
        .to_string();
    let doc_b = mutator
        .create(
            "Counters",
            Document::from_json_str(r#"{"count": 20}"#).unwrap(),
        )
        .await
        .expect("create b")
        .doc_id
        .to_string();
    drop(mutator);
    drop(ctx);
    registry.commit(&handle).await.expect("commit creates");

    // Increment BOTH docs in a single explicit txn (multi-doc finalize).
    let handle = registry.begin(false).await.expect("begin");
    let ctx = registry.get(&handle).into_result().unwrap().unwrap();
    let mutator = ctx.doc_mutator().expect("mutator");

    let mut up_a = Document::from_json_str(r#"{"count": 11}"#).unwrap();
    up_a.set_id(document::DocID::from_string(&doc_a).unwrap());
    up_a.set_counter_delta("count".to_string(), document::NormalValue::Int(1));
    let mut mod_a = std::collections::HashSet::new();
    mod_a.insert("count".to_string());
    mutator
        .update("Counters", up_a, mod_a)
        .await
        .expect("update a");

    let mut up_b = Document::from_json_str(r#"{"count": 25}"#).unwrap();
    up_b.set_id(document::DocID::from_string(&doc_b).unwrap());
    up_b.set_counter_delta("count".to_string(), document::NormalValue::Int(5));
    let mut mod_b = std::collections::HashSet::new();
    mod_b.insert("count".to_string());
    mutator
        .update("Counters", up_b, mod_b)
        .await
        .expect("update b");

    drop(mutator);
    drop(ctx);
    registry.commit(&handle).await.expect("commit updates");

    assert_eq!(
        read_counter_store(&db, "cv1", &doc_a, "count").await,
        11,
        "doc A store must reflect +1"
    );
    assert_eq!(
        read_counter_store(&db, "cv1", &doc_b, "count").await,
        25,
        "doc B store must reflect +5"
    );
}

/// Read the counter accumulation store value, returning `None` when the store
/// key is absent (the counter was never durably finalized). Used by the
/// discard test to prove a rolled-back interactive txn ran no RMW.
async fn read_counter_store_opt(
    db: &Arc<DB<MemoryStore>>,
    schema_version_id: &str,
    doc_id: &str,
    field: &str,
) -> Option<i64> {
    use crdt::traits::ValueReader;
    use crdt::{Counter, NumericKind};

    let txn = db.new_txn(true).await.expect("read txn");
    let datastore = txn.datastore().expect("datastore");
    let counter = Counter::new(
        schema_version_id.to_string(),
        doc_id.as_bytes(),
        field.to_string(),
        true,
        NumericKind::Int64,
    )
    .expect("counter");
    match ValueReader::value(&counter, &datastore).await {
        Ok(bytes) => {
            assert_eq!(bytes.len(), 8, "int64 counter store value is 8 bytes");
            Some(i64::from_be_bytes(bytes.try_into().unwrap()))
        }
        Err(_) => None,
    }
}

/// Read a doc from the committed store via a fresh read txn; `None` if absent.
async fn read_committed_doc(
    db: &Arc<DB<MemoryStore>>,
    collection_name: &str,
    doc_id: &str,
) -> Option<Document> {
    let collection = db
        .get_collection(collection_name)
        .expect("get collection")
        .expect("collection exists");
    let txn = db.new_txn(true).await.expect("read txn");
    let datastore = txn.datastore().expect("datastore");
    let systemstore = txn.systemstore().expect("systemstore");
    let doc_id_typed = document::DocID::from_string(doc_id).expect("doc id");
    collection
        .get_by_doc_id(&datastore, &systemstore, &doc_id_typed)
        .await
        .expect("get doc")
}

/// PCounter create-then-update in ONE registry txn: create at 5 then +3 in the
/// SAME txn, commit. The update's `committed_pre_write` read sees the
/// same-txn-staged create (5), so the recorded base is 5 and the finalize ends
/// at exactly 8 (NOT 11 from a double-apply).
///
/// Note: this test does NOT exercise base-capture's load-bearing path. Because
/// the create stages the accumulation store at 5 in the same txn, the update's
/// reconcile(base) is a no-op for any base ≤ 5 (the store is already present at
/// 5; reconcile is init-if-absent / migrate-via-max, and neither base=5 nor a
/// missing base=0 raises a present 5), and the +3 is then added UNCONDITIONALLY
/// (counter merge is plain addition, not max) → 8. So a missing base would also
/// yield 8 here — the base value is irrelevant once the create has seeded the
/// store. The missing-base → wrong-value guard lives in
/// `update_seeds_absent_store_from_committed_base_load_bearing`, where the
/// accumulation store is absent at update-finalize.
#[tokio::test]
async fn explicit_txn_pcounter_create_then_update_same_txn() {
    use crate::txn::registry::DbTransactionRegistry;
    use query::txn::TransactionRegistry;

    let (db, _bus) = make_test_db_with_bus().await;
    db.create_collection(pcounter_collection())
        .await
        .expect("schema");
    let registry = DbTransactionRegistry::new(Arc::clone(&db));

    let handle = registry.begin(false).await.expect("begin");
    let ctx = registry.get(&handle).into_result().unwrap().unwrap();
    let mutator = ctx.doc_mutator().expect("mutator");

    let created = mutator
        .create(
            "PCounters",
            Document::from_json_str(r#"{"count": 5}"#).unwrap(),
        )
        .await
        .expect("create");
    let doc_id = created.doc_id.to_string();

    let mut update_doc = Document::from_json_str(r#"{"count": 8}"#).unwrap();
    update_doc.set_id(document::DocID::from_string(&doc_id).unwrap());
    update_doc.set_counter_delta("count".to_string(), document::NormalValue::Int(3));
    let mut modified = std::collections::HashSet::new();
    modified.insert("count".to_string());
    mutator
        .update("PCounters", update_doc, modified)
        .await
        .expect("update");

    drop(mutator);
    drop(ctx);
    registry.commit(&handle).await.expect("commit");

    assert_eq!(
        read_counter_store(&db, "pcv1", &doc_id, "count").await,
        8,
        "PCounter create(5)+update(+3) in one txn must finalize to exactly 8"
    );
}

/// LOAD-BEARING regression for the #1044 base-capture: the finalize must seed
/// the accumulation store init-if-absent from the PRE-WRITE COMMITTED value
/// (`base`) before applying the delta. This is the ONLY scenario where base is
/// load-bearing — a doc whose materialized blob holds a counter value but whose
/// accumulation store is ABSENT (e.g. a legacy / pre-#1021 / migrated doc that
/// was written before counter stores were seeded on create).
///
/// Setup writes the doc DIRECTLY via `create_with_indexes` (blob + indexes
/// only), deliberately bypassing `init_counter_stores_on_create`, so the store
/// key is absent while the blob value is 5. A PNCounter field is used so the
/// PCounter migrate-via-max can't mask a missing base. An interactive UPDATE +3
/// then commits: the finalize seeds 5 from the committed base (init-if-absent)
/// and applies +3 → 8.
///
/// Without base-capture (forcing `base = None`) the finalize would seed 0 then
/// apply +3 → 3 (verified fail-before / pass-after: 3 vs 8). The assertion of
/// EXACTLY 8 is what makes base-capture load-bearing.
#[tokio::test]
async fn update_seeds_absent_store_from_committed_base_load_bearing() {
    use crate::index_manager::IndexManager;
    use crate::txn::registry::DbTransactionRegistry;
    use query::txn::TransactionRegistry;

    let (db, _bus) = make_test_db_with_bus().await;
    db.create_collection(counter_collection())
        .await
        .expect("schema");

    // SETUP: write a doc with blob `count = 5` directly via create_with_indexes
    // (blob + indexes), WITHOUT seeding the counter accumulation store. Commit
    // via force_commit so the registry finalize never runs for this setup.
    let collection = db
        .get_collection("Counters")
        .expect("get collection")
        .expect("collection exists");
    let index_manager =
        IndexManager::from_collection(collection.resolved_root_id(), collection.schema())
            .expect("index manager");

    let mut setup_doc = Document::from_json_str(r#"{"count": 5}"#).expect("doc");
    setup_doc.set_id(document::DocID::new_v0_from_seed("legacy-counter-doc"));
    let doc_id = setup_doc.id().expect("doc id").to_string();

    let setup_txn = db.new_txn(false).await.expect("write txn");
    let datastore = setup_txn.datastore().expect("datastore");
    let systemstore = setup_txn.systemstore().expect("systemstore");
    let doc_short_id = crate::docid::map::next_doc_short_id(&systemstore)
        .await
        .expect("short id");
    crate::docid::map::set_doc_id_mapping(
        &systemstore,
        collection.resolved_root_id(),
        doc_short_id,
        &doc_id,
    )
    .await
    .expect("doc id mapping");
    collection
        .create_with_indexes(&datastore, &setup_doc, doc_short_id, &index_manager)
        .await
        .expect("direct create");
    drop(datastore);
    drop(systemstore);
    setup_txn.force_commit().await.expect("force commit setup");

    // Confirm the setup left the store ABSENT but the blob value present.
    assert_eq!(
        read_counter_store_opt(&db, "cv1", &doc_id, "count").await,
        None,
        "setup must leave the accumulation store ABSENT (blob-only doc)"
    );
    let committed = read_committed_doc(&db, "Counters", &doc_id)
        .await
        .expect("committed doc present after setup");
    assert_eq!(
        committed.get("count"),
        Some(&document::NormalValue::Int(5)),
        "setup blob must hold the committed counter value 5"
    );

    // Interactive UPDATE +3 on the absent-store doc, commit via the registry.
    let registry = DbTransactionRegistry::new(Arc::clone(&db));
    let handle = registry.begin(false).await.expect("begin");
    let ctx = registry.get(&handle).into_result().unwrap().unwrap();
    let mutator = ctx.doc_mutator().expect("mutator");
    let mut update_doc = Document::from_json_str(r#"{"count": 8}"#).expect("doc");
    update_doc.set_id(document::DocID::from_string(&doc_id).expect("doc id"));
    update_doc.set_counter_delta("count".to_string(), document::NormalValue::Int(3));
    let mut modified = std::collections::HashSet::new();
    modified.insert("count".to_string());
    mutator
        .update("Counters", update_doc, modified)
        .await
        .expect("update");
    drop(mutator);
    drop(ctx);
    registry.commit(&handle).await.expect("commit");

    // base-capture seeds the absent store from the committed base (5), then
    // applies +3 → 8. A missing base would seed 0 and finalize to 3.
    assert_eq!(
        read_counter_store(&db, "cv1", &doc_id, "count").await,
        8,
        "absent-store finalize must seed committed base 5 (init-if-absent) then +3 → 8 (NOT 3)"
    );
}

/// PNCounter create-then-update in ONE registry txn: +3 then -5 in one txn
/// (create at 3, then decrement by 5). The signed accumulation store result is
/// -2 (3 + (-5)), proving the same-txn decrement path stages and finalizes the
/// signed delta against the same-txn-staged base.
#[tokio::test]
async fn explicit_txn_pncounter_create_then_decrement_same_txn() {
    use crate::txn::registry::DbTransactionRegistry;
    use query::txn::TransactionRegistry;

    let (db, _bus) = make_test_db_with_bus().await;
    db.create_collection(counter_collection())
        .await
        .expect("schema");
    let registry = DbTransactionRegistry::new(Arc::clone(&db));

    let handle = registry.begin(false).await.expect("begin");
    let ctx = registry.get(&handle).into_result().unwrap().unwrap();
    let mutator = ctx.doc_mutator().expect("mutator");

    let created = mutator
        .create(
            "Counters",
            Document::from_json_str(r#"{"count": 3}"#).unwrap(),
        )
        .await
        .expect("create");
    let doc_id = created.doc_id.to_string();

    let mut update_doc = Document::from_json_str(r#"{"count": -2}"#).unwrap();
    update_doc.set_id(document::DocID::from_string(&doc_id).unwrap());
    update_doc.set_counter_delta("count".to_string(), document::NormalValue::Int(-5));
    let mut modified = std::collections::HashSet::new();
    modified.insert("count".to_string());
    mutator
        .update("Counters", update_doc, modified)
        .await
        .expect("update");

    drop(mutator);
    drop(ctx);
    registry.commit(&handle).await.expect("commit");

    assert_eq!(
        read_counter_store(&db, "cv1", &doc_id, "count").await,
        -2,
        "PNCounter create(+3)+decrement(-5) in one txn must finalize to exactly -2"
    );
}

/// Discard with pending counter ops: create a counter doc and increment it in
/// one registry txn, then roll back. The accumulation store must have NO value
/// and the doc must be absent — proving discard drops the pending ops and never
/// ran the finalize RMW.
#[tokio::test]
async fn explicit_txn_discard_drops_pending_counter_ops() {
    use crate::txn::registry::DbTransactionRegistry;
    use query::txn::TransactionRegistry;

    let (db, _bus) = make_test_db_with_bus().await;
    db.create_collection(counter_collection())
        .await
        .expect("schema");
    let registry = DbTransactionRegistry::new(Arc::clone(&db));

    let handle = registry.begin(false).await.expect("begin");
    let ctx = registry.get(&handle).into_result().unwrap().unwrap();
    let mutator = ctx.doc_mutator().expect("mutator");

    let created = mutator
        .create(
            "Counters",
            Document::from_json_str(r#"{"count": 7}"#).unwrap(),
        )
        .await
        .expect("create");
    let doc_id = created.doc_id.to_string();

    let mut update_doc = Document::from_json_str(r#"{"count": 9}"#).unwrap();
    update_doc.set_id(document::DocID::from_string(&doc_id).unwrap());
    update_doc.set_counter_delta("count".to_string(), document::NormalValue::Int(2));
    let mut modified = std::collections::HashSet::new();
    modified.insert("count".to_string());
    mutator
        .update("Counters", update_doc, modified)
        .await
        .expect("update");

    drop(mutator);
    drop(ctx);
    registry.rollback(&handle).await.expect("rollback");

    assert_eq!(
        read_counter_store_opt(&db, "cv1", &doc_id, "count").await,
        None,
        "discard must leave NO accumulation store value (finalize RMW never ran)"
    );
    assert!(
        read_committed_doc(&db, "Counters", &doc_id).await.is_none(),
        "discard must leave the doc absent in the committed store"
    );
}

/// Multiple updates to the SAME counter field in ONE registry txn: create at 0,
/// then +3 then +2 in the same txn. The two recorded delta ops both finalize
/// against the SAME doc/field, summing to exactly 5 (each delta applied once).
#[tokio::test]
async fn explicit_txn_multiple_updates_same_field_sum_once() {
    use crate::txn::registry::DbTransactionRegistry;
    use query::txn::TransactionRegistry;

    let (db, _bus) = make_test_db_with_bus().await;
    db.create_collection(counter_collection())
        .await
        .expect("schema");
    let registry = DbTransactionRegistry::new(Arc::clone(&db));

    // Seed a doc at 0 in its own committed txn.
    let handle = registry.begin(false).await.expect("begin");
    let ctx = registry.get(&handle).into_result().unwrap().unwrap();
    let mutator = ctx.doc_mutator().expect("mutator");
    let created = mutator
        .create(
            "Counters",
            Document::from_json_str(r#"{"count": 0}"#).unwrap(),
        )
        .await
        .expect("create");
    let doc_id = created.doc_id.to_string();
    drop(mutator);
    drop(ctx);
    registry.commit(&handle).await.expect("commit create");

    // Two updates to the same field in ONE txn: +3 then +2.
    let handle = registry.begin(false).await.expect("begin");
    let ctx = registry.get(&handle).into_result().unwrap().unwrap();
    let mutator = ctx.doc_mutator().expect("mutator");

    let mut up1 = Document::from_json_str(r#"{"count": 3}"#).unwrap();
    up1.set_id(document::DocID::from_string(&doc_id).unwrap());
    up1.set_counter_delta("count".to_string(), document::NormalValue::Int(3));
    let mut m1 = std::collections::HashSet::new();
    m1.insert("count".to_string());
    mutator.update("Counters", up1, m1).await.expect("update 1");

    let mut up2 = Document::from_json_str(r#"{"count": 5}"#).unwrap();
    up2.set_id(document::DocID::from_string(&doc_id).unwrap());
    up2.set_counter_delta("count".to_string(), document::NormalValue::Int(2));
    let mut m2 = std::collections::HashSet::new();
    m2.insert("count".to_string());
    mutator.update("Counters", up2, m2).await.expect("update 2");

    drop(mutator);
    drop(ctx);
    registry.commit(&handle).await.expect("commit updates");

    assert_eq!(
        read_counter_store(&db, "cv1", &doc_id, "count").await,
        5,
        "two same-field updates (+3,+2) in one txn must sum to exactly 5"
    );
}

/// Counter collection with an @index on the counter field, exercising the
/// finalize blob-correction's `update_with_indexes` index maintenance.
fn indexed_counter_collection() -> CollectionVersion {
    let mut col = CollectionVersion::new(
        "IdxCounters",
        "icv1",
        "col-idx-counters",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "count", FieldKind::int()).with_crdt_type(CType::PnCounter),
        ],
    );
    col.indexes = vec![schema::IndexDescription::new("idx_count").with_field("count", false)];
    col
}

/// Indexed counter: increment in an interactive txn, commit, then assert the
/// index entry materialized at the AUTHORITATIVE post-RMW value (8), proving
/// the finalize blob-correction maintained the index. The unit-test layer has
/// no GraphQL filter-query executor, so this asserts the index entry directly
/// (the value a filter query would resolve against).
#[tokio::test]
async fn explicit_txn_indexed_counter_index_reflects_post_rmw_value() {
    use crate::index_manager::IndexManager;
    use crate::txn::registry::DbTransactionRegistry;
    use query::txn::TransactionRegistry;
    use storage::index::IndexIterator;

    let (db, _bus) = make_test_db_with_bus().await;
    let col_version = indexed_counter_collection();
    db.create_collection(col_version).await.expect("schema");
    let registry = DbTransactionRegistry::new(Arc::clone(&db));

    // Create at 5, commit.
    let handle = registry.begin(false).await.expect("begin");
    let ctx = registry.get(&handle).into_result().unwrap().unwrap();
    let mutator = ctx.doc_mutator().expect("mutator");
    let created = mutator
        .create(
            "IdxCounters",
            Document::from_json_str(r#"{"count": 5}"#).unwrap(),
        )
        .await
        .expect("create");
    let doc_id = created.doc_id.to_string();
    drop(mutator);
    drop(ctx);
    registry.commit(&handle).await.expect("commit create");

    // Increment +3 in an interactive txn, commit.
    let handle = registry.begin(false).await.expect("begin");
    let ctx = registry.get(&handle).into_result().unwrap().unwrap();
    let mutator = ctx.doc_mutator().expect("mutator");
    let mut update_doc = Document::from_json_str(r#"{"count": 8}"#).unwrap();
    update_doc.set_id(document::DocID::from_string(&doc_id).unwrap());
    update_doc.set_counter_delta("count".to_string(), document::NormalValue::Int(3));
    let mut modified = std::collections::HashSet::new();
    modified.insert("count".to_string());
    mutator
        .update("IdxCounters", update_doc, modified)
        .await
        .expect("update");
    drop(mutator);
    drop(ctx);
    registry.commit(&handle).await.expect("commit update");

    // Authoritative store advanced to 8.
    assert_eq!(
        read_counter_store(&db, "icv1", &doc_id, "count").await,
        8,
        "indexed counter store must reflect +3 → 8"
    );

    // The index entry must exist at the post-RMW value 8 (what a filter query
    // `count: {_eq: 8}` would resolve), and must NOT exist at the stale 5.
    let collection = db
        .get_collection("IdxCounters")
        .expect("get collection")
        .expect("collection exists");
    let manager = IndexManager::from_collection(collection.resolved_root_id(), collection.schema())
        .expect("index manager");
    let index = manager.get_index("idx_count").expect("idx_count present");

    let txn = db.new_txn(true).await.expect("read txn");
    let datastore = txn.datastore().expect("datastore");

    let mut iter_8 = index
        .get(&datastore, &[document::NormalValue::Int(8)])
        .await
        .expect("index get 8");
    let entries_8 = iter_8.collect_all().await.expect("collect 8");
    assert_eq!(
        entries_8.len(),
        1,
        "index must have exactly one entry at the post-RMW value 8"
    );

    let mut iter_5 = index
        .get(&datastore, &[document::NormalValue::Int(5)])
        .await
        .expect("index get 5");
    let entries_5 = iter_5.collect_all().await.expect("collect 5");
    assert!(
        entries_5.is_empty(),
        "index must NOT have a stale entry at the pre-update value 5"
    );
}

// finalize-error-rollback and concurrent-finalize-vs-merge are intentionally
// NOT tested at this unit layer: there is no fault-injection seam to force a
// finalize error mid-RMW and no deterministic interleave seam for a concurrent
// merge. The error path is covered by the whole-txn discard semantics
// (`explicit_txn_discard_drops_pending_counter_ops` proves a non-committed txn
// applies no RMW), and the concurrent-finalize-vs-merge guard lifecycle is
// covered by `proofs/tla/InteractiveTxnCounter.tla`.

#[tokio::test]
async fn create_in_tx_publishes_event_on_commit() {
    let (db, bus) = make_test_db_with_bus().await;
    db.create_collection(test_collection())
        .await
        .expect("schema");

    let mut sub = bus.subscribe(&[EventName::Update]);

    let txn = db.new_txn(false).await.expect("new_txn");
    let mutator = DbDocMutator::new(Arc::clone(&db), txn);

    let doc = Document::from_json_str(r#"{"x": 1}"#).expect("doc");
    let result = mutator.create("TestDoc", doc).await.expect("create");

    // Before commit: no event should have fired
    assert!(
        sub.try_recv().is_err(),
        "no event should fire before commit"
    );

    let txn = mutator.take_txn().await.expect("take txn");
    txn.commit().await.expect("commit");

    // After commit: one Update event arrives
    let msg = tokio::time::timeout(std::time::Duration::from_secs(1), sub.recv())
        .await
        .expect("event arrived within timeout")
        .expect("subscription not closed");

    let update = msg.as_update().expect("expected Update message");
    assert_eq!(update.doc_id, result.doc_id.to_string());
    assert_ne!(update.cid, cid::Cid::default(), "cid should be populated");
    assert!(
        !update.block.is_empty(),
        "block bytes should be populated (matches Go's sendUpdate)"
    );
}

#[tokio::test]
async fn auto_commit_create_registers_acp_before_publishing_update() {
    let (db, bus) = make_test_db_with_bus().await;
    db.create_collection(
        test_collection().with_policy(PolicyDescription::new("policy1", "test-docs")),
    )
    .await
    .expect("schema");

    let acp: Arc<dyn DocumentACP> =
        Arc::new(LocalDocumentACP::new(Arc::new(MemoryAcpStore::new())));
    let mutator = crate::AutoCommitMutator::new(db);
    mutator.set_document_acp(acp.clone());
    let mut sub = bus.subscribe(&[EventName::Update]);
    let owner = "did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK";
    let created = defra_core::signing::scope_broadcast_creator_did(
        Some(owner.to_string()),
        mutator.create(
            "TestDoc",
            Document::from_json_str(r#"{"x": 1}"#).expect("doc"),
        ),
    )
    .await
    .expect("create");

    let message = tokio::time::timeout(std::time::Duration::from_secs(1), sub.recv())
        .await
        .expect("event arrived within timeout")
        .expect("subscription not closed");
    assert_eq!(
        message.as_update().expect("update").doc_id,
        created.doc_id.to_string()
    );
    assert!(acp
        .is_doc_registered("policy1", "test-docs", &created.doc_id.to_string())
        .await
        .expect("registration check"));
}

#[tokio::test]
async fn create_in_tx_publishes_no_event_on_discard() {
    let (db, bus) = make_test_db_with_bus().await;
    db.create_collection(test_collection())
        .await
        .expect("schema");

    let mut sub = bus.subscribe(&[EventName::Update]);

    let txn = db.new_txn(false).await.expect("new_txn");
    let mutator = DbDocMutator::new(Arc::clone(&db), txn);

    let doc = Document::from_json_str(r#"{"x": 1}"#).expect("doc");
    mutator.create("TestDoc", doc).await.expect("create");

    let txn = mutator.take_txn().await.expect("take txn");
    txn.discard().expect("discard");

    // Allow a brief window for any (unexpected) async delivery
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert!(sub.try_recv().is_err(), "discard should not publish events");
}

#[tokio::test]
async fn delete_missing_doc_publishes_no_event_and_writes_no_block() {
    // DeleteNode treats existed==false as a no-op; the mutator must not
    // create a tombstone commit or fire an Update event for a missing doc.
    let (db, bus) = make_test_db_with_bus().await;
    db.create_collection(test_collection())
        .await
        .expect("schema");

    let mut sub = bus.subscribe(&[EventName::Update]);

    let txn = db.new_txn(false).await.expect("new_txn");
    let mutator = DbDocMutator::new(Arc::clone(&db), txn);

    let missing_doc_id = document::DocID::new_v0_from_seed("missing-doc");

    let result = mutator
        .delete("TestDoc", &missing_doc_id)
        .await
        .expect("delete should succeed even on missing doc");
    assert!(!result.existed, "doc should not have existed");

    let txn = mutator.take_txn().await.expect("take txn");
    txn.commit().await.expect("commit");

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert!(
        sub.try_recv().is_err(),
        "deleting a non-existent doc should not publish an Update event"
    );
}

/// `TxnBroadcaster` test double: captures every event it's asked to
/// broadcast for inspection.
struct CapturingBroadcaster {
    events: Arc<std::sync::Mutex<Vec<crate::event::emission::TxnBroadcastEvent>>>,
}

#[async_trait::async_trait]
impl crate::event::emission::TxnBroadcaster for CapturingBroadcaster {
    async fn broadcast_update(&self, event: crate::event::emission::TxnBroadcastEvent) {
        self.events.lock().unwrap().push(event);
    }
}

#[tokio::test]
async fn create_in_tx_forwards_to_broadcaster_on_commit() {
    // F1 regression: a tx with a TxnBroadcaster wired in must invoke
    // broadcast_update for each committed mutation so P2P peers see
    // transactional writes (Go: db.sendUpdate → p2p.SendUpdate).
    let (db, _bus) = make_test_db_with_bus().await;
    db.create_collection(test_collection())
        .await
        .expect("schema");

    let captured: Arc<std::sync::Mutex<Vec<crate::event::emission::TxnBroadcastEvent>>> =
        Arc::new(std::sync::Mutex::new(Vec::new()));
    let broadcaster: Arc<dyn crate::event::emission::TxnBroadcaster> =
        Arc::new(CapturingBroadcaster {
            events: Arc::clone(&captured),
        });

    let txn = db.new_txn(false).await.expect("new_txn");
    let txn_arc = Arc::new(TokioMutex::new(Some(txn)));
    let mutator =
        DbDocMutator::from_shared_txn_with_broadcaster(Arc::clone(&db), txn_arc, Some(broadcaster));

    let doc = Document::from_json_str(r#"{"x": 1}"#).expect("doc");
    let result = mutator.create("TestDoc", doc).await.expect("create");

    // Broadcaster must NOT see anything before commit
    assert!(
        captured.lock().unwrap().is_empty(),
        "no broadcast before commit"
    );

    let txn = mutator.take_txn().await.expect("take txn");
    txn.commit().await.expect("commit");

    // Wait briefly for the on_success_async callback to fire
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;

    let events = captured.lock().unwrap().clone();
    assert_eq!(events.len(), 1, "exactly one broadcast after commit");
    let event = &events[0];
    assert_eq!(event.doc_id, result.doc_id.to_string());
    assert_eq!(event.collection_name, "TestDoc");
    assert_ne!(event.doc_cid, cid::Cid::default(), "doc_cid populated");
    assert!(!event.doc_block.is_empty(), "doc_block populated");
    assert_eq!(
        event.document_json.as_ref().and_then(|json| json.get("x")),
        Some(&serde_json::json!(1)),
        "document_json populated for filtered transaction replication"
    );
}

#[tokio::test]
async fn create_in_tx_does_not_broadcast_on_discard() {
    let (db, _bus) = make_test_db_with_bus().await;
    db.create_collection(test_collection())
        .await
        .expect("schema");

    let captured: Arc<std::sync::Mutex<Vec<crate::event::emission::TxnBroadcastEvent>>> =
        Arc::new(std::sync::Mutex::new(Vec::new()));
    let broadcaster: Arc<dyn crate::event::emission::TxnBroadcaster> =
        Arc::new(CapturingBroadcaster {
            events: Arc::clone(&captured),
        });

    let txn = db.new_txn(false).await.expect("new_txn");
    let txn_arc = Arc::new(TokioMutex::new(Some(txn)));
    let mutator =
        DbDocMutator::from_shared_txn_with_broadcaster(Arc::clone(&db), txn_arc, Some(broadcaster));

    let doc = Document::from_json_str(r#"{"x": 1}"#).expect("doc");
    mutator.create("TestDoc", doc).await.expect("create");

    let txn = mutator.take_txn().await.expect("take txn");
    txn.discard().expect("discard");

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert!(
        captured.lock().unwrap().is_empty(),
        "discard should not trigger broadcast"
    );
}
