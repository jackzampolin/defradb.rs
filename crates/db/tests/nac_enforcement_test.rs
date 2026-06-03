//! End-to-end proof that `DB::check_node_access` actually enforces NAC.
//!
//! Installs a real `NacManager` over a memory store, enables it, and
//! asserts both the allow path (owner, via explicit param and via the
//! ambient thread-local) and the deny path (anonymous / non-owner).

use std::sync::Arc;

use acp::nac::NodePermission;
use acp::MemoryZanzibarStore;
use db::{NacConfig, NacManager, DB};
use identity::Did;
use schema::{CollectionVersion, FieldDescription, FieldKind};
use storage::backends::MemoryStore;
use tokio::sync::Mutex;

/// Tests that drive the ambient thread-local identity must not run concurrently:
/// `scoped_current_identity` mutates process-global state, and parallel mutation
/// would let one test observe another's identity. A tokio mutex is used (held
/// across `.await`) to avoid `await_holding_lock`.
static AMBIENT_GUARD: Mutex<()> = Mutex::const_new(());

const OWNER_DID: &str = "did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK";
const STRANGER_DID: &str = "did:key:z6MkfXG2FkNy3u7Eg3jm8e2YQpGz7Z1JqWgHDAP1hLk9r2bR";

fn owner() -> Did {
    Did::new(OWNER_DID).unwrap()
}

fn stranger() -> Did {
    Did::new(STRANGER_DID).unwrap()
}

async fn enabled_manager() -> Arc<NacManager<MemoryZanzibarStore>> {
    let store = Arc::new(MemoryZanzibarStore::new());
    let config = NacConfig::new().with_enabled().with_dev_mode();
    let manager = Arc::new(NacManager::new(store, config));
    manager.initialize(Some(&owner())).await.unwrap();
    assert!(manager.is_enabled().await, "NAC should be enabled");
    manager
}

#[tokio::test]
async fn unset_manager_allows_everything() {
    let db = DB::new(MemoryStore::new()).unwrap();
    // No nac_manager installed: every node operation is permitted.
    db.check_node_access(None, NodePermission::DocumentUpdate)
        .await
        .expect("unset NAC manager must allow all operations");
}

#[tokio::test]
async fn owner_allowed_via_explicit_param() {
    let db = DB::new(MemoryStore::new()).unwrap();
    db.set_nac_manager(enabled_manager().await);

    db.check_node_access(Some(&owner()), NodePermission::DocumentUpdate)
        .await
        .expect("owner must be allowed when passed explicitly");
}

#[tokio::test]
async fn owner_allowed_via_ambient_identity() {
    let db = DB::new(MemoryStore::new()).unwrap();
    db.set_nac_manager(enabled_manager().await);

    let guard = defra_core::current_identity::scoped_current_identity(Some(OWNER_DID.to_string()));
    db.check_node_access(None, NodePermission::DocumentUpdate)
        .await
        .expect("owner must be allowed via ambient thread-local identity");
    drop(guard);

    // After the guard drops, the ambient identity is cleared again.
    assert_eq!(defra_core::current_identity::get_current_identity(), None);
}

#[tokio::test]
async fn anonymous_is_denied() {
    let db = DB::new(MemoryStore::new()).unwrap();
    db.set_nac_manager(enabled_manager().await);

    // No explicit identity and no ambient identity => wildcard, which is
    // not the owner and holds no grants.
    assert_eq!(defra_core::current_identity::get_current_identity(), None);
    let err = db
        .check_node_access(None, NodePermission::DocumentUpdate)
        .await
        .expect_err("anonymous caller must be denied");
    assert!(
        matches!(err, db::error::Error::NotAuthorized { .. }),
        "expected NotAuthorized, got: {err:?}"
    );
}

#[tokio::test]
async fn non_owner_is_denied() {
    let db = DB::new(MemoryStore::new()).unwrap();
    db.set_nac_manager(enabled_manager().await);

    let err = db
        .check_node_access(Some(&stranger()), NodePermission::DocumentUpdate)
        .await
        .expect_err("non-owner caller must be denied");
    assert!(
        matches!(err, db::error::Error::NotAuthorized { .. }),
        "expected NotAuthorized, got: {err:?}"
    );
}

/// Minimal valid collection schema for raw `create_collection` gating tests.
fn widget_schema() -> CollectionVersion {
    CollectionVersion::new(
        "Widget",
        "v-widget-1",
        "col-widget",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "name", FieldKind::string()),
        ],
    )
}

// =============================================================================
// Raw DB schema-mutation gate (`create_collection`).
//
// These prove `DB::create_collection` calls `check_node_access` directly,
// independent of the GraphQL/HTTP surface. The DB has NAC enabled but NO node
// identity configured, so the node-identity bypass cannot mask the check — the
// only thing that authorizes is the ambient (or owner) identity.
// =============================================================================

#[tokio::test]
async fn create_collection_denied_for_non_owner() {
    let _serial = AMBIENT_GUARD.lock().await;

    let db = DB::new(MemoryStore::new()).unwrap();
    db.set_nac_manager(enabled_manager().await);

    let guard =
        defra_core::current_identity::scoped_current_identity(Some(STRANGER_DID.to_string()));
    let err = db
        .create_collection(widget_schema())
        .await
        .expect_err("non-owner must be denied raw create_collection");
    drop(guard);

    assert!(
        matches!(err, db::error::Error::NotAuthorized { .. }),
        "expected NotAuthorized from create_collection, got: {err:?}"
    );
    assert!(
        db.get_collection("Widget").unwrap().is_none(),
        "denied create_collection must not persist the collection"
    );
}

#[tokio::test]
async fn create_collection_allowed_for_owner() {
    let _serial = AMBIENT_GUARD.lock().await;

    let db = DB::new(MemoryStore::new()).unwrap();
    db.set_nac_manager(enabled_manager().await);

    let guard = defra_core::current_identity::scoped_current_identity(Some(OWNER_DID.to_string()));
    db.create_collection(widget_schema())
        .await
        .expect("owner must be allowed to create_collection");
    drop(guard);

    assert!(
        db.get_collection("Widget").unwrap().is_some(),
        "owner create_collection must persist the collection"
    );
}

#[tokio::test]
async fn create_collection_allowed_when_nac_unset() {
    // Guards against over-gating: with no NAC manager installed,
    // `check_node_access` is a no-op and raw schema mutation must succeed
    // regardless of ambient identity.
    let _serial = AMBIENT_GUARD.lock().await;

    let db = DB::new(MemoryStore::new()).unwrap();

    let guard =
        defra_core::current_identity::scoped_current_identity(Some(STRANGER_DID.to_string()));
    db.create_collection(widget_schema())
        .await
        .expect("create_collection must succeed when NAC is unset (no-op gate)");
    drop(guard);

    assert!(
        db.get_collection("Widget").unwrap().is_some(),
        "create_collection with NAC unset must persist the collection"
    );
}

#[tokio::test]
async fn delete_collection_gated_by_nac() {
    // Proves the raw `delete_collection` honors `check_node_access`:
    // a non-owner is denied (and the collection survives), the owner succeeds.
    let _serial = AMBIENT_GUARD.lock().await;

    let db = DB::new(MemoryStore::new()).unwrap();
    db.set_nac_manager(enabled_manager().await);

    // Seed: owner creates the collection so there is something to delete.
    {
        let guard =
            defra_core::current_identity::scoped_current_identity(Some(OWNER_DID.to_string()));
        db.create_collection(widget_schema())
            .await
            .expect("owner must be allowed to create_collection");
        drop(guard);
    }
    assert!(
        db.get_collection("Widget").unwrap().is_some(),
        "collection must exist before delete test"
    );

    // Non-owner: denied, collection survives.
    {
        let guard =
            defra_core::current_identity::scoped_current_identity(Some(STRANGER_DID.to_string()));
        let err = db
            .delete_collection("Widget")
            .await
            .expect_err("non-owner must be denied raw delete_collection");
        drop(guard);

        assert!(
            matches!(err, db::error::Error::NotAuthorized { .. }),
            "expected NotAuthorized from delete_collection, got: {err:?}"
        );
    }
    assert!(
        db.get_collection("Widget").unwrap().is_some(),
        "denied delete_collection must not remove the collection"
    );

    // Owner: delete succeeds.
    {
        let guard =
            defra_core::current_identity::scoped_current_identity(Some(OWNER_DID.to_string()));
        db.delete_collection("Widget")
            .await
            .expect("owner must be allowed to delete_collection");
        drop(guard);
    }
    assert!(
        db.get_collection("Widget").unwrap().is_none(),
        "owner delete_collection must remove the collection"
    );
}

#[tokio::test]
async fn set_active_collection_version_denied_for_non_owner() {
    // The other raw schema-mutation method is also gated.
    let _serial = AMBIENT_GUARD.lock().await;

    let db = DB::new(MemoryStore::new()).unwrap();
    db.set_nac_manager(enabled_manager().await);

    let guard =
        defra_core::current_identity::scoped_current_identity(Some(STRANGER_DID.to_string()));
    let err = db
        .set_active_collection_version("v-widget-1")
        .await
        .expect_err("non-owner must be denied set_active_collection_version");
    drop(guard);

    assert!(
        matches!(err, db::error::Error::NotAuthorized { .. }),
        "expected NotAuthorized from set_active_collection_version, got: {err:?}"
    );
}
