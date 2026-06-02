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
use storage::backends::MemoryStore;

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

    let guard =
        defra_core::current_identity::scoped_current_identity(Some(OWNER_DID.to_string()));
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
