//! Tests for Node Access Control (NAC).

use std::sync::Arc;

use acp::{MemoryZanzibarStore, NacStatus, NodeACP, NodePermission};
use identity::Did;

fn test_did() -> Did {
    Did::new("did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK").unwrap()
}

fn test_did2() -> Did {
    Did::new("did:key:z6MkpTHR8VNsBxYAAWHut2Geadd9jSwuBV8xRoAnwWsdvktH").unwrap()
}

#[tokio::test]
async fn test_nac_starts_not_configured() {
    let store = Arc::new(MemoryZanzibarStore::new());
    let nac = NodeACP::new(store);

    assert_eq!(nac.status().await, NacStatus::NotConfigured);
    assert!(nac.owner().await.is_none());
}

#[tokio::test]
async fn test_nac_allows_all_when_not_enabled() {
    let store = Arc::new(MemoryZanzibarStore::new());
    let nac = NodeACP::new(store);

    let identity = test_did();

    for perm in NodePermission::all() {
        let allowed = nac.check_permission(&identity, *perm).await.unwrap();
        assert!(
            allowed,
            "permission {} should be allowed when NAC not enabled",
            perm
        );
    }
}

#[tokio::test]
async fn test_enable_nac() {
    let store = Arc::new(MemoryZanzibarStore::new());
    let nac = NodeACP::new(store);

    let owner = test_did();
    nac.enable(&owner).await.unwrap();

    assert_eq!(nac.status().await, NacStatus::Enabled);
    assert_eq!(nac.owner().await, Some(owner.clone()));
}

#[tokio::test]
async fn test_owner_has_all_permissions() {
    let store = Arc::new(MemoryZanzibarStore::new());
    let nac = NodeACP::new(store);

    let owner = test_did();
    nac.enable(&owner).await.unwrap();

    for perm in NodePermission::all() {
        let allowed = nac.check_permission(&owner, *perm).await.unwrap();
        assert!(allowed, "owner should have permission {}", perm);
    }
}

#[tokio::test]
async fn test_non_owner_denied_when_nac_enabled() {
    let store = Arc::new(MemoryZanzibarStore::new());
    let nac = NodeACP::new(store);

    let owner = test_did();
    let other = test_did2();
    nac.enable(&owner).await.unwrap();

    let allowed = nac
        .check_permission(&other, NodePermission::DacBypass)
        .await
        .unwrap();
    assert!(!allowed);
}

#[tokio::test]
async fn test_disable_and_re_enable() {
    let store = Arc::new(MemoryZanzibarStore::new());
    let nac = NodeACP::new(store);

    let owner = test_did();
    nac.enable(&owner).await.unwrap();

    nac.disable().await.unwrap();
    assert_eq!(nac.status().await, NacStatus::DisabledTemporarily);

    let other = test_did2();
    let allowed = nac
        .check_permission(&other, NodePermission::DacBypass)
        .await
        .unwrap();
    assert!(allowed);

    nac.re_enable().await.unwrap();
    assert_eq!(nac.status().await, NacStatus::Enabled);

    let allowed = nac
        .check_permission(&other, NodePermission::DacBypass)
        .await
        .unwrap();
    assert!(!allowed);
}

#[tokio::test]
async fn test_add_admin() {
    let store = Arc::new(MemoryZanzibarStore::new());
    let nac = NodeACP::new(store);

    let owner = test_did();
    let admin = test_did2();
    nac.enable(&owner).await.unwrap();

    let added = nac.add_admin(&owner, &admin).await.unwrap();
    assert!(added);

    for perm in NodePermission::all() {
        let allowed = nac.check_permission(&admin, *perm).await.unwrap();
        assert!(allowed, "admin should have permission {}", perm);
    }
}

#[tokio::test]
async fn test_remove_admin() {
    let store = Arc::new(MemoryZanzibarStore::new());
    let nac = NodeACP::new(store);

    let owner = test_did();
    let admin = test_did2();
    nac.enable(&owner).await.unwrap();

    nac.add_admin(&owner, &admin).await.unwrap();
    let removed = nac.remove_admin(&owner, &admin).await.unwrap();
    assert!(removed);

    let allowed = nac
        .check_permission(&admin, NodePermission::DacBypass)
        .await
        .unwrap();
    assert!(!allowed);
}

#[tokio::test]
async fn test_cannot_remove_owner() {
    let store = Arc::new(MemoryZanzibarStore::new());
    let nac = NodeACP::new(store);

    let owner = test_did();
    nac.enable(&owner).await.unwrap();

    let result = nac.remove_admin(&owner, &owner).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_purge_nac() {
    let store = Arc::new(MemoryZanzibarStore::new());
    let nac = NodeACP::new(store);

    let owner = test_did();
    nac.enable(&owner).await.unwrap();

    nac.purge().await.unwrap();

    assert_eq!(nac.status().await, NacStatus::NotConfigured);
    assert!(nac.owner().await.is_none());
}

#[tokio::test]
async fn test_load_existing_state() {
    let store = Arc::new(MemoryZanzibarStore::new());

    {
        let nac = NodeACP::new(store.clone());
        let owner = test_did();
        nac.enable(&owner).await.unwrap();
    }

    {
        let nac = NodeACP::new(store);
        nac.load().await.unwrap();

        assert_eq!(nac.status().await, NacStatus::Enabled);
        assert_eq!(nac.owner().await, Some(test_did()));
    }
}

#[tokio::test]
async fn test_write_operations_blocked_when_disabled() {
    let store = Arc::new(MemoryZanzibarStore::new());
    let nac = NodeACP::new(store);

    let owner = test_did();
    let other = test_did2();
    nac.enable(&owner).await.unwrap();

    nac.disable().await.unwrap();
    assert_eq!(nac.status().await, NacStatus::DisabledTemporarily);

    let result = nac.add_admin(&owner, &other).await;
    assert!(
        result.is_err(),
        "add_admin should be blocked when NAC is disabled"
    );

    let result = nac.remove_admin(&owner, &other).await;
    assert!(
        result.is_err(),
        "remove_admin should be blocked when NAC is disabled"
    );

    let result = nac
        .add_permission_grant(&owner, &other, NodePermission::DacBypass)
        .await;
    assert!(
        result.is_err(),
        "add_permission_grant should be blocked when NAC is disabled"
    );

    let result = nac
        .remove_permission_grant(&owner, &other, NodePermission::DacBypass)
        .await;
    assert!(
        result.is_err(),
        "remove_permission_grant should be blocked when NAC is disabled"
    );

    nac.re_enable().await.unwrap();

    let result = nac.add_admin(&owner, &other).await;
    assert!(result.is_ok(), "add_admin should work when NAC is enabled");
}

#[tokio::test]
async fn test_wildcard_admin_grants_all_permissions() {
    let store = Arc::new(MemoryZanzibarStore::new());
    let nac = NodeACP::new(store);

    let owner = test_did();
    let other = test_did2();
    let wildcard = Did::wildcard();

    nac.enable(&owner).await.unwrap();

    let added = nac.add_admin(&owner, &wildcard).await.unwrap();
    assert!(added, "should successfully add wildcard admin");

    let has_perm = nac
        .check_permission(&other, NodePermission::NacStatus)
        .await
        .unwrap();
    assert!(
        has_perm,
        "any identity should have NacStatus after wildcard admin grant"
    );

    let has_perm = nac
        .check_permission(&other, NodePermission::CollectionGet)
        .await
        .unwrap();
    assert!(
        has_perm,
        "any identity should have CollectionGet after wildcard admin grant"
    );
}
