#[cfg(test)]
mod tests {
    use acp::nac::{NacStatus, NodePermission};
    use identity::Did;

    use super::super::factory::create_memory_nac_manager;
    use super::super::NacConfig;

    fn test_did() -> Did {
        Did::new("did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK").unwrap()
    }

    fn test_did2() -> Did {
        Did::new("did:key:z6MkpTHR8VNsBxYAAWHut2Geadd9jSwuBV8xRoAnwWsdvktH").unwrap()
    }

    #[tokio::test]
    async fn test_nac_manager_disabled_by_default() {
        let manager = create_memory_nac_manager(NacConfig::default());

        assert!(!manager.is_enabled().await);
        assert_eq!(manager.status().await, NacStatus::NotConfigured);

        let identity = test_did();
        let allowed = manager
            .check_permission(&identity, NodePermission::DacBypass)
            .await
            .unwrap();
        assert!(allowed);
    }

    #[tokio::test]
    async fn test_nac_manager_enable() {
        let manager = create_memory_nac_manager(NacConfig::new().with_enabled());

        let owner = test_did();
        manager.initialize(Some(&owner)).await.unwrap();

        assert!(manager.is_enabled().await);
        assert_eq!(manager.status().await, NacStatus::Enabled);
        assert!(manager.is_owner(&owner).await);
    }

    #[tokio::test]
    async fn test_nac_manager_permission_check() {
        let manager = create_memory_nac_manager(NacConfig::new().with_enabled());

        let owner = test_did();
        let other = test_did2();
        manager.initialize(Some(&owner)).await.unwrap();

        assert!(manager
            .check_permission(&owner, NodePermission::DacBypass)
            .await
            .unwrap());

        assert!(!manager
            .check_permission(&other, NodePermission::DacBypass)
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn test_nac_manager_admin() {
        let manager = create_memory_nac_manager(NacConfig::new().with_enabled());

        let owner = test_did();
        let admin = test_did2();
        manager.initialize(Some(&owner)).await.unwrap();

        let added = manager.add_admin(&owner, &admin).await.unwrap();
        assert!(added);

        assert!(manager
            .check_permission(&admin, NodePermission::DacBypass)
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn test_nac_manager_disable_reenable() {
        let manager = create_memory_nac_manager(NacConfig::new().with_enabled());

        let owner = test_did();
        let other = test_did2();
        manager.initialize(Some(&owner)).await.unwrap();

        manager.disable(&owner).await.unwrap();
        assert_eq!(manager.status().await, NacStatus::DisabledTemporarily);

        assert!(manager
            .check_permission(&other, NodePermission::DacBypass)
            .await
            .unwrap());

        manager.re_enable(&owner).await.unwrap();
        assert_eq!(manager.status().await, NacStatus::Enabled);

        assert!(!manager
            .check_permission(&other, NodePermission::DacBypass)
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn test_nac_manager_purge_requires_dev_mode() {
        let manager = create_memory_nac_manager(NacConfig::new().with_enabled());

        let owner = test_did();
        manager.initialize(Some(&owner)).await.unwrap();

        let result = manager.purge(&owner).await;
        assert!(result.is_err());

        let manager = create_memory_nac_manager(NacConfig::new().with_enabled().with_dev_mode());
        manager.initialize(Some(&owner)).await.unwrap();

        let result = manager.purge(&owner).await;
        assert!(result.is_ok());
        assert_eq!(manager.status().await, NacStatus::NotConfigured);
    }

    #[tokio::test]
    async fn test_nac_manager_info() {
        let manager = create_memory_nac_manager(NacConfig::new().with_enabled().with_dev_mode());

        let owner = test_did();
        manager.initialize(Some(&owner)).await.unwrap();

        let info = manager.info().await;
        assert_eq!(info.status, "enabled");
        assert!(info.configured_enabled);
        assert!(info.dev_mode);
        assert!(info.owner.is_some());
    }
}
