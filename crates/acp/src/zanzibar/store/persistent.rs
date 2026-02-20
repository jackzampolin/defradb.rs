//! Persistent Zanzibar store backed by any Store implementation.

use async_trait::async_trait;
use std::sync::Arc;

use storage::corekv::{IterOptions, Reader, Store, Writer};
use storage::namespace::{Namespace, NamespacedStore};
#[cfg(not(target_arch = "wasm32"))]
use storage::RedbStore;

use zanzibar::did::Did;
use zanzibar::error::{Error, Result};
use zanzibar::store::ZanzibarStore;
use zanzibar::types::{ObjectRef, Policy, Relationship, Subject};

/// Persistent Zanzibar store backed by any Store implementation.
pub struct PersistentZanzibarStore<S: Store> {
    store: NamespacedStore<S>,
}

impl<S: Store> PersistentZanzibarStore<S> {
    /// Create from an existing Store with ACP namespace isolation.
    pub fn from_store(store: Arc<S>) -> Self {
        Self {
            store: NamespacedStore::new(store, Namespace::Acpstore),
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl PersistentZanzibarStore<RedbStore> {
    /// Open a persistent store at the given path.
    pub fn open(path: &std::path::Path) -> Result<Self> {
        let store = RedbStore::open(path).map_err(|e| Error::Serialization(e.to_string()))?;
        Ok(Self::from_store(Arc::new(store)))
    }
}

impl<S: Store> PersistentZanzibarStore<S> {
    fn policy_key(policy_id: &str) -> String {
        format!("/zanzibar/policy/{}", policy_id)
    }

    fn relationship_key(policy_id: &str, rel: &Relationship) -> String {
        format!("/zanzibar/{}{}", policy_id, rel.storage_key())
    }

    fn relationship_prefix(policy_id: &str, resource: &str, object_id: &str) -> String {
        format!(
            "/zanzibar/{}{}",
            policy_id,
            Relationship::object_prefix(resource, object_id)
        )
    }

    fn relation_prefix(policy_id: &str, resource: &str, object_id: &str, relation: &str) -> String {
        format!(
            "/zanzibar/{}{}",
            policy_id,
            Relationship::relation_prefix(resource, object_id, relation)
        )
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<S: Store + Send + Sync> ZanzibarStore for PersistentZanzibarStore<S> {
    async fn store_policy(&self, policy: &Policy) -> Result<()> {
        let mut txn = self.store.new_txn(false).await.map_err(|e| {
            Error::Serialization(format!("store_policy: create transaction: {}", e))
        })?;

        let key = Self::policy_key(&policy.id);
        let value = serde_json::to_vec(policy)?;

        txn.set(key.as_bytes(), &value)
            .await
            .map_err(|e| Error::Serialization(format!("store_policy: set key {}: {}", key, e)))?;

        txn.commit()
            .await
            .map_err(|e| Error::Serialization(format!("store_policy: commit: {}", e)))?;

        Ok(())
    }

    async fn get_policy(&self, policy_id: &str) -> Result<Option<Policy>> {
        let txn =
            self.store.new_txn(true).await.map_err(|e| {
                Error::Serialization(format!("get_policy: create transaction: {}", e))
            })?;

        let key = Self::policy_key(policy_id);

        match txn
            .get(key.as_bytes())
            .await
            .map_err(|e| Error::Serialization(format!("get_policy: get key {}: {}", key, e)))?
        {
            Some(data) => {
                let policy: Policy = serde_json::from_slice(&data)?;
                Ok(Some(policy))
            }
            None => Ok(None),
        }
    }

    async fn list_policies(&self) -> Result<Vec<Policy>> {
        let txn = self.store.new_txn(true).await.map_err(|e| {
            Error::Serialization(format!("list_policies: create transaction: {}", e))
        })?;

        let prefix = "/zanzibar/policy/";
        let iter_opts = IterOptions::new().with_prefix(prefix.as_bytes().to_vec());

        let mut iter = txn
            .iterator(iter_opts)
            .await
            .map_err(|e| Error::Serialization(format!("list_policies: create iterator: {}", e)))?;

        let mut policies = Vec::new();
        while let Some(kv) = iter
            .next()
            .await
            .map_err(|e| Error::Serialization(format!("list_policies: iterate: {}", e)))?
        {
            let policy: Policy = serde_json::from_slice(&kv.value)?;
            policies.push(policy);
        }

        Ok(policies)
    }

    async fn delete_policy(&self, policy_id: &str) -> Result<bool> {
        let mut txn = self
            .store
            .new_txn(false)
            .await
            .map_err(|e| Error::Serialization(e.to_string()))?;

        let key = Self::policy_key(policy_id);

        let exists = txn
            .has(key.as_bytes())
            .await
            .map_err(|e| Error::Serialization(e.to_string()))?;

        if exists {
            txn.delete(key.as_bytes())
                .await
                .map_err(|e| Error::Serialization(e.to_string()))?;

            let rel_prefix = format!("/zanzibar/{}/rel/", policy_id);
            let iter_opts = IterOptions::new().with_prefix(rel_prefix.into_bytes());

            let mut keys_to_delete = Vec::new();
            {
                let mut iter = txn
                    .iterator(iter_opts)
                    .await
                    .map_err(|e| Error::Serialization(e.to_string()))?;

                while let Some(kv) = iter
                    .next()
                    .await
                    .map_err(|e| Error::Serialization(e.to_string()))?
                {
                    keys_to_delete.push(kv.key);
                }
            }

            for key in keys_to_delete {
                txn.delete(&key)
                    .await
                    .map_err(|e| Error::Serialization(e.to_string()))?;
            }
        }

        txn.commit()
            .await
            .map_err(|e| Error::Serialization(e.to_string()))?;

        Ok(exists)
    }

    async fn store_relationship(&self, policy_id: &str, rel: &Relationship) -> Result<()> {
        let mut txn = self
            .store
            .new_txn(false)
            .await
            .map_err(|e| Error::Serialization(e.to_string()))?;

        let key = Self::relationship_key(policy_id, rel);
        let value = serde_json::to_vec(rel)?;

        txn.set(key.as_bytes(), &value)
            .await
            .map_err(|e| Error::Serialization(e.to_string()))?;

        txn.commit()
            .await
            .map_err(|e| Error::Serialization(e.to_string()))?;

        Ok(())
    }

    async fn delete_relationship(&self, policy_id: &str, rel: &Relationship) -> Result<bool> {
        let mut txn = self
            .store
            .new_txn(false)
            .await
            .map_err(|e| Error::Serialization(e.to_string()))?;

        let key = Self::relationship_key(policy_id, rel);

        let exists = txn
            .has(key.as_bytes())
            .await
            .map_err(|e| Error::Serialization(e.to_string()))?;

        if exists {
            txn.delete(key.as_bytes())
                .await
                .map_err(|e| Error::Serialization(e.to_string()))?;
        }

        txn.commit()
            .await
            .map_err(|e| Error::Serialization(e.to_string()))?;

        Ok(exists)
    }

    async fn has_relationship(
        &self,
        policy_id: &str,
        resource: &str,
        object_id: &str,
        relation: &str,
        subject: &Subject,
    ) -> Result<bool> {
        let txn = self
            .store
            .new_txn(true)
            .await
            .map_err(|e| Error::Serialization(e.to_string()))?;

        let rel = Relationship::new(resource, object_id, relation, subject.clone());
        let key = Self::relationship_key(policy_id, &rel);

        txn.has(key.as_bytes())
            .await
            .map_err(|e| Error::Serialization(e.to_string()))
    }

    async fn check_permission_direct(
        &self,
        policy_id: &str,
        resource: &str,
        object_id: &str,
        relation: &str,
        subject: &Did,
    ) -> Result<bool> {
        let txn = self
            .store
            .new_txn(true)
            .await
            .map_err(|e| Error::Serialization(e.to_string()))?;

        let direct = Relationship::with_entity(resource, object_id, relation, subject.clone());
        let direct_key = Self::relationship_key(policy_id, &direct);

        if txn
            .has(direct_key.as_bytes())
            .await
            .map_err(|e| Error::Serialization(e.to_string()))?
        {
            return Ok(true);
        }

        let wildcard = Relationship::new(resource, object_id, relation, Subject::Wildcard);
        let wildcard_key = Self::relationship_key(policy_id, &wildcard);

        if txn
            .has(wildcard_key.as_bytes())
            .await
            .map_err(|e| Error::Serialization(e.to_string()))?
        {
            return Ok(true);
        }

        let prefix = Self::relation_prefix(policy_id, resource, object_id, relation);
        let iter_opts = IterOptions::new().with_prefix(prefix.into_bytes());

        let mut iter = txn
            .iterator(iter_opts)
            .await
            .map_err(|e| Error::Serialization(e.to_string()))?;

        while let Some(kv) = iter
            .next()
            .await
            .map_err(|e| Error::Serialization(e.to_string()))?
        {
            let rel: Relationship = serde_json::from_slice(&kv.value)?;
            if rel.subject.is_typed_wildcard() {
                return Ok(true);
            }
        }

        Ok(false)
    }

    async fn get_relation_subjects(
        &self,
        policy_id: &str,
        resource: &str,
        object_id: &str,
        relation: &str,
    ) -> Result<Vec<Subject>> {
        let txn = self
            .store
            .new_txn(true)
            .await
            .map_err(|e| Error::Serialization(e.to_string()))?;

        let prefix = Self::relation_prefix(policy_id, resource, object_id, relation);
        let iter_opts = IterOptions::new().with_prefix(prefix.into_bytes());

        let mut iter = txn
            .iterator(iter_opts)
            .await
            .map_err(|e| Error::Serialization(e.to_string()))?;

        let mut subjects = Vec::new();

        while let Some(kv) = iter
            .next()
            .await
            .map_err(|e| Error::Serialization(e.to_string()))?
        {
            let rel: Relationship = serde_json::from_slice(&kv.value)?;
            subjects.push(rel.subject);
        }

        Ok(subjects)
    }

    async fn get_relation_targets(
        &self,
        policy_id: &str,
        resource: &str,
        object_id: &str,
        relation: &str,
    ) -> Result<Vec<ObjectRef>> {
        let subjects = self
            .get_relation_subjects(policy_id, resource, object_id, relation)
            .await?;

        let targets: Vec<_> = subjects
            .into_iter()
            .filter_map(|s| match s {
                Subject::EntitySet {
                    resource,
                    object_id,
                    ..
                } => Some(ObjectRef::new(resource, object_id)),
                _ => None,
            })
            .collect();

        Ok(targets)
    }

    async fn delete_object_relationships(
        &self,
        policy_id: &str,
        resource: &str,
        object_id: &str,
    ) -> Result<()> {
        let mut txn = self
            .store
            .new_txn(false)
            .await
            .map_err(|e| Error::Serialization(e.to_string()))?;

        let prefix = Self::relationship_prefix(policy_id, resource, object_id);
        let iter_opts = IterOptions::new().with_prefix(prefix.clone().into_bytes());

        let mut keys_to_delete = Vec::new();
        {
            let mut iter = txn
                .iterator(iter_opts)
                .await
                .map_err(|e| Error::Serialization(e.to_string()))?;

            while let Some(kv) = iter
                .next()
                .await
                .map_err(|e| Error::Serialization(e.to_string()))?
            {
                keys_to_delete.push(kv.key);
            }
        }

        for key in keys_to_delete {
            txn.delete(&key)
                .await
                .map_err(|e| Error::Serialization(e.to_string()))?;
        }

        txn.commit()
            .await
            .map_err(|e| Error::Serialization(e.to_string()))?;

        Ok(())
    }
}
