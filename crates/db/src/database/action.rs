use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use defra_core::{Action, ActionExecution, ActionStatus};
use parking_lot::Mutex;
use storage::corekv::{IterOptions, Key, Store};
use storage::keys::systemstore::{ActionReasonKey, ActionStatusKey};

use crate::error::{Error, Result};

pub fn encode_status(status: ActionStatus) -> Vec<u8> {
    let mut value = status.value();
    let mut encoded = Vec::with_capacity(3);
    while value >= 0x80 {
        encoded.push(value as u8 | 0x80);
        value >>= 7;
    }
    encoded.push(value as u8);
    encoded
}

pub fn decode_status(bytes: &[u8]) -> Option<ActionStatus> {
    let mut value = 0u32;
    for (index, byte) in bytes.iter().copied().enumerate() {
        if index >= 3 {
            return None;
        }
        value |= u32::from(byte & 0x7f) << (index * 7);
        if byte & 0x80 == 0 {
            if index + 1 != bytes.len() {
                return None;
            }
            return u16::try_from(value).ok().map(ActionStatus::new);
        }
    }
    None
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ActionKey {
    collection_id: String,
    action: Action,
    subject: String,
}

#[derive(Debug, Default)]
pub struct ActionRegistry {
    pub active: Mutex<HashSet<ActionKey>>,
}

impl ActionRegistry {
    /// True when no process-local action claim is held.
    pub(crate) fn is_empty(&self) -> bool {
        self.active.lock().is_empty()
    }
}

/// Owns the process-local claim for one collection-wide action.
///
/// Dropping the lease always releases the claim, including when the operation
/// future is cancelled or persisting its terminal state fails. The status in
/// the system store remains the externally visible lifecycle record and may be
/// overwritten by the next execution after a restart or abandoned operation.
pub struct ActionExecutionLease {
    registry: Arc<ActionRegistry>,
    key: ActionKey,
}

impl ActionExecutionLease {
    fn acquire(
        registry: Arc<ActionRegistry>,
        collection_id: &str,
        action: Action,
        subject: &str,
    ) -> Result<Self> {
        let key = ActionKey {
            collection_id: collection_id.to_string(),
            action,
            subject: subject.to_string(),
        };
        if !registry.active.lock().insert(key.clone()) {
            return Err(Error::ActionInProgress {
                collection_id: collection_id.to_string(),
                action: action.value(),
            });
        }
        Ok(Self { registry, key })
    }

    fn collection_id(&self) -> &str {
        &self.key.collection_id
    }

    fn action(&self) -> Action {
        self.key.action
    }

    fn subject(&self) -> &str {
        &self.key.subject
    }
}

impl Drop for ActionExecutionLease {
    fn drop(&mut self) {
        self.registry.active.lock().remove(&self.key);
    }
}

impl<S: Store> crate::database::DB<S> {
    pub async fn register_action(
        &self,
        collection_id: &str,
        action: Action,
    ) -> Result<ActionExecutionLease> {
        self.register_action_with_subject(collection_id, action, "")
            .await
    }

    pub async fn register_action_with_subject(
        &self,
        collection_id: &str,
        action: Action,
        subject: &str,
    ) -> Result<ActionExecutionLease> {
        let txn = self.new_txn(false).await?;
        let systemstore = txn.systemstore()?;
        let lease = self
            .stage_action(&systemstore, collection_id, action, subject)
            .await?;
        drop(systemstore);
        txn.commit().await?;
        self.publish_started_action(&lease);
        Ok(lease)
    }

    pub async fn stage_action(
        &self,
        systemstore: &datastore::NamespaceView,
        collection_id: &str,
        action: Action,
        subject: &str,
    ) -> Result<ActionExecutionLease> {
        let lease = ActionExecutionLease::acquire(
            Arc::clone(&self.active_actions),
            collection_id,
            action,
            subject,
        )?;
        systemstore
            .set(
                &ActionStatusKey::with_subject(collection_id, action, subject).bytes(),
                &encode_status(ActionStatus::IN_PROGRESS),
            )
            .await
            .map_err(Error::Storage)?;
        systemstore
            .delete(&ActionReasonKey::new(collection_id, action, subject).bytes())
            .await
            .map_err(Error::Storage)?;
        Ok(lease)
    }

    pub fn publish_started_action(&self, lease: &ActionExecutionLease) {
        self.publish_action(ActionExecution {
            collection_id: lease.collection_id().to_string(),
            action: lease.action(),
            subject: lease.subject().to_string(),
            status: ActionStatus::IN_PROGRESS,
            ..Default::default()
        });
    }

    pub async fn fail_action(&self, lease: ActionExecutionLease, reason: &str) -> Result<()> {
        let collection_id = lease.collection_id();
        let action = lease.action();
        let subject = lease.subject();
        let txn = self.new_txn(false).await?;
        let systemstore = txn.systemstore()?;
        systemstore
            .set(
                &ActionStatusKey::with_subject(collection_id, action, subject).bytes(),
                &encode_status(ActionStatus::ERRORED),
            )
            .await
            .map_err(Error::Storage)?;
        systemstore
            .set(
                &ActionReasonKey::new(collection_id, action, subject).bytes(),
                reason.as_bytes(),
            )
            .await
            .map_err(Error::Storage)?;
        drop(systemstore);
        txn.commit().await?;

        self.publish_action(ActionExecution {
            collection_id: collection_id.to_string(),
            action,
            subject: subject.to_string(),
            status: ActionStatus::ERRORED,
            reason: reason.to_string(),
        });
        Ok(())
    }

    pub async fn complete_action(&self, lease: ActionExecutionLease) -> Result<()> {
        let collection_id = lease.collection_id();
        let action = lease.action();
        let subject = lease.subject();
        let txn = self.new_txn(false).await?;
        let systemstore = txn.systemstore()?;
        systemstore
            .delete(&ActionStatusKey::with_subject(collection_id, action, subject).bytes())
            .await
            .map_err(Error::Storage)?;
        systemstore
            .delete(&ActionReasonKey::new(collection_id, action, subject).bytes())
            .await
            .map_err(Error::Storage)?;
        drop(systemstore);
        txn.commit().await?;

        self.publish_action(ActionExecution {
            collection_id: collection_id.to_string(),
            action,
            subject: subject.to_string(),
            status: ActionStatus::COMPLETED,
            ..Default::default()
        });
        Ok(())
    }

    pub async fn clear_action(
        &self,
        collection_id: &str,
        action: Action,
        subject: &str,
    ) -> Result<()> {
        let txn = self.new_txn(false).await?;
        let systemstore = txn.systemstore()?;
        systemstore
            .delete(&ActionStatusKey::with_subject(collection_id, action, subject).bytes())
            .await
            .map_err(Error::Storage)?;
        systemstore
            .delete(&ActionReasonKey::new(collection_id, action, subject).bytes())
            .await
            .map_err(Error::Storage)?;
        drop(systemstore);
        txn.commit().await
    }

    /// List actions that are in progress or ended with an error.
    pub async fn list_actions(&self) -> Result<Vec<ActionExecution>> {
        self.check_node_access(None, acp::nac::NodePermission::ActionList)
            .await?;

        let txn = self.new_txn(true).await?;
        let systemstore = txn.systemstore()?;
        let mut iter = systemstore
            .iterator(IterOptions::new().with_prefix(ActionStatusKey::prefix()))
            .await
            .map_err(Error::Storage)?;
        let pairs = iter.collect_all().await.map_err(Error::Storage)?;
        iter.close().await.map_err(Error::Storage)?;

        let mut executions = Vec::with_capacity(pairs.len());
        for pair in pairs {
            let key = ActionStatusKey::parse(&pair.key).ok_or_else(|| {
                Error::Serialization(format!(
                    "invalid action status key: {}",
                    String::from_utf8_lossy(&pair.key)
                ))
            })?;
            let status = decode_status(&pair.value).ok_or_else(|| {
                Error::Serialization(format!(
                    "invalid action status for collection '{}'",
                    key.collection_id
                ))
            })?;
            let reason = systemstore
                .get(&ActionReasonKey::new(&key.collection_id, key.action, &key.subject).bytes())
                .await
                .map_err(Error::Storage)?
                .map(|value| String::from_utf8_lossy(&value).into_owned())
                .unwrap_or_default();
            executions.push(ActionExecution {
                collection_id: key.collection_id,
                action: key.action,
                subject: key.subject,
                status,
                reason,
            });
        }

        drop(systemstore);
        txn.discard()?;
        Ok(executions)
    }

    pub async fn list_index_actions(
        &self,
        collection_id: &str,
    ) -> Result<HashMap<u32, ActionExecution>> {
        self.check_node_access(None, acp::nac::NodePermission::IndexList)
            .await?;

        let txn = self.new_txn(true).await?;
        let systemstore = txn.systemstore()?;
        let executions = index_action_executions(&systemstore, collection_id).await?;
        drop(systemstore);
        txn.discard()?;
        Ok(executions)
    }

    fn publish_action(&self, execution: ActionExecution) {
        if let Some(bus) = self.event_bus() {
            bus.publish(events::Message::action_execution(execution));
        }
    }
}

pub(crate) async fn index_action_statuses(
    systemstore: &datastore::NamespaceView,
    collection_id: &str,
) -> Result<HashMap<u32, ActionStatus>> {
    Ok(index_action_executions(systemstore, collection_id)
        .await?
        .into_iter()
        .map(|(index_id, execution)| (index_id, execution.status))
        .collect())
}

async fn index_action_executions(
    systemstore: &datastore::NamespaceView,
    collection_id: &str,
) -> Result<HashMap<u32, ActionExecution>> {
    let mut iter = systemstore
        .iterator(IterOptions::new().with_prefix(ActionStatusKey::collection_prefix(collection_id)))
        .await
        .map_err(Error::Storage)?;
    let pairs = iter.collect_all().await.map_err(Error::Storage)?;
    iter.close().await.map_err(Error::Storage)?;

    let mut executions = HashMap::new();
    for pair in pairs {
        let Some(key) = ActionStatusKey::parse(&pair.key) else {
            continue;
        };
        if key.action != Action::BACKFILL_INDEX {
            continue;
        }
        let Some(status) = decode_status(&pair.value) else {
            continue;
        };
        let Ok(index_id) = key.subject.parse() else {
            continue;
        };
        let reason = systemstore
            .get(&ActionReasonKey::new(&key.collection_id, key.action, &key.subject).bytes())
            .await
            .map_err(Error::Storage)?
            .map(|value| String::from_utf8_lossy(&value).into_owned())
            .unwrap_or_default();
        executions.insert(
            index_id,
            ActionExecution {
                collection_id: key.collection_id,
                action: key.action,
                subject: key.subject,
                status,
                reason,
            },
        );
    }
    Ok(executions)
}
