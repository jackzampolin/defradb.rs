use defra_core::{Action, ActionExecution, ActionStatus};
use storage::corekv::{IterOptions, Key, Store};
use storage::keys::systemstore::{ActionReasonKey, ActionStatusKey};

use crate::error::{Error, Result};

fn encode_status(status: ActionStatus) -> Vec<u8> {
    let mut value = status.value();
    let mut encoded = Vec::with_capacity(3);
    while value >= 0x80 {
        encoded.push(value as u8 | 0x80);
        value >>= 7;
    }
    encoded.push(value as u8);
    encoded
}

fn decode_status(bytes: &[u8]) -> Option<ActionStatus> {
    let mut value = 0u32;
    for (index, byte) in bytes.iter().copied().enumerate() {
        if index >= 3 {
            return None;
        }
        value |= u32::from(byte & 0x7f) << (index * 7);
        if byte & 0x80 == 0 {
            return u16::try_from(value).ok().map(ActionStatus::new);
        }
    }
    None
}

impl<S: Store> crate::database::DB<S> {
    pub(crate) async fn register_action(&self, collection_id: &str, action: Action) -> Result<()> {
        let txn = self.new_txn(false).await?;
        let systemstore = txn.systemstore()?;
        let status_key = ActionStatusKey::new(collection_id, action).bytes();

        let already_in_progress = systemstore
            .get(&status_key)
            .await
            .map_err(Error::Storage)?
            .and_then(|status| decode_status(&status))
            == Some(ActionStatus::IN_PROGRESS);
        if already_in_progress {
            drop(systemstore);
            txn.discard()?;
            return Err(Error::ActionInProgress {
                collection_id: collection_id.to_string(),
                action: action.value(),
            });
        }

        systemstore
            .set(&status_key, &encode_status(ActionStatus::IN_PROGRESS))
            .await
            .map_err(Error::Storage)?;
        systemstore
            .delete(&ActionReasonKey::new(collection_id, action, "").bytes())
            .await
            .map_err(Error::Storage)?;
        drop(systemstore);
        txn.commit().await?;

        self.publish_action(ActionExecution {
            collection_id: collection_id.to_string(),
            action,
            status: ActionStatus::IN_PROGRESS,
            ..Default::default()
        });
        Ok(())
    }

    pub(crate) async fn fail_action(
        &self,
        collection_id: &str,
        action: Action,
        reason: &str,
    ) -> Result<()> {
        let txn = self.new_txn(false).await?;
        let systemstore = txn.systemstore()?;
        systemstore
            .set(
                &ActionStatusKey::new(collection_id, action).bytes(),
                &encode_status(ActionStatus::ERRORED),
            )
            .await
            .map_err(Error::Storage)?;
        systemstore
            .set(
                &ActionReasonKey::new(collection_id, action, "").bytes(),
                reason.as_bytes(),
            )
            .await
            .map_err(Error::Storage)?;
        drop(systemstore);
        txn.commit().await?;

        self.publish_action(ActionExecution {
            collection_id: collection_id.to_string(),
            action,
            status: ActionStatus::ERRORED,
            reason: reason.to_string(),
            ..Default::default()
        });
        Ok(())
    }

    pub(crate) async fn complete_action(&self, collection_id: &str, action: Action) -> Result<()> {
        let txn = self.new_txn(false).await?;
        let systemstore = txn.systemstore()?;
        systemstore
            .delete(&ActionStatusKey::new(collection_id, action).bytes())
            .await
            .map_err(Error::Storage)?;
        systemstore
            .delete(&ActionReasonKey::new(collection_id, action, "").bytes())
            .await
            .map_err(Error::Storage)?;
        drop(systemstore);
        txn.commit().await?;

        self.publish_action(ActionExecution {
            collection_id: collection_id.to_string(),
            action,
            status: ActionStatus::COMPLETED,
            ..Default::default()
        });
        Ok(())
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

    fn publish_action(&self, execution: ActionExecution) {
        if let Some(bus) = self.event_bus() {
            bus.publish(events::Message::action_execution(execution));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use events::{Bus, ChannelBus, EventName};
    use storage::backends::MemoryStore;

    #[tokio::test]
    async fn action_lifecycle_retains_only_incomplete_executions() {
        let bus: std::sync::Arc<dyn Bus> = std::sync::Arc::new(ChannelBus::new());
        let mut db = crate::DB::new(MemoryStore::new()).unwrap();
        db.set_event_bus(std::sync::Arc::clone(&bus));
        let mut events = bus.subscribe(&[EventName::ActionExecution]);

        db.register_action("collection", Action::TRUNCATE)
            .await
            .unwrap();
        let actions = db.list_actions().await.unwrap();
        assert_eq!(actions.len(), 1);
        assert_eq!(actions[0].status, ActionStatus::IN_PROGRESS);
        assert_eq!(
            events
                .try_recv()
                .unwrap()
                .as_action_execution()
                .unwrap()
                .status,
            ActionStatus::IN_PROGRESS
        );

        assert!(matches!(
            db.register_action("collection", Action::TRUNCATE).await,
            Err(Error::ActionInProgress { .. })
        ));
        assert!(events.try_recv().is_err());

        db.fail_action("collection", Action::TRUNCATE, "failed")
            .await
            .unwrap();
        let actions = db.list_actions().await.unwrap();
        assert_eq!(actions[0].status, ActionStatus::ERRORED);
        assert_eq!(actions[0].reason, "failed");
        let event = events.try_recv().unwrap();
        let execution = event.as_action_execution().unwrap();
        assert_eq!(execution.status, ActionStatus::ERRORED);
        assert_eq!(execution.reason, "failed");

        db.register_action("collection", Action::TRUNCATE)
            .await
            .unwrap();
        assert!(db.list_actions().await.unwrap()[0].reason.is_empty());
        assert_eq!(
            events
                .try_recv()
                .unwrap()
                .as_action_execution()
                .unwrap()
                .status,
            ActionStatus::IN_PROGRESS
        );

        db.complete_action("collection", Action::TRUNCATE)
            .await
            .unwrap();
        assert!(db.list_actions().await.unwrap().is_empty());
        assert_eq!(
            events
                .try_recv()
                .unwrap()
                .as_action_execution()
                .unwrap()
                .status,
            ActionStatus::COMPLETED
        );
    }
}
