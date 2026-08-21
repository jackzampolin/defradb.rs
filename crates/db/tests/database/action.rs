use db::database::action::*;
use db::error::Error;
use defra_core::Action;
use defra_core::ActionStatus;
use events::Bus;
use events::ChannelBus;
use events::EventName;
use storage::backends::MemoryStore;
use storage::corekv::Store;

#[test]
fn status_decoder_rejects_trailing_bytes() {
    assert_eq!(
        decode_status(&encode_status(ActionStatus::IN_PROGRESS)),
        Some(ActionStatus::IN_PROGRESS)
    );
    assert_eq!(decode_status(&[1, 0xff]), None);
}

#[tokio::test]
async fn action_lifecycle_retains_only_incomplete_executions() {
    let bus: std::sync::Arc<dyn Bus> = std::sync::Arc::new(ChannelBus::new());
    let mut db = db::DB::new(MemoryStore::new()).unwrap();
    db.set_event_bus(std::sync::Arc::clone(&bus));
    let mut events = bus.subscribe(&[EventName::ActionExecution]);

    let lease = db
        .register_action("collection", Action::TRUNCATE)
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

    db.fail_action(lease, "failed").await.unwrap();
    let actions = db.list_actions().await.unwrap();
    assert_eq!(actions[0].status, ActionStatus::ERRORED);
    assert_eq!(actions[0].reason, "failed");
    let event = events.try_recv().unwrap();
    let execution = event.as_action_execution().unwrap();
    assert_eq!(execution.status, ActionStatus::ERRORED);
    assert_eq!(execution.reason, "failed");

    let lease = db
        .register_action("collection", Action::TRUNCATE)
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

    db.complete_action(lease).await.unwrap();
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

#[tokio::test]
async fn abandoned_execution_can_overwrite_stale_persisted_status() {
    let store = MemoryStore::new();
    let db = db::DB::new(store.clone()).unwrap();

    let abandoned = db
        .register_action("collection", Action::TRUNCATE)
        .await
        .unwrap();
    drop(abandoned);

    let retry = db
        .register_action("collection", Action::TRUNCATE)
        .await
        .expect("dropping a lease must release its process-local claim");
    drop(retry);
    drop(db);

    let reopened = db::DB::new(store).unwrap();
    let recovered = reopened
        .register_action("collection", Action::TRUNCATE)
        .await
        .expect("a persisted in-progress status from an earlier process is not a lock");
    reopened.complete_action(recovered).await.unwrap();
}

#[tokio::test]
async fn finalization_failure_releases_process_local_claim() {
    let store = MemoryStore::new();
    let db = db::DB::new(store.clone()).unwrap();
    let lease = db
        .register_action("collection", Action::TRUNCATE)
        .await
        .unwrap();

    store.close().await.unwrap();
    assert!(db.complete_action(lease).await.is_err());
    assert!(db.has_no_active_actions());
}
