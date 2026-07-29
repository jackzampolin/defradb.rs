use super::*;

#[tokio::test]
async fn test_subscription_recv() {
    let (tx, rx) = async_channel::bounded(10);
    let mut sub = Subscription::new(1, rx);

    // Send a message
    let msg = Message::merge();
    tx.send(msg).await.unwrap();

    // Receive it
    let received = sub.recv().await;
    assert!(received.is_some());
    assert_eq!(received.unwrap().name, crate::event::EventName::Merge);

    // Close the channel
    drop(tx);

    // Receive should return None
    let received = sub.recv().await;
    assert!(received.is_none());
}

#[test]
fn test_subscription_try_recv() {
    let (tx, rx) = async_channel::bounded(10);
    let mut sub = Subscription::new(1, rx);

    // No message yet
    assert!(sub.try_recv().is_err());

    // Send a message (blocking send in sync context)
    let msg = Message::merge();
    tx.send_blocking(msg).unwrap();

    // Now we can receive
    let received = sub.try_recv();
    assert!(received.is_ok());
}
