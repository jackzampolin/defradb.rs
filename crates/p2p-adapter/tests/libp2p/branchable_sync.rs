use std::future::Future;
use std::task::{Context, Waker};
use std::time::Duration;

use events::{Bus, ChannelBus, EventName, MergeCompleteData, Message};

use super::*;

fn heads() -> [Cid; 2] {
    [
        "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi",
        "bafkreigh2akiscaildcqabsyg3dfr6chu3fgpregiymsck7e7aqa4s52zy",
    ]
    .map(|value| value.parse().unwrap())
}

fn merged(bus: &ChannelBus, collection_id: &str, cid: Cid) {
    bus.publish(Message::merge_complete(MergeCompleteData {
        collection_id: collection_id.to_string(),
        cid,
        doc_id: String::new(),
        subject_doc_id: None,
        by_peer: String::new(),
    }));
}

#[tokio::test(start_paused = true)]
async fn waits_for_all_advertised_heads_despite_idle_gaps() {
    let bus = ChannelBus::default();
    let mut sub = bus.subscribe(&[EventName::MergeComplete]);
    let [first, second] = heads();
    let mut wait = std::pin::pin!(wait_for_heads(
        &mut sub,
        "collection",
        HashSet::from([first, second]),
        Instant::now() + Duration::from_secs(30),
    ));
    let mut context = Context::from_waker(Waker::noop());
    assert!(wait.as_mut().poll(&mut context).is_pending());

    tokio::time::advance(Duration::from_secs(4)).await;
    assert!(wait.as_mut().poll(&mut context).is_pending());
    merged(&bus, "collection", first);
    assert!(wait.as_mut().poll(&mut context).is_pending());

    merged(&bus, "collection", first);
    merged(&bus, "other-collection", second);
    tokio::time::advance(Duration::from_secs(4)).await;
    assert!(wait.as_mut().poll(&mut context).is_pending());

    merged(&bus, "collection", second);
    assert!(matches!(
        wait.as_mut().poll(&mut context),
        std::task::Poll::Ready(Ok(()))
    ));
}

#[tokio::test(start_paused = true)]
async fn partial_completion_does_not_reset_deadline_or_report_success() {
    let bus = ChannelBus::default();
    let mut sub = bus.subscribe(&[EventName::MergeComplete]);
    let [first, second] = heads();
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut wait = std::pin::pin!(wait_for_heads(
        &mut sub,
        "collection",
        HashSet::from([first, second]),
        deadline,
    ));
    let mut context = Context::from_waker(Waker::noop());
    assert!(wait.as_mut().poll(&mut context).is_pending());

    tokio::time::advance(Duration::from_secs(29)).await;
    merged(&bus, "collection", first);
    assert!(wait.as_mut().poll(&mut context).is_pending());

    let error = wait.await.unwrap_err();
    assert!(error.to_string().contains("1 heads remain unmerged"));
    assert_eq!(Instant::now(), deadline);
}

#[tokio::test]
async fn closed_event_bus_does_not_report_unmerged_heads_as_complete() {
    let bus = ChannelBus::default();
    let mut sub = bus.subscribe(&[EventName::MergeComplete]);
    bus.close();

    let error = wait_for_heads(
        &mut sub,
        "collection",
        HashSet::from(heads()),
        Instant::now() + Duration::from_secs(30),
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("event bus closed"));
}

#[tokio::test]
async fn already_merged_heads_need_no_events_or_remaining_time() {
    let bus = ChannelBus::default();
    let mut sub = bus.subscribe(&[EventName::MergeComplete]);
    bus.close();

    wait_for_heads(&mut sub, "collection", HashSet::new(), Instant::now())
        .await
        .unwrap();
}
