use std::collections::HashSet;
use std::sync::Arc;

use defra_http::P2PResult;

use crate::doc_sync::dispatch::DocSyncDispatch;
use crate::{P2PError, P2PErrorExt as _};

/// Explicit document sync: ask every connected peer for `doc_ids`, then wait
/// for the merges those requests trigger.
///
/// `overall_timeout` and `parallelism` are the only points where the two
/// transports differ: iroh waits 10s and dispatches up to 16 sends at once,
/// libp2p waits 30s and sends one peer at a time. Returning `Ok` without any
/// merge is deliberate — see the retry loop below.
pub(crate) async fn sync_documents<D>(
    dispatch: Arc<D>,
    event_bus: &dyn events::Bus,
    doc_ids: Vec<String>,
    overall_timeout: std::time::Duration,
    parallelism: usize,
) -> P2PResult<()>
where
    D: DocSyncDispatch + 'static,
    D::Peer: Clone + std::fmt::Display + 'static,
{
    let connected_peers = dispatch.connected_peers().await?;
    if connected_peers.is_empty() {
        return Err(P2PError::transport("no connected peers to sync with"));
    }

    let mut sub = event_bus.subscribe(&[events::EventName::MergeComplete]);
    let total_expected = connected_peers.len() * doc_ids.len();
    let mut total_received = 0;
    let idle_timeout = std::time::Duration::from_secs(3);
    let start = std::time::Instant::now();
    let doc_set: HashSet<String> = doc_ids.iter().cloned().collect();

    for _attempt in 0..3 {
        if total_received >= total_expected || start.elapsed() >= overall_timeout {
            break;
        }

        let mut request = p2p::message::DocSyncRequest::new(doc_ids.clone());
        if let Err(error) = dispatch.sign_request(&mut request) {
            event_bus.unsubscribe(sub.id());
            return Err(error);
        }

        // Track whether any request was dispatched. If none were, further
        // attempts cannot produce merges — exit instead of burning the full
        // overall timeout.
        let any_sent = send_requests(&dispatch, &connected_peers, request, parallelism).await;
        if !any_sent {
            break;
        }

        // Idle completion: exit after `idle_timeout` with no MergeComplete
        // events, even when zero merges arrived. Requiring a minimum merge
        // count held HTTP handlers for the full overall timeout when peers had
        // nothing to contribute — e.g. source-side explicit sync while
        // collection replication delivers the doc out of band.
        let mut last_merge = std::time::Instant::now();
        while total_received < total_expected && start.elapsed() < overall_timeout {
            if last_merge.elapsed() > idle_timeout {
                break;
            }

            match tokio::time::timeout(std::time::Duration::from_millis(100), sub.recv()).await {
                Ok(Some(msg)) => {
                    if let Some(data) = msg.as_merge_complete() {
                        if doc_set.contains(&data.doc_id) {
                            total_received += 1;
                            last_merge = std::time::Instant::now();
                        }
                    }
                }
                Ok(None) => break,
                Err(_) => {}
            }
        }
    }

    event_bus.unsubscribe(sub.id());
    Ok(())
}

/// Sends `request` to every peer, at most `parallelism` at a time, and reports
/// whether any send succeeded.
async fn send_requests<D>(
    dispatch: &Arc<D>,
    peers: &[D::Peer],
    request: p2p::message::DocSyncRequest,
    parallelism: usize,
) -> bool
where
    D: DocSyncDispatch + 'static,
    D::Peer: Clone + std::fmt::Display + 'static,
{
    let mut peer_iter = peers.iter().cloned();
    let mut tasks = tokio::task::JoinSet::new();
    let mut any_sent = false;

    loop {
        while tasks.len() < parallelism {
            let Some(peer) = peer_iter.next() else {
                break;
            };
            let dispatch = Arc::clone(dispatch);
            let request = request.clone();
            tasks.spawn(async move {
                let result = dispatch.send_doc_sync_request(&peer, request).await;
                (peer, result)
            });
        }

        if tasks.is_empty() {
            break;
        }

        match tasks.join_next().await {
            Some(Ok((peer, Ok(())))) => {
                any_sent = true;
                tracing::debug!(peer_id = %peer, "sent DocSync request");
            }
            Some(Ok((peer, Err(error)))) => {
                tracing::warn!(peer_id = %peer, error = %error, "failed to send DocSync request");
            }
            Some(Err(error)) => {
                tracing::warn!(error = %error, "DocSync dispatch task failed");
            }
            None => break,
        }
    }

    any_sent
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use std::time::Duration;

    use crate::doc_sync::test_support::FailingDispatch;

    /// With peers present and every send failing, the sync reaches the
    /// `!any_sent` branch. It currently returns `Ok`; #1299's remaining rows
    /// change that to an error, and this test is the seam that makes the
    /// change testable.
    #[tokio::test]
    async fn all_sends_failing_reaches_the_no_send_branch() {
        let dispatch = Arc::new(FailingDispatch::with_peers(2));
        let bus = Arc::new(events::ChannelBus::default());

        let result = super::sync_documents(
            Arc::clone(&dispatch),
            bus.as_ref(),
            vec!["bae-does-not-matter".to_string()],
            Duration::from_millis(50),
            2,
        )
        .await;

        assert!(
            dispatch.send_attempts.load(Ordering::SeqCst) >= 2,
            "every connected peer should have been attempted"
        );
        assert!(
            result.is_ok(),
            "current contract returns Ok when nothing was sent, got: {result:?}"
        );
    }
}
