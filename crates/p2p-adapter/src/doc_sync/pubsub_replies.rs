use std::collections::HashSet;

use cid::Cid;
use defra_http::P2PResult;
use p2p::message::pubsub::DocSyncReply;

use crate::{P2PError, P2PErrorExt as _};

/// Heads advertised by the peers that answered a pubsub doc-sync request, or
/// the timeout error Go raises when the request produced nothing at all.
///
/// Go parity, `waitAndHandleDocSyncResponses` (`internal/db/p2p/sync_doc.go:135-164`):
/// the wait loop drains one pending peer per reply and returns the collected
/// heads once every peer has answered — an empty result is a success in that
/// case. `ErrTimeoutDocSync` is returned only from the `ctx.Done()` arm, i.e.
/// when the deadline fired with at least one peer still pending *and* no head
/// was collected.
///
/// The returned heads are the raw advertisement, exactly like Go's `result`
/// map, which `handleDocSyncItem` fills from every parsed head regardless of
/// whether the local node already has the block. Callers may narrow them (e.g.
/// to the unmerged ones) only *after* this check, or a peer advertising a head
/// we already hold would be misread as silence.
///
/// Like Go's `requestedDocIDs` guard, heads for documents that were not asked
/// for are dropped.
pub(crate) fn advertised_heads(
    expected_peers: usize,
    requested_doc_ids: &HashSet<String>,
    replies: &[(String, DocSyncReply)],
) -> P2PResult<Vec<Cid>> {
    let heads: Vec<Cid> = replies
        .iter()
        .flat_map(|(_, reply)| &reply.results)
        .filter(|item| requested_doc_ids.contains(&item.doc_id))
        .flat_map(|item| &item.heads)
        .filter_map(|head| Cid::try_from(head.as_slice()).ok())
        .collect();

    // Peers are counted by authenticated sender, mirroring the way Go clears
    // one entry of `pendingPeers` per peer: two replies from the same peer
    // still leave the rest of the network pending.
    let responded: HashSet<&str> = replies.iter().map(|(peer, _)| peer.as_str()).collect();
    if responded.len() < expected_peers && heads.is_empty() {
        return Err(P2PError::transport("timeout while syncing doc"));
    }

    Ok(heads)
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use cid::Cid;
    use p2p::message::pubsub::{DocSyncItem, DocSyncReply};

    use super::advertised_heads;

    const DOC_ID: &str = "bae-doc-1";

    fn head() -> Cid {
        Cid::try_from("bafkreigh2akiscaildcqabsyg3dfr6chu3fgpregiymsck7e7aqa4s52zy").unwrap()
    }

    fn requested() -> HashSet<String> {
        HashSet::from([DOC_ID.to_string()])
    }

    fn reply(peer: &str, items: Vec<DocSyncItem>) -> (String, DocSyncReply) {
        (
            peer.to_string(),
            DocSyncReply {
                results: items,
                sender: peer.to_string(),
            },
        )
    }

    fn item(doc_id: &str, heads: Vec<Cid>) -> DocSyncItem {
        DocSyncItem {
            doc_id: doc_id.to_string(),
            heads: heads.iter().map(|head| head.to_bytes()).collect(),
        }
    }

    /// #1299: a peer that never answered plus no advertised head is Go's
    /// `ctx.Done()` arm with an empty `result` — `ErrTimeoutDocSync`.
    #[test]
    fn silent_peer_with_no_heads_is_a_timeout() {
        let error = advertised_heads(2, &requested(), &[reply("peer-a", vec![])])
            .expect_err("one peer never replied and nothing was advertised");

        assert!(
            error.to_string().contains("timeout while syncing doc"),
            "expected Go's ErrTimeoutDocSync text, got: {error}"
        );
    }

    /// Go exits the loop normally once `pendingPeers` is empty and returns
    /// `result, nil`, so "everyone answered, nobody had the document" is a
    /// success, not a timeout.
    #[test]
    fn every_peer_replying_with_no_heads_is_success() {
        let replies = [reply("peer-a", vec![]), reply("peer-b", vec![])];

        let heads = advertised_heads(2, &requested(), &replies)
            .expect("all peers replied, so an empty result is success");

        assert!(heads.is_empty());
    }

    /// A single advertised head satisfies Go's `len(result) == 0` guard even
    /// though a peer is still pending. The head is reported raw: whether the
    /// local node already merged it is not part of this decision.
    #[test]
    fn advertised_head_prevents_timeout_despite_silent_peer() {
        let replies = [reply("peer-a", vec![item(DOC_ID, vec![head()])])];

        let heads = advertised_heads(2, &requested(), &replies)
            .expect("a head was advertised, so Go returns the result");

        assert_eq!(heads, vec![head()]);
    }

    /// Go skips items whose docID was not requested before they can reach
    /// `result`, so they cannot rescue a timeout either.
    #[test]
    fn unrequested_doc_heads_do_not_prevent_timeout() {
        let replies = [reply(
            "peer-a",
            vec![item("bae-not-asked-for", vec![head()])],
        )];

        let error = advertised_heads(2, &requested(), &replies)
            .expect_err("heads for unrequested documents are dropped like Go's guard");

        assert!(error.to_string().contains("timeout while syncing doc"));
    }

    /// Two replies from the same peer clear one pending peer in Go's map, so
    /// they must not be mistaken for full coverage of a two-peer network.
    #[test]
    fn duplicate_replies_from_one_peer_do_not_cover_the_network() {
        let replies = [reply("peer-a", vec![]), reply("peer-a", vec![])];

        let error = advertised_heads(2, &requested(), &replies)
            .expect_err("both replies came from the same peer, so one is still pending");

        assert!(error.to_string().contains("timeout while syncing doc"));
    }
}
