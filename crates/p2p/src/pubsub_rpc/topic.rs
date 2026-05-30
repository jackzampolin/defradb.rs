//! Response-topic naming.
//!
//! Go's pubsub_rpc builds a dynamic response sub-topic per (base-topic, peer)
//! pair:
//!
//! ```text
//! responseTopic(base, pid) = path.Join(base, pid.String(), "_response")
//! ```
//!
//! (`rpc.go:69-71`). The caller subscribes to
//! `<base>/<self>/_response` when it joins the base topic; responders publish
//! to `<base>/<caller>/_response` to deliver replies. Path normalization uses
//! Go's `path.Join`, which collapses duplicate slashes but does *not* add
//! leading/trailing ones — the Rust port matches via explicit separator
//! concatenation rather than a platform path API.

use libp2p::PeerId;

/// Format the response sub-topic for `(base, peer)` exactly as Go does.
///
/// Go's `path.Join("doc-sync", "12D3KooW...", "_response")` →
/// `"doc-sync/12D3KooW.../_response"`. We avoid `std::path::Path` because it
/// uses platform separators; pubsub topics must always be forward-slash joined.
pub fn response_topic(base: &str, peer: &PeerId) -> String {
    format!("{base}/{peer}/_response")
}

/// Returns `Some(base)` if `topic` is a response sub-topic of the form
/// `<base>/<peer>/_response` addressed to `self_peer`, else `None`.
///
/// Used by the dispatcher to decide whether an incoming gossipsub message
/// belongs on the request-handling path or the response-correlation path.
///
/// For hot-path dispatch (many messages, same `self_peer`), prefer
/// [`strip_response_topic_with_suffix`] with a pre-computed suffix.
pub fn response_topic_suffix(self_peer: &PeerId) -> String {
    format!("/{self_peer}/_response")
}

/// Like [`strip_response_topic`] but reuses a pre-built suffix
/// (`/{self_peer}/_response`) to avoid allocating per call.
pub fn strip_response_topic_with_suffix<'a>(topic: &'a str, suffix: &str) -> Option<&'a str> {
    topic.strip_suffix(suffix)
}

pub fn strip_response_topic<'a>(topic: &'a str, self_peer: &PeerId) -> Option<&'a str> {
    strip_response_topic_with_suffix(topic, &response_topic_suffix(self_peer))
}

#[cfg(test)]
mod tests {
    use super::*;
    use libp2p::identity::Keypair;

    fn a_peer() -> PeerId {
        PeerId::from_public_key(&Keypair::generate_ed25519().public())
    }

    #[test]
    fn topic_shape_matches_go_join() {
        let p = a_peer();
        let t = response_topic("doc-sync", &p);
        let expected = format!("doc-sync/{p}/_response");
        assert_eq!(t, expected);
    }

    #[test]
    fn strip_response_recovers_base_for_self() {
        let me = a_peer();
        let t = response_topic("sync-branchable", &me);
        assert_eq!(strip_response_topic(&t, &me), Some("sync-branchable"));
    }

    #[test]
    fn strip_response_rejects_other_peer() {
        let me = a_peer();
        let other = a_peer();
        let t = response_topic("doc-sync", &other);
        // `_response` topics addressed to other peers must not decode for us.
        assert_eq!(strip_response_topic(&t, &me), None);
    }

    #[test]
    fn strip_response_rejects_plain_topic() {
        let me = a_peer();
        assert_eq!(strip_response_topic("doc-sync", &me), None);
    }
}
