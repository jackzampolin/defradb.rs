//! Outbound management-channel requester.
//!
//! Mirrors the SE query requester (`db::merge::DbMergeSeQueryTransport`): build +
//! sign the request (the signature also mints the UUID `message_id` used as the
//! correlation key), register a correlator slot, send, then await the reply with
//! a timeout. Generic over the concrete [`P2PTransport`] (libp2p or iroh).

use std::time::Duration;

use defra_http::MANAGE_UNAUTHORIZED;
use p2p::message::{
    ManageMutateOp, ManageQueryOp, ManageQueryReply, ManageQueryRequest, ManageReply, ManageRequest,
};
use p2p::transport::PeerId;
use p2p::{Error, ManageCorrelator, ManageQueryCorrelator, P2PTransport};

/// How long to send a management request and receive its reply before giving up.
/// The SE query requester uses a 10s per-replicator timeout; management requests
/// can drive heavier work and target a single peer, so we allow longer.
const MANAGE_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// The sentinel `err_message` the serve side returns when authorization fails;
/// mapped to [`Error::Unauthorized`]. Any other `err_message` is a remote
/// operational failure and is surfaced as [`Error::Transport`].
const UNAUTHORIZED_ERR: &str = MANAGE_UNAUTHORIZED;

/// Sends management requests to peers and correlates the replies.
#[derive(Clone)]
pub struct ManageClient<T: P2PTransport> {
    transport: T,
    correlator: ManageCorrelator,
    query_correlator: ManageQueryCorrelator,
}

impl<T: P2PTransport> ManageClient<T> {
    pub fn new(
        transport: T,
        correlator: ManageCorrelator,
        query_correlator: ManageQueryCorrelator,
    ) -> Self {
        Self {
            transport,
            correlator,
            query_correlator,
        }
    }

    /// Borrow the underlying transport (used by the outbound requester to parse
    /// + dial the target peer before sending a management request).
    pub fn transport(&self) -> &T {
        &self.transport
    }

    /// Send a mutate op to `peer_id`, authenticated by `auth_token` (a JWT minted
    /// with `aud` = the TARGET node's peer-id). Returns the reply; an
    /// `unauthorized` reply is surfaced as [`Error::Unauthorized`], any other
    /// remote error as [`Error::Transport`].
    pub async fn manage(
        &self,
        peer_id: &PeerId,
        op: ManageMutateOp,
        auth_token: Vec<u8>,
    ) -> Result<ManageReply, Error> {
        let mut req = ManageRequest::new(op, auth_token);

        // Sign FIRST: this generates the UUID message_id used as the correlation
        // key and is required by the receiver's verify_message check.
        p2p::signing::sign_with_transport(&self.transport, &mut req)?;

        let mut pending = self.correlator.register(req.message_id.clone());
        let reply = tokio::time::timeout(MANAGE_REQUEST_TIMEOUT, async {
            self.transport.send_manage_request(peer_id, req).await?;
            pending.recv().await.map_err(|_| Error::ResponseTimeout)
        })
        .await
        .map_err(|_| Error::ResponseTimeout)??;

        map_reply(reply)
    }

    /// Send a read-only query op to `peer_id`. Same correlation + error mapping
    /// as [`ManageClient::manage`].
    pub async fn manage_query(
        &self,
        peer_id: &PeerId,
        op: ManageQueryOp,
        auth_token: Vec<u8>,
    ) -> Result<ManageQueryReply, Error> {
        let mut req = ManageQueryRequest::new(op, auth_token);

        p2p::signing::sign_with_transport(&self.transport, &mut req)?;

        let mut pending = self.query_correlator.register(req.message_id.clone());
        let reply = tokio::time::timeout(MANAGE_REQUEST_TIMEOUT, async {
            self.transport
                .send_manage_query_request(peer_id, req)
                .await?;
            pending.recv().await.map_err(|_| Error::ResponseTimeout)
        })
        .await
        .map_err(|_| Error::ResponseTimeout)??;

        map_query_reply(reply)
    }
}

/// Map a [`ManageReply`] into a `Result`: an `unauthorized` `err_message` becomes
/// [`Error::Unauthorized`]; any other `err_message` is a remote operational
/// failure ([`Error::Transport`]); no `err_message` is success.
fn map_reply(reply: ManageReply) -> Result<ManageReply, Error> {
    match reply.err_message.as_deref() {
        Some(UNAUTHORIZED_ERR) => Err(Error::Unauthorized(UNAUTHORIZED_ERR.to_string())),
        Some(other) => Err(Error::Transport(other.to_string())),
        None => Ok(reply),
    }
}

/// Query-reply variant of [`map_reply`].
fn map_query_reply(reply: ManageQueryReply) -> Result<ManageQueryReply, Error> {
    match reply.err_message.as_deref() {
        Some(UNAUTHORIZED_ERR) => Err(Error::Unauthorized(UNAUTHORIZED_ERR.to_string())),
        Some(other) => Err(Error::Transport(other.to_string())),
        None => Ok(reply),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use p2p::message::{ManageQueryReply, ManageQueryResult, ManageReply};

    #[test]
    fn map_reply_success() {
        let reply = ManageReply::success("msg-1");
        let out = map_reply(reply).expect("no err_message should be Ok");
        assert_eq!(out.message_id, "msg-1");
    }

    #[test]
    fn map_reply_unauthorized() {
        let reply = ManageReply::error("msg-1", UNAUTHORIZED_ERR);
        match map_reply(reply) {
            Err(Error::Unauthorized(msg)) => assert_eq!(msg, UNAUTHORIZED_ERR),
            other => panic!("expected Unauthorized, got {other:?}"),
        }
    }

    #[test]
    fn map_reply_other_error_is_transport() {
        let reply = ManageReply::error("msg-1", "invalid peer id");
        match map_reply(reply) {
            Err(Error::Transport(msg)) => assert_eq!(msg, "invalid peer id"),
            other => panic!("expected Transport, got {other:?}"),
        }
    }

    #[test]
    fn map_query_reply_success() {
        let reply =
            ManageQueryReply::success("q-1", ManageQueryResult::Strings { values: Vec::new() });
        let out = map_query_reply(reply).expect("no err_message should be Ok");
        assert_eq!(out.message_id, "q-1");
    }

    #[test]
    fn map_query_reply_unauthorized() {
        let reply = ManageQueryReply::error("q-1", UNAUTHORIZED_ERR);
        match map_query_reply(reply) {
            Err(Error::Unauthorized(msg)) => assert_eq!(msg, UNAUTHORIZED_ERR),
            other => panic!("expected Unauthorized, got {other:?}"),
        }
    }

    #[test]
    fn map_query_reply_other_error_is_transport() {
        let reply = ManageQueryReply::error("q-1", "collection not found");
        match map_query_reply(reply) {
            Err(Error::Transport(msg)) => assert_eq!(msg, "collection not found"),
            other => panic!("expected Transport, got {other:?}"),
        }
    }
}
