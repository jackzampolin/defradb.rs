//! HTTP-facing [`ManageRequester`] implementation for [`ManageClient`].
//!
//! Bridges the http-native `RemoteManage*` types (http does not depend on p2p)
//! to the p2p wire ops: parse + dial the target's address via the transport,
//! map the op, send via [`ManageClient`], then map the reply (including the
//! `unauthorized` sentinel) back to an http-native result.

use defra_http::{
    ManageRequester, RemoteManageDocRef, RemoteManageOp, RemoteManageQueryOp,
    RemoteManageQueryResult,
};
use p2p::message::{ManageDocRef, ManageMutateOp, ManageQueryOp, ManageQueryResult};
use p2p::{Error, P2PTransport};

use super::client::ManageClient;

/// The error string the http layer matches to detect a remote NAC denial.
const UNAUTHORIZED: &str = "unauthorized";

#[async_trait::async_trait]
impl<T: P2PTransport> ManageRequester for ManageClient<T> {
    async fn manage(
        &self,
        target_addr: &str,
        auth_token: Vec<u8>,
        op: RemoteManageOp,
    ) -> Result<(), String> {
        let (peer_id, addrs) = self
            .transport()
            .parse_dial_addr(target_addr)
            .map_err(|e| e.to_string())?;
        self.transport()
            .dial(&peer_id, addrs)
            .await
            .map_err(|e| e.to_string())?;

        match ManageClient::manage(self, &peer_id, to_mutate_op(op), auth_token).await {
            Ok(_reply) => Ok(()),
            Err(e) => Err(map_error(e)),
        }
    }

    async fn manage_query(
        &self,
        target_addr: &str,
        auth_token: Vec<u8>,
        op: RemoteManageQueryOp,
    ) -> Result<RemoteManageQueryResult, String> {
        let (peer_id, addrs) = self
            .transport()
            .parse_dial_addr(target_addr)
            .map_err(|e| e.to_string())?;
        self.transport()
            .dial(&peer_id, addrs)
            .await
            .map_err(|e| e.to_string())?;

        let reply = ManageClient::manage_query(self, &peer_id, to_query_op(op), auth_token)
            .await
            .map_err(map_error)?;

        let result = reply
            .result
            .ok_or_else(|| "remote returned an empty management query result".to_string())?;
        Ok(to_http_query_result(result))
    }
}

/// Map a p2p [`Error`] to an http-native error string, normalizing the
/// authorization-denied case to the [`UNAUTHORIZED`] sentinel so the HTTP layer
/// can detect it regardless of the inner message.
fn map_error(error: Error) -> String {
    match error {
        Error::Unauthorized(_) => UNAUTHORIZED.to_string(),
        other => other.to_string(),
    }
}

fn to_mutate_op(op: RemoteManageOp) -> ManageMutateOp {
    match op {
        RemoteManageOp::ReplicatorAdd {
            addresses,
            collection_ids,
        } => ManageMutateOp::ReplicatorAdd {
            addresses,
            collection_ids,
        },
        RemoteManageOp::ReplicatorDelete {
            addresses,
            collection_ids,
        } => ManageMutateOp::ReplicatorDelete {
            addresses,
            collection_ids,
        },
        RemoteManageOp::CollectionAdd { collection_ids } => {
            ManageMutateOp::CollectionAdd { collection_ids }
        }
        RemoteManageOp::CollectionRemove { collection_ids } => {
            ManageMutateOp::CollectionRemove { collection_ids }
        }
        RemoteManageOp::DocumentAdd { docs } => ManageMutateOp::DocumentAdd {
            docs: docs.into_iter().map(to_doc_ref).collect(),
        },
        RemoteManageOp::DocumentRemove { docs } => ManageMutateOp::DocumentRemove {
            docs: docs.into_iter().map(to_doc_ref).collect(),
        },
        RemoteManageOp::PeerConnect { address } => ManageMutateOp::PeerConnect { address },
    }
}

fn to_doc_ref(doc: RemoteManageDocRef) -> ManageDocRef {
    ManageDocRef {
        collection: doc.collection,
        doc_id: doc.doc_id,
    }
}

fn to_query_op(op: RemoteManageQueryOp) -> ManageQueryOp {
    match op {
        RemoteManageQueryOp::ReplicatorList => ManageQueryOp::ReplicatorList,
        RemoteManageQueryOp::CollectionList => ManageQueryOp::CollectionList,
    }
}

fn to_http_query_result(result: ManageQueryResult) -> RemoteManageQueryResult {
    match result {
        ManageQueryResult::Replicators { replicators } => RemoteManageQueryResult::Replicators {
            replicators: replicators
                .into_iter()
                .map(crate::to_http_replicator_info)
                .collect(),
        },
        ManageQueryResult::Strings { values } => RemoteManageQueryResult::Strings { values },
    }
}
