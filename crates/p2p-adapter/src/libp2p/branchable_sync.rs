use std::collections::HashSet;

use cid::Cid;
use events::Subscription;
use tokio::time::{timeout_at, Instant};

use crate::{P2PError, P2PErrorExt as _, P2PResult};

pub(super) async fn wait_for_heads(
    sub: &mut Subscription,
    collection_id: &str,
    mut pending: HashSet<Cid>,
    deadline: Instant,
) -> P2PResult<()> {
    while !pending.is_empty() {
        match timeout_at(deadline, sub.recv()).await {
            Ok(Some(message)) => {
                if let Some(merge) = message.as_merge_complete() {
                    if merge.collection_id == collection_id {
                        pending.remove(&merge.cid);
                    }
                }
            }
            Ok(None) => {
                return Err(P2PError::transport(
                    "event bus closed while syncing branchable collection",
                ));
            }
            Err(_) => {
                return Err(P2PError::transport(format!(
                    "timeout while syncing branchable collection: {} heads remain unmerged",
                    pending.len()
                )));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "../../tests/libp2p/branchable_sync.rs"]
mod tests;
