//! Streaming key-results aggregator. Mirrors Go's `*encryption.Results`.
//!
//! KMS `get_keys` returns a `KeyResults` because keys may arrive from
//! multiple peers asynchronously. Per-CID first-wins semantics; callers
//! either drain the receiver as keys arrive (preferred for large fetches)
//! or `wait_all()` for a HashMap once the producer side closes.

use std::collections::HashMap;

use crate::channel;
use crate::error::Result;
use crate::types::EncryptionCid;

/// One resolved (CID → 32-byte AES key) pair.
pub type ResolvedKey = (EncryptionCid, [u8; 32]);

/// Receiver half of the results channel.
pub type ResultsReceiver = channel::Receiver<Result<ResolvedKey>>;

/// Sender half, given to the producer (DefraKms or transport adapter).
pub type ResultsSender = channel::Sender<Result<ResolvedKey>>;

/// Streaming aggregator over `(EncryptionCid, [u8;32])` resolutions.
pub struct KeyResults {
    rx: ResultsReceiver,
}

impl KeyResults {
    /// Build a new pair (results consumer, sender). `buffer` is the channel
    /// capacity; clamped to ≥1.
    pub fn new(buffer: usize) -> (Self, ResultsSender) {
        let (tx, rx) = channel::bounded(buffer.max(1));
        (Self { rx }, tx)
    }

    /// Take ownership of the underlying receiver for streaming consumption.
    pub fn into_receiver(self) -> ResultsReceiver {
        self.rx
    }

    /// Drain to completion and return the resolved (CID → key) map.
    /// Propagates the first error encountered in the stream.
    pub async fn wait_all(mut self) -> Result<HashMap<EncryptionCid, [u8; 32]>> {
        let mut out = HashMap::new();
        while let Some(item) = self.rx.recv().await {
            let (cid, key) = item?;
            out.insert(cid, key);
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    fn fake_cid(s: &str) -> EncryptionCid {
        cid::Cid::from_str(s).unwrap()
    }

    #[tokio::test]
    async fn wait_all_collects_results() {
        let (results, tx) = KeyResults::new(2);
        let a = fake_cid("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi");
        let b = fake_cid("bafybeic7soaixfijgvjxfgmolzkckfsalh7c66odzgjlp6h53zjsuktcgi");
        tx.send(Ok((a, [1u8; 32]))).await.unwrap();
        tx.send(Ok((b, [2u8; 32]))).await.unwrap();
        drop(tx);
        let map = results.wait_all().await.unwrap();
        assert_eq!(map.len(), 2);
        assert_eq!(map[&a], [1u8; 32]);
    }

    #[tokio::test]
    async fn streaming_receiver_works() {
        let (results, tx) = KeyResults::new(1);
        let a = fake_cid("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi");
        tx.send(Ok((a, [9u8; 32]))).await.unwrap();
        drop(tx);
        let mut rx = results.into_receiver();
        let (cid, key) = rx.recv().await.unwrap().unwrap();
        assert_eq!(cid, a);
        assert_eq!(key, [9u8; 32]);
        assert!(rx.recv().await.is_none());
    }

    #[tokio::test]
    async fn wait_all_propagates_first_error() {
        let (results, tx) = KeyResults::new(2);
        let a = fake_cid("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi");
        tx.send(Ok((a, [1u8; 32]))).await.unwrap();
        tx.send(Err(crate::Error::KeyUnavailable)).await.unwrap();
        drop(tx);
        let result = results.wait_all().await;
        assert!(matches!(result, Err(crate::Error::KeyUnavailable)));
    }
}
