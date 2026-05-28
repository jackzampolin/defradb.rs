//! No-op `KmsService` for tests and pre-NAC default.

use async_trait::async_trait;

use crate::context::RequestContext;
use crate::error::{Error, Result};
use crate::results::KeyResults;
use crate::service::{KmsService, PeerIdentity};
use crate::types::{EncryptionCid, KeyScope};
use crate::wire::{FetchEncryptionKeyReply, FetchEncryptionKeyRequest};

/// `KmsService` that holds no keys. Every `get_keys` CID returns
/// `KeyUnavailable`; `generate_key` and `serve_request` return
/// `Unsupported`. Useful as a default before KMS is wired or in tests
/// that don't exercise KMS behavior.
#[derive(Default)]
pub struct NoopKms;

#[async_trait]
impl KmsService for NoopKms {
    async fn get_keys(&self, _ctx: &RequestContext, cids: &[EncryptionCid]) -> Result<KeyResults> {
        let (results, tx) = KeyResults::new(cids.len().max(1));
        for _ in cids {
            let _ = tx.send(Err(Error::KeyUnavailable)).await;
        }
        drop(tx);
        Ok(results)
    }

    async fn generate_key(
        &self,
        _: &RequestContext,
        _: KeyScope,
    ) -> Result<(EncryptionCid, [u8; 32])> {
        Err(Error::Unsupported("NoopKms cannot generate keys"))
    }

    async fn serve_request(
        &self,
        _: PeerIdentity,
        _: FetchEncryptionKeyRequest,
    ) -> Result<FetchEncryptionKeyReply> {
        Err(Error::Unsupported("NoopKms cannot serve requests"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn get_keys_returns_unavailable_for_each_cid() {
        let kms = NoopKms;
        let cid: EncryptionCid = "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi"
            .parse()
            .unwrap();
        let results = kms
            .get_keys(&RequestContext::anonymous(), &[cid])
            .await
            .unwrap();
        let mut rx = results.into_receiver();
        let first = rx.recv().await.unwrap();
        assert!(matches!(first, Err(Error::KeyUnavailable)));
    }

    #[tokio::test]
    async fn generate_key_returns_unsupported() {
        let kms = NoopKms;
        let result = kms
            .generate_key(
                &RequestContext::anonymous(),
                KeyScope::Document {
                    doc_id: "x".into(),
                    field: None,
                },
            )
            .await;
        assert!(matches!(result, Err(Error::Unsupported(_))));
    }
}
