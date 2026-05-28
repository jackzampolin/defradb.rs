//! Thread-local KMS handle for the document write path.
//!
//! Mirrors the `get_encryption_config()` / `set_encryption_config()` pattern
//! in `defra-core` (which cannot hold an `Arc<dyn kms::KmsService>` because
//! `kms` depends on `defra-core` — a thread-local here avoids the cycle).
//!
//! The request/node layer sets this before invoking a mutation; the mutator
//! reads it and passes it explicitly to `write_document_blocks`.

use std::cell::RefCell;
use std::sync::Arc;

thread_local! {
    static CURRENT_REQUEST_KMS: RefCell<Option<Arc<dyn kms::KmsService>>> =
        const { RefCell::new(None) };
}

/// Set the KMS handle for the current thread. Pass `None` to clear.
pub fn set_request_kms(kms: Option<Arc<dyn kms::KmsService>>) {
    CURRENT_REQUEST_KMS.with(|c| {
        *c.borrow_mut() = kms;
    });
}

/// Get the KMS handle for the current thread, if set.
pub fn get_request_kms() -> Option<Arc<dyn kms::KmsService>> {
    CURRENT_REQUEST_KMS.with(|c| c.borrow().clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;

    struct DummyKms;

    #[async_trait]
    impl kms::KmsService for DummyKms {
        async fn get_keys(
            &self,
            _: &kms::RequestContext,
            _: &[kms::EncryptionCid],
        ) -> kms::Result<kms::KeyResults> {
            let (r, tx) = kms::KeyResults::new(1);
            drop(tx);
            Ok(r)
        }

        async fn generate_key(
            &self,
            _: &kms::RequestContext,
            _: kms::KeyScope,
        ) -> kms::Result<(kms::EncryptionCid, [u8; 32])> {
            Err(kms::Error::Unsupported("dummy"))
        }

        async fn serve_request(
            &self,
            _: kms::PeerIdentity,
            _: kms::FetchEncryptionKeyRequest,
        ) -> kms::Result<kms::FetchEncryptionKeyReply> {
            Err(kms::Error::Unsupported("dummy"))
        }
    }

    #[test]
    fn request_kms_round_trips() {
        assert!(get_request_kms().is_none());

        let handle: Arc<dyn kms::KmsService> = Arc::new(DummyKms);
        set_request_kms(Some(handle));
        assert!(get_request_kms().is_some());

        set_request_kms(None);
        assert!(get_request_kms().is_none());
    }
}
