use async_trait::async_trait;
use db::database::*;
use std::sync::Arc;
use storage::RegolithStore;

struct StubKms;

#[async_trait]
impl kms::KmsService for StubKms {
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
        Err(kms::Error::Unsupported("stub"))
    }

    async fn serve_request(
        &self,
        _: kms::PeerIdentity,
        _: kms::FetchEncryptionKeyRequest,
    ) -> kms::Result<kms::FetchEncryptionKeyReply> {
        Err(kms::Error::Unsupported("stub"))
    }
}

#[test]
fn db_kms_accessor_round_trips() {
    let db = DB::new(RegolithStore::in_memory().unwrap()).unwrap();
    assert!(db.kms().is_none());

    let first: Arc<dyn kms::KmsService> = Arc::new(StubKms);
    db.set_kms(first);
    assert!(db.kms().is_some());

    // OnceLock: second set is silently ignored.
    let second: Arc<dyn kms::KmsService> = Arc::new(StubKms);
    db.set_kms(second);
    assert!(db.kms().is_some());
}
