use std::sync::Arc;

use async_trait::async_trait;

use defra_http::router::BlockOperations;
use storage::corekv::Store;

/// Adapter that implements BlockOperations using database.
pub struct BlockAdapter<S: Store> {
    database: Arc<db::DB<S>>,
    document_acp: Arc<dyn acp::DocumentACP>,
}

impl<S: Store + 'static> BlockAdapter<S> {
    /// Create an Arc-wrapped adapter.
    pub fn new_arc(
        database: Arc<db::DB<S>>,
        document_acp: Arc<dyn acp::DocumentACP>,
    ) -> Arc<dyn BlockOperations> {
        Arc::new(Self {
            database,
            document_acp,
        })
    }
}

#[async_trait]
impl<S: Store + 'static> BlockOperations for BlockAdapter<S> {
    async fn verify_signature(
        &self,
        cid: &str,
        public_key: &str,
        key_type: Option<&str>,
    ) -> Result<(), String> {
        let crypto_key_type = match key_type.unwrap_or("secp256k1") {
            "ed25519" => crypto::KeyType::Ed25519,
            "secp256k1" => crypto::KeyType::Secp256k1,
            "secp256r1" => crypto::KeyType::Secp256r1,
            other => return Err(format!("unsupported key type: {}", other)),
        };

        let caller_identity = acp::Identity::anonymous();

        db::block_verify::verify_block_signature(
            &self.database,
            self.document_acp.as_ref(),
            cid,
            public_key,
            crypto_key_type,
            &caller_identity,
        )
        .await
    }
}
