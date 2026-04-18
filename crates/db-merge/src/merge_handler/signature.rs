//! Block signature verification for merge operations.

use cid::Cid;
use defra_core::block::Block;
use storage::corekv::Store;

use super::error::MergeError;
use super::DbMergeHandler;

impl<S: Store, B: blockstore::Blockstore + Send + Sync> DbMergeHandler<S, B> {
    /// Verify block signature and return the verified creator identity.
    ///
    /// Returns:
    /// - `Ok(Some(identity))` -- signature valid, identity is the verified signer
    /// - `Ok(None)` -- unsigned block or BLS (unsupported), proceed with warning
    /// - `Err(SignatureVerificationFailed)` -- invalid signature, block MUST be rejected
    pub(crate) async fn verify_block_signature(
        &self,
        cid: &Cid,
        block: &Block,
        _block_data: &[u8],
    ) -> Result<Option<String>, MergeError> {
        let sig_cid = match &block.signature {
            Some(sig_cid) => sig_cid,
            None => {
                // Unsigned blocks are the default in Go-compatible deployments;
                // warning per block produced hundreds of noisy lines per
                // replicated document (issue #858). Keep at debug.
                tracing::debug!(
                    cid = %cid,
                    "P2P block has no signature — cannot verify authenticity"
                );
                return Ok(None);
            }
        };

        let sig_data = match self.blockstore.get(sig_cid).await {
            Ok(Some(data)) => data,
            Ok(None) => {
                return Err(MergeError::SignatureVerificationFailed {
                    cid: *cid,
                    reason: format!("signature block {} not found in blockstore", sig_cid),
                });
            }
            Err(e) => {
                return Err(MergeError::SignatureVerificationFailed {
                    cid: *cid,
                    reason: format!("failed to load signature block {}: {}", sig_cid, e),
                });
            }
        };

        let signature = match defra_core::block::Signature::from_dag_cbor(&sig_data) {
            Ok(sig) => sig,
            Err(e) => {
                return Err(MergeError::SignatureVerificationFailed {
                    cid: *cid,
                    reason: format!("failed to decode signature block: {}", e),
                });
            }
        };

        let sig_identity = String::from_utf8_lossy(&signature.header.identity).to_string();

        // Verify the signature over the block data (block without signature field)
        let mut block_to_verify = block.clone();
        block_to_verify.signature = None;
        let signed_bytes =
            block_to_verify
                .to_dag_cbor()
                .map_err(|e| MergeError::SignatureVerificationFailed {
                    cid: *cid,
                    reason: format!("failed to serialize block for verification: {}", e),
                })?;

        let sig_type = signature.header.sig_type;
        let key_type = match sig_type {
            defra_core::block::SignatureType::ES256K => crypto::KeyType::Secp256k1,
            defra_core::block::SignatureType::ES256 => crypto::KeyType::Secp256r1,
            defra_core::block::SignatureType::EdDSA => crypto::KeyType::Ed25519,
            defra_core::block::SignatureType::BLS => crypto::KeyType::Bls12381,
        };

        let pub_key = crypto::public_key_from_string(key_type, &sig_identity).map_err(|e| {
            MergeError::SignatureVerificationFailed {
                cid: *cid,
                reason: format!("failed to parse public key from identity: {}", e),
            }
        })?;

        match pub_key.verify(&signed_bytes, &signature.value) {
            Ok(true) => {
                // Convert the hex public key to a did:key: DID so that
                // effective_creator() returns a format compatible with ACP
                // registration (which checks for "did:key:" prefix).
                let verified_did =
                    pub_key
                        .did()
                        .map_err(|e| MergeError::SignatureVerificationFailed {
                            cid: *cid,
                            reason: format!("failed to derive DID from verified key: {}", e),
                        })?;
                tracing::debug!(
                    cid = %cid,
                    identity = %verified_did,
                    "Block signature verified successfully"
                );
                Ok(Some(verified_did))
            }
            Err(e) => Err(MergeError::SignatureVerificationFailed {
                cid: *cid,
                reason: format!("signature verification error: {}", e),
            }),
            Ok(false) => Err(MergeError::SignatureVerificationFailed {
                cid: *cid,
                reason: "signature verification returned false unexpectedly".to_string(),
            }),
        }
    }
}
