use alloy_consensus::SignableTransaction;
use alloy_consensus::TxLegacy;
use alloy_eips::eip2718::Encodable2718;
use alloy_primitives::{Address, Bytes, TxKind, U256};
use alloy_signer::{Signer as _, SignerSync};
use alloy_signer_local::PrivateKeySigner;

const GAS_PRICE: u128 = 1_000_000_000; // 1 gwei
const GAS_LIMIT: u64 = 5_000_000;

pub struct EvmSigner {
    signer: PrivateKeySigner,
    chain_id: u64,
}

impl EvmSigner {
    pub fn new(private_key: &[u8], chain_id: u64) -> Result<Self, SignerError> {
        let signer = PrivateKeySigner::from_slice(private_key)
            .map_err(|e| SignerError::Key(format!("invalid private key: {}", e)))?;
        let signer = signer.with_chain_id(Some(chain_id));
        Ok(Self { signer, chain_id })
    }

    pub fn address(&self) -> Address {
        self.signer.address()
    }

    pub fn did(&self) -> String {
        let verifying_key = self.signer.credential().verifying_key();
        let compressed = verifying_key.to_sec1_bytes();
        // multicodec prefix for secp256k1-pub: 0xe7 0x01
        let mut multicodec = vec![0xe7, 0x01];
        multicodec.extend_from_slice(&compressed);
        let encoded = bs58::encode(&multicodec).into_string();
        format!("did:key:z{}", encoded)
    }

    pub fn sign_tx(&self, nonce: u64, to: Address, data: Bytes) -> Result<Bytes, SignerError> {
        let tx = TxLegacy {
            chain_id: Some(self.chain_id),
            nonce,
            gas_price: GAS_PRICE,
            gas_limit: GAS_LIMIT,
            to: TxKind::Call(to),
            value: U256::ZERO,
            input: data,
        };

        let sig_hash = tx.signature_hash();
        let signature = self
            .signer
            .sign_hash_sync(&sig_hash)
            .map_err(|e| SignerError::Sign(format!("signing failed: {}", e)))?;

        let signed_tx = tx.into_signed(signature);
        let mut buf = Vec::new();
        signed_tx.encode_2718(&mut buf);
        Ok(Bytes::from(buf))
    }
}

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SignerError {
    #[error("key error: {0}")]
    Key(String),

    #[error("signing error: {0}")]
    Sign(String),
}
