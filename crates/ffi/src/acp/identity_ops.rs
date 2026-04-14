//! Identity validation and storage helpers for FFI operations.

use std::ffi::CString;
use std::sync::Arc;

use crate::types::DefraRemoteSignCallback;

pub(crate) const MAX_REMOTE_SIGNATURE_LEN: usize = 512;

pub(crate) struct FfiRemoteSigner {
    pub did: CString,
    pub key_type: CString,
    pub signer_handle: usize,
    pub callback: DefraRemoteSignCallback,
}

impl defra_core::signing::RemoteSigner for FfiRemoteSigner {
    fn sign_sync(
        &self,
        data: &[u8],
        _authorization: Option<&defra_core::signing::SigningAuthorization>,
    ) -> Result<Vec<u8>, String> {
        let mut signature = vec![0u8; MAX_REMOTE_SIGNATURE_LEN];
        let mut signature_len = 0usize;
        let status = unsafe {
            (self.callback)(
                self.signer_handle,
                self.did.as_ptr(),
                self.key_type.as_ptr(),
                data.as_ptr(),
                data.len(),
                signature.as_mut_ptr(),
                signature.len(),
                &mut signature_len,
            )
        };

        if status != 0 {
            return Err(format!(
                "remote signer callback failed with status {}",
                status
            ));
        }
        if signature_len == 0 {
            return Err("remote signer callback returned an empty signature".to_string());
        }
        if signature_len > signature.len() {
            return Err(format!(
                "remote signer callback reported {} bytes, exceeding the {} byte limit",
                signature_len, MAX_REMOTE_SIGNATURE_LEN
            ));
        }

        signature.truncate(signature_len);
        Ok(signature)
    }
}

pub(crate) fn parse_signing_key_type(
    key_type: &str,
) -> Result<defra_core::signing::SigningKeyType, String> {
    key_type.parse()
}

pub(crate) fn parse_crypto_key_type(
    key_type: defra_core::signing::SigningKeyType,
) -> Result<crypto::KeyType, String> {
    match key_type {
        defra_core::signing::SigningKeyType::Ed25519 => Ok(crypto::KeyType::Ed25519),
        defra_core::signing::SigningKeyType::Secp256k1 => Ok(crypto::KeyType::Secp256k1),
        defra_core::signing::SigningKeyType::Secp256r1 => Ok(crypto::KeyType::Secp256r1),
        defra_core::signing::SigningKeyType::Bls => Ok(crypto::KeyType::Bls12381),
        other => Err(format!("unsupported signing key type: {}", other)),
    }
}

pub(crate) fn decode_and_validate_public_key(
    did: &str,
    public_key_hex: &str,
    key_type: defra_core::signing::SigningKeyType,
) -> Result<Vec<u8>, String> {
    let public_key_bytes = hex::decode(public_key_hex)
        .map_err(|error| format!("invalid public key hex: {}", error))?;

    validate_public_key_bytes(did, &public_key_bytes, key_type)?;
    Ok(public_key_bytes)
}

pub(crate) fn validate_public_key_bytes(
    did: &str,
    public_key_bytes: &[u8],
    key_type: defra_core::signing::SigningKeyType,
) -> Result<(), String> {
    let crypto_key_type = parse_crypto_key_type(key_type)?;

    let public_key = crypto::public_key_from_bytes(crypto_key_type, public_key_bytes)
        .map_err(|error| format!("invalid public key bytes: {}", error))?;
    let derived_did = public_key
        .did()
        .map_err(|error| format!("failed to derive DID from public key: {}", error))?;

    if derived_did != did {
        return Err(format!(
            "public key DID mismatch: expected {}, derived {}",
            did, derived_did
        ));
    }

    Ok(())
}

pub(crate) fn store_remote_identity(
    did: String,
    public_key_bytes: Vec<u8>,
    public_key_hex: String,
    key_type: defra_core::signing::SigningKeyType,
    signer_handle: usize,
    callback: DefraRemoteSignCallback,
) -> Result<String, String> {
    let signer = Arc::new(FfiRemoteSigner {
        did: CString::new(did.clone())
            .map_err(|_| "did contains an embedded null byte".to_string())?,
        key_type: CString::new(key_type.to_string())
            .map_err(|_| "key_type contains an embedded null byte".to_string())?,
        signer_handle,
        callback,
    });

    defra_core::signing::store_identity(
        &did,
        defra_core::signing::SigningConfig {
            key_type,
            private_key_bytes: Vec::new(),
            public_key_bytes,
            public_key_hex,
            remote_signer: Some(signer),
            signing_authorization: None,
        },
    );

    Ok("{}".to_string())
}
