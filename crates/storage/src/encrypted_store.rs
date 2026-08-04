//! Transparent at-rest value encryption for the corekv storage layer.
//!
//! [`EncryptedStore`] wraps any [`Store`] backend and transparently encrypts
//! VALUES with AES-256-GCM (random nonce prepended per value), keyed by the
//! 32-byte keyring `encryption-key`. This is the Rust equivalent of Go
//! DefraDB's `SetBadgerEncryptionKey`.
//!
//! # Divergence from Go
//!
//! Go DefraDB relies on Badger's native encryption, which encrypts whole
//! blocks INCLUDING keys. This wrapper encrypts VALUES ONLY and leaves keys
//! in plaintext. This is a deliberate, necessary divergence: the corekv
//! iterator contract (prefix/range/seek in [`crate::corekv::IterOptions`])
//! relies on plaintext lexicographic key ordering. Encrypting keys would
//! destroy prefix and range iteration, which the entire store hierarchy
//! depends on. Values carry no ordering requirement, so they are encrypted.
//!
//! # Authentication
//!
//! Each value is encrypted with the storage key as AES-GCM additional
//! authenticated data (AAD). This binds every ciphertext to the key it lives
//! under: a value relocated to a different key (or read under the wrong key)
//! fails authentication and surfaces a loud decrypt error rather than silent
//! garbage. A mismatched encryption key likewise fails authentication.
//!
//! # Opt-in
//!
//! Encryption is opt-in and, once enabled for a store, must stay enabled:
//! reading encrypted data without the matching key (or vice versa) produces a
//! decrypt error, never a silent wrong result.

use async_trait::async_trait;
use zeroize::Zeroizing;

use crate::corekv::errors::{Error, Result};
use crate::corekv::iterator::{Iterator as KvIterator, KvPair};
use crate::corekv::traits::{
    private::Sealed, AsyncTxnCallback, Reader, Store, Txn, TxnCallback, Writer,
};
use crate::corekv::types::IterOptions;

/// AES-256 key length in bytes.
pub const ENCRYPTION_KEY_LEN: usize = 32;

fn encrypt_value(
    key: &[u8; ENCRYPTION_KEY_LEN],
    storage_key: &[u8],
    value: &[u8],
) -> Result<Vec<u8>> {
    let (ciphertext, _nonce) = crypto::encrypt_aes(value, key, storage_key, true)
        .map_err(|e| Error::Other(format!("at-rest encryption failed: {e}")))?;
    Ok(ciphertext)
}

fn decrypt_value(
    key: &[u8; ENCRYPTION_KEY_LEN],
    storage_key: &[u8],
    ciphertext: &[u8],
) -> Result<Vec<u8>> {
    crypto::decrypt_aes(None, ciphertext, key, storage_key).map_err(|e| {
        Error::Other(format!(
            "at-rest decryption failed (wrong key or corrupt store): {e}"
        ))
    })
}

/// A [`Store`] wrapper that transparently encrypts values at rest.
///
/// See the [module documentation](self) for the encryption scheme and the
/// rationale for value-only encryption.
pub struct EncryptedStore<S: Store> {
    inner: S,
    key: Zeroizing<[u8; ENCRYPTION_KEY_LEN]>,
}

impl<S: Store> EncryptedStore<S> {
    /// Wrap `inner`, encrypting all values with `key`.
    pub fn new(inner: S, key: [u8; ENCRYPTION_KEY_LEN]) -> Self {
        Self {
            inner,
            key: Zeroizing::new(key),
        }
    }
}

impl<S: Store> Sealed for EncryptedStore<S> {}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<S: Store> Store for EncryptedStore<S> {
    #[cfg(not(target_arch = "wasm32"))]
    fn transaction_stats_handle(&self) -> Option<crate::backends::TransactionStatsHandle> {
        self.inner.transaction_stats_handle()
    }

    async fn new_txn(&self, readonly: bool) -> Result<Box<dyn Txn>> {
        let inner = self.inner.new_txn(readonly).await?;
        Ok(Box::new(EncryptedTxn {
            inner,
            key: self.key.clone(),
        }))
    }

    async fn close(&self) -> Result<()> {
        self.inner.close().await
    }
}

/// A [`Txn`] wrapper that transparently encrypts values at rest.
struct EncryptedTxn {
    inner: Box<dyn Txn>,
    key: Zeroizing<[u8; ENCRYPTION_KEY_LEN]>,
}

impl Sealed for EncryptedTxn {}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Reader for EncryptedTxn {
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        match self.inner.get(key).await? {
            Some(ct) => Ok(Some(decrypt_value(&self.key, key, &ct)?)),
            None => Ok(None),
        }
    }

    async fn has(&self, key: &[u8]) -> Result<bool> {
        self.inner.has(key).await
    }

    async fn get_size(&self, key: &[u8]) -> Result<Option<usize>> {
        match self.inner.get(key).await? {
            Some(ct) => Ok(Some(decrypt_value(&self.key, key, &ct)?.len())),
            None => Ok(None),
        }
    }

    async fn iterator(&self, opts: IterOptions) -> Result<Box<dyn KvIterator>> {
        let inner = self.inner.iterator(opts.clone()).await?;
        Ok(Box::new(EncryptedIterator {
            inner,
            key: self.key.clone(),
            keys_only: opts.keys_only(),
        }))
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Writer for EncryptedTxn {
    async fn set(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let ct = encrypt_value(&self.key, key, value)?;
        self.inner.set(key, &ct).await
    }

    async fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.inner.delete(key).await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Txn for EncryptedTxn {
    async fn commit(self: Box<Self>) -> Result<()> {
        self.inner.commit().await
    }

    fn discard(self: Box<Self>) {
        self.inner.discard()
    }

    fn on_success(&mut self, callback: TxnCallback) {
        self.inner.on_success(callback)
    }

    fn on_success_async(&mut self, callback: AsyncTxnCallback) {
        self.inner.on_success_async(callback)
    }

    fn on_error(&mut self, callback: TxnCallback) {
        self.inner.on_error(callback)
    }

    fn on_error_async(&mut self, callback: AsyncTxnCallback) {
        self.inner.on_error_async(callback)
    }

    fn on_discard(&mut self, callback: TxnCallback) {
        self.inner.on_discard(callback)
    }

    fn on_discard_async(&mut self, callback: AsyncTxnCallback) {
        self.inner.on_discard_async(callback)
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }

    fn is_readonly(&self) -> bool {
        self.inner.is_readonly()
    }

    fn callback_count(&self) -> usize {
        self.inner.callback_count()
    }
}

/// An [`Iterator`](KvIterator) wrapper that decrypts values, passing keys through.
struct EncryptedIterator {
    inner: Box<dyn KvIterator>,
    key: Zeroizing<[u8; ENCRYPTION_KEY_LEN]>,
    keys_only: bool,
}

impl Sealed for EncryptedIterator {}

impl EncryptedIterator {
    fn decrypt_pair(&self, pair: Option<KvPair>) -> Result<Option<KvPair>> {
        match pair {
            None => Ok(None),
            Some(pair) if self.keys_only => Ok(Some(pair)),
            Some(pair) => {
                let value = decrypt_value(&self.key, &pair.key, &pair.value)?;
                Ok(Some(KvPair::new(pair.key, value)))
            }
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl KvIterator for EncryptedIterator {
    async fn next(&mut self) -> Result<Option<KvPair>> {
        let pair = self.inner.next().await?;
        self.decrypt_pair(pair)
    }

    async fn close(&mut self) -> Result<()> {
        self.inner.close().await
    }

    async fn seek(&mut self, key: &[u8]) -> Result<bool> {
        self.inner.seek(key).await
    }

    async fn reset(&mut self) -> Result<()> {
        self.inner.reset().await
    }

    fn is_valid(&self) -> bool {
        self.inner.is_valid()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backends::MemoryStore;

    fn key_a() -> [u8; 32] {
        [7u8; 32]
    }

    fn key_b() -> [u8; 32] {
        [9u8; 32]
    }

    async fn set(store: &dyn Store, k: &[u8], v: &[u8]) {
        let mut txn = store.new_txn(false).await.unwrap();
        txn.set(k, v).await.unwrap();
        txn.commit().await.unwrap();
    }

    async fn get(store: &dyn Store, k: &[u8]) -> Option<Vec<u8>> {
        let txn = store.new_txn(true).await.unwrap();
        txn.get(k).await.unwrap()
    }

    /// Read a raw value directly from a backing store (bypassing decryption).
    async fn raw_get(store: &dyn Store, k: &[u8]) -> Option<Vec<u8>> {
        let txn = store.new_txn(true).await.unwrap();
        txn.get(k).await.unwrap()
    }

    #[tokio::test]
    async fn roundtrip_store_returns_plaintext_inner_holds_ciphertext() {
        let inner = MemoryStore::new();
        let enc = EncryptedStore::new(inner, key_a());

        // store-level set goes through the txn path; use a txn here, then read
        // the raw inner value to confirm it is ciphertext.
        let mut txn = enc.new_txn(false).await.unwrap();
        txn.set(b"k1", b"hello world").await.unwrap();
        txn.commit().await.unwrap();

        assert_eq!(get(&enc, b"k1").await, Some(b"hello world".to_vec()));

        // Raw inner value must be ciphertext: different and longer than plaintext.
        let raw = raw_get(&enc.inner, b"k1").await.unwrap();
        assert_ne!(raw, b"hello world");
        assert!(raw.len() > b"hello world".len());
    }

    #[tokio::test]
    async fn prefix_iteration_returns_keys_and_decrypted_values() {
        let enc = EncryptedStore::new(MemoryStore::new(), key_a());

        let mut txn = enc.new_txn(false).await.unwrap();
        txn.set(b"user:1", b"alice").await.unwrap();
        txn.set(b"user:2", b"bob").await.unwrap();
        txn.set(b"other:1", b"zzz").await.unwrap();
        txn.commit().await.unwrap();

        let ro = enc.new_txn(true).await.unwrap();
        let mut iter = ro
            .iterator(IterOptions::new().with_prefix(b"user:".to_vec()))
            .await
            .unwrap();
        let pairs = iter.collect_all().await.unwrap();

        assert_eq!(pairs.len(), 2);
        assert_eq!(pairs[0].key, b"user:1");
        assert_eq!(pairs[0].value, b"alice");
        assert_eq!(pairs[1].key, b"user:2");
        assert_eq!(pairs[1].value, b"bob");
    }

    #[tokio::test]
    async fn keys_only_iteration_passes_through() {
        let enc = EncryptedStore::new(MemoryStore::new(), key_a());
        set(&enc, b"k1", b"value").await;

        let ro = enc.new_txn(true).await.unwrap();
        let mut iter = ro
            .iterator(IterOptions::new().with_keys_only(true))
            .await
            .unwrap();
        let pairs = iter.collect_all().await.unwrap();
        assert_eq!(pairs.len(), 1);
        assert_eq!(pairs[0].key, b"k1");
        assert!(pairs[0].value.is_empty());
    }

    #[tokio::test]
    async fn txn_commit_persists_discard_rolls_back() {
        let enc = EncryptedStore::new(MemoryStore::new(), key_a());

        let mut txn = enc.new_txn(false).await.unwrap();
        txn.set(b"c", b"committed").await.unwrap();
        txn.commit().await.unwrap();
        assert_eq!(get(&enc, b"c").await, Some(b"committed".to_vec()));

        let mut txn = enc.new_txn(false).await.unwrap();
        txn.set(b"d", b"discarded").await.unwrap();
        txn.discard();
        assert_eq!(get(&enc, b"d").await, None);
    }

    #[tokio::test]
    async fn empty_value_roundtrips() {
        let enc = EncryptedStore::new(MemoryStore::new(), key_a());
        set(&enc, b"e", b"").await;
        assert_eq!(get(&enc, b"e").await, Some(b"".to_vec()));
    }

    #[tokio::test]
    async fn delete_removes_value() {
        let enc = EncryptedStore::new(MemoryStore::new(), key_a());
        set(&enc, b"k", b"v").await;
        assert_eq!(get(&enc, b"k").await, Some(b"v".to_vec()));

        let mut txn = enc.new_txn(false).await.unwrap();
        txn.delete(b"k").await.unwrap();
        txn.commit().await.unwrap();
        assert_eq!(get(&enc, b"k").await, None);
    }

    #[tokio::test]
    async fn wrong_key_fails_loudly() {
        // Encrypt with key A into a shared inner store, then read via key B.
        let inner = MemoryStore::new();
        {
            let enc_a = EncryptedStore::new(inner.clone(), key_a());
            set(&enc_a, b"k", b"secret").await;
        }
        let enc_b = EncryptedStore::new(inner, key_b());
        let txn = enc_b.new_txn(true).await.unwrap();
        let err = txn.get(b"k").await.unwrap_err();
        assert!(
            matches!(err, Error::Other(ref m) if m.contains("decryption failed")),
            "expected loud decrypt error, got: {err:?}"
        );
    }

    #[tokio::test]
    async fn aad_binding_rejects_value_relocation() {
        // A ciphertext written under k1 must not decrypt when read under k2.
        let inner = MemoryStore::new();
        let enc = EncryptedStore::new(inner.clone(), key_a());
        set(&enc, b"k1", b"payload").await;

        // Relocate the raw ciphertext from k1 to k2 directly in the inner store.
        let ct = raw_get(&inner, b"k1").await.unwrap();
        {
            let mut raw = inner.new_txn(false).await.unwrap();
            raw.set(b"k2", &ct).await.unwrap();
            raw.commit().await.unwrap();
        }

        let txn = enc.new_txn(true).await.unwrap();
        let err = txn.get(b"k2").await.unwrap_err();
        assert!(
            matches!(err, Error::Other(ref m) if m.contains("decryption failed")),
            "relocated value must fail AAD check, got: {err:?}"
        );
    }

    #[tokio::test]
    async fn get_size_returns_plaintext_length() {
        let enc = EncryptedStore::new(MemoryStore::new(), key_a());
        set(&enc, b"k", b"0123456789").await;
        let txn = enc.new_txn(true).await.unwrap();
        assert_eq!(txn.get_size(b"k").await.unwrap(), Some(10));
    }
}
