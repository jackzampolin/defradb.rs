//! FullTextIndex implementation using BM25 scoring
//!
//! Stores an inverted index mapping terms to doc short IDs with term frequency
//! and field length data for BM25 scoring at query time.
//!
//! Key layout:
//!   Posting:  /[col_id]/[idx_id]/[term]/[doc_short_id] -> [term_freq, field_len]
//!   Legacy stats: /[col_id]/[idx_id]/_stats             -> [total_docs, total_field_len]
//!   Stats shard:  /[col_id]/[idx_id]/0xff/s/[shard]     -> [doc_delta, field_len_delta]

use async_trait::async_trait;
use bm25::{DefaultTokenizer, Language, Tokenizer};
use bytes::Bytes;
use document::NormalValue;
use schema::{FullTextIndexDescription, IndexDescription};
use std::collections::HashMap;

use super::validate_doc_short_id;
use super::CollectionIndex;
use crate::corekv::{IterOptions, MaybeSend, Reader, Result, Writer};
use crate::keys::doc_id_index::{decode_doc_short_id, encode_doc_short_id};

const METADATA_PREFIX: u8 = 0xff;
const STATS_SHARD_TAG: u8 = b's';
const STATS_SHARD_HASH: u64 = 0x9e37_79b9_7f4a_7c15;

/// Map a language string to the bm25 crate's Language enum.
pub fn parse_language(lang: &str) -> Language {
    match lang.to_lowercase().as_str() {
        "arabic" => Language::Arabic,
        "danish" => Language::Danish,
        "dutch" => Language::Dutch,
        "french" => Language::French,
        "german" => Language::German,
        "greek" => Language::Greek,
        "hungarian" => Language::Hungarian,
        "italian" => Language::Italian,
        "norwegian" => Language::Norwegian,
        "portuguese" => Language::Portuguese,
        "romanian" => Language::Romanian,
        "russian" => Language::Russian,
        "spanish" => Language::Spanish,
        "swedish" => Language::Swedish,
        "tamil" => Language::Tamil,
        "turkish" => Language::Turkish,
        _ => Language::English,
    }
}

/// A BM25 full-text search index.
///
/// Maintains an inverted index of terms to document postings, plus global
/// corpus statistics (total docs and total field length) for BM25 scoring.
pub struct FullTextIndex {
    collection_short_id: u32,
    desc: IndexDescription,
    ft_desc: FullTextIndexDescription,
    tokenizer: DefaultTokenizer,
}

impl FullTextIndex {
    pub fn new(
        collection_short_id: u32,
        desc: IndexDescription,
        ft_desc: FullTextIndexDescription,
    ) -> Self {
        let lang = parse_language(&ft_desc.language);
        let tokenizer = DefaultTokenizer::new(lang);
        Self {
            collection_short_id,
            desc,
            ft_desc,
            tokenizer,
        }
    }

    pub fn k1(&self) -> f64 {
        self.ft_desc.k1
    }

    pub fn b(&self) -> f64 {
        self.ft_desc.b
    }

    pub fn ft_description(&self) -> &FullTextIndexDescription {
        &self.ft_desc
    }

    fn index_prefix(&self) -> Vec<u8> {
        let mut prefix = Vec::new();
        prefix.extend_from_slice(&self.collection_short_id.to_be_bytes());
        prefix.push(b'/');
        prefix.extend_from_slice(&self.desc.id.to_be_bytes());
        prefix.push(b'/');
        prefix
    }

    fn posting_key(&self, term: &str, doc_short_id: u64) -> Vec<u8> {
        let mut key = self.index_prefix();
        key.extend_from_slice(term.as_bytes());
        key.push(b'/');
        key.extend_from_slice(&encode_doc_short_id(doc_short_id));
        key
    }

    fn stats_key(&self) -> Vec<u8> {
        let mut key = self.index_prefix();
        key.extend_from_slice(b"_stats");
        key
    }

    fn metadata_prefix(&self, tag: u8) -> Vec<u8> {
        let mut key = self.index_prefix();
        // Terms are UTF-8 strings, so 0xff reserves a disjoint metadata namespace.
        key.extend_from_slice(&[METADATA_PREFIX, tag, b'/']);
        key
    }

    fn stats_shard_prefix(&self) -> Vec<u8> {
        self.metadata_prefix(STATS_SHARD_TAG)
    }

    fn stats_shard_key(&self, doc_short_id: u64) -> Vec<u8> {
        let mut key = self.stats_shard_prefix();
        key.push((doc_short_id.wrapping_mul(STATS_SHARD_HASH) >> (u64::BITS - u8::BITS)) as u8);
        key
    }

    /// Tokenize text and return term frequencies.
    fn tokenize_with_freqs(&self, text: &str) -> HashMap<String, u32> {
        let tokens = self.tokenizer.tokenize(text);
        let mut freqs = HashMap::new();
        for token in tokens {
            *freqs.entry(token).or_insert(0u32) += 1;
        }
        freqs
    }

    fn decode_stats(value: Option<Bytes>) -> (u64, u64) {
        match value {
            Some(bytes) if bytes.len() == 16 => {
                let total_docs = u64::from_be_bytes(bytes[0..8].try_into().unwrap());
                let total_field_len = u64::from_be_bytes(bytes[8..16].try_into().unwrap());
                (total_docs, total_field_len)
            }
            _ => (0, 0),
        }
    }

    fn decode_stats_delta(value: Option<Bytes>) -> (i128, i128) {
        match value {
            Some(bytes) if bytes.len() == 32 => {
                let docs = i128::from_be_bytes(bytes[0..16].try_into().unwrap());
                let field_len = i128::from_be_bytes(bytes[16..32].try_into().unwrap());
                (docs, field_len)
            }
            _ => (0, 0),
        }
    }

    async fn apply_stats_delta<T: Reader + Writer + MaybeSend>(
        &self,
        txn: &mut T,
        doc_short_id: u64,
        docs_delta: i128,
        field_len_delta: i128,
    ) -> Result<()> {
        let key = self.stats_shard_key(doc_short_id);
        let (docs, field_len) = Self::decode_stats_delta(txn.get(&key).await?);
        let docs = docs
            .checked_add(docs_delta)
            .ok_or_else(|| crate::corekv::Error::Other("full-text stats overflow".to_string()))?;
        let field_len = field_len
            .checked_add(field_len_delta)
            .ok_or_else(|| crate::corekv::Error::Other("full-text stats overflow".to_string()))?;
        let mut value = Vec::with_capacity(32);
        value.extend_from_slice(&docs.to_be_bytes());
        value.extend_from_slice(&field_len.to_be_bytes());
        txn.set(&key, &value).await
    }

    async fn read_stats<R: Reader + MaybeSend>(&self, txn: &R) -> Result<(u64, u64)> {
        let (legacy_docs, legacy_field_len) = Self::decode_stats(txn.get(&self.stats_key()).await?);
        let mut total_docs = i128::from(legacy_docs);
        let mut total_field_len = i128::from(legacy_field_len);
        let prefix = self.stats_shard_prefix();
        let mut iter = txn
            .iterator(IterOptions::new().with_prefix(prefix.clone()))
            .await?;
        while let Some(kv) = iter.next().await? {
            if kv.key.len() != prefix.len() + 1 {
                continue;
            }
            let (shard_docs, shard_field_len) = Self::decode_stats_delta(Some(kv.value));
            total_docs = total_docs.checked_add(shard_docs).ok_or_else(|| {
                crate::corekv::Error::Other("full-text stats overflow".to_string())
            })?;
            total_field_len = total_field_len
                .checked_add(shard_field_len)
                .ok_or_else(|| {
                    crate::corekv::Error::Other("full-text stats overflow".to_string())
                })?;
        }
        let total_docs = u64::try_from(total_docs.max(0))
            .map_err(|_| crate::corekv::Error::Other("full-text stats overflow".to_string()))?;
        let total_field_len = u64::try_from(total_field_len.max(0))
            .map_err(|_| crate::corekv::Error::Other("full-text stats overflow".to_string()))?;
        Ok((total_docs, total_field_len))
    }

    fn extract_text(values: &[NormalValue]) -> &str {
        if let Some(NormalValue::String(s)) = values.first() {
            s.as_str()
        } else {
            ""
        }
    }

    /// Write posting entries for a document's tokenized text.
    async fn write_postings<T: Reader + Writer + MaybeSend>(
        &self,
        txn: &mut T,
        doc_short_id: u64,
        text: &str,
    ) -> Result<u64> {
        let freqs = self.tokenize_with_freqs(text);
        let field_len = freqs.values().sum::<u32>() as u64;
        for (term, freq) in &freqs {
            let key = self.posting_key(term, doc_short_id);
            let mut value = Vec::with_capacity(12);
            value.extend_from_slice(&freq.to_be_bytes());
            value.extend_from_slice(&field_len.to_be_bytes());
            txn.set(&key, &value).await?;
        }
        Ok(field_len)
    }

    /// Remove all posting entries for a document's tokenized text.
    async fn remove_postings<T: Reader + Writer + MaybeSend>(
        &self,
        txn: &mut T,
        doc_short_id: u64,
        text: &str,
    ) -> Result<u64> {
        let freqs = self.tokenize_with_freqs(text);
        let field_len = freqs.values().sum::<u32>() as u64;
        for term in freqs.keys() {
            let key = self.posting_key(term, doc_short_id);
            txn.delete(&key).await?;
        }
        Ok(field_len)
    }

    /// Search the index for documents matching query terms.
    /// Returns Vec of (doc_short_id, Vec<(term, term_freq, field_len)>).
    pub async fn search<R: Reader + MaybeSend>(
        &self,
        txn: &R,
        query: &str,
    ) -> Result<Vec<(u64, Vec<(String, u32, u64)>)>> {
        let query_terms = self.tokenizer.tokenize(query);
        let mut doc_postings: HashMap<u64, Vec<(String, u32, u64)>> = HashMap::new();

        for term in &query_terms {
            let mut key_prefix = self.index_prefix();
            key_prefix.extend_from_slice(term.as_bytes());
            key_prefix.push(b'/');

            let opts = IterOptions::default().with_prefix(key_prefix.clone());
            let mut iter = txn.iterator(opts).await?;
            let items = iter.collect_all().await?;

            for kv in items {
                let Ok(doc_short_id) = decode_doc_short_id(&kv.key[key_prefix.len()..]) else {
                    continue;
                };
                if kv.value.len() == 12 {
                    let freq = u32::from_be_bytes(kv.value[0..4].try_into().unwrap());
                    let field_len = u64::from_be_bytes(kv.value[4..12].try_into().unwrap());
                    doc_postings.entry(doc_short_id).or_default().push((
                        term.clone(),
                        freq,
                        field_len,
                    ));
                }
            }
        }

        Ok(doc_postings.into_iter().collect())
    }

    /// Get corpus statistics for BM25 scoring.
    pub async fn stats<R: Reader + MaybeSend>(&self, txn: &R) -> Result<(u64, f64)> {
        let (total_docs, total_field_len) = self.read_stats(txn).await?;
        let avg_field_len = if total_docs > 0 {
            total_field_len as f64 / total_docs as f64
        } else {
            0.0
        };
        Ok((total_docs, avg_field_len))
    }

    /// Count how many documents contain a given term.
    pub async fn doc_freq<R: Reader + MaybeSend>(&self, txn: &R, term: &str) -> Result<u64> {
        let mut key_prefix = self.index_prefix();
        key_prefix.extend_from_slice(term.as_bytes());
        key_prefix.push(b'/');

        let opts = IterOptions::default().with_prefix(key_prefix);
        let mut iter = txn.iterator(opts).await?;
        let items = iter.collect_all().await?;
        Ok(items.len() as u64)
    }

    /// Compute BM25 scores for all documents matching the query.
    ///
    /// Uses the stored inverted index and corpus statistics to compute full
    /// BM25 scores without re-tokenizing any document text.
    pub async fn search_scored<R: Reader + MaybeSend>(
        &self,
        txn: &R,
        query: &str,
    ) -> Result<HashMap<u64, f64>> {
        let query_terms = self.tokenizer.tokenize(query);
        if query_terms.is_empty() {
            return Ok(HashMap::new());
        }

        let (total_docs, avg_field_len) = self.stats(txn).await?;
        if total_docs == 0 {
            return Ok(HashMap::new());
        }

        let n = total_docs as f64;
        let avgdl = avg_field_len;
        let k1 = self.k1();
        let b = self.b();

        let mut scores: HashMap<u64, f64> = HashMap::new();

        for term in &query_terms {
            let mut key_prefix = self.index_prefix();
            key_prefix.extend_from_slice(term.as_bytes());
            key_prefix.push(b'/');

            let opts = IterOptions::default().with_prefix(key_prefix.clone());
            let mut iter = txn.iterator(opts).await?;
            let items = iter.collect_all().await?;

            let df = items.len() as f64;
            let idf = ((n - df + 0.5) / (df + 0.5) + 1.0).ln();

            for kv in &items {
                if kv.value.len() == 12 {
                    let Ok(doc_short_id) = decode_doc_short_id(&kv.key[key_prefix.len()..]) else {
                        continue;
                    };
                    let tf = u32::from_be_bytes(kv.value[0..4].try_into().unwrap()) as f64;
                    let dl = u64::from_be_bytes(kv.value[4..12].try_into().unwrap()) as f64;

                    let denom = if avgdl > 0.0 {
                        tf + k1 * (1.0 - b + b * dl / avgdl)
                    } else {
                        tf + k1
                    };
                    let tf_norm = (tf * (k1 + 1.0)) / denom;

                    *scores.entry(doc_short_id).or_insert(0.0) += idf * tf_norm;
                }
            }
        }

        Ok(scores)
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl CollectionIndex for FullTextIndex {
    fn description(&self) -> &IndexDescription {
        &self.desc
    }

    async fn save<T: Reader + Writer + MaybeSend>(
        &self,
        txn: &mut T,
        doc_short_id: u64,
        values: &[NormalValue],
    ) -> Result<()> {
        validate_doc_short_id(doc_short_id, &self.desc.name)?;
        let text = Self::extract_text(values);
        if text.is_empty() {
            return Ok(());
        }
        let field_len = self.write_postings(txn, doc_short_id, text).await?;
        self.apply_stats_delta(txn, doc_short_id, 1, i128::from(field_len))
            .await
    }

    async fn update<T: Reader + Writer + MaybeSend>(
        &self,
        txn: &mut T,
        doc_short_id: u64,
        old_values: &[NormalValue],
        new_values: &[NormalValue],
    ) -> Result<()> {
        validate_doc_short_id(doc_short_id, &self.desc.name)?;
        let old_text = Self::extract_text(old_values);
        let new_text = Self::extract_text(new_values);
        if old_text == new_text {
            return Ok(());
        }

        let old_field_len = if old_text.is_empty() {
            0
        } else {
            self.remove_postings(txn, doc_short_id, old_text).await?
        };
        let new_field_len = if new_text.is_empty() {
            0
        } else {
            self.write_postings(txn, doc_short_id, new_text).await?
        };
        let docs_delta =
            i128::from(u8::from(!new_text.is_empty())) - i128::from(u8::from(!old_text.is_empty()));
        let field_len_delta = i128::from(new_field_len) - i128::from(old_field_len);
        if docs_delta != 0 || field_len_delta != 0 {
            self.apply_stats_delta(txn, doc_short_id, docs_delta, field_len_delta)
                .await?;
        }
        Ok(())
    }

    async fn delete<T: Reader + Writer + MaybeSend>(
        &self,
        txn: &mut T,
        doc_short_id: u64,
        values: &[NormalValue],
    ) -> Result<()> {
        validate_doc_short_id(doc_short_id, &self.desc.name)?;
        let text = Self::extract_text(values);
        if text.is_empty() {
            return Ok(());
        }
        let field_len = self.remove_postings(txn, doc_short_id, text).await?;
        self.apply_stats_delta(txn, doc_short_id, -1, -i128::from(field_len))
            .await
    }

    async fn remove_all<T: Reader + Writer + MaybeSend>(&self, txn: &mut T) -> Result<()> {
        let prefix = self.index_prefix();
        let opts = IterOptions::default().with_prefix(prefix);
        let mut iter = txn.iterator(opts).await?;
        let items = iter.collect_all().await?;
        for kv in items {
            txn.delete(&kv.key).await?;
        }
        let stats_key = self.stats_key();
        txn.delete(&stats_key).await?;
        Ok(())
    }
}
