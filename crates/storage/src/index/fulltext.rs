//! FullTextIndex implementation using BM25 scoring
//!
//! Stores an inverted index mapping terms to document IDs with term frequency
//! and field length data for BM25 scoring at query time.
//!
//! Key layout:
//!   Posting:  /[col_id]/[idx_id]/[term]/[doc_id] -> [term_freq, field_len]
//!   Stats:    /[col_id]/[idx_id]/_stats           -> [total_docs, total_field_len]

use async_trait::async_trait;
use bm25::{DefaultTokenizer, Language, Tokenizer};
use document::NormalValue;
use schema::{FullTextIndexDescription, IndexDescription};
use std::collections::HashMap;

use super::validate_doc_id;
use super::CollectionIndex;
use crate::corekv::{IterOptions, MaybeSend, Reader, Result, Writer};

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

    fn posting_key(&self, term: &str, doc_id: &str) -> Vec<u8> {
        let mut key = self.index_prefix();
        key.extend_from_slice(term.as_bytes());
        key.push(b'/');
        key.extend_from_slice(doc_id.as_bytes());
        key
    }

    fn stats_key(&self) -> Vec<u8> {
        let mut key = self.index_prefix();
        key.extend_from_slice(b"_stats");
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

    async fn read_stats<R: Reader + MaybeSend>(&self, txn: &R) -> Result<(u64, u64)> {
        let key = self.stats_key();
        match txn.get(&key).await? {
            Some(bytes) if bytes.len() == 16 => {
                let total_docs = u64::from_be_bytes(bytes[0..8].try_into().unwrap());
                let total_field_len = u64::from_be_bytes(bytes[8..16].try_into().unwrap());
                Ok((total_docs, total_field_len))
            }
            _ => Ok((0, 0)),
        }
    }

    async fn write_stats<T: Writer + MaybeSend>(
        &self,
        txn: &mut T,
        total_docs: u64,
        total_field_len: u64,
    ) -> Result<()> {
        let key = self.stats_key();
        let mut value = Vec::with_capacity(16);
        value.extend_from_slice(&total_docs.to_be_bytes());
        value.extend_from_slice(&total_field_len.to_be_bytes());
        txn.set(&key, &value).await
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
        doc_id: &str,
        text: &str,
    ) -> Result<u64> {
        let freqs = self.tokenize_with_freqs(text);
        let field_len = freqs.values().sum::<u32>() as u64;
        for (term, freq) in &freqs {
            let key = self.posting_key(term, doc_id);
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
        doc_id: &str,
        text: &str,
    ) -> Result<u64> {
        let freqs = self.tokenize_with_freqs(text);
        let field_len = freqs.values().sum::<u32>() as u64;
        for term in freqs.keys() {
            let key = self.posting_key(term, doc_id);
            txn.delete(&key).await?;
        }
        Ok(field_len)
    }

    /// Search the index for documents matching query terms.
    /// Returns Vec of (doc_id, Vec<(term, term_freq, field_len)>).
    pub async fn search<R: Reader + MaybeSend>(
        &self,
        txn: &R,
        query: &str,
    ) -> Result<Vec<(String, Vec<(String, u32, u64)>)>> {
        let query_terms = self.tokenizer.tokenize(query);
        let mut doc_postings: HashMap<String, Vec<(String, u32, u64)>> = HashMap::new();

        for term in &query_terms {
            let mut key_prefix = self.index_prefix();
            key_prefix.extend_from_slice(term.as_bytes());
            key_prefix.push(b'/');

            let opts = IterOptions::default().with_prefix(key_prefix.clone());
            let mut iter = txn.iterator(opts).await?;
            let items = iter.collect_all().await?;

            for kv in items {
                let doc_id_bytes = &kv.key[key_prefix.len()..];
                let doc_id = String::from_utf8_lossy(doc_id_bytes).to_string();
                if kv.value.len() == 12 {
                    let freq = u32::from_be_bytes(kv.value[0..4].try_into().unwrap());
                    let field_len = u64::from_be_bytes(kv.value[4..12].try_into().unwrap());
                    doc_postings
                        .entry(doc_id)
                        .or_default()
                        .push((term.clone(), freq, field_len));
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
        doc_id: &str,
        values: &[NormalValue],
    ) -> Result<()> {
        validate_doc_id(doc_id, &self.desc.name)?;
        let text = Self::extract_text(values);
        if text.is_empty() {
            return Ok(());
        }
        let (total_docs, total_field_len) = self.read_stats(txn).await?;
        let field_len = self.write_postings(txn, doc_id, text).await?;
        self.write_stats(txn, total_docs + 1, total_field_len + field_len)
            .await
    }

    async fn update<T: Reader + Writer + MaybeSend>(
        &self,
        txn: &mut T,
        doc_id: &str,
        old_values: &[NormalValue],
        new_values: &[NormalValue],
    ) -> Result<()> {
        validate_doc_id(doc_id, &self.desc.name)?;
        let old_text = Self::extract_text(old_values);
        let new_text = Self::extract_text(new_values);
        let (mut total_docs, mut total_field_len) = self.read_stats(txn).await?;

        if !old_text.is_empty() {
            let old_field_len = self.remove_postings(txn, doc_id, old_text).await?;
            total_docs = total_docs.saturating_sub(1);
            total_field_len = total_field_len.saturating_sub(old_field_len);
        }

        if !new_text.is_empty() {
            let new_field_len = self.write_postings(txn, doc_id, new_text).await?;
            total_docs += 1;
            total_field_len += new_field_len;
        }

        self.write_stats(txn, total_docs, total_field_len).await
    }

    async fn delete<T: Reader + Writer + MaybeSend>(
        &self,
        txn: &mut T,
        doc_id: &str,
        values: &[NormalValue],
    ) -> Result<()> {
        validate_doc_id(doc_id, &self.desc.name)?;
        let text = Self::extract_text(values);
        if text.is_empty() {
            return Ok(());
        }
        let (total_docs, total_field_len) = self.read_stats(txn).await?;
        let field_len = self.remove_postings(txn, doc_id, text).await?;
        self.write_stats(
            txn,
            total_docs.saturating_sub(1),
            total_field_len.saturating_sub(field_len),
        )
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
