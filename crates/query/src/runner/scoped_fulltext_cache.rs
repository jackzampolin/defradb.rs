use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::Arc;

use bm25::{DefaultTokenizer as LegacyTokenizer, Language, Tokenizer as LegacyTokenizerTrait};
use bm25_turbo::{BM25Builder, BM25Index, Method};
use lru::LruCache;
use parking_lot::Mutex;
use serde_json::Value as JsonValue;
use sha2::{Digest, Sha256};

const DEFAULT_SCOPED_FULLTEXT_CACHE_CAPACITY: usize = 64;

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub(crate) struct ScopedFulltextCacheKey {
    scope_fingerprint: [u8; 32],
    target_field: String,
}

pub(crate) struct ScopedFulltextCacheEntry {
    doc_ids: Vec<String>,
    index: Option<BM25Index>,
}

pub(crate) struct ScopedFulltextCache {
    tokenizer: LegacyTokenizer,
    entries: Mutex<LruCache<ScopedFulltextCacheKey, Arc<ScopedFulltextCacheEntry>>>,
}

impl ScopedFulltextCache {
    pub(crate) fn new() -> Self {
        let capacity =
            NonZeroUsize::new(DEFAULT_SCOPED_FULLTEXT_CACHE_CAPACITY).expect("cache capacity > 0");
        Self {
            tokenizer: LegacyTokenizer::new(Language::English),
            entries: Mutex::new(LruCache::new(capacity)),
        }
    }

    pub(crate) fn clear(&self) {
        self.entries.lock().clear();
    }

    pub(crate) fn search(
        &self,
        items: &[JsonValue],
        target_field: &str,
        query: &str,
    ) -> HashMap<String, f64> {
        if items.is_empty() || query.trim().is_empty() {
            return HashMap::new();
        }

        let key = ScopedFulltextCacheKey {
            scope_fingerprint: scope_fingerprint(items),
            target_field: target_field.to_string(),
        };

        let entry = {
            let mut entries = self.entries.lock();
            if let Some(entry) = entries.get(&key) {
                Arc::clone(entry)
            } else {
                let entry = Arc::new(self.build_entry(items, target_field));
                entries.put(key, Arc::clone(&entry));
                entry
            }
        };

        let Some(index) = &entry.index else {
            return HashMap::new();
        };

        let query_tokens = self.tokenizer.tokenize(query);
        if query_tokens.is_empty() {
            return HashMap::new();
        }

        let Ok(results) = index.search_tokens(&query_tokens, entry.doc_ids.len()) else {
            return HashMap::new();
        };

        results
            .doc_ids
            .into_iter()
            .zip(results.scores.into_iter())
            .filter_map(|(doc_index, score)| {
                entry
                    .doc_ids
                    .get(doc_index as usize)
                    .map(|doc_id| (doc_id.clone(), score as f64))
            })
            .collect()
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.entries.lock().len()
    }

    fn build_entry(&self, items: &[JsonValue], target_field: &str) -> ScopedFulltextCacheEntry {
        let mut doc_ids = Vec::new();
        let mut tokenized_docs = Vec::new();

        for item in items {
            let Some(obj) = item.as_object() else {
                continue;
            };
            let Some(doc_id) = obj.get("_docID").and_then(|value| value.as_str()) else {
                continue;
            };
            let Some(contents) = obj.get(target_field).and_then(|value| value.as_str()) else {
                continue;
            };
            if contents.trim().is_empty() {
                continue;
            }

            let tokens = self.tokenizer.tokenize(contents);
            if tokens.is_empty() {
                continue;
            }

            doc_ids.push(doc_id.to_string());
            tokenized_docs.push(tokens);
        }

        let index = if tokenized_docs.is_empty() {
            None
        } else {
            BM25Builder::new()
                .method(Method::Lucene)
                .k1(1.2)
                .b(0.75)
                .build_from_tokens(&tokenized_docs)
                .ok()
        };

        ScopedFulltextCacheEntry { doc_ids, index }
    }
}

fn scope_fingerprint(items: &[JsonValue]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    let mut doc_ids = items
        .iter()
        .filter_map(|item| {
            item.as_object()
                .and_then(|obj| obj.get("_docID"))
                .and_then(|value| value.as_str())
                .map(str::to_string)
        })
        .collect::<Vec<_>>();
    doc_ids.sort_unstable();

    hasher.update(doc_ids.len().to_le_bytes());

    for doc_id in doc_ids {
        hasher.update(doc_id.as_bytes());
        hasher.update([0xff]);
    }

    let digest = hasher.finalize();
    let mut fingerprint = [0u8; 32];
    fingerprint.copy_from_slice(&digest);
    fingerprint
}
