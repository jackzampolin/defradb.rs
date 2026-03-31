//! Generic derived search-chunk helpers.
//!
//! These utilities intentionally stay source-agnostic. A producer can insert
//! any logical source document it wants, then materialize one or more search
//! chunks from selected text fields under the hood.

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchChunkingConfig {
    pub max_chars: usize,
    pub overlap_chars: usize,
}

impl Default for SearchChunkingConfig {
    fn default() -> Self {
        Self {
            max_chars: 640,
            overlap_chars: 96,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DerivedSearchChunk {
    pub chunk_id: String,
    pub chunk_index: usize,
    pub chunk_count: usize,
    pub content: String,
}

pub fn derive_search_chunks(
    parent_key: &str,
    source_field: &str,
    content: &str,
    config: &SearchChunkingConfig,
) -> Vec<DerivedSearchChunk> {
    let content = content.trim();
    if content.is_empty() {
        return Vec::new();
    }

    let max_chars = config.max_chars.max(1);
    let overlap_chars = config.overlap_chars.min(max_chars.saturating_sub(1));
    let chars = content.chars().collect::<Vec<_>>();
    if chars.len() <= max_chars {
        return vec![DerivedSearchChunk {
            chunk_id: chunk_id(parent_key, source_field, 0),
            chunk_index: 0,
            chunk_count: 1,
            content: content.to_string(),
        }];
    }

    let mut raw_chunks = Vec::new();
    let mut start = 0usize;
    while start < chars.len() {
        let hard_end = (start + max_chars).min(chars.len());
        let mut end = hard_end;
        if hard_end < chars.len() {
            let soft_start = start + ((hard_end - start) * 3 / 5);
            if let Some(boundary) = find_boundary(&chars, soft_start, hard_end) {
                end = boundary;
            }
        }

        let segment = chars[start..end].iter().collect::<String>();
        let segment = segment.trim();
        if !segment.is_empty() {
            raw_chunks.push(segment.to_string());
        }

        if end >= chars.len() {
            break;
        }

        let mut next_start = end.saturating_sub(overlap_chars);
        while next_start < chars.len() && chars[next_start].is_whitespace() {
            next_start += 1;
        }
        if next_start <= start {
            next_start = end;
        }
        start = next_start;
    }

    let chunk_count = raw_chunks.len();
    raw_chunks
        .into_iter()
        .enumerate()
        .map(|(chunk_index, content)| DerivedSearchChunk {
            chunk_id: chunk_id(parent_key, source_field, chunk_index),
            chunk_index,
            chunk_count,
            content,
        })
        .collect()
}

fn find_boundary(chars: &[char], start: usize, end: usize) -> Option<usize> {
    for index in (start..end).rev() {
        if matches!(chars[index], '\n' | '\r')
            || chars[index].is_whitespace()
            || matches!(chars[index], '.' | ',' | ';' | ':' | ')' | ']' | '}')
        {
            return Some(index + 1);
        }
    }
    None
}

fn chunk_id(parent_key: &str, source_field: &str, chunk_index: usize) -> String {
    format!(
        "{}:{}:{chunk_index:04}",
        sanitize_id_component(parent_key),
        sanitize_id_component(source_field),
    )
}

fn sanitize_id_component(value: &str) -> String {
    let mut sanitized = value
        .chars()
        .map(|char| {
            if char.is_ascii_alphanumeric() || matches!(char, '-' | '_') {
                char
            } else {
                '-'
            }
        })
        .collect::<String>();
    sanitized.truncate(48);
    sanitized.trim_matches('-').to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn derive_search_chunks_returns_single_chunk_for_small_content() {
        let chunks = derive_search_chunks(
            "message-1",
            "content",
            "short assistant reply",
            &SearchChunkingConfig::default(),
        );

        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].chunk_index, 0);
        assert_eq!(chunks[0].chunk_count, 1);
        assert_eq!(chunks[0].content, "short assistant reply");
    }

    #[test]
    fn derive_search_chunks_splits_large_content_deterministically() {
        let content = (0..40)
            .map(|index| format!("chunk-token-{index:02}"))
            .collect::<Vec<_>>()
            .join(" ");
        let config = SearchChunkingConfig {
            max_chars: 80,
            overlap_chars: 16,
        };

        let first = derive_search_chunks("message-1", "content", &content, &config);
        let second = derive_search_chunks("message-1", "content", &content, &config);

        assert_eq!(first, second);
        assert!(first.len() > 1);
        assert_eq!(first[0].chunk_index, 0);
        assert_eq!(first.last().unwrap().chunk_count, first.len());
    }
}
