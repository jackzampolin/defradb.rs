//! Wire types for browser-to-server document synchronization.

use serde::{Deserialize, Serialize};

pub const MAX_SYNC_BODY_BYTES: usize = 33 * 1024 * 1024;
pub const MAX_SYNC_PAYLOAD_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_SYNC_BLOCK_BYTES: usize = 4 * 1024 * 1024;
pub const MAX_SYNC_BLOCKS_PER_DOCUMENT: usize = 4096;
pub const MAX_SYNC_DOCUMENTS_PER_REQUEST: usize = 32;
pub const MAX_SYNC_ROOTS_PER_DOCUMENT: usize = 16;
pub const MAX_SYNC_PULL_DOC_IDS: usize = 64;
pub const DEFAULT_SYNC_PAGE_SIZE: usize = 32;
pub const MAX_SYNC_PAGE_SIZE: usize = 64;
pub const MAX_SYNC_ID_BYTES: usize = 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowserSyncBlock {
    pub cid: String,
    pub data: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowserSyncDocument {
    pub doc_id: String,
    pub collection_id: String,
    pub roots: Vec<String>,
    pub blocks: Vec<BrowserSyncBlock>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowserSyncPull {
    #[serde(default)]
    pub doc_ids: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u16>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowserSyncRequest {
    #[serde(default)]
    pub documents: Vec<BrowserSyncDocument>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pull: Option<BrowserSyncPull>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowserSyncResponse {
    #[serde(default)]
    pub documents: Vec<BrowserSyncDocument>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}
