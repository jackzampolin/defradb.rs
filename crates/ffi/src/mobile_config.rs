//! Configuration types for mobile FFI wrappers.

use std::ffi::{c_char, CString};
use std::ptr;

use crate::types::FfiResult;

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MobileNodeConfig {
    pub db_path: Option<String>,
    pub in_memory: Option<bool>,
    pub datastore_backend: Option<String>,
    pub signing: Option<MobileSigningConfig>,
    pub default_identity_did: Option<String>,
    pub sourcehub: Option<MobileSourceHubConfig>,
    pub p2p: Option<MobileP2pConfig>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MobileSigningConfig {
    pub enable: Option<bool>,
    pub key_type: Option<String>,
    pub private_key_hex: Option<String>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MobileSourceHubConfig {
    pub grpc_address: String,
    pub comet_rpc_address: String,
    pub chain_id: String,
    pub signer_key_hex: String,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MobileP2pConfig {
    pub transport: Option<String>,
    pub listen_address: Option<String>,
    pub iroh: Option<MobileIrohConfig>,
    pub max_concurrent_dag_fetches: Option<usize>,
    pub max_concurrent_push_tasks: Option<usize>,
    pub max_doc_sync_request_doc_ids: Option<usize>,
    pub rate_limit_burst: Option<u32>,
    pub rate_limit_rate: Option<f64>,
}

#[derive(Debug, Default, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MobileIrohConfig {
    pub relay_mode: Option<String>,
    pub relay_url: Option<String>,
    pub relay_urls: Option<Vec<String>>,
    pub bind_address: Option<String>,
    pub bind_port: Option<u16>,
    pub discovery: Option<bool>,
    pub discovery_origin_domain: Option<String>,
    pub pkarr_relay_url: Option<String>,
    pub key_path: Option<String>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MobileExecuteRequest {
    pub identity_did: Option<String>,
    pub query: String,
    pub operation_name: Option<String>,
    pub variables: Option<serde_json::Value>,
    pub batch_session_id: Option<String>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MobileSyncRequest {
    pub identity_did: Option<String>,
    pub collection_id: Option<String>,
    pub collection_name: Option<String>,
    pub doc_ids: Option<Vec<String>>,
    pub version_ids: Option<Vec<String>>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MobileAddReplicatorRequest {
    pub identity_did: Option<String>,
    pub collections: Vec<String>,
    #[serde(alias = "address")]
    pub peer_addr: String,
    pub filters: Option<defra_http::router::ReplicationFilters>,
}

pub(crate) fn maybe_cstring(
    value: Option<&str>,
    field_name: &str,
) -> Result<Option<CString>, String> {
    value
        .map(|value| {
            CString::new(value)
                .map_err(|_| format!("{} contains an embedded null byte", field_name))
        })
        .transpose()
}

pub(crate) fn decode_hex_field(value: Option<&str>, field_name: &str) -> Result<Vec<u8>, String> {
    match value {
        Some(value) if !value.is_empty() => {
            hex::decode(value).map_err(|error| format!("invalid {} hex: {}", field_name, error))
        }
        _ => Ok(Vec::new()),
    }
}

pub(crate) fn c_string_ptr(value: &Option<CString>) -> *const c_char {
    value.as_ref().map_or(ptr::null(), |value| value.as_ptr())
}

pub(crate) fn ffi_result_error(result: FfiResult) -> String {
    let message = if result.error.is_null() {
        "unknown FFI error".to_string()
    } else {
        unsafe { std::ffi::CStr::from_ptr(result.error) }
            .to_string_lossy()
            .into_owned()
    };

    unsafe {
        if !result.error.is_null() {
            crate::types::defra_free_string(result.error);
        }
        if !result.value.is_null() {
            crate::types::defra_free_string(result.value);
        }
    }

    message
}
