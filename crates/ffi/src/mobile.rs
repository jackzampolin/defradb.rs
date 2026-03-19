//! Thin mobile-oriented FFI wrappers for Swift/Xcode embedding.

use std::collections::HashSet;
use std::ffi::{c_char, CString};
use std::ptr;

use acp::nac::NodePermission;

use crate::helpers::get_rt;
use crate::nac_check::check_nac_for_node;
use crate::node::{new_node, node_close};
use crate::p2p::{
    new_node_with_p2p, p2p_connect, p2p_notify_network_change, p2p_peer_info,
    p2p_sync_branchable_collection, p2p_sync_collection_versions, p2p_sync_documents,
};
use crate::query::exec_request;
use crate::schema::validate_collection_policy;
use crate::state::NODES;
use crate::types::{c_str_to_string, defra_free_string, FfiResult, NewNodeResult, NodeInitOptions};
use crate::{ffi_async, ffi_entry, try_ffi, ERR_INVALID_NODE_HANDLE};

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct MobileNodeConfig {
    db_path: Option<String>,
    in_memory: Option<bool>,
    datastore_backend: Option<String>,
    signing: Option<MobileSigningConfig>,
    default_identity_did: Option<String>,
    sourcehub: Option<MobileSourceHubConfig>,
    p2p: Option<MobileP2pConfig>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct MobileSigningConfig {
    enable: Option<bool>,
    key_type: Option<String>,
    private_key_hex: Option<String>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct MobileSourceHubConfig {
    grpc_address: String,
    comet_rpc_address: String,
    chain_id: String,
    signer_key_hex: String,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct MobileP2pConfig {
    transport: Option<String>,
    listen_address: Option<String>,
    iroh: Option<MobileIrohConfig>,
}

#[derive(Debug, Default, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct MobileIrohConfig {
    relay_mode: Option<String>,
    relay_url: Option<String>,
    relay_urls: Option<Vec<String>>,
    bind_address: Option<String>,
    bind_port: Option<u16>,
    discovery: Option<bool>,
    discovery_origin_domain: Option<String>,
    pkarr_relay_url: Option<String>,
    key_path: Option<String>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct MobileExecuteRequest {
    identity_did: Option<String>,
    query: String,
    operation_name: Option<String>,
    variables: Option<serde_json::Value>,
    batch_session_id: Option<String>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct MobileSyncRequest {
    identity_did: Option<String>,
    collection_id: Option<String>,
    collection_name: Option<String>,
    doc_ids: Option<Vec<String>>,
    version_ids: Option<Vec<String>>,
}

fn maybe_cstring(value: Option<&str>, field_name: &str) -> Result<Option<CString>, String> {
    value
        .map(|value| {
            CString::new(value)
                .map_err(|_| format!("{} contains an embedded null byte", field_name))
        })
        .transpose()
}

fn decode_hex_field(value: Option<&str>, field_name: &str) -> Result<Vec<u8>, String> {
    match value {
        Some(value) if !value.is_empty() => {
            hex::decode(value).map_err(|error| format!("invalid {} hex: {}", field_name, error))
        }
        _ => Ok(Vec::new()),
    }
}

fn c_string_ptr(value: &Option<CString>) -> *const c_char {
    value.as_ref().map_or(ptr::null(), |value| value.as_ptr())
}

fn ffi_result_error(result: FfiResult) -> String {
    let message = if result.error.is_null() {
        "unknown FFI error".to_string()
    } else {
        unsafe { std::ffi::CStr::from_ptr(result.error) }
            .to_string_lossy()
            .into_owned()
    };

    unsafe {
        if !result.error.is_null() {
            defra_free_string(result.error);
        }
        if !result.value.is_null() {
            defra_free_string(result.value);
        }
    }

    message
}

fn default_identity_cstring(node_ptr: usize) -> Result<Option<CString>, String> {
    let Some(identity_did) = NODES.get(node_ptr, |state| state.node_identity_did.clone()) else {
        return Err(ERR_INVALID_NODE_HANDLE.to_string());
    };
    maybe_cstring(identity_did.as_deref(), "default identity")
}

/// Initialize the runtime for mobile embedding.
#[no_mangle]
pub extern "C" fn defra_mobile_init() -> FfiResult {
    ffi_entry! {
        crate::defra_init();
        FfiResult::success(serde_json::json!({ "version": env!("CARGO_PKG_VERSION") }).to_string())
    }
}

/// Open a node from a single JSON config blob.
#[allow(clippy::not_unsafe_ptr_arg_deref)]
#[no_mangle]
pub extern "C" fn defra_mobile_open_node(config_json: *const c_char) -> NewNodeResult {
    ffi_entry! {
        crate::defra_init();

        let config_str = match unsafe { c_str_to_string(config_json) } {
            Some(value) => value,
            None => return NewNodeResult::error("invalid config_json parameter"),
        };

        let config: MobileNodeConfig = match serde_json::from_str(&config_str) {
            Ok(config) => config,
            Err(error) => return NewNodeResult::error(format!("invalid config_json: {}", error)),
        };

        let db_path = match maybe_cstring(config.db_path.as_deref(), "dbPath") {
            Ok(value) => value,
            Err(error) => return NewNodeResult::error(error),
        };
        let datastore_backend =
            match maybe_cstring(config.datastore_backend.as_deref(), "datastoreBackend") {
                Ok(value) => value,
                Err(error) => return NewNodeResult::error(error),
            };

        let mut signing_key_type = None;
        let signing_key_bytes = if let Some(signing) = config.signing.as_ref() {
            if signing.private_key_hex.is_none() && signing.key_type.is_some() {
                return NewNodeResult::error(
                    "signing.privateKeyHex is required when signing.keyType is provided",
                );
            }
            signing_key_type = match maybe_cstring(signing.key_type.as_deref(), "signing.keyType")
            {
                Ok(value) => value,
                Err(error) => return NewNodeResult::error(error),
            };
            match decode_hex_field(signing.private_key_hex.as_deref(), "signing.privateKeyHex") {
                Ok(bytes) => bytes,
                Err(error) => return NewNodeResult::error(error),
            }
        } else {
            Vec::new()
        };
        let enable_signing = config
            .signing
            .as_ref()
            .and_then(|signing| signing.enable)
            .unwrap_or(!signing_key_bytes.is_empty());

        let (sourcehub_grpc_address, sourcehub_comet_rpc_address, sourcehub_chain_id, sourcehub_signer_key) =
            if let Some(sourcehub) = config.sourcehub.as_ref() {
                let grpc = match maybe_cstring(Some(sourcehub.grpc_address.as_str()), "sourcehub.grpcAddress") {
                    Ok(Some(value)) => Some(value),
                    Ok(None) => None,
                    Err(error) => return NewNodeResult::error(error),
                };
                let comet = match maybe_cstring(
                    Some(sourcehub.comet_rpc_address.as_str()),
                    "sourcehub.cometRpcAddress",
                ) {
                    Ok(Some(value)) => Some(value),
                    Ok(None) => None,
                    Err(error) => return NewNodeResult::error(error),
                };
                let chain = match maybe_cstring(Some(sourcehub.chain_id.as_str()), "sourcehub.chainId")
                {
                    Ok(Some(value)) => Some(value),
                    Ok(None) => None,
                    Err(error) => return NewNodeResult::error(error),
                };
                let signer_key =
                    match decode_hex_field(Some(sourcehub.signer_key_hex.as_str()), "sourcehub.signerKeyHex")
                    {
                        Ok(bytes) => bytes,
                        Err(error) => return NewNodeResult::error(error),
                    };
                (grpc, comet, chain, signer_key)
            } else {
                (None, None, None, Vec::new())
            };

        let p2p_transport_name = config
            .p2p
            .as_ref()
            .and_then(|p2p| p2p.transport.clone().or_else(|| p2p.iroh.as_ref().map(|_| "iroh".to_string())));
        let p2p_transport =
            match maybe_cstring(p2p_transport_name.as_deref(), "p2p.transport") {
                Ok(value) => value,
                Err(error) => return NewNodeResult::error(error),
            };
        let iroh_relay_url = match maybe_cstring(
            config
                .p2p
                .as_ref()
                .and_then(|p2p| p2p.iroh.as_ref())
                .and_then(|iroh| iroh.relay_url.as_deref()),
            "p2p.iroh.relayUrl",
        ) {
            Ok(value) => value,
            Err(error) => return NewNodeResult::error(error),
        };
        let iroh_relay_mode = match maybe_cstring(
            config
                .p2p
                .as_ref()
                .and_then(|p2p| p2p.iroh.as_ref())
                .and_then(|iroh| iroh.relay_mode.as_deref()),
            "p2p.iroh.relayMode",
        ) {
            Ok(value) => value,
            Err(error) => return NewNodeResult::error(error),
        };
        let iroh_relay_urls_json_string = match config
            .p2p
            .as_ref()
            .and_then(|p2p| p2p.iroh.as_ref())
            .and_then(|iroh| iroh.relay_urls.as_ref())
        {
            Some(urls) => match serde_json::to_string(urls) {
                Ok(value) => Some(value),
                Err(error) => {
                    return NewNodeResult::error(format!(
                        "failed to serialize p2p.iroh.relayUrls: {}",
                        error
                    ))
                }
            },
            None => None,
        };
        let iroh_relay_urls_json =
            match maybe_cstring(iroh_relay_urls_json_string.as_deref(), "p2p.iroh.relayUrls") {
                Ok(value) => value,
                Err(error) => return NewNodeResult::error(error),
            };
        let iroh_bind_addr = match maybe_cstring(
            config
                .p2p
                .as_ref()
                .and_then(|p2p| p2p.iroh.as_ref())
                .and_then(|iroh| iroh.bind_address.as_deref()),
            "p2p.iroh.bindAddress",
        ) {
            Ok(value) => value,
            Err(error) => return NewNodeResult::error(error),
        };
        let iroh_discovery_origin_domain = match maybe_cstring(
            config
                .p2p
                .as_ref()
                .and_then(|p2p| p2p.iroh.as_ref())
                .and_then(|iroh| iroh.discovery_origin_domain.as_deref()),
            "p2p.iroh.discoveryOriginDomain",
        ) {
            Ok(value) => value,
            Err(error) => return NewNodeResult::error(error),
        };
        let iroh_pkarr_relay_url = match maybe_cstring(
            config
                .p2p
                .as_ref()
                .and_then(|p2p| p2p.iroh.as_ref())
                .and_then(|iroh| iroh.pkarr_relay_url.as_deref()),
            "p2p.iroh.pkarrRelayUrl",
        ) {
            Ok(value) => value,
            Err(error) => return NewNodeResult::error(error),
        };
        let iroh_key_path = match maybe_cstring(
            config
                .p2p
                .as_ref()
                .and_then(|p2p| p2p.iroh.as_ref())
                .and_then(|iroh| iroh.key_path.as_deref()),
            "p2p.iroh.keyPath",
        ) {
            Ok(value) => value,
            Err(error) => return NewNodeResult::error(error),
        };
        let listen_address = match maybe_cstring(
            config
                .p2p
                .as_ref()
                .and_then(|p2p| p2p.listen_address.as_deref()),
            "p2p.listenAddress",
        ) {
            Ok(value) => value,
            Err(error) => return NewNodeResult::error(error),
        };

        let options = NodeInitOptions {
            db_path: c_string_ptr(&db_path),
            in_memory: i32::from(config.in_memory.unwrap_or_else(|| config.db_path.is_none())),
            datastore_backend: c_string_ptr(&datastore_backend),
            enable_signing: i32::from(enable_signing),
            signing_key_type: c_string_ptr(&signing_key_type),
            signing_private_key: if signing_key_bytes.is_empty() {
                ptr::null()
            } else {
                signing_key_bytes.as_ptr()
            },
            signing_private_key_len: signing_key_bytes.len(),
            sourcehub_grpc_address: c_string_ptr(&sourcehub_grpc_address),
            sourcehub_comet_rpc_address: c_string_ptr(&sourcehub_comet_rpc_address),
            sourcehub_chain_id: c_string_ptr(&sourcehub_chain_id),
            sourcehub_signer_key: if sourcehub_signer_key.is_empty() {
                ptr::null()
            } else {
                sourcehub_signer_key.as_ptr()
            },
            sourcehub_signer_key_len: sourcehub_signer_key.len(),
            p2p_transport: c_string_ptr(&p2p_transport),
            iroh_relay_url: c_string_ptr(&iroh_relay_url),
            iroh_relay_mode: c_string_ptr(&iroh_relay_mode),
            iroh_relay_urls_json: c_string_ptr(&iroh_relay_urls_json),
            iroh_bind_addr: c_string_ptr(&iroh_bind_addr),
            iroh_bind_port: config
                .p2p
                .as_ref()
                .and_then(|p2p| p2p.iroh.as_ref())
                .and_then(|iroh| iroh.bind_port)
                .unwrap_or_default(),
            iroh_discovery: i32::from(
                config
                    .p2p
                    .as_ref()
                    .and_then(|p2p| p2p.iroh.as_ref())
                    .and_then(|iroh| iroh.discovery)
                    .unwrap_or(true),
            ),
            iroh_discovery_origin_domain: c_string_ptr(&iroh_discovery_origin_domain),
            iroh_pkarr_relay_url: c_string_ptr(&iroh_pkarr_relay_url),
            iroh_key_path: c_string_ptr(&iroh_key_path),
        };

        let mut result = if config.p2p.is_some() {
            unsafe {
                new_node_with_p2p(
                    options,
                    listen_address
                        .as_ref()
                        .map_or(ptr::null(), |value| value.as_ptr()),
                )
            }
        } else {
            new_node(options)
        };

        if result.status != 0 {
            return result;
        }

        if let Some(default_identity_did) = config.default_identity_did.as_deref() {
            let did = match CString::new(default_identity_did) {
                Ok(value) => value,
                Err(_) => {
                    let _ = node_close(result.node_ptr);
                    return NewNodeResult::error("defaultIdentityDid contains an embedded null byte");
                }
            };
            let set_identity = crate::acp::node_set_default_identity(result.node_ptr, did.as_ptr());
            if set_identity.status != 0 {
                let error = ffi_result_error(set_identity);
                let _ = node_close(result.node_ptr);
                result = NewNodeResult::error(error);
            } else {
                unsafe {
                    if !set_identity.value.is_null() {
                        defra_free_string(set_identity.value);
                    }
                }
            }
        }

        result
    }
}

/// Close a node opened via the mobile wrapper.
#[no_mangle]
pub extern "C" fn defra_mobile_close_node(node_ptr: usize) -> FfiResult {
    node_close(node_ptr)
}

/// Idempotently ensure an SDL schema exists on a node.
#[allow(clippy::not_unsafe_ptr_arg_deref)]
#[no_mangle]
pub extern "C" fn defra_mobile_ensure_schema(
    node_ptr: usize,
    schema_sdl: *const c_char,
) -> FfiResult {
    ffi_entry! {
        let schema_str = match unsafe { c_str_to_string(schema_sdl) } {
            Some(value) => value,
            None => return FfiResult::error("invalid schema_sdl parameter"),
        };
        let rt = try_ffi!(get_rt());
        let default_identity = match default_identity_cstring(node_ptr) {
            Ok(value) => value,
            Err(error) => return FfiResult::error(error),
        };
        try_ffi!(check_nac_for_node(
            rt,
            node_ptr,
            c_string_ptr(&default_identity),
            NodePermission::CollectionPatch
        ));

        let (database, policy_store) = match NODES.get(node_ptr, |state| {
            (state.database.clone(), state.policy_store.clone())
        }) {
            Some(value) => value,
            None => return FfiResult::error(ERR_INVALID_NODE_HANDLE),
        };

        ffi_async!(rt, {
            let existing_collections: HashSet<String> =
                database.list_collections().unwrap_or_default().into_iter().collect();
            let known_types = existing_collections.clone();
            let collections = query::parse_sdl_with_known_types(&schema_str, known_types)
                .map_err(|error| format!("failed to parse schema: {}", error))?;

            let mut to_create = Vec::new();
            let mut skipped = Vec::new();
            for collection in collections {
                if existing_collections.contains(&collection.name) {
                    skipped.push(collection.name.clone());
                } else {
                    to_create.push(collection);
                }
            }

            db::definition_validation::validate_new_collections(&to_create)
                .map_err(|error| format!("failed to validate schema: {}", error))?;

            let mut created = Vec::new();
            for collection in to_create {
                if let Some(ref policy) = collection.policy {
                    validate_collection_policy(policy, &policy_store)?;
                }
                created.push(collection.name.clone());
                database
                    .create_collection(collection)
                    .await
                    .map_err(|error| format!("failed to create collection: {}", error))?;
            }

            Ok(serde_json::json!({
                "created": created,
                "skipped": skipped,
            })
            .to_string())
        })
    }
}

/// Execute a GraphQL request from a single JSON payload.
#[allow(clippy::not_unsafe_ptr_arg_deref)]
#[no_mangle]
pub extern "C" fn defra_mobile_execute(node_ptr: usize, request_json: *const c_char) -> FfiResult {
    ffi_entry! {
        let request_str = match unsafe { c_str_to_string(request_json) } {
            Some(value) => value,
            None => return FfiResult::error("invalid request_json parameter"),
        };

        let request: MobileExecuteRequest = match serde_json::from_str(&request_str) {
            Ok(request) => request,
            Err(error) => return FfiResult::error(format!("invalid request_json: {}", error)),
        };

        let identity_did = if request.identity_did.is_some() {
            match maybe_cstring(request.identity_did.as_deref(), "identityDid") {
                Ok(value) => value,
                Err(error) => return FfiResult::error(error),
            }
        } else {
            match default_identity_cstring(node_ptr) {
                Ok(value) => value,
                Err(error) => return FfiResult::error(error),
            }
        };
        let query = match CString::new(request.query) {
            Ok(value) => value,
            Err(_) => return FfiResult::error("query contains an embedded null byte"),
        };
        let operation_name =
            match maybe_cstring(request.operation_name.as_deref(), "operationName") {
                Ok(value) => value,
                Err(error) => return FfiResult::error(error),
            };
        let variables_json = match request.variables {
            Some(value) => match CString::new(value.to_string()) {
                Ok(value) => Some(value),
                Err(_) => return FfiResult::error("variables contains an embedded null byte"),
            },
            None => None,
        };
        let batch_session_id =
            match maybe_cstring(request.batch_session_id.as_deref(), "batchSessionId") {
                Ok(value) => value,
                Err(error) => return FfiResult::error(error),
            };

        unsafe {
            exec_request(
                node_ptr,
                c_string_ptr(&identity_did),
                query.as_ptr(),
                c_string_ptr(&operation_name),
                c_string_ptr(&variables_json),
                c_string_ptr(&batch_session_id),
            )
        }
    }
}

/// Return local peer info for the configured mobile transport.
#[no_mangle]
pub extern "C" fn defra_mobile_peer_info(node_ptr: usize) -> FfiResult {
    let identity = match default_identity_cstring(node_ptr) {
        Ok(value) => value,
        Err(error) => return FfiResult::error(error),
    };
    unsafe { p2p_peer_info(node_ptr, c_string_ptr(&identity)) }
}

/// Connect the node to a peer address.
#[allow(clippy::not_unsafe_ptr_arg_deref)]
#[no_mangle]
pub extern "C" fn defra_mobile_connect(node_ptr: usize, addr: *const c_char) -> FfiResult {
    let identity = match default_identity_cstring(node_ptr) {
        Ok(value) => value,
        Err(error) => return FfiResult::error(error),
    };
    unsafe { p2p_connect(node_ptr, c_string_ptr(&identity), addr) }
}

/// Notify the embedded iroh transport that network conditions may have changed.
#[no_mangle]
pub extern "C" fn defra_mobile_notify_network_change(node_ptr: usize) -> FfiResult {
    let identity = match default_identity_cstring(node_ptr) {
        Ok(value) => value,
        Err(error) => return FfiResult::error(error),
    };
    unsafe { p2p_notify_network_change(node_ptr, c_string_ptr(&identity)) }
}

/// Sync branchable collections, schema versions, or specific documents.
#[allow(clippy::not_unsafe_ptr_arg_deref)]
#[no_mangle]
pub extern "C" fn defra_mobile_sync_collection(
    node_ptr: usize,
    request_json: *const c_char,
) -> FfiResult {
    ffi_entry! {
        let request_str = match unsafe { c_str_to_string(request_json) } {
            Some(value) => value,
            None => return FfiResult::error("invalid request_json parameter"),
        };
        let request: MobileSyncRequest = match serde_json::from_str(&request_str) {
            Ok(request) => request,
            Err(error) => return FfiResult::error(format!("invalid request_json: {}", error)),
        };

        let identity_did = if request.identity_did.is_some() {
            match maybe_cstring(request.identity_did.as_deref(), "identityDid") {
                Ok(value) => value,
                Err(error) => return FfiResult::error(error),
            }
        } else {
            match default_identity_cstring(node_ptr) {
                Ok(value) => value,
                Err(error) => return FfiResult::error(error),
            }
        };

        if let Some(version_ids) = request.version_ids {
            let version_ids = match CString::new(serde_json::to_string(&version_ids).unwrap_or_default()) {
                Ok(value) => value,
                Err(_) => return FfiResult::error("versionIds contains an embedded null byte"),
            };
            return unsafe {
                p2p_sync_collection_versions(
                    node_ptr,
                    c_string_ptr(&identity_did),
                    version_ids.as_ptr(),
                )
            };
        }

        if let Some(doc_ids) = request.doc_ids {
            let collection_name = match request.collection_name {
                Some(value) => value,
                None => return FfiResult::error("collectionName is required when docIds are provided"),
            };
            let collection_name = match CString::new(collection_name) {
                Ok(value) => value,
                Err(_) => return FfiResult::error("collectionName contains an embedded null byte"),
            };
            let doc_ids = match CString::new(serde_json::to_string(&doc_ids).unwrap_or_default()) {
                Ok(value) => value,
                Err(_) => return FfiResult::error("docIds contains an embedded null byte"),
            };
            return unsafe {
                p2p_sync_documents(
                    node_ptr,
                    c_string_ptr(&identity_did),
                    collection_name.as_ptr(),
                    doc_ids.as_ptr(),
                )
            };
        }

        if let Some(collection_id) = request.collection_id {
            let collection_id = match CString::new(collection_id) {
                Ok(value) => value,
                Err(_) => return FfiResult::error("collectionId contains an embedded null byte"),
            };
            return unsafe {
                p2p_sync_branchable_collection(
                    node_ptr,
                    c_string_ptr(&identity_did),
                    collection_id.as_ptr(),
                )
            };
        }

        FfiResult::error(
            "request_json must include versionIds, docIds, or collectionId".to_string(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CStr;

    #[test]
    fn test_mobile_open_schema_and_execute() {
        let init = defra_mobile_init();
        assert_eq!(init.status, 0);
        unsafe { defra_free_string(init.value) };

        let config = CString::new(r#"{"inMemory":true}"#).unwrap();
        let node = defra_mobile_open_node(config.as_ptr());
        assert_eq!(node.status, 0, "mobile open should succeed");

        let schema = CString::new("type Book { name: String }").unwrap();
        let ensure = defra_mobile_ensure_schema(node.node_ptr, schema.as_ptr());
        assert_eq!(ensure.status, 0, "mobile ensure schema should succeed");
        unsafe { defra_free_string(ensure.value) };

        let mutation = CString::new(
            r#"{"query":"mutation { add_Book(input: {name: \"Dune\"}) { _docID } }"}"#,
        )
        .unwrap();
        let mutation_result = defra_mobile_execute(node.node_ptr, mutation.as_ptr());
        assert_eq!(mutation_result.status, 0, "mobile execute should succeed");
        let mutation_json = unsafe { CStr::from_ptr(mutation_result.value).to_string_lossy() };
        let parsed: serde_json::Value =
            serde_json::from_str(&mutation_json).expect("mutation response should be valid JSON");
        assert!(
            parsed
                .get("errors")
                .and_then(|value| value.as_array())
                .map(|errors| errors.is_empty())
                .unwrap_or(true),
            "mutation should not return errors: {}",
            mutation_json
        );
        assert!(
            parsed["data"].get("add_Book").is_some(),
            "mutation should include add_Book data: {}",
            mutation_json
        );
        unsafe { defra_free_string(mutation_result.value) };

        let close = defra_mobile_close_node(node.node_ptr);
        assert_eq!(close.status, 0, "mobile close should succeed");
    }
}
