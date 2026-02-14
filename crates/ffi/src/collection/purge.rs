use std::ffi::c_char;

use crate::helpers::{get_node_database, get_rt, require_c_str};
use crate::types::FfiResult;
use crate::{ffi_async, try_ffi};

use db::auto_commit_mutator::AutoCommitMutator;
use document::DocID;
use query::mutator::DocMutator;

/// Delete multiple documents by their docIDs.
///
/// Takes a collection name and a JSON array of docID strings,
/// deletes each document, and returns the count of deleted documents.
///
/// # Arguments
///
/// * `node_ptr` - Handle to the node
/// * `identity_did` - Optional identity DID (nullable)
/// * `collection_name` - The collection to delete from
/// * `doc_ids_json` - JSON array of docID strings: `["bae-...", "bae-..."]`
///
/// # Returns
///
/// - Status 0: Success (value is `{"deleted": N}`)
/// - Status 1: Error (error field contains message)
///
/// # Safety
///
/// `collection_name` and `doc_ids_json` must be valid null-terminated UTF-8 strings.
#[no_mangle]
pub unsafe extern "C" fn delete_documents(
    node_ptr: usize,
    _identity_did: *const c_char,
    collection_name: *const c_char,
    doc_ids_json: *const c_char,
) -> FfiResult {
    let rt = try_ffi!(get_rt());
    let col_name = try_ffi!(require_c_str(collection_name, "collection_name"));
    let ids_json = try_ffi!(require_c_str(doc_ids_json, "doc_ids_json"));
    let database = try_ffi!(get_node_database(node_ptr));

    // Parse JSON array of docID strings
    let doc_id_strings: Vec<String> = try_ffi!(serde_json::from_str(&ids_json)
        .map_err(|e| FfiResult::error(format!("invalid doc_ids_json: {}", e))));

    ffi_async!(rt, {
        let mutator = AutoCommitMutator::new(database.clone());
        let mut deleted: u64 = 0;

        for id_str in &doc_id_strings {
            let doc_id = DocID::from_string(id_str).map_err(|e| {
                format!("invalid docID '{}': {}", id_str, e)
            })?;
            match mutator.delete(&col_name, &doc_id).await {
                Ok(result) => {
                    if result.existed {
                        deleted += 1;
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        collection = %col_name,
                        doc_id = %id_str,
                        error = %e,
                        "Failed to delete document during purge"
                    );
                }
            }
        }

        Ok(format!("{{\"deleted\":{}}}", deleted))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::{new_node, node_close};
    use crate::query::exec_request;
    use crate::schema::add_schema;
    use crate::types::NodeInitOptions;
    use std::ffi::CString;

    #[test]
    fn test_delete_documents_empty_array() {
        assert!(crate::runtime::init_runtime());

        let options = NodeInitOptions::default();
        let result = new_node(options);
        assert_eq!(result.status, 0);
        let node = result.node_ptr;

        // Add schema
        let sdl = CString::new("type PurgeTest { name: String }").unwrap();
        let result = unsafe { add_schema(node, std::ptr::null(), sdl.as_ptr()) };
        assert_eq!(result.status, 0);
        unsafe { crate::types::defra_free_string(result.value) };

        // Delete with empty array
        let col = CString::new("PurgeTest").unwrap();
        let ids = CString::new("[]").unwrap();
        let result =
            unsafe { delete_documents(node, std::ptr::null(), col.as_ptr(), ids.as_ptr()) };
        assert_eq!(result.status, 0);
        let value = unsafe { std::ffi::CStr::from_ptr(result.value).to_string_lossy() };
        assert_eq!(value, "{\"deleted\":0}");
        unsafe { crate::types::defra_free_string(result.value) };

        node_close(node);
    }

    #[test]
    fn test_delete_documents_with_data() {
        assert!(crate::runtime::init_runtime());

        let options = NodeInitOptions::default();
        let result = new_node(options);
        assert_eq!(result.status, 0);
        let node = result.node_ptr;

        // Add schema
        let sdl = CString::new("type PurgeData { name: String }").unwrap();
        let result = unsafe { add_schema(node, std::ptr::null(), sdl.as_ptr()) };
        assert_eq!(result.status, 0);
        unsafe { crate::types::defra_free_string(result.value) };

        // Create a document
        let mutation =
            CString::new(r#"mutation { create_PurgeData(input: {name: "test"}) { _docID } }"#)
                .unwrap();
        let result = unsafe {
            exec_request(
                node,
                std::ptr::null(),
                mutation.as_ptr(),
                std::ptr::null(),
                std::ptr::null(),
                std::ptr::null(),
            )
        };
        assert_eq!(result.status, 0);
        let resp = unsafe { std::ffi::CStr::from_ptr(result.value).to_string_lossy() };

        // Extract docID from response
        let parsed: serde_json::Value = serde_json::from_str(&resp).unwrap();
        let doc_id = parsed["data"]["create_PurgeData"][0]["_docID"]
            .as_str()
            .unwrap()
            .to_string();
        unsafe { crate::types::defra_free_string(result.value) };

        // Delete the document
        let col = CString::new("PurgeData").unwrap();
        let ids = CString::new(format!("[\"{}\"]", doc_id)).unwrap();
        let result =
            unsafe { delete_documents(node, std::ptr::null(), col.as_ptr(), ids.as_ptr()) };
        assert_eq!(result.status, 0);
        let value = unsafe { std::ffi::CStr::from_ptr(result.value).to_string_lossy() };
        assert_eq!(value, "{\"deleted\":1}");
        unsafe { crate::types::defra_free_string(result.value) };

        node_close(node);
    }
}
