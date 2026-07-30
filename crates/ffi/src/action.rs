use std::ffi::c_char;

use acp::nac::NodePermission;

use crate::ffi_node_db_async_body;
use crate::types::FfiResult;

/// List actions that are in progress or ended with an error.
///
/// # Safety
///
/// `identity_did` must be null or a valid null-terminated UTF-8 string.
#[no_mangle]
pub unsafe extern "C" fn list_actions(node_ptr: usize, identity_did: *const c_char) -> FfiResult {
    ffi_node_db_async_body! {
        node = node_ptr,
        identity = identity_did,
        database = database,
        permission = NodePermission::ActionList;
        {
            let actions = database
                .list_actions()
                .await
                .map_err(|e| format!("failed to list actions: {}", e))?;

            serde_json::to_string(&actions)
                .map_err(|e| format!("failed to serialize actions: {}", e))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::{new_node, node_close};
    use crate::types::NodeInitOptions;

    #[test]
    fn lists_no_actions_for_new_node() {
        assert!(crate::runtime::init_runtime());

        let result = new_node(NodeInitOptions::default());
        assert_eq!(result.status, 0);
        let node = result.node_ptr;

        let result = unsafe { list_actions(node, std::ptr::null()) };
        assert_eq!(result.status, 0);
        let value = unsafe { std::ffi::CStr::from_ptr(result.value).to_string_lossy() };
        assert_eq!(value, "[]");
        unsafe { crate::types::defra_free_string(result.value) };

        node_close(node);
    }
}
