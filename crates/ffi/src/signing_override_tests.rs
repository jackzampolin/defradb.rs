//! Per-operation signing override tests (#1600).
//!
//! `exec_request_with_signing` takes `-1` (node default), `0` (disable) and
//! `1` (enable). These tests run both branches against a signing-enabled
//! in-memory node and assert the resulting blocks carry — or do not carry — a
//! signature.

#[cfg(test)]
mod tests {
    use std::ffi::{c_int, CStr, CString};
    use std::ptr;

    use serde_json::Value;

    use crate::node::{new_node, node_close};
    use crate::query::exec_request_with_signing;
    use crate::schema::add_schema;
    use crate::types::{defra_free_string, NodeInitOptions};

    /// A signing-enabled in-memory node with a `Widget` collection.
    ///
    /// The node identity is generated per node, so concurrently running tests
    /// never share signing config through the process-global identity store.
    fn signing_node() -> usize {
        assert!(crate::runtime::init_runtime(), "runtime init must succeed");

        let result = new_node(NodeInitOptions {
            enable_signing: 1,
            ..NodeInitOptions::default()
        });
        assert_eq!(result.status, 0, "new_node must succeed");
        let node = result.node_ptr;

        let sdl = CString::new("type Widget { name: String }").unwrap();
        let result = unsafe { add_schema(node, ptr::null(), sdl.as_ptr()) };
        assert_eq!(result.status, 0, "add_schema must succeed");
        if !result.value.is_null() {
            unsafe { defra_free_string(result.value) };
        }

        node
    }

    fn exec(node: usize, query: &str, signing_override: c_int) -> Value {
        let query = CString::new(query).unwrap();
        let result = unsafe {
            exec_request_with_signing(
                node,
                ptr::null(),
                query.as_ptr(),
                ptr::null(),
                ptr::null(),
                ptr::null(),
                signing_override,
            )
        };

        if result.status != 0 {
            let message = unsafe { CStr::from_ptr(result.error).to_string_lossy().into_owned() };
            unsafe { defra_free_string(result.error) };
            panic!("exec_request_with_signing failed: {message}");
        }

        let json = unsafe { CStr::from_ptr(result.value).to_string_lossy().into_owned() };
        unsafe { defra_free_string(result.value) };
        let response: Value = serde_json::from_str(&json).expect("response must be JSON");
        assert!(
            response.get("errors").is_none_or(Value::is_null),
            "request returned errors: {response}"
        );
        response
    }

    fn create_widget(node: usize, name: &str, signing_override: c_int) -> String {
        let response = exec(
            node,
            &format!(r#"mutation {{ add_Widget(input: {{name: "{name}"}}) {{ _docID }} }}"#),
            signing_override,
        );
        response
            .pointer("/data/add_Widget/0/_docID")
            .and_then(Value::as_str)
            .unwrap_or_else(|| panic!("created document id missing from {response}"))
            .to_string()
    }

    /// Every commit's `signature` for a document, queried with the node default.
    fn commit_signatures(node: usize, doc_id: &str) -> Vec<Value> {
        let response = exec(
            node,
            &format!(
                r#"query {{ _commits(docID: "{doc_id}") {{ cid signature {{ type identity value }} }} }}"#
            ),
            -1,
        );
        let rows = response
            .pointer("/data/_commits")
            .and_then(Value::as_array)
            .unwrap_or_else(|| panic!("commit rows missing from {response}"))
            .clone();
        assert!(!rows.is_empty(), "document must have commits");
        rows.into_iter()
            .map(|row| row.get("signature").cloned().unwrap_or(Value::Null))
            .collect()
    }

    /// Sanity: `-1` follows the node default, so a signing node signs.
    ///
    /// Without this the "no signature" assertion below would pass even if the
    /// query never surfaced signatures at all.
    #[test]
    fn signing_override_default_signs_on_signing_node() {
        let node = signing_node();

        let doc_id = create_widget(node, "default", -1);
        let signatures = commit_signatures(node, &doc_id);

        assert!(
            signatures.iter().any(|signature| !signature.is_null()),
            "node default must produce at least one signed block, got: {signatures:?}"
        );

        node_close(node);
    }

    /// `0` disables signing for that operation even on a signing node (#1600).
    #[test]
    fn signing_override_disabled_leaves_blocks_unsigned() {
        let node = signing_node();

        let doc_id = create_widget(node, "unsigned", 0);
        let signatures = commit_signatures(node, &doc_id);

        assert!(
            signatures.iter().all(Value::is_null),
            "signing_override=0 must leave every block unsigned, got: {signatures:?}"
        );

        node_close(node);
    }
}
