/// Extract a docID value from a GraphQL query string.
///
/// Looks for patterns like `docID: "bae-xxx"` or `docID:"bae-xxx"` in the query.
/// Returns None if no docID is found.
pub(crate) fn extract_doc_id_from_query(query: &str) -> Option<String> {
    // Look for docID: "..." or docID:"..."
    let doc_id_marker = "docID:";
    let pos = query.find(doc_id_marker)?;
    let after = &query[pos + doc_id_marker.len()..];
    let after = after.trim_start();

    // Find the quoted value
    if after.starts_with('"') {
        let value_start = 1;
        let value_end = after[value_start..].find('"')?;
        Some(after[value_start..value_start + value_end].to_string())
    } else {
        None
    }
}

/// Convert a subscription query to a regular query scoped to a specific docID.
///
/// Transforms: `subscription { User(filter: ...) { fields } }`
/// Into: `{ User(docID: "bae-xxx", filter: ...) { fields } }`
pub(crate) fn subscription_to_query_with_doc_id(subscription_query: &str, doc_id: &str) -> String {
    // Step 1: Remove "subscription" keyword
    let trimmed = subscription_query.trim_start();
    let query = if let Some(after) = trimmed.strip_prefix("subscription") {
        let after = after.trim_start();
        if after.starts_with('{') {
            after.to_string()
        } else if let Some(brace_pos) = after.find('{') {
            after[brace_pos..].to_string()
        } else {
            after.to_string()
        }
    } else {
        trimmed.to_string()
    };

    // Step 2: Find the root field name and inject docID
    let brace_pos = match query.find('{') {
        Some(p) => p,
        None => return query,
    };

    let after_brace = &query[brace_pos + 1..];
    let ws_len = after_brace.len() - after_brace.trim_start().len();
    let field_start_in_q = brace_pos + 1 + ws_len;

    let rest = &query[field_start_in_q..];
    let field_end = rest
        .find(|c: char| !c.is_alphanumeric() && c != '_')
        .unwrap_or(rest.len());
    let after_field = field_start_in_q + field_end;

    // Check what follows the field name
    let post_field = query[after_field..].trim_start();
    if post_field.starts_with('(') {
        // Already has arguments
        let paren_offset = query[after_field..].find('(').unwrap();
        let paren_idx = after_field + paren_offset;

        // Check if there's already a docID argument
        if extract_doc_id_from_query(&query).is_some() {
            // Keep the existing docID filter - just convert subscription to query
            query
        } else {
            // Inject docID at start of existing args
            format!(
                "{}docID: \"{}\", {}",
                &query[..paren_idx + 1],
                doc_id,
                &query[paren_idx + 1..]
            )
        }
    } else {
        // No arguments - inject (docID: "...")
        format!(
            "{}(docID: \"{}\"){}",
            &query[..after_field],
            doc_id,
            &query[after_field..]
        )
    }
}

/// Check if a subscription query targets the `_commits` root field.
pub(crate) fn is_commits_subscription(query: &str) -> bool {
    let trimmed = query.trim_start();
    let after_sub = trimmed.strip_prefix("subscription").unwrap_or(trimmed);
    let brace_pos = match after_sub.find('{') {
        Some(p) => p,
        None => return false,
    };
    let after_brace = after_sub[brace_pos + 1..].trim_start();
    after_brace.starts_with("_commits")
}

/// Convert a _commits subscription to a query scoped to a specific CID.
///
/// Transforms: `subscription { _commits(docID: "...") { fields } }`
/// Into: `{ _commits(cid: "bafyrei-xxx") { fields } }`
pub(crate) fn subscription_to_commits_query_with_cid(
    subscription_query: &str,
    cid: &str,
) -> String {
    // Step 1: Remove "subscription" keyword
    let trimmed = subscription_query.trim_start();
    let query = if let Some(after) = trimmed.strip_prefix("subscription") {
        let after = after.trim_start();
        if after.starts_with('{') {
            after.to_string()
        } else if let Some(brace_pos) = after.find('{') {
            after[brace_pos..].to_string()
        } else {
            after.to_string()
        }
    } else {
        trimmed.to_string()
    };

    // Step 2: Find the root field name (_commits)
    let brace_pos = match query.find('{') {
        Some(p) => p,
        None => return query,
    };

    let after_brace = &query[brace_pos + 1..];
    let ws_len = after_brace.len() - after_brace.trim_start().len();
    let field_start_in_q = brace_pos + 1 + ws_len;

    let rest = &query[field_start_in_q..];
    let field_end = rest
        .find(|c: char| !c.is_alphanumeric() && c != '_')
        .unwrap_or(rest.len());
    let after_field = field_start_in_q + field_end;

    // Step 3: Replace or inject cid argument
    let post_field = query[after_field..].trim_start();
    if post_field.starts_with('(') {
        // Has existing arguments - find the closing paren and replace all args with cid
        let paren_start = query[after_field..].find('(').unwrap();
        let paren_start_abs = after_field + paren_start;
        let mut depth = 0;
        let mut close_paren_abs = paren_start_abs;
        for (i, c) in query[paren_start_abs..].char_indices() {
            if c == '(' {
                depth += 1;
            }
            if c == ')' {
                depth -= 1;
                if depth == 0 {
                    close_paren_abs = paren_start_abs + i;
                    break;
                }
            }
        }
        format!(
            "{}(cid: \"{}\"){}",
            &query[..paren_start_abs],
            cid,
            &query[close_paren_abs + 1..]
        )
    } else {
        // No arguments - inject (cid: "...")
        format!(
            "{}(cid: \"{}\"){}",
            &query[..after_field],
            cid,
            &query[after_field..]
        )
    }
}
