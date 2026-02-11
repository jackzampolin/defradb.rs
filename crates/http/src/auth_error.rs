/// Normalize authorization errors to match Go's generic error format.
///
/// Go returns "not authorized to perform operation" for all authorization
/// failures. Rust returns more specific messages like "only admins can disable NAC"
/// or "UNAUTHORIZED: not document owner". This function maps them to the
/// Go-compatible generic message.
pub fn normalize_auth_error(err: String, permission: &str) -> String {
    let lower = err.to_lowercase();
    if lower.contains("only admins can")
        || lower.contains("unauthorized")
        || lower.contains("not document owner")
        || lower.contains("notowner")
    {
        format!(
            "not authorized to perform operation. Permission: {}",
            permission
        )
    } else {
        err
    }
}
