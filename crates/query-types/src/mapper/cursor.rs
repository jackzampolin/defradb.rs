//! Cursor pagination request types.

/// Parsed cursor pagination args from a GraphQL cursor query.
/// `first`/`after` are mutually exclusive with `last`/`before`
/// (validated by the parser).
#[derive(Debug, Clone, Default)]
pub struct CursorParams {
    pub first: Option<u64>,
    pub after: Option<String>, // raw base64 token; decoded in planner
    pub last: Option<u64>,
    pub before: Option<String>,
}

impl CursorParams {
    pub fn is_forward(&self) -> bool {
        self.first.is_some() || self.after.is_some()
    }

    pub fn is_backward(&self) -> bool {
        self.last.is_some() || self.before.is_some()
    }
}

/// Which `_pageInfo` fields the client selected, and the output key to emit them under.
/// `None` means the field was not selected; `Some(key)` means it was selected and should
/// appear in the response under that key (the alias if provided, else the canonical name).
#[derive(Debug, Clone, Default)]
pub struct CursorPageInfoFields {
    pub has_next: Option<String>,
    pub has_prev: Option<String>,
    pub start_cursor: Option<String>,
    pub end_cursor: Option<String>,
}

impl CursorPageInfoFields {
    pub fn any_selected(&self) -> bool {
        self.has_next.is_some()
            || self.has_prev.is_some()
            || self.start_cursor.is_some()
            || self.end_cursor.is_some()
    }
}

/// Tracks the GraphQL aliases on a cursor query so response shaping can
/// emit results under the correct keys. The wrapper alias is the alias
/// (if any) on `_cursor` itself; `select.field.alias` continues to carry
/// the alias on the inner collection field.
#[derive(Debug, Clone, Default)]
pub struct CursorAliases {
    /// Alias on `_cursor` (e.g., `{ paged: _cursor { ... } }` => Some("paged")).
    /// None => emit under the literal key `_cursor`.
    pub wrapper_alias: Option<String>,
    /// Alias on the inner `_pageInfo` selection
    /// (e.g., `{ _cursor { ... info: _pageInfo { ... } } }` => Some("info")).
    /// None => emit under the literal key `_pageInfo`.
    /// Boxed to keep the size of `CursorAliases` (and therefore `Select`)
    /// minimal; deeply nested non-cursor plans were stack-overflowing when
    /// `Select` grew beyond ~120 bytes.
    pub page_info_alias: Option<Box<String>>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursor_params_default_is_neither_direction() {
        let p = CursorParams::default();
        assert!(!p.is_forward());
        assert!(!p.is_backward());
    }

    #[test]
    fn cursor_params_first_is_forward() {
        let p = CursorParams {
            first: Some(10),
            ..Default::default()
        };
        assert!(p.is_forward());
        assert!(!p.is_backward());
    }

    #[test]
    fn cursor_page_info_any_selected() {
        let p = CursorPageInfoFields::default();
        assert!(!p.any_selected());

        let p = CursorPageInfoFields {
            has_next: Some("hasNext".to_string()),
            ..Default::default()
        };
        assert!(p.any_selected());
    }
}
