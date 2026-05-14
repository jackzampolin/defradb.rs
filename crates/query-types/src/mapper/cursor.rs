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

/// Which `_pageInfo` fields the client selected. Used to gate
/// response emission so we don't compute or serialize unrequested fields.
#[derive(Debug, Clone, Copy, Default)]
pub struct CursorPageInfoFields {
    pub has_next: bool,
    pub has_prev: bool,
    pub start_cursor: bool,
    pub end_cursor: bool,
}

impl CursorPageInfoFields {
    pub fn any_selected(&self) -> bool {
        self.has_next || self.has_prev || self.start_cursor || self.end_cursor
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
            has_next: true,
            ..Default::default()
        };
        assert!(p.any_selected());
    }
}
