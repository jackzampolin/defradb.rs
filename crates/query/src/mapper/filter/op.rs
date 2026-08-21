//! FilterOp enum - Filter operators for condition matching

use serde::{Deserialize, Serialize};

/// Filter operators for condition matching
/// Uses Go DefraDB naming conventions for compatibility
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum FilterOp {
    /// Equal (_eq)
    #[serde(rename = "_eq")]
    Eq,
    /// Not equal (_neq) - Go DefraDB naming
    #[serde(rename = "_neq", alias = "_ne")]
    Ne,
    /// Greater than (_gt)
    #[serde(rename = "_gt")]
    Gt,
    /// Greater than or equal (_geq) - Go DefraDB naming
    #[serde(rename = "_geq", alias = "_gte")]
    Gte,
    /// Less than (_lt)
    #[serde(rename = "_lt")]
    Lt,
    /// Less than or equal (_leq) - Go DefraDB naming
    #[serde(rename = "_leq", alias = "_lte")]
    Lte,
    /// In array (_in)
    #[serde(rename = "_in")]
    In,
    /// Not in array (_nin)
    #[serde(rename = "_nin")]
    Nin,
    /// Pattern match (_like)
    #[serde(rename = "_like")]
    Like,
    /// Negated pattern match (_nlike)
    #[serde(rename = "_nlike")]
    Nlike,
    /// Case-insensitive pattern match (_ilike)
    #[serde(rename = "_ilike")]
    Ilike,
    /// Negated case-insensitive pattern match (_nilike)
    #[serde(rename = "_nilike")]
    Nilike,
    /// Array contains value (_contains)
    #[serde(rename = "_contains")]
    Contains,
    /// Array is contained in given array (_contained_in)
    #[serde(rename = "_contained_in")]
    ContainedIn,
    /// Object/map has key (_has_key)
    #[serde(rename = "_has_key")]
    HasKey,
    /// Logical AND (_and)
    #[serde(rename = "_and")]
    And,
    /// Logical OR (_or)
    #[serde(rename = "_or")]
    Or,
    /// Logical NOT (_not)
    #[serde(rename = "_not")]
    Not,
    /// Any array element matches condition (_any)
    #[serde(rename = "_any")]
    Any,
    /// All array elements match condition (_all)
    #[serde(rename = "_all")]
    All,
    /// No array elements match condition (_none)
    #[serde(rename = "_none")]
    None,
}

impl FilterOp {
    /// Parse a filter operator from string.
    /// Accepts both Go DefraDB naming (_neq, _geq, _leq) and alternative naming (_ne, _gte, _lte).
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "_eq" => Some(Self::Eq),
            "_neq" | "_ne" => Some(Self::Ne),
            "_gt" => Some(Self::Gt),
            "_geq" | "_gte" | "_ge" => Some(Self::Gte),
            "_lt" => Some(Self::Lt),
            "_leq" | "_lte" | "_le" => Some(Self::Lte),
            "_in" => Some(Self::In),
            "_nin" => Some(Self::Nin),
            "_like" => Some(Self::Like),
            "_nlike" => Some(Self::Nlike),
            "_ilike" => Some(Self::Ilike),
            "_nilike" => Some(Self::Nilike),
            "_contains" => Some(Self::Contains),
            "_contained_in" => Some(Self::ContainedIn),
            "_has_key" => Some(Self::HasKey),
            "_and" => Some(Self::And),
            "_or" => Some(Self::Or),
            "_not" => Some(Self::Not),
            "_any" => Some(Self::Any),
            "_all" => Some(Self::All),
            "_none" => Some(Self::None),
            _ => Option::None,
        }
    }

    /// Get the string representation (uses Go DefraDB naming)
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Eq => "_eq",
            Self::Ne => "_neq",
            Self::Gt => "_gt",
            Self::Gte => "_geq",
            Self::Lt => "_lt",
            Self::Lte => "_leq",
            Self::In => "_in",
            Self::Nin => "_nin",
            Self::Like => "_like",
            Self::Nlike => "_nlike",
            Self::Ilike => "_ilike",
            Self::Nilike => "_nilike",
            Self::Contains => "_contains",
            Self::ContainedIn => "_contained_in",
            Self::HasKey => "_has_key",
            Self::And => "_and",
            Self::Or => "_or",
            Self::Not => "_not",
            Self::Any => "_any",
            Self::All => "_all",
            Self::None => "_none",
        }
    }

    /// Check if this is a logical operator
    pub fn is_logical(&self) -> bool {
        matches!(self, Self::And | Self::Or | Self::Not)
    }

    /// Check if this is an array element operator
    pub fn is_array_element_op(&self) -> bool {
        matches!(self, Self::Any | Self::All | Self::None)
    }
}
