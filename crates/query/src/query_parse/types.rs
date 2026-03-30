//! Types for parsed GraphQL operations

use crate::mapper::{Mutation, Select};

/// Type of explain output requested.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum ExplainType {
    /// Simple explanation showing query plan structure without execution.
    #[default]
    Simple,
    /// Execute the query and return both the plan structure and execution metrics.
    Execute,
    /// Debug mode showing all plan nodes including internal ones.
    Debug,
}

impl ExplainType {
    /// Parse explain type from string.
    pub fn parse_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "simple" => Some(Self::Simple),
            "execute" => Some(Self::Execute),
            "debug" => Some(Self::Debug),
            _ => None,
        }
    }
}

/// Result of parsing a GraphQL request.
#[derive(Debug)]
#[non_exhaustive]
pub enum ParsedOperation {
    /// Query operations (SELECT)
    Query {
        selects: Vec<Select>,
        /// Whether @explain directive was used and which type
        explain: Option<ExplainType>,
        /// Whether @exhaustive directive was used
        exhaustive: bool,
    },
    /// Mutation operations (CREATE, UPDATE, DELETE)
    Mutation {
        mutations: Vec<Mutation>,
        /// Whether @explain directive was used and which type
        explain: Option<ExplainType>,
    },
    /// Subscription operations (single root field only per GraphQL spec)
    Subscription {
        /// The single select for the subscription.
        select: Box<Select>,
    },
    /// Introspection query (__schema or __type)
    ///
    /// Introspection queries are handled separately using the GraphQL schema
    /// rather than the document storage.
    Introspection {
        /// The original query string to be executed against the schema
        query: String,
    },
}
