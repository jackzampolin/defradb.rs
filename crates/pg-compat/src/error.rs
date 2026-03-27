use std::fmt;

#[derive(Debug)]
#[non_exhaustive]
pub enum PgCompatError {
    SqlParse(String),
    UnsupportedSql(String),
    QueryExecution(String),
    CollectionNotFound(String),
    Transaction(String),
}

impl fmt::Display for PgCompatError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SqlParse(msg) => write!(f, "SQL parse error: {}", msg),
            Self::UnsupportedSql(msg) => write!(f, "unsupported SQL: {}", msg),
            Self::QueryExecution(msg) => write!(f, "query execution error: {}", msg),
            Self::CollectionNotFound(name) => write!(f, "table not found: {}", name),
            Self::Transaction(msg) => write!(f, "transaction error: {}", msg),
        }
    }
}

impl std::error::Error for PgCompatError {}
