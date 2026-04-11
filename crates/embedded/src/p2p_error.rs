use thiserror::Error;

/// Shared error categories for embedded P2P operations.
///
/// This is introduced as groundwork for migrating the embedded P2P surface away
/// from `Result<_, String>` without forcing a one-shot API break across all
/// downstream callers.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum P2PError {
    #[error("invalid input: {0}")]
    InvalidInput(String),

    #[error("not found: {0}")]
    NotFound(String),

    #[error("unsupported operation: {0}")]
    Unsupported(String),

    #[error("transport error: {0}")]
    Transport(String),

    #[error("persistence error: {0}")]
    Persistence(String),

    #[error("internal error: {0}")]
    Internal(String),
}

/// Convenience result alias for embedded P2P operations.
pub type P2PResult<T> = Result<T, P2PError>;

impl P2PError {
    pub fn invalid_input(message: impl Into<String>) -> Self {
        Self::InvalidInput(message.into())
    }

    pub fn not_found(message: impl Into<String>) -> Self {
        Self::NotFound(message.into())
    }

    pub fn unsupported(message: impl Into<String>) -> Self {
        Self::Unsupported(message.into())
    }

    pub fn transport(message: impl Into<String>) -> Self {
        Self::Transport(message.into())
    }

    pub fn persistence(message: impl Into<String>) -> Self {
        Self::Persistence(message.into())
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::Internal(message.into())
    }
}

impl From<String> for P2PError {
    fn from(message: String) -> Self {
        Self::Internal(message)
    }
}

impl From<&str> for P2PError {
    fn from(message: &str) -> Self {
        Self::Internal(message.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::{P2PError, P2PResult};

    #[test]
    fn helpers_assign_expected_variants() {
        assert_eq!(
            P2PError::invalid_input("bad addr"),
            P2PError::InvalidInput("bad addr".to_string())
        );
        assert_eq!(
            P2PError::not_found("peer missing"),
            P2PError::NotFound("peer missing".to_string())
        );
        assert_eq!(
            P2PError::unsupported("not wired"),
            P2PError::Unsupported("not wired".to_string())
        );
        assert_eq!(
            P2PError::transport("dial failed"),
            P2PError::Transport("dial failed".to_string())
        );
        assert_eq!(
            P2PError::persistence("store failed"),
            P2PError::Persistence("store failed".to_string())
        );
        assert_eq!(
            P2PError::internal("unexpected"),
            P2PError::Internal("unexpected".to_string())
        );
    }

    #[test]
    fn string_conversion_defaults_to_internal() {
        let err: P2PError = "boom".into();
        let result: P2PResult<()> = Err(String::from("bad state").into());

        assert_eq!(err, P2PError::Internal("boom".to_string()));
        assert_eq!(result, Err(P2PError::Internal("bad state".to_string())));
    }
}
