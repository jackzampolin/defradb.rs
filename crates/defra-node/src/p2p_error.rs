use thiserror::Error;

/// Error categories for defra-node P2P operations.
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

/// Convenience result alias for defra-node P2P operations.
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
