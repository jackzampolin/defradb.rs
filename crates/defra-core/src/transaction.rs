//! Transaction management

/// Transaction identifier
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TransactionId(u64);

impl TransactionId {
    /// Create a new transaction ID
    pub fn new(id: u64) -> Self {
        Self(id)
    }

    /// Get the numeric ID
    pub fn as_u64(&self) -> u64 {
        self.0
    }
}
