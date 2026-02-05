//! Message trait for P2P protocol messages.

/// Trait for types that can be P2P messages.
pub trait Message {
    /// Get the message version.
    fn version(&self) -> &str;

    /// Set the message version.
    fn set_version(&mut self, version: String);

    /// Get the message ID.
    fn message_id(&self) -> &str;

    /// Set the message ID.
    fn set_message_id(&mut self, id: String);

    /// Get the sender ID.
    fn sender_id(&self) -> &str;

    /// Set the sender ID.
    fn set_sender_id(&mut self, id: String);

    /// Get the public key.
    fn pubkey(&self) -> &[u8];

    /// Set the public key.
    fn set_pubkey(&mut self, pubkey: Vec<u8>);

    /// Get the signature if present.
    fn signature(&self) -> Option<&[u8]>;

    /// Set the signature.
    fn set_signature(&mut self, signature: Option<Vec<u8>>);

    /// Get the error message if present.
    fn err_message(&self) -> Option<&str>;
}
