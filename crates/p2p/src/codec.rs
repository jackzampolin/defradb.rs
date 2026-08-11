//! CBOR codec for P2P messages.
//!
//! This module provides CBOR serialization/deserialization for P2P messages,
//! compatible with the Go implementation using `github.com/fxamacker/cbor/v2`.
//!
//! # Wire Format
//!
//! Messages are CBOR-encoded with PascalCase field names to match Go's default
//! struct field naming. The Rust structs use `#[serde(rename = "...")]` to
//! ensure wire compatibility.

use std::io;
use std::sync::Arc;

use async_trait::async_trait;
use futures::prelude::*;
use libp2p::identity::Keypair;
use libp2p::request_response;
use serde::{de::DeserializeOwned, Serialize};

use crate::error::{Error, Result};
use crate::message::{PushLogReply, PushLogRequest};
use crate::signing::{sign_message, verify_message};

/// Maximum message size (16 MB).
/// This limit prevents memory exhaustion from malicious oversized messages.
pub const MAX_MESSAGE_SIZE: u64 = 16 * 1024 * 1024;

/// Encode a message to CBOR bytes.
pub fn encode<T: Serialize>(msg: &T) -> Result<Vec<u8>> {
    defra_core::cbor::to_vec(msg).map_err(|e| Error::CborSerialization(e.to_string()))
}

/// Decode a message from CBOR bytes.
pub fn decode<T: DeserializeOwned>(data: &[u8]) -> Result<T> {
    defra_core::cbor::from_slice(data).map_err(|e| Error::CborDeserialization(e.to_string()))
}

/// Read a CBOR message from an async reader with size limit.
pub async fn read_message<T, R>(reader: &mut R) -> io::Result<T>
where
    T: DeserializeOwned,
    R: AsyncRead + Unpin + Send,
{
    let mut buf = Vec::new();
    reader.take(MAX_MESSAGE_SIZE).read_to_end(&mut buf).await?;

    if buf.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "empty message received",
        ));
    }

    defra_core::cbor::from_slice(&buf).map_err(|e| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("CBOR deserialization error: {}", e),
        )
    })
}

/// Write a CBOR message to an async writer.
pub async fn write_message<T, W>(writer: &mut W, msg: &T) -> io::Result<()>
where
    T: Serialize,
    W: AsyncWrite + Unpin + Send,
{
    let data = defra_core::cbor::to_vec(msg).map_err(|e| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("CBOR serialization error: {}", e),
        )
    })?;

    // Log first bytes for debugging CBOR encoding issues
    let hex_preview: String = data
        .iter()
        .take(100)
        .map(|b| format!("{:02x}", b))
        .collect();
    tracing::info!(
        cbor_len = data.len(),
        cbor_hex_preview = %hex_preview,
        "Writing CBOR message"
    );

    writer.write_all(&data).await?;
    writer.close().await?;

    Ok(())
}

/// CBOR codec for PushLog messages using libp2p request-response.
///
/// This codec implements the libp2p `request_response::Codec` trait for
/// PushLog message exchange.
///
/// # Message Signing
///
/// When constructed with a keypair (via `with_keypair`), the codec will:
/// - Sign outgoing requests/responses before sending
/// - Verify incoming requests/responses after receiving
///
/// This matches the Go implementation which signs all messages and verifies
/// signatures on receipt.
#[derive(Clone)]
pub struct PushLogCodec {
    /// Keypair for signing/verification. If None, signing is disabled.
    keypair: Option<Arc<Keypair>>,
}

impl Default for PushLogCodec {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for PushLogCodec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PushLogCodec")
            .field("signing_enabled", &self.keypair.is_some())
            .finish()
    }
}

impl PushLogCodec {
    /// Create a new codec without signing/verification.
    ///
    /// Messages will be sent without signatures and incoming signatures
    /// will not be verified. This is useful for testing but should not
    /// be used in production.
    pub fn new() -> Self {
        Self { keypair: None }
    }

    /// Create a new codec with signing/verification enabled.
    ///
    /// All outgoing messages will be signed and all incoming messages
    /// will have their signatures verified.
    pub fn with_keypair(keypair: Keypair) -> Self {
        Self {
            keypair: Some(Arc::new(keypair)),
        }
    }

    /// Check if signing is enabled.
    pub fn signing_enabled(&self) -> bool {
        self.keypair.is_some()
    }
}

#[async_trait]
impl request_response::Codec for PushLogCodec {
    type Protocol = libp2p::StreamProtocol;
    type Request = PushLogRequest;
    type Response = PushLogReply;

    async fn read_request<T>(
        &mut self,
        _protocol: &Self::Protocol,
        io: &mut T,
    ) -> io::Result<Self::Request>
    where
        T: AsyncRead + Unpin + Send,
    {
        let msg: Self::Request = read_message(io).await?;

        // Verify signature if signing is enabled
        if self.keypair.is_some() {
            verify_message(&msg).map_err(|e| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("signature verification failed: {}", e),
                )
            })?;
        }

        Ok(msg)
    }

    async fn read_response<T>(
        &mut self,
        _protocol: &Self::Protocol,
        io: &mut T,
    ) -> io::Result<Self::Response>
    where
        T: AsyncRead + Unpin + Send,
    {
        let msg: Self::Response = read_message(io).await?;

        // Verify signature if signing is enabled
        if self.keypair.is_some() {
            verify_message(&msg).map_err(|e| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("signature verification failed: {}", e),
                )
            })?;
        }

        Ok(msg)
    }

    async fn write_request<T>(
        &mut self,
        _protocol: &Self::Protocol,
        io: &mut T,
        mut req: Self::Request,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        // Sign the message if signing is enabled
        if let Some(keypair) = &self.keypair {
            sign_message(keypair, &mut req).map_err(|e| {
                io::Error::new(io::ErrorKind::InvalidData, format!("signing failed: {}", e))
            })?;
        }

        write_message(io, &req).await
    }

    async fn write_response<T>(
        &mut self,
        _protocol: &Self::Protocol,
        io: &mut T,
        mut res: Self::Response,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        // Sign the message if signing is enabled
        if let Some(keypair) = &self.keypair {
            sign_message(keypair, &mut res).map_err(|e| {
                io::Error::new(io::ErrorKind::InvalidData, format!("signing failed: {}", e))
            })?;
        }

        write_message(io, &res).await
    }
}
