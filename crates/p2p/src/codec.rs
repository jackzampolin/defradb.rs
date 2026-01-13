// Copyright 2025 Democratized Data Foundation
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
    serde_cbor::to_vec(msg).map_err(|e| Error::CborSerialization(e.to_string()))
}

/// Decode a message from CBOR bytes.
pub fn decode<T: DeserializeOwned>(data: &[u8]) -> Result<T> {
    serde_cbor::from_slice(data).map_err(|e| Error::CborDeserialization(e.to_string()))
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

    serde_cbor::from_slice(&buf).map_err(|e| {
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
    let data = serde_cbor::to_vec(msg).map_err(|e| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("CBOR serialization error: {}", e),
        )
    })?;

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
                io::Error::new(io::ErrorKind::InvalidData, format!("signature verification failed: {}", e))
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
                io::Error::new(io::ErrorKind::InvalidData, format!("signature verification failed: {}", e))
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{rep_request_protocol, rep_response_protocol};
    use futures::io::Cursor;
    use libp2p::request_response::Codec;

    #[tokio::test]
    async fn test_codec_roundtrip_request() {
        let mut codec = PushLogCodec::new();
        let protocol = rep_request_protocol();

        let original = PushLogRequest::new(
            "doc123".to_string(),
            vec![1, 2, 3],
            "collection1".to_string(),
            "creator1".to_string(),
            vec![4, 5, 6],
        );

        // Write
        let mut write_buf = Cursor::new(Vec::new());
        codec
            .write_request(&protocol, &mut write_buf, original.clone())
            .await
            .expect("write failed");

        // Read
        let mut read_buf = Cursor::new(write_buf.into_inner());
        let decoded = codec
            .read_request(&protocol, &mut read_buf)
            .await
            .expect("read failed");

        assert_eq!(decoded.doc_id, original.doc_id);
        assert_eq!(decoded.cid, original.cid);
        assert_eq!(decoded.collection_id, original.collection_id);
    }

    #[tokio::test]
    async fn test_codec_roundtrip_response() {
        let mut codec = PushLogCodec::new();
        let protocol = rep_response_protocol();

        let original = PushLogReply::success("msg123");

        // Write
        let mut write_buf = Cursor::new(Vec::new());
        codec
            .write_response(&protocol, &mut write_buf, original.clone())
            .await
            .expect("write failed");

        // Read
        let mut read_buf = Cursor::new(write_buf.into_inner());
        let decoded = codec
            .read_response(&protocol, &mut read_buf)
            .await
            .expect("read failed");

        assert_eq!(decoded.metadata.message_id, original.metadata.message_id);
    }

    #[tokio::test]
    async fn test_codec_invalid_cbor_request() {
        let mut codec = PushLogCodec::new();
        let protocol = rep_request_protocol();

        // Invalid CBOR data
        let mut read_buf = Cursor::new(vec![0xFF, 0xFF, 0xFF, 0xFF]);
        let result = codec.read_request(&protocol, &mut read_buf).await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("CBOR deserialization error"));
    }

    #[tokio::test]
    async fn test_codec_invalid_cbor_response() {
        let mut codec = PushLogCodec::new();
        let protocol = rep_response_protocol();

        // Invalid CBOR data
        let mut read_buf = Cursor::new(vec![0xFE, 0xFE, 0xFE]);
        let result = codec.read_response(&protocol, &mut read_buf).await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[tokio::test]
    async fn test_codec_empty_message() {
        let mut codec = PushLogCodec::new();
        let protocol = rep_request_protocol();

        // Empty data
        let mut read_buf = Cursor::new(Vec::new());
        let result = codec.read_request(&protocol, &mut read_buf).await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
    }

    #[tokio::test]
    async fn test_codec_truncated_cbor() {
        let mut codec = PushLogCodec::new();
        let protocol = rep_request_protocol();

        let original = PushLogRequest::new(
            "doc123".to_string(),
            vec![1, 2, 3],
            "collection1".to_string(),
            "creator1".to_string(),
            vec![4, 5, 6],
        );

        // Encode then truncate
        let full_data = serde_cbor::to_vec(&original).unwrap();
        let truncated = &full_data[..full_data.len() / 2];

        let mut read_buf = Cursor::new(truncated.to_vec());
        let result = codec.read_request(&protocol, &mut read_buf).await;

        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn test_encode_decode_roundtrip() {
        let request = PushLogRequest::new(
            "doc456".to_string(),
            vec![10, 20, 30],
            "col2".to_string(),
            "creator2".to_string(),
            vec![40, 50, 60],
        );

        let encoded = encode(&request).expect("encode failed");
        let decoded: PushLogRequest = decode(&encoded).expect("decode failed");

        assert_eq!(decoded.doc_id, request.doc_id);
        assert_eq!(decoded.cid, request.cid);
        assert_eq!(decoded.collection_id, request.collection_id);
    }

    #[test]
    fn test_decode_invalid_cbor() {
        let invalid_data = vec![0xFF, 0xFF];
        let result: Result<PushLogRequest> = decode(&invalid_data);

        assert!(result.is_err());
        match result {
            Err(Error::CborDeserialization(_)) => {}
            _ => panic!("Expected CborDeserialization error"),
        }
    }

    #[test]
    fn test_max_message_size_constant() {
        // Verify the constant is 16 MB
        assert_eq!(MAX_MESSAGE_SIZE, 16 * 1024 * 1024);
    }
}
