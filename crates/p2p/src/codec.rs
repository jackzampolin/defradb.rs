// Copyright 2025 Democratized Data Foundation
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//! CBOR codec for request-response protocol.
//!
//! This module implements a CBOR-based codec for the libp2p request-response
//! protocol, providing wire compatibility with the Go implementation.

use std::io;

use async_trait::async_trait;
use futures::prelude::*;
use libp2p::request_response;

use crate::message::{PushLogReply, PushLogRequest};

/// Maximum message size (16 MB).
const MAX_MESSAGE_SIZE: u64 = 16 * 1024 * 1024;

/// CBOR codec for PushLog messages.
#[derive(Debug, Clone, Default)]
pub struct PushLogCodec;

impl PushLogCodec {
    pub fn new() -> Self {
        Self
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
        let mut buf = Vec::new();
        io.take(MAX_MESSAGE_SIZE).read_to_end(&mut buf).await?;

        serde_cbor::from_slice(&buf).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("CBOR deserialization error: {}", e),
            )
        })
    }

    async fn read_response<T>(
        &mut self,
        _protocol: &Self::Protocol,
        io: &mut T,
    ) -> io::Result<Self::Response>
    where
        T: AsyncRead + Unpin + Send,
    {
        let mut buf = Vec::new();
        io.take(MAX_MESSAGE_SIZE).read_to_end(&mut buf).await?;

        serde_cbor::from_slice(&buf).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("CBOR deserialization error: {}", e),
            )
        })
    }

    async fn write_request<T>(
        &mut self,
        _protocol: &Self::Protocol,
        io: &mut T,
        req: Self::Request,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        let data = serde_cbor::to_vec(&req).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("CBOR serialization error: {}", e),
            )
        })?;

        io.write_all(&data).await?;
        io.close().await?;

        Ok(())
    }

    async fn write_response<T>(
        &mut self,
        _protocol: &Self::Protocol,
        io: &mut T,
        res: Self::Response,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        let data = serde_cbor::to_vec(&res).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("CBOR serialization error: {}", e),
            )
        })?;

        io.write_all(&data).await?;
        io.close().await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::io::Cursor;
    use libp2p::request_response::Codec;

    #[tokio::test]
    async fn test_codec_roundtrip_request() {
        let mut codec = PushLogCodec::new();
        let protocol = libp2p::StreamProtocol::new("/defra/0.0.1");

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
        let protocol = libp2p::StreamProtocol::new("/defra/0.0.1");

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
}
