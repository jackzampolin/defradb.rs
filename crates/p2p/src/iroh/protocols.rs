//! ALPN protocol constants and wire format helpers for iroh transport.

use iroh::endpoint::{RecvStream, SendStream};

/// ALPN for PushLog request-response.
pub const ALPN_PUSHLOG: &[u8] = b"/defra-iroh/pushlog/0.1";

/// ALPN for document sync request.
pub const ALPN_DOCSYNC: &[u8] = b"/defra-iroh/docsync/0.1";

/// ALPN for document sync response (separate from request to avoid ambiguous decoding).
pub const ALPN_DOCSYNC_RESP: &[u8] = b"/defra-iroh/docsync/0.1/resp";

/// ALPN for branchable sync request.
pub const ALPN_BRANCHABLE: &[u8] = b"/defra-iroh/branchable/0.1";

/// ALPN for branchable sync response.
pub const ALPN_BRANCHABLE_RESP: &[u8] = b"/defra-iroh/branchable/0.1/resp";

/// ALPN for CAR block transfer request.
pub const ALPN_CAR: &[u8] = b"/defra-iroh/car/0.1";

/// ALPN for CAR block transfer response.
pub const ALPN_CAR_RESP: &[u8] = b"/defra-iroh/car/0.1/resp";

/// ALPN for searchable encryption artifacts.
pub const ALPN_SE: &[u8] = b"/defra-iroh/se/0.1";

/// ALPN for two-stream push protocol.
pub const ALPN_TWOSTREAM: &[u8] = b"/defra-iroh/twostream/0.1";

/// All ALPNs this node should accept.
pub const ALL_ALPNS: &[&[u8]] = &[
    ALPN_PUSHLOG,
    ALPN_DOCSYNC,
    ALPN_BRANCHABLE,
    ALPN_CAR,
    ALPN_CAR_RESP,
    ALPN_SE,
    ALPN_TWOSTREAM,
];

/// Maximum size for general messages (matches libp2p default).
pub const MAX_MESSAGE_SIZE: usize = 16 * 1024 * 1024; // 16 MiB

/// Maximum size for CAR block transfers (matches libp2p default).
pub const MAX_CAR_SIZE: usize = 64 * 1024 * 1024; // 64 MiB

/// Read a length-prefixed CBOR message from a QUIC recv stream.
///
/// The `max_size` parameter caps the allocation to prevent a malicious peer
/// from sending a large length prefix and causing an OOM.
pub async fn read_message<T: serde::de::DeserializeOwned>(
    recv: &mut RecvStream,
    max_size: usize,
) -> crate::error::Result<T> {
    let mut len_buf = [0u8; 4];
    recv.read_exact(&mut len_buf)
        .await
        .map_err(|e| crate::error::Error::Codec(format!("failed to read length: {}", e)))?;
    let len = u32::from_be_bytes(len_buf) as usize;

    if len > max_size {
        return Err(crate::error::Error::Codec(format!(
            "message too large: {} bytes exceeds limit of {} bytes",
            len, max_size
        )));
    }

    let mut payload = vec![0u8; len];
    recv.read_exact(&mut payload)
        .await
        .map_err(|e| crate::error::Error::Codec(format!("failed to read payload: {}", e)))?;

    serde_cbor::from_slice(&payload).map_err(|e| crate::error::Error::Codec(e.to_string()))
}

/// Write a length-prefixed CBOR message to a QUIC send stream.
pub async fn write_message<T: serde::Serialize>(
    send: &mut SendStream,
    value: &T,
) -> crate::error::Result<()> {
    let payload =
        serde_cbor::to_vec(value).map_err(|e| crate::error::Error::Codec(e.to_string()))?;
    let len = (payload.len() as u32).to_be_bytes();

    send.write_all(&len)
        .await
        .map_err(|e| crate::error::Error::Codec(format!("failed to write length: {}", e)))?;
    send.write_all(&payload)
        .await
        .map_err(|e| crate::error::Error::Codec(format!("failed to write payload: {}", e)))?;

    Ok(())
}
