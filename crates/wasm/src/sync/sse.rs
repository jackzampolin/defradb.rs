use std::collections::VecDeque;

use futures::StreamExt;
use serde::Deserialize;
use wasm_streams::readable::IntoStream;

use crate::error::{Result, WasmError};

const MAX_EVENT_BUFFER_BYTES: usize = defra_core::browser_sync::MAX_SYNC_BODY_BYTES;

pub(super) struct SseStream {
    stream: IntoStream<'static>,
    decoder: SseDecoder,
    pending_doc_ids: VecDeque<String>,
}

impl SseStream {
    pub(super) fn new(stream: IntoStream<'static>) -> Self {
        Self {
            stream,
            decoder: SseDecoder::default(),
            pending_doc_ids: VecDeque::new(),
        }
    }

    pub(super) async fn next_document_id(&mut self) -> Result<Option<String>> {
        loop {
            if let Some(doc_id) = self.pending_doc_ids.pop_front() {
                return Ok(Some(doc_id));
            }
            let Some(chunk) = self.stream.next().await else {
                return Ok(None);
            };
            let chunk = chunk
                .map_err(|error| WasmError::Sync(format!("event stream read failed: {error:?}")))?;
            let bytes = js_sys::Uint8Array::new(&chunk).to_vec();
            self.pending_doc_ids.extend(self.decoder.push(&bytes)?);
        }
    }
}

#[derive(Default)]
struct SseDecoder {
    buffer: Vec<u8>,
}

impl SseDecoder {
    fn push(&mut self, bytes: &[u8]) -> Result<Vec<String>> {
        if self.buffer.len().saturating_add(bytes.len()) > MAX_EVENT_BUFFER_BYTES {
            return Err(WasmError::Sync(format!(
                "event frame exceeds {MAX_EVENT_BUFFER_BYTES} bytes"
            )));
        }
        self.buffer.extend_from_slice(bytes);

        let mut doc_ids = Vec::new();
        while let Some((index, delimiter_len)) = find_frame_end(&self.buffer) {
            if index > MAX_EVENT_BUFFER_BYTES {
                return Err(WasmError::Sync(format!(
                    "event frame exceeds {MAX_EVENT_BUFFER_BYTES} bytes"
                )));
            }
            let frame = self.buffer[..index].to_vec();
            self.buffer.drain(..index + delimiter_len);
            if let Some(doc_id) = parse_update(&frame) {
                doc_ids.push(doc_id);
            }
        }
        if self.buffer.len() > MAX_EVENT_BUFFER_BYTES {
            return Err(WasmError::Sync(format!(
                "event frame exceeds {MAX_EVENT_BUFFER_BYTES} bytes"
            )));
        }
        Ok(doc_ids)
    }
}

#[derive(Deserialize)]
struct EventEnvelope {
    name: String,
    data: EventData,
}

#[derive(Deserialize)]
struct EventData {
    doc_id: String,
}

fn parse_update(frame: &[u8]) -> Option<String> {
    let frame = std::str::from_utf8(frame).ok()?;
    let data = frame
        .lines()
        .filter_map(|line| line.strip_prefix("data:"))
        .map(|line| line.strip_prefix(' ').unwrap_or(line))
        .collect::<Vec<_>>()
        .join("\n");
    let event: EventEnvelope = serde_json::from_str(&data).ok()?;
    (event.name == "update" && !event.data.doc_id.is_empty()).then_some(event.data.doc_id)
}

fn find_frame_end(buffer: &[u8]) -> Option<(usize, usize)> {
    let lf = buffer.windows(2).position(|window| window == b"\n\n");
    let crlf = buffer.windows(4).position(|window| window == b"\r\n\r\n");
    match (lf, crlf) {
        (Some(left), Some(right)) if left <= right => Some((left, 2)),
        (Some(_), Some(right)) => Some((right, 4)),
        (Some(index), None) => Some((index, 2)),
        (None, Some(index)) => Some((index, 4)),
        (None, None) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wasm_bindgen_test::wasm_bindgen_test;

    #[wasm_bindgen_test]
    fn decodes_split_crlf_event() {
        let mut decoder = SseDecoder::default();
        assert!(decoder
            .push(b"event: next\r\ndata: {\"name\":\"up")
            .unwrap()
            .is_empty());
        assert_eq!(
            decoder
                .push(b"date\",\"data\":{\"doc_id\":\"bae-1\"}}\r\n\r\n")
                .unwrap(),
            vec!["bae-1"]
        );
    }

    #[wasm_bindgen_test]
    fn ignores_non_document_events() {
        let mut decoder = SseDecoder::default();
        assert!(decoder
            .push(b"data: {\"name\":\"update\",\"data\":{\"doc_id\":\"\"}}\n\n")
            .unwrap()
            .is_empty());
    }
}
