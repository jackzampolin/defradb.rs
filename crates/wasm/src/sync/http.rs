use defra_core::browser_sync::{BrowserSyncRequest, BrowserSyncResponse, MAX_SYNC_BODY_BYTES};
use futures::StreamExt;
use wasm_bindgen::{JsCast, JsValue};
use wasm_bindgen_futures::JsFuture;
use wasm_streams::ReadableStream;
use web_sys::{Headers, Request, RequestInit, Response};

use crate::error::{Result, WasmError};

use super::sse::SseStream;

const MAX_ERROR_BODY_BYTES: usize = 8 * 1024;

#[derive(Clone)]
pub(super) struct SyncHttpClient {
    base_url: String,
    auth_token: Option<String>,
}

impl SyncHttpClient {
    pub(super) fn new(server_url: &str, auth_token: Option<String>) -> Result<Self> {
        let base_url = server_url.trim().trim_end_matches('/');
        if base_url.is_empty() {
            return Err(WasmError::InvalidArgument(
                "sync server URL cannot be empty".into(),
            ));
        }
        let auth_token = auth_token
            .map(|token| normalize_token(&token))
            .transpose()?;
        Ok(Self {
            base_url: base_url.to_string(),
            auth_token,
        })
    }

    pub(super) async fn sync(&self, request: &BrowserSyncRequest) -> Result<BrowserSyncResponse> {
        let body = serde_json::to_string(request)?;
        let response = self
            .fetch(
                "POST",
                &format!("{}/api/v1/sync", self.base_url),
                "application/json",
                Some(&body),
            )
            .await?;
        let body = response_text(&response, MAX_SYNC_BODY_BYTES).await?;
        serde_json::from_str(&body).map_err(|error| {
            WasmError::Sync(format!("server returned an invalid sync response: {error}"))
        })
    }

    pub(super) async fn events(&self) -> Result<SseStream> {
        let response = self
            .fetch(
                "GET",
                &format!("{}/api/v1/events?event=update", self.base_url),
                "text/event-stream",
                None,
            )
            .await?;
        let content_type = response
            .headers()
            .get("content-type")
            .map_err(|error| js_error("failed to read event content type", error))?
            .unwrap_or_default();
        if !content_type.starts_with("text/event-stream") {
            return Err(WasmError::Sync(format!(
                "event endpoint returned unexpected content type '{content_type}'"
            )));
        }
        let body = response
            .body()
            .ok_or_else(|| WasmError::Sync("event endpoint returned no response body".into()))?;
        Ok(SseStream::new(ReadableStream::from_raw(body).into_stream()))
    }

    async fn fetch(
        &self,
        method: &str,
        url: &str,
        accept: &str,
        body: Option<&str>,
    ) -> Result<Response> {
        let headers =
            Headers::new().map_err(|error| js_error("failed to create headers", error))?;
        headers
            .set("Accept", accept)
            .map_err(|error| js_error("failed to set Accept header", error))?;
        if body.is_some() {
            headers
                .set("Content-Type", "application/json")
                .map_err(|error| js_error("failed to set Content-Type header", error))?;
        }
        if let Some(token) = &self.auth_token {
            headers
                .set("Authorization", &format!("Bearer {token}"))
                .map_err(|error| js_error("failed to set Authorization header", error))?;
        }

        let init = RequestInit::new();
        init.set_method(method);
        init.set_headers_headers(&headers);
        if let Some(body) = body {
            init.set_body(&JsValue::from_str(body));
        }
        let request = Request::new_with_str_and_init(url, &init)
            .map_err(|error| js_error("failed to create sync request", error))?;
        let window = web_sys::window()
            .ok_or_else(|| WasmError::Sync("browser Window is unavailable".into()))?;
        let value = JsFuture::from(window.fetch_with_request(&request))
            .await
            .map_err(|error| js_error("sync request failed", error))?;
        let response: Response = value
            .dyn_into()
            .map_err(|error| js_error("fetch returned a non-response value", error))?;
        if !response.ok() {
            let status = response.status();
            let message = response_text(&response, MAX_ERROR_BODY_BYTES)
                .await
                .unwrap_or_else(|error| error.to_string());
            return Err(WasmError::Sync(format!(
                "server returned HTTP {status}: {}",
                truncate(&message, 1024)
            )));
        }
        Ok(response)
    }
}

async fn response_text(response: &Response, max_bytes: usize) -> Result<String> {
    let Some(body) = response.body() else {
        return Ok(String::new());
    };
    let mut stream = ReadableStream::from_raw(body).into_stream();
    let mut bytes = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk
            .map_err(|error| WasmError::Sync(format!("response body read failed: {error:?}")))?;
        let chunk = js_sys::Uint8Array::new(&chunk).to_vec();
        if bytes.len().saturating_add(chunk.len()) > max_bytes {
            return Err(WasmError::Sync(format!(
                "response body exceeds {max_bytes} bytes"
            )));
        }
        bytes.extend_from_slice(&chunk);
    }
    String::from_utf8(bytes)
        .map_err(|error| WasmError::Sync(format!("response body is not valid UTF-8: {error}")))
}

fn normalize_token(token: &str) -> Result<String> {
    let token = token.trim();
    let token = token
        .strip_prefix("Bearer ")
        .or_else(|| token.strip_prefix("bearer "))
        .unwrap_or(token)
        .trim();
    if token.is_empty() {
        return Err(WasmError::InvalidArgument(
            "sync authentication token cannot be empty".into(),
        ));
    }
    Ok(token.to_string())
}

fn js_error(context: &str, error: JsValue) -> WasmError {
    let detail = error.as_string().unwrap_or_else(|| format!("{error:?}"));
    WasmError::Sync(format!("{context}: {detail}"))
}

fn truncate(value: &str, max_chars: usize) -> String {
    value.chars().take(max_chars).collect()
}
