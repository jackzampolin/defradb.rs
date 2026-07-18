#[cfg(test)]
use std::net::SocketAddr;

use axum::{routing::post, Json, Router};
use serde_json::{json, Value};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

use crate::error::{FfiTestError, Result};

const OLLAMA_TEST_ADDRESS: &str = "localhost:11434";
const OLLAMA_TEST_IPV4: &str = "127.0.0.1:11434";
const OLLAMA_TEST_IPV6: &str = "[::1]:11434";
const EMBEDDING_DIMENSIONS: usize = 768;

pub(crate) struct EmbeddingFixture {
    tasks: Vec<JoinHandle<()>>,
}

impl EmbeddingFixture {
    pub(crate) async fn start_for(packages: &[String]) -> Result<Option<Self>> {
        if !packages
            .iter()
            .any(|package| package.starts_with("mutation/") && package.ends_with("/embeddings"))
        {
            return Ok(None);
        }

        Self::start_localhost()
            .await
            .map(Some)
            .map_err(|error| {
                FfiTestError::TestExecution(format!(
                    "cannot start the embedding fixture on {OLLAMA_TEST_ADDRESS}: {error}; stop Ollama or any other service using that port"
                ))
            })
    }

    async fn start_localhost() -> std::io::Result<Self> {
        let ipv4 = TcpListener::bind(OLLAMA_TEST_IPV4).await?;
        let ipv6 = match TcpListener::bind(OLLAMA_TEST_IPV6).await {
            Ok(listener) => Some(listener),
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::AddrNotAvailable | std::io::ErrorKind::Unsupported
                ) =>
            {
                None
            }
            Err(error) => return Err(error),
        };

        let mut tasks = vec![Self::serve(ipv4)];
        if let Some(listener) = ipv6 {
            tasks.push(Self::serve(listener));
        }
        Ok(Self { tasks })
    }

    #[cfg(test)]
    async fn start_on(address: &str) -> std::io::Result<(Self, SocketAddr)> {
        let listener = TcpListener::bind(address).await?;
        let local_addr = listener.local_addr()?;
        Ok((
            Self {
                tasks: vec![Self::serve(listener)],
            },
            local_addr,
        ))
    }

    fn serve(listener: TcpListener) -> JoinHandle<()> {
        let app = Router::new().route("/api/embeddings", post(embeddings));
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        })
    }
}

impl Drop for EmbeddingFixture {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

async fn embeddings(Json(_request): Json<Value>) -> Json<Value> {
    Json(json!({
        "data": [{
            "embedding": vec![0.0; EMBEDDING_DIMENSIONS],
        }],
    }))
}

#[cfg(test)]
mod tests {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    use super::*;

    #[tokio::test]
    async fn serves_openai_compatible_embedding() {
        let (_fixture, address) = EmbeddingFixture::start_on("127.0.0.1:0").await.unwrap();
        let body = r#"{"model":"nomic-embed-text","input":"hello"}"#;
        let request = format!(
            "POST /api/embeddings HTTP/1.1\r\nHost: {address}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );

        let mut stream = TcpStream::connect(address).await.unwrap();
        stream.write_all(request.as_bytes()).await.unwrap();
        let mut response = Vec::new();
        stream.read_to_end(&mut response).await.unwrap();

        let response = String::from_utf8(response).unwrap();
        let (_, body) = response.split_once("\r\n\r\n").unwrap();
        let json: Value = serde_json::from_str(body).unwrap();
        assert_eq!(
            json.pointer("/data/0/embedding")
                .and_then(Value::as_array)
                .map(Vec::len),
            Some(EMBEDDING_DIMENSIONS)
        );
    }
}
