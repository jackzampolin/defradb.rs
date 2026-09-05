//! HTTPS configuration and connection shutdown.

use std::io;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;

use axum::Router;
use axum_server::tls_rustls::RustlsConfig;
use rustls::pki_types::{pem::PemObject, CertificateDer, PrivateKeyDer};
use tokio::net::TcpListener;

/// A validated TLS certificate chain and matching private key.
pub struct TlsConfig(RustlsConfig);

impl TlsConfig {
    /// Load PEM files before starting the server. Invalid or mismatched keys
    /// are rejected rather than falling back to plaintext.
    pub async fn from_pem_file(cert: impl AsRef<Path>, key: impl AsRef<Path>) -> io::Result<Self> {
        let cert = cert.as_ref().to_path_buf();
        let key = key.as_ref().to_path_buf();
        tokio::task::spawn_blocking(move || {
            let certificates = CertificateDer::pem_file_iter(&cert)
                .and_then(Iterator::collect::<std::result::Result<Vec<_>, _>>)
                .map_err(|e| io::Error::other(format!("certificate {}: {e}", cert.display())))?;
            let key = PrivateKeyDer::from_pem_file(&key)
                .map_err(|e| io::Error::other(format!("private key {}: {e}", key.display())))?;
            // Select the provider explicitly: other transports can enable a
            // different provider in the same binary.
            let mut config = rustls::ServerConfig::builder_with_provider(Arc::new(
                rustls::crypto::ring::default_provider(),
            ))
            .with_safe_default_protocol_versions()
            .map_err(io::Error::other)?
            .with_no_client_auth()
            .with_single_cert(certificates, key)
            .map_err(io::Error::other)?;
            config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
            Ok(Self(RustlsConfig::from_config(Arc::new(config))))
        })
        .await
        .map_err(io::Error::other)?
    }

    pub(crate) async fn serve(self, listener: TcpListener, router: Router) -> io::Result<()> {
        let shutdown = ShutdownOnDrop(axum_server::Handle::new());
        axum_server::from_tcp_rustls(listener.into_std()?, self.0)?
            .handle(shutdown.0.clone())
            .serve(router.into_make_service())
            .await
    }
}

// Node shutdown cancels the server future. Also notify its connection tasks
// so established keep-alive connections cannot continue serving requests.
struct ShutdownOnDrop(axum_server::Handle<SocketAddr>);

impl Drop for ShutdownOnDrop {
    fn drop(&mut self) {
        self.0.shutdown();
    }
}
