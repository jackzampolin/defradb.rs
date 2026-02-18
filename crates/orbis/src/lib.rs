//! Orbis integration for DefraDB
//!
//! Provides a gRPC client for delegating document signing to an Orbis ring's
//! threshold BLS signing service.

mod client;

pub mod proto {
    tonic::include_proto!("orbis.utility.v1");
}

pub use client::OrbisClient;
