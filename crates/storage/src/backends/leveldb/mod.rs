//! LevelDB backend implementation using rusty-leveldb (WASM only).
//!
//! This backend provides a pure Rust LevelDB implementation for WASM targets.
//! In WASM, it can use OPFS via the `OpfsEnv` environment for browser persistence.
//!
//! # Platform Notes
//!
//! This module is WASM-only because rusty-leveldb uses `Rc` internally which
//! is not `Send + Sync`. For native platforms, use `RedbStore` instead which
//! provides full concurrency support.
//!
//! # Features
//!
//! - Pure Rust LSM-tree implementation (no C dependencies)
//! - Supports custom `Env` implementations for different storage backends
//! - Full transaction support with snapshot isolation
//! - Compatible with Go DefraDB's LevelDB storage
mod iterator;
mod store;
mod transaction;

pub use store::LevelDbStore;
