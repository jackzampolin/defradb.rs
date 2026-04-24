//! Replicator types for persistent peer replication configuration.
//!
//! A replicator is a peer that is authorized to replicate specific collections.
//! This module defines the types used to persist and manage replicator state.
//!
//! # Wire format parity with Go
//!
//! Go DefraDB persists `client.Replicator` with `encoding/json` (see
//! `defradb/internal/db/p2p/replicator.go:149,508,525`), using the field
//! names `ID`, `Addresses`, `CollectionIDs`, `Status`, `LastStatusChange`.
//! Rust must match byte-for-byte so a shared peerstore (or future migration
//! path) can round-trip between implementations.

use chrono::{DateTime, NaiveDate, SecondsFormat, Utc};
use libp2p::{Multiaddr, PeerId};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use thiserror::Error;

/// Error type for replicator operations.
#[derive(Debug, Clone, Error)]
#[non_exhaustive]
pub enum ReplicatorError {
    /// The peer ID string is invalid.
    #[error("invalid peer ID: {0}")]
    InvalidPeerId(String),
    /// No collections specified.
    #[error("collections cannot be empty")]
    EmptyCollections,
    /// Unknown `ReplicatorStatus` byte on decode.
    #[error("unknown ReplicatorStatus byte: {0}")]
    InvalidStatus(u8),
}

/// Status of a replicator, mirroring Go's `client.ReplicatorStatus`.
///
/// Go defines it as `uint8` with `Active = 0`, `Inactive = 1`
/// (`defradb/client/replicator.go:27-34`). Rust serializes as the same
/// integer so JSON round-trips byte-for-byte.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
#[serde(into = "u8", try_from = "u8")]
pub enum ReplicatorStatus {
    #[default]
    Active = 0,
    Inactive = 1,
}

impl From<ReplicatorStatus> for u8 {
    fn from(s: ReplicatorStatus) -> u8 {
        s as u8
    }
}

impl TryFrom<u8> for ReplicatorStatus {
    type Error = ReplicatorError;
    fn try_from(v: u8) -> Result<Self, Self::Error> {
        match v {
            0 => Ok(Self::Active),
            1 => Ok(Self::Inactive),
            other => Err(ReplicatorError::InvalidStatus(other)),
        }
    }
}

/// Go's `time.Time` zero value serializes as `"0001-01-01T00:00:00Z"`.
/// Used as the default `LastStatusChange` for freshly-created replicators
/// so the wire format stays identical to a Go-produced record.
fn go_time_zero() -> DateTime<Utc> {
    NaiveDate::from_ymd_opt(1, 1, 1)
        .expect("year 0001-01-01 is a valid date")
        .and_hms_opt(0, 0, 0)
        .expect("00:00:00 is a valid time")
        .and_utc()
}

/// Accept either `null` or `[]` for list fields.
///
/// Go distinguishes `nil` (emits `null`) from empty `[]string{}` (emits `[]`);
/// Rust's `Vec<String>` collapses both into an empty Vec. On decode we accept
/// either so Rust-side parsing is tolerant of both Go-side cases.
fn null_as_empty_vec<'de, D>(d: D) -> Result<Vec<String>, D::Error>
where
    D: Deserializer<'de>,
{
    Ok(Option::<Vec<String>>::deserialize(d)?.unwrap_or_default())
}

/// Serde adapter that matches Go's `time.Time.MarshalJSON`.
///
/// Go's encoder uses `RFC3339Nano`, which strips trailing zeros from the
/// fractional seconds (e.g. 100 ms serializes as `.1`, 1 ms as `.001`).
/// Chrono's default serializer pads to 3/6/9 digits, which means a timestamp
/// like 100 ms round-trips as `.100` — byte-different from Go. Since the
/// whole point of this struct is a byte-exact match against Go's peerstore
/// output, we implement the Go rule ourselves on the write side. Decode
/// stays on chrono's default, which already accepts both forms.
mod go_time_serde {
    use super::*;

    pub fn serialize<S: Serializer>(t: &DateTime<Utc>, s: S) -> Result<S::Ok, S::Error> {
        // `SecondsFormat::AutoSi` picks 0/3/6/9 digits; trim trailing zeros
        // off the fractional tail to match Go's RFC3339Nano rule.
        let formatted = t.to_rfc3339_opts(SecondsFormat::AutoSi, true);
        let normalized = match formatted.find('.') {
            None => formatted,
            Some(dot_idx) => {
                // Suffix after the digits is always "Z" for UTC here.
                let z_idx = formatted.len() - 1;
                let digits = &formatted[dot_idx + 1..z_idx];
                let trimmed = digits.trim_end_matches('0');
                if trimmed.is_empty() {
                    // All zeros → drop the fractional component entirely.
                    format!("{}Z", &formatted[..dot_idx])
                } else {
                    format!("{}.{}Z", &formatted[..dot_idx], trimmed)
                }
            }
        };
        s.serialize_str(&normalized)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<DateTime<Utc>, D::Error> {
        DateTime::<Utc>::deserialize(d)
    }
}

/// Information about a replicator peer.
///
/// Persisted to the peerstore as JSON, matching Go's `client.Replicator`
/// wire format exactly so a shared datastore can be read by either
/// implementation. See `defradb/client/replicator.go:18-24`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicatorInfo {
    #[serde(rename = "ID")]
    pub id: String,

    #[serde(rename = "Addresses", default, deserialize_with = "null_as_empty_vec")]
    pub addresses: Vec<String>,

    #[serde(
        rename = "CollectionIDs",
        default,
        deserialize_with = "null_as_empty_vec"
    )]
    pub collections: Vec<String>,

    #[serde(rename = "Status", default)]
    pub status: ReplicatorStatus,

    #[serde(
        rename = "LastStatusChange",
        default = "go_time_zero",
        with = "go_time_serde"
    )]
    pub last_status_change: DateTime<Utc>,
}

impl ReplicatorInfo {
    /// Create a new replicator info.
    ///
    /// Requires a non-empty collections list — an empty list would register a
    /// replicator with nothing to replicate, which is never a valid state.
    /// Use [`ReplicatorInfo::from_raw`] when reconstructing from storage or
    /// test fixtures where validation is not desired.
    pub fn new(peer_id: PeerId, collections: Vec<String>) -> Result<Self, ReplicatorError> {
        if collections.is_empty() {
            return Err(ReplicatorError::EmptyCollections);
        }
        Ok(Self {
            id: peer_id.to_string(),
            addresses: Vec::new(),
            collections,
            status: ReplicatorStatus::Active,
            last_status_change: go_time_zero(),
        })
    }

    /// Unchecked constructor for test fixtures and raw peerstore reads.
    ///
    /// Skips collection-non-empty validation and peer-id parsing. Leaves
    /// `status` at `Active` and `last_status_change` at Go's `time.Time{}`
    /// zero value; callers that care about those fields must set them
    /// explicitly. Real persisted data is normally decoded directly via
    /// `from_bytes`, which honors the on-wire values.
    pub fn from_raw(peer_id: String, collections: Vec<String>, addresses: Vec<String>) -> Self {
        Self {
            id: peer_id,
            addresses,
            collections,
            status: ReplicatorStatus::Active,
            last_status_change: go_time_zero(),
        }
    }

    /// Get the peer ID. Returns `None` if the stored ID is not a valid libp2p PeerId.
    pub fn peer_id(&self) -> Option<PeerId> {
        self.id.parse().ok()
    }

    /// Get the peer ID string (raw, possibly invalid).
    pub fn peer_id_str(&self) -> &str {
        &self.id
    }

    /// Try to get the peer ID, returning an error if invalid.
    pub fn try_peer_id(&self) -> Result<PeerId, ReplicatorError> {
        self.id
            .parse()
            .map_err(|_| ReplicatorError::InvalidPeerId(self.id.clone()))
    }

    /// Parsed addresses. Invalid entries are filtered out.
    pub fn addresses(&self) -> Vec<Multiaddr> {
        self.addresses
            .iter()
            .filter_map(|a| a.parse().ok())
            .collect()
    }

    /// Raw address strings as persisted.
    pub fn addresses_str(&self) -> &[String] {
        &self.addresses
    }

    /// Serialize to JSON bytes for peerstore persistence.
    ///
    /// Matches the byte layout Go produces via `encoding/json`
    /// (`defradb/internal/db/p2p/replicator.go:149`).
    pub fn to_bytes(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(self)
    }

    /// Deserialize from JSON bytes written by either Go or Rust.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(bytes)
    }
}
