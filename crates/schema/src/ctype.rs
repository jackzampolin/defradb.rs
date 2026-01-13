//! CRDT type definitions
//!
//! # JSON Serialization Format (Go-compatible)
//!
//! CType is serialized as its integer value to match Go DefraDB:
//! - 0 = None
//! - 1 = LwwRegister
//! - 2 = Object
//! - 3 = Composite
//! - 4 = PnCounter
//! - 5 = PCounter

use crate::FieldKind;
use serde::{de, Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;

/// Which CRDT to use for a field
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[repr(u8)]
pub enum CType {
    /// No CRDT (for relations and special fields)
    None = 0,
    /// Last-Write-Wins Register (default for most fields)
    #[default]
    LwwRegister = 1,
    /// Object CRDT (for embedded objects)
    Object = 2,
    /// Composite CRDT (document level)
    Composite = 3,
    /// Positive-Negative Counter (increment/decrement)
    PnCounter = 4,
    /// Positive Counter (increment only)
    PCounter = 5,
}

impl Serialize for CType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u8(*self as u8)
    }
}

impl<'de> Deserialize<'de> for CType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = serde_json::Value::deserialize(deserializer)?;

        match &value {
            // Integer format (Go's default)
            serde_json::Value::Number(n) => {
                let kind = n
                    .as_u64()
                    .ok_or_else(|| de::Error::custom("CType must be a positive integer"))?
                    as u8;
                Ok(match kind {
                    0 => CType::None,
                    1 => CType::LwwRegister,
                    2 => CType::Object,
                    3 => CType::Composite,
                    4 => CType::PnCounter,
                    5 => CType::PCounter,
                    _ => CType::None, // Unknown defaults to None
                })
            }
            // String format (for human-readable configs)
            serde_json::Value::String(s) => match s.as_str() {
                "None" | "none" | "NONE_CRDT" => Ok(CType::None),
                "LwwRegister" | "lww" | "LWW_REGISTER" => Ok(CType::LwwRegister),
                "Object" | "object" | "OBJECT" => Ok(CType::Object),
                "Composite" | "composite" | "COMPOSITE" => Ok(CType::Composite),
                "PnCounter" | "pncounter" | "PN_COUNTER" => Ok(CType::PnCounter),
                "PCounter" | "pcounter" | "P_COUNTER" => Ok(CType::PCounter),
                _ => Err(de::Error::custom(format!("Unknown CType: {}", s))),
            },
            // Null defaults to LwwRegister
            serde_json::Value::Null => Ok(CType::LwwRegister),
            _ => Err(de::Error::custom("CType must be a number or string")),
        }
    }
}

impl CType {
    /// Check if this CRDT type is compatible with a field kind
    pub fn is_compatible_with(&self, kind: &FieldKind) -> bool {
        match self {
            CType::None => true,
            CType::LwwRegister => true,
            CType::Object => kind.is_object(),
            CType::Composite => true,
            // Counters only work with numeric types
            CType::PnCounter | CType::PCounter => kind.is_numeric(),
        }
    }

    /// Returns true if this is a counter type
    pub fn is_counter(&self) -> bool {
        matches!(self, CType::PnCounter | CType::PCounter)
    }
}

impl fmt::Display for CType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CType::None => write!(f, "None"),
            CType::LwwRegister => write!(f, "LwwRegister"),
            CType::Object => write!(f, "Object"),
            CType::Composite => write!(f, "Composite"),
            CType::PnCounter => write!(f, "PnCounter"),
            CType::PCounter => write!(f, "PCounter"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default() {
        assert_eq!(CType::default(), CType::LwwRegister);
    }

    #[test]
    fn test_counter_compatibility() {
        assert!(CType::PnCounter.is_compatible_with(&FieldKind::int()));
        assert!(CType::PnCounter.is_compatible_with(&FieldKind::float64()));
        assert!(CType::PnCounter.is_compatible_with(&FieldKind::float32()));
        assert!(!CType::PnCounter.is_compatible_with(&FieldKind::string()));
        assert!(!CType::PnCounter.is_compatible_with(&FieldKind::bool()));
        assert!(!CType::PCounter.is_compatible_with(&FieldKind::datetime()));
    }

    #[test]
    fn test_lww_compatibility() {
        assert!(CType::LwwRegister.is_compatible_with(&FieldKind::string()));
        assert!(CType::LwwRegister.is_compatible_with(&FieldKind::int()));
        assert!(CType::LwwRegister.is_compatible_with(&FieldKind::bool()));
    }

    #[test]
    fn test_object_compatibility() {
        assert!(CType::Object.is_compatible_with(&FieldKind::relation("users", false)));
        assert!(!CType::Object.is_compatible_with(&FieldKind::string()));
    }

    #[test]
    fn test_is_counter() {
        assert!(CType::PnCounter.is_counter());
        assert!(CType::PCounter.is_counter());
        assert!(!CType::LwwRegister.is_counter());
        assert!(!CType::None.is_counter());
    }

    #[test]
    fn test_display() {
        assert_eq!(CType::LwwRegister.to_string(), "LwwRegister");
        assert_eq!(CType::PnCounter.to_string(), "PnCounter");
    }

    #[test]
    fn test_repr_values() {
        assert_eq!(CType::None as u8, 0);
        assert_eq!(CType::LwwRegister as u8, 1);
        assert_eq!(CType::Object as u8, 2);
        assert_eq!(CType::Composite as u8, 3);
        assert_eq!(CType::PnCounter as u8, 4);
        assert_eq!(CType::PCounter as u8, 5);
    }

    #[test]
    fn test_serialization_roundtrip() {
        let ctypes = vec![
            CType::None,
            CType::LwwRegister,
            CType::Object,
            CType::Composite,
            CType::PnCounter,
            CType::PCounter,
        ];

        for ct in ctypes {
            let json = serde_json::to_string(&ct).unwrap();
            let parsed: CType = serde_json::from_str(&json).unwrap();
            assert_eq!(ct, parsed);
        }
    }
}
