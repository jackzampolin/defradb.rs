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
#[non_exhaustive]
pub enum CType {
    /// No CRDT (for relations and special fields)
    None,
    /// Last-Write-Wins Register (default for most fields)
    #[default]
    LwwRegister,
    /// Object CRDT (for embedded objects)
    Object,
    /// Composite CRDT (document level)
    Composite,
    /// Positive-Negative Counter (increment/decrement)
    PnCounter,
    /// Positive Counter (increment only)
    PCounter,
    /// Unknown CRDT type (preserves raw value for validation errors)
    Unknown(u8),
}

impl Serialize for CType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let value = match self {
            CType::None => 0u8,
            CType::LwwRegister => 1,
            CType::Object => 2,
            CType::Composite => 3,
            CType::PnCounter => 4,
            CType::PCounter => 5,
            CType::Unknown(v) => *v,
        };
        serializer.serialize_u8(value)
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
                    _ => CType::Unknown(kind), // Preserve unknown for validation
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
    /// Convert to u8 representation for CID generation
    pub fn to_u8(self) -> u8 {
        match self {
            CType::None => 0,
            CType::LwwRegister => 1,
            CType::Object => 2,
            CType::Composite => 3,
            CType::PnCounter => 4,
            CType::PCounter => 5,
            CType::Unknown(v) => v,
        }
    }

    /// Convert from u8 representation
    pub fn from_u8(value: u8) -> Self {
        match value {
            0 => CType::None,
            1 => CType::LwwRegister,
            2 => CType::Object,
            3 => CType::Composite,
            4 => CType::PnCounter,
            5 => CType::PCounter,
            v => CType::Unknown(v),
        }
    }

    /// Check if this CRDT type is compatible with a field kind
    pub fn is_compatible_with(&self, kind: &FieldKind) -> bool {
        match self {
            CType::None => true,
            CType::LwwRegister => true,
            CType::Object => kind.is_object(),
            CType::Composite => true,
            // Counters only work with numeric types
            CType::PnCounter | CType::PCounter => kind.is_numeric(),
            CType::Unknown(_) => false,
        }
    }

    /// Returns true if this is a counter type
    pub fn is_counter(&self) -> bool {
        matches!(self, CType::PnCounter | CType::PCounter)
    }

    /// Returns true if this counter type allows decrement (negative increments)
    ///
    /// - PnCounter (Positive-Negative Counter): allows both increment and decrement
    /// - PCounter (Positive Counter): only allows increment
    pub fn allows_decrement(&self) -> bool {
        matches!(self, CType::PnCounter)
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
            CType::Unknown(v) => write!(f, "Unknown({})", v),
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
    fn test_serialization_values() {
        // Verify each variant serializes to the expected integer
        assert_eq!(serde_json::to_string(&CType::None).unwrap(), "0");
        assert_eq!(serde_json::to_string(&CType::LwwRegister).unwrap(), "1");
        assert_eq!(serde_json::to_string(&CType::Object).unwrap(), "2");
        assert_eq!(serde_json::to_string(&CType::Composite).unwrap(), "3");
        assert_eq!(serde_json::to_string(&CType::PnCounter).unwrap(), "4");
        assert_eq!(serde_json::to_string(&CType::PCounter).unwrap(), "5");
    }

    #[test]
    fn test_unknown_preserved() {
        let parsed: CType = serde_json::from_str("99").unwrap();
        assert_eq!(parsed, CType::Unknown(99));
        assert!(!matches!(
            parsed,
            CType::None
                | CType::LwwRegister
                | CType::Object
                | CType::Composite
                | CType::PnCounter
                | CType::PCounter
        ));
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
