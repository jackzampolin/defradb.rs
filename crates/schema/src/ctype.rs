//! CRDT type definitions

use crate::FieldKind;
use serde::{Deserialize, Serialize};
use std::fmt;

/// Which CRDT to use for a field
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
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
