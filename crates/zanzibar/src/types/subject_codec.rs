//! Canonical codec between [`Subject`] and the frozen cross-object ACP wire
//! tuple `(kind: u8, resource, object_id, relation)`.
//!
//! This is the single source of truth for the cross-object ACP wire encoding.
//! Both defradb's SourceHub provider encoder and hub's `decode_subject` consume
//! it, so the encode/decode pair must stay an exact inverse on the frozen
//! grammar.
//!
//! # Frozen contract (`kind` → [`Subject`])
//!
//! | kind | meaning      | fields set                       | Subject                                                 |
//! |------|--------------|----------------------------------|---------------------------------------------------------|
//! | 0    | Entity       | `object_id` (DID)                | [`Subject::Entity`]                                      |
//! | 1    | Wildcard     | none                             | [`Subject::Wildcard`]                                   |
//! | 2    | Object edge  | `resource`, `object_id`          | [`Subject::EntitySet`] with empty `relation`            |
//! | 3    | Userset      | `resource`, `object_id`, `relation` | [`Subject::EntitySet`] with non-empty `relation`     |
//! | 4    | reserved     | (TypedWildcard)                  | not implemented                                         |

use crate::did::Did;
use crate::error::{Error, Result};
use crate::types::Subject;

/// Encodes a [`Subject`] into the frozen wire tuple `(kind, resource, object_id, relation)`.
///
/// # Errors
///
/// Returns [`Error::InvalidSubjectEncoding`] for [`Subject::TypedWildcard`],
/// which is reserved (`kind` 4) but not yet part of the frozen grammar.
pub fn encode_subject(subject: &Subject) -> Result<(u8, String, String, String)> {
    match subject {
        Subject::Entity(did) => Ok((0, String::new(), did.to_string(), String::new())),
        Subject::Wildcard => Ok((1, String::new(), String::new(), String::new())),
        Subject::EntitySet {
            resource,
            object_id,
            relation,
        } => {
            if relation.is_empty() {
                Ok((2, resource.clone(), object_id.clone(), String::new()))
            } else {
                Ok((3, resource.clone(), object_id.clone(), relation.clone()))
            }
        }
        Subject::TypedWildcard { resource } => Err(Error::InvalidSubjectEncoding(format!(
            "TypedWildcard (resource '{resource}') is reserved (kind 4) but not implemented"
        ))),
    }
}

/// Decodes the frozen wire tuple `(kind, resource, object_id, relation)` into a [`Subject`].
///
/// This is a trust boundary: it is total and strict, rejecting any tuple that
/// does not exactly match the frozen grammar rather than producing a degenerate
/// subject.
///
/// # Errors
///
/// Returns [`Error::InvalidSubjectEncoding`] when fields are inconsistent with
/// the given `kind`, when `kind` 0's `object_id` is not a valid DID, or when
/// `kind` is unrecognized (including the reserved `4`).
pub fn decode_subject(
    kind: u8,
    resource: &str,
    object_id: &str,
    relation: &str,
) -> Result<Subject> {
    match kind {
        0 => {
            if !resource.is_empty() {
                return Err(Error::InvalidSubjectEncoding(format!(
                    "kind 0 (Entity) requires empty resource, got '{resource}'"
                )));
            }
            if !relation.is_empty() {
                return Err(Error::InvalidSubjectEncoding(format!(
                    "kind 0 (Entity) requires empty relation, got '{relation}'"
                )));
            }
            let did = Did::new(object_id).map_err(|e| {
                Error::InvalidSubjectEncoding(format!(
                    "kind 0 (Entity) object_id '{object_id}' is not a valid DID: {e}"
                ))
            })?;
            Ok(Subject::Entity(did))
        }
        1 => {
            if !resource.is_empty() || !object_id.is_empty() || !relation.is_empty() {
                return Err(Error::InvalidSubjectEncoding(format!(
                    "kind 1 (Wildcard) requires all fields empty, got resource '{resource}', object_id '{object_id}', relation '{relation}'"
                )));
            }
            Ok(Subject::Wildcard)
        }
        2 => {
            if resource.is_empty() {
                return Err(Error::InvalidSubjectEncoding(
                    "kind 2 (object edge) requires non-empty resource".to_string(),
                ));
            }
            if object_id.is_empty() {
                return Err(Error::InvalidSubjectEncoding(
                    "kind 2 (object edge) requires non-empty object_id".to_string(),
                ));
            }
            if !relation.is_empty() {
                return Err(Error::InvalidSubjectEncoding(format!(
                    "kind 2 (object edge) requires empty relation, got '{relation}'"
                )));
            }
            Ok(Subject::EntitySet {
                resource: resource.to_string(),
                object_id: object_id.to_string(),
                relation: String::new(),
            })
        }
        3 => {
            if resource.is_empty() {
                return Err(Error::InvalidSubjectEncoding(
                    "kind 3 (userset) requires non-empty resource".to_string(),
                ));
            }
            if object_id.is_empty() {
                return Err(Error::InvalidSubjectEncoding(
                    "kind 3 (userset) requires non-empty object_id".to_string(),
                ));
            }
            if relation.is_empty() {
                return Err(Error::InvalidSubjectEncoding(
                    "kind 3 (userset) requires non-empty relation".to_string(),
                ));
            }
            Ok(Subject::EntitySet {
                resource: resource.to_string(),
                object_id: object_id.to_string(),
                relation: relation.to_string(),
            })
        }
        other => Err(Error::InvalidSubjectEncoding(format!(
            "unrecognized subject kind {other}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn entity(did: &str) -> Subject {
        Subject::Entity(Did::new(did).unwrap())
    }

    // A valid did:key DID for testing. Did::new only enforces the prefix.
    const SAMPLE_DID: &str = "did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK";

    #[test]
    fn encode_typed_wildcard_is_error() {
        let s = Subject::TypedWildcard {
            resource: "Document".to_string(),
        };
        assert!(matches!(
            encode_subject(&s),
            Err(Error::InvalidSubjectEncoding(_))
        ));
    }

    #[test]
    fn encode_entity_kind_0() {
        let (kind, resource, object_id, relation) = encode_subject(&entity(SAMPLE_DID)).unwrap();
        assert_eq!(kind, 0);
        assert_eq!(resource, "");
        assert_eq!(object_id, SAMPLE_DID);
        assert_eq!(relation, "");
    }

    #[test]
    fn encode_wildcard_kind_1() {
        let (kind, resource, object_id, relation) = encode_subject(&Subject::Wildcard).unwrap();
        assert_eq!(kind, 1);
        assert_eq!(
            (resource.as_str(), object_id.as_str(), relation.as_str()),
            ("", "", "")
        );
    }

    #[test]
    fn encode_object_edge_kind_2() {
        let s = Subject::entity_set("Document", "bae-123", "");
        let (kind, resource, object_id, relation) = encode_subject(&s).unwrap();
        assert_eq!(kind, 2);
        assert_eq!(resource, "Document");
        assert_eq!(object_id, "bae-123");
        assert_eq!(relation, "");
    }

    #[test]
    fn encode_userset_kind_3() {
        let s = Subject::entity_set("Document", "bae-123", "owner");
        let (kind, resource, object_id, relation) = encode_subject(&s).unwrap();
        assert_eq!(kind, 3);
        assert_eq!(resource, "Document");
        assert_eq!(object_id, "bae-123");
        assert_eq!(relation, "owner");
    }

    #[test]
    fn decode_kind_2_with_relation_is_error() {
        assert!(matches!(
            decode_subject(2, "Document", "bae-123", "owner"),
            Err(Error::InvalidSubjectEncoding(_))
        ));
    }

    #[test]
    fn decode_kind_3_without_relation_is_error() {
        assert!(matches!(
            decode_subject(3, "Document", "bae-123", ""),
            Err(Error::InvalidSubjectEncoding(_))
        ));
    }

    #[test]
    fn decode_kind_0_with_non_did_is_error() {
        assert!(matches!(
            decode_subject(0, "", "not-a-did", ""),
            Err(Error::InvalidSubjectEncoding(_))
        ));
    }

    #[test]
    fn decode_kind_0_with_non_empty_resource_is_error() {
        assert!(matches!(
            decode_subject(0, "Document", SAMPLE_DID, ""),
            Err(Error::InvalidSubjectEncoding(_))
        ));
    }

    #[test]
    fn decode_kind_0_with_non_empty_relation_is_error() {
        assert!(matches!(
            decode_subject(0, "", SAMPLE_DID, "owner"),
            Err(Error::InvalidSubjectEncoding(_))
        ));
    }

    #[test]
    fn decode_kind_1_with_non_empty_field_is_error() {
        assert!(matches!(
            decode_subject(1, "Document", "", ""),
            Err(Error::InvalidSubjectEncoding(_))
        ));
        assert!(matches!(
            decode_subject(1, "", "bae-123", ""),
            Err(Error::InvalidSubjectEncoding(_))
        ));
        assert!(matches!(
            decode_subject(1, "", "", "owner"),
            Err(Error::InvalidSubjectEncoding(_))
        ));
    }

    #[test]
    fn decode_kind_4_is_error() {
        assert!(matches!(
            decode_subject(4, "Document", "", ""),
            Err(Error::InvalidSubjectEncoding(_))
        ));
    }

    #[test]
    fn decode_unknown_kind_is_error() {
        assert!(matches!(
            decode_subject(99, "", "", ""),
            Err(Error::InvalidSubjectEncoding(_))
        ));
    }

    #[test]
    fn decode_kind_2_empty_resource_or_object_id_is_error() {
        assert!(matches!(
            decode_subject(2, "", "bae-123", ""),
            Err(Error::InvalidSubjectEncoding(_))
        ));
        assert!(matches!(
            decode_subject(2, "Document", "", ""),
            Err(Error::InvalidSubjectEncoding(_))
        ));
    }

    #[test]
    fn decode_kind_3_empty_resource_or_object_id_is_error() {
        assert!(matches!(
            decode_subject(3, "", "bae-123", "owner"),
            Err(Error::InvalidSubjectEncoding(_))
        ));
        assert!(matches!(
            decode_subject(3, "Document", "", "owner"),
            Err(Error::InvalidSubjectEncoding(_))
        ));
    }

    // Non-empty strings free of the empty sentinel, so they are valid field
    // values for resource/object_id/relation under the frozen grammar.
    fn non_empty() -> impl Strategy<Value = String> {
        "[A-Za-z0-9_:#-]{1,32}"
    }

    // Arbitrary did:key string. Did::new only validates the prefix, so any
    // suffix yields a valid DID.
    fn arb_did() -> impl Strategy<Value = String> {
        "[A-Za-z0-9]{1,40}".prop_map(|s| format!("did:key:z{s}"))
    }

    fn arb_subject() -> impl Strategy<Value = Subject> {
        prop_oneof![
            arb_did().prop_map(|d| Subject::Entity(Did::new(d).unwrap())),
            Just(Subject::Wildcard),
            (non_empty(), non_empty()).prop_map(|(r, o)| Subject::entity_set(r, o, "")),
            (non_empty(), non_empty(), non_empty())
                .prop_map(|(r, o, rel)| Subject::entity_set(r, o, rel)),
        ]
    }

    proptest! {
        #[test]
        fn round_trip(subject in arb_subject()) {
            let (kind, resource, object_id, relation) = encode_subject(&subject).unwrap();
            let decoded = decode_subject(kind, &resource, &object_id, &relation).unwrap();
            prop_assert_eq!(decoded, subject);
        }
    }
}
