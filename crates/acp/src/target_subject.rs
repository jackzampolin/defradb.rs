//! Parse a relationship-target string from the CLI/HTTP edge into a structured
//! [`Subject`]. This is the *only* string→`Subject` boundary; everything
//! downstream (the `DocumentACP` API, the SourceHub provider) carries the
//! structured subject, never a re-stringified form.
//!
//! Grammar:
//! ```text
//! target    := "*"                         (all actors)
//!            | "did:" …                     (a single actor)
//!            | resource ":" object_id [ "#" relation ]
//! object_id := '"' any-chars '"'           (quoted — may contain : / # …)
//!            | bare_token                   (no ':' '#' or quotes)
//! ```
//! A quoted `object_id` lets collection-level object ids be path-like
//! (`directory:"/team"`) or otherwise carry separators without mis-splitting.

use identity::Did;
use zanzibar::types::Subject;

use crate::error::{Error, Result};

fn invalid(target: &str, why: &str) -> Error {
    Error::InvalidRelation(format!("invalid relationship target '{}': {}", target, why))
}

/// Parse a target string into a [`Subject`]. See the module docs for the grammar.
pub fn parse_target_subject(target: &str) -> Result<Subject> {
    if target == "*" {
        return Ok(Subject::Wildcard);
    }
    if target.starts_with("did:") {
        let did = Did::new(target)
            .map_err(|e| invalid(target, &format!("not a valid actor DID: {}", e)))?;
        return Ok(Subject::Entity(did));
    }

    let (resource, rest) = target.split_once(':').ok_or_else(|| {
        invalid(
            target,
            "expected 'did:…', '*', 'resource:id', or 'resource:id#relation'",
        )
    })?;
    if resource.is_empty() {
        return Err(invalid(target, "empty resource"));
    }

    let (object_id, relation) = if let Some(after_quote) = rest.strip_prefix('"') {
        // Quoted object id: take everything up to the closing quote verbatim.
        let close = after_quote
            .find('"')
            .ok_or_else(|| invalid(target, "unterminated quoted object id"))?;
        let object_id = &after_quote[..close];
        let tail = &after_quote[close + 1..];
        let relation = match tail.strip_prefix('#') {
            Some(rel) => rel,
            None if tail.is_empty() => "",
            None => {
                return Err(invalid(
                    target,
                    "unexpected characters after quoted object id",
                ))
            }
        };
        (object_id, relation)
    } else {
        // Unquoted: a bare object id with an optional '#relation'.
        match rest.split_once('#') {
            Some((object_id, relation)) => (object_id, relation),
            None => (rest, ""),
        }
    };

    if object_id.is_empty() {
        return Err(invalid(target, "empty object id"));
    }
    if !relation.is_empty() && relation.contains('#') {
        return Err(invalid(target, "relation must not contain '#'"));
    }

    Ok(Subject::entity_set(resource, object_id, relation))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn es(s: &str) -> (String, String, String) {
        match parse_target_subject(s).unwrap() {
            Subject::EntitySet {
                resource,
                object_id,
                relation,
            } => (resource, object_id, relation),
            other => panic!("expected EntitySet, got {:?}", other),
        }
    }

    #[test]
    fn parses_wildcard() {
        assert!(matches!(
            parse_target_subject("*").unwrap(),
            Subject::Wildcard
        ));
    }

    #[test]
    fn parses_actor_did() {
        let s = parse_target_subject("did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK")
            .unwrap();
        assert!(s.is_entity());
    }

    #[test]
    fn parses_object_edge() {
        assert_eq!(
            es("directory:teamdir"),
            ("directory".into(), "teamdir".into(), String::new())
        );
    }

    #[test]
    fn parses_userset() {
        assert_eq!(
            es("group:hr#participant"),
            ("group".into(), "hr".into(), "participant".into())
        );
    }

    #[test]
    fn parses_quoted_path_object_id() {
        // The hardening case: a path-like object id that the naive split mangles.
        assert_eq!(
            es("directory:\"/team\""),
            ("directory".into(), "/team".into(), String::new())
        );
    }

    #[test]
    fn parses_quoted_object_id_with_embedded_separators() {
        assert_eq!(
            es("directory:\"a:b#c\""),
            ("directory".into(), "a:b#c".into(), String::new())
        );
    }

    #[test]
    fn parses_quoted_object_id_with_userset_relation() {
        assert_eq!(
            es("directory:\"/team\"#reader"),
            ("directory".into(), "/team".into(), "reader".into())
        );
    }

    #[test]
    fn rejects_unqualified_target() {
        assert!(parse_target_subject("not-a-target").is_err());
    }

    #[test]
    fn rejects_unterminated_quote() {
        assert!(parse_target_subject("directory:\"/team").is_err());
    }

    #[test]
    fn rejects_malformed_did() {
        assert!(parse_target_subject("did:bogus").is_err());
    }

    #[test]
    fn rejects_empty_object_id() {
        assert!(parse_target_subject("directory:").is_err());
    }
}
