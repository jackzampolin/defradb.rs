//! Command encoding helpers for hub.rs provider operations.

use crate::provider::SubjectRef;

pub(crate) fn subject_to_cmd_json(subject: &SubjectRef) -> serde_json::Value {
    match subject {
        SubjectRef::Actor(did) => serde_json::json!({ "Entity": did }),
        SubjectRef::AllActors => serde_json::json!("Wildcard"),
    }
}

pub(crate) fn encode_register_object_cmd(resource: &str, object_id: &str) -> Vec<u8> {
    // Matches hub.rs PolicyCmd::RegisterObject(Object { resource, id })
    serde_json::to_vec(&serde_json::json!({
        "RegisterObject": { "resource": resource, "id": object_id }
    }))
    .unwrap_or_default()
}

pub(crate) fn encode_archive_object_cmd(resource: &str, object_id: &str) -> Vec<u8> {
    // Matches hub.rs PolicyCmd::ArchiveObject(Object { resource, id })
    serde_json::to_vec(&serde_json::json!({
        "ArchiveObject": { "resource": resource, "id": object_id }
    }))
    .unwrap_or_default()
}

pub(crate) fn encode_set_relationship_cmd(
    resource: &str,
    object_id: &str,
    relation: &str,
    subject: &SubjectRef,
) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "SetRelationship": {
            "resource": resource,
            "object_id": object_id,
            "relation": relation,
            "subject": subject_to_cmd_json(subject),
        }
    }))
    .unwrap_or_default()
}

pub(crate) fn encode_delete_relationship_cmd(
    resource: &str,
    object_id: &str,
    relation: &str,
    subject: &SubjectRef,
) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "DeleteRelationship": {
            "resource": resource,
            "object_id": object_id,
            "relation": relation,
            "subject": subject_to_cmd_json(subject),
        }
    }))
    .unwrap_or_default()
}

pub(crate) fn resolve_registered_or_passthrough_bearer_token(
    did: &str,
) -> Result<Option<String>, crate::provider::ProviderError> {
    use k256::ecdsa::SigningKey;

    if let Some(signing_config) = defra_core::signing::get_identity(did) {
        if signing_config.has_local_private_key()
            && signing_config.key_type == defra_core::signing::SigningKeyType::Secp256k1
        {
            let key = SigningKey::from_slice(&signing_config.private_key_bytes).map_err(|e| {
                crate::provider::ProviderError::Config(format!("invalid signing key: {}", e))
            })?;

            return super::bearer::create_bearer_token(&key, did, 300)
                .map(Some)
                .map_err(|e| {
                    crate::provider::ProviderError::Config(format!(
                        "bearer token creation failed: {}",
                        e
                    ))
                });
        }
    }

    Ok(defra_core::signing::get_request_bearer_token(did))
}

#[cfg(test)]
mod tests {
    use serde_json::{json, Value};

    use super::*;

    #[test]
    fn encodes_register_object_as_hub_rs_policy_cmd() {
        let value: Value =
            serde_json::from_slice(&encode_register_object_cmd("users", "doc-1")).unwrap();

        assert_eq!(
            value,
            json!({
                "RegisterObject": { "resource": "users", "id": "doc-1" }
            })
        );
    }

    #[test]
    fn encodes_relationship_subjects_as_hub_rs_policy_cmds() {
        let actor = crate::provider::SubjectRef::Actor("did:key:zActor".to_string());
        let value: Value = serde_json::from_slice(&encode_set_relationship_cmd(
            "users", "doc-1", "reader", &actor,
        ))
        .unwrap();

        assert_eq!(
            value,
            json!({
                "SetRelationship": {
                    "resource": "users",
                    "object_id": "doc-1",
                    "relation": "reader",
                    "subject": { "Entity": "did:key:zActor" }
                }
            })
        );

        let value: Value = serde_json::from_slice(&encode_delete_relationship_cmd(
            "users",
            "doc-1",
            "reader",
            &crate::provider::SubjectRef::AllActors,
        ))
        .unwrap();

        assert_eq!(
            value,
            json!({
                "DeleteRelationship": {
                    "resource": "users",
                    "object_id": "doc-1",
                    "relation": "reader",
                    "subject": "Wildcard"
                }
            })
        );
    }
}
