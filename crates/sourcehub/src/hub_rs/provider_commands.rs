//! Command encoding helpers for hub.rs provider operations.

use crate::provider::SubjectRef;

pub(crate) fn subject_to_cmd_json(subject: &SubjectRef) -> serde_json::Value {
    match subject {
        SubjectRef::Actor(did) => serde_json::json!({ "actor": { "id": did } }),
        SubjectRef::AllActors => serde_json::json!({ "all_actors": {} }),
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
        "set_relationship_cmd": {
            "relationship": {
                "object": { "resource": resource, "id": object_id },
                "relation": relation,
                "subject": subject_to_cmd_json(subject),
            }
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
        "delete_relationship_cmd": {
            "relationship": {
                "object": { "resource": resource, "id": object_id },
                "relation": relation,
                "subject": subject_to_cmd_json(subject),
            }
        }
    }))
    .unwrap_or_default()
}

pub(crate) fn resolve_registered_or_passthrough_bearer_token(
    did: &str,
) -> Result<Option<String>, crate::provider::ProviderError> {
    use k256::ecdsa::SigningKey;

    if let Some(signing_config) = defra_core::signing::get_identity(did) {
        if signing_config.has_local_private_key() && signing_config.key_type == "secp256k1" {
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
