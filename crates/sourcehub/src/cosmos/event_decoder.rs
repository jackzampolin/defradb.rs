use base64::Engine as _;
use prost::Message;

const BEARER_POLICY_COMMAND: &str = "vera.acp.MsgBearerPolicyCmd";
const CHECK_ACCESS: &str = "vera.acp.MsgCheckAccess";
const CREATE_POLICY: &str = "vera.acp.MsgCreatePolicy";
const DIRECT_POLICY_COMMAND: &str = "vera.acp.MsgDirectPolicyCmd";
const EDIT_POLICY: &str = "vera.acp.MsgEditPolicy";
const SIGNED_POLICY_COMMAND: &str = "vera.acp.MsgSignedPolicyCmd";

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum CacheInvalidation {
    Object {
        policy_id: String,
        resource: String,
        object_id: String,
    },
    Policy(String),
    All,
}

#[derive(Debug, thiserror::Error)]
pub(super) enum EventDecodeError {
    #[error("invalid CometBFT event JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("CometBFT subscription failed: {0}")]
    Subscription(String),
    #[error("transaction event is missing its transaction bytes")]
    MissingTransaction,
    #[error("invalid transaction base64: {0}")]
    Base64(#[from] base64::DecodeError),
    #[error("invalid Cosmos transaction: {0}")]
    Transaction(#[source] prost::DecodeError),
    #[error("invalid {message_type}: {source}")]
    AcpMessage {
        message_type: String,
        #[source]
        source: prost::DecodeError,
    },
}

pub(super) fn decode_event(message: &str) -> Result<Vec<CacheInvalidation>, EventDecodeError> {
    let event: serde_json::Value = serde_json::from_str(message)?;
    if let Some(error) = event.get("error") {
        return Err(EventDecodeError::Subscription(error.to_string()));
    }

    let Some(data) = event.pointer("/result/data") else {
        return Ok(Vec::new());
    };
    if data.get("type").and_then(|value| value.as_str()) != Some("tendermint/event/Tx") {
        return Ok(Vec::new());
    }

    let tx_result = data
        .pointer("/value/TxResult")
        .or_else(|| data.pointer("/value/tx_result"))
        .ok_or(EventDecodeError::MissingTransaction)?;
    if !transaction_succeeded(tx_result.pointer("/result/code")) {
        return Ok(Vec::new());
    }

    let encoded_tx = tx_result
        .get("tx")
        .and_then(|value| value.as_str())
        .ok_or(EventDecodeError::MissingTransaction)?;
    let tx_bytes = base64::engine::general_purpose::STANDARD.decode(encoded_tx)?;
    decode_transaction(&tx_bytes)
}

fn transaction_succeeded(code: Option<&serde_json::Value>) -> bool {
    match code {
        None => true,
        Some(serde_json::Value::Number(value)) => value.as_u64() == Some(0),
        Some(serde_json::Value::String(value)) => value == "0",
        Some(_) => false,
    }
}

fn decode_transaction(tx_bytes: &[u8]) -> Result<Vec<CacheInvalidation>, EventDecodeError> {
    let raw = RawTransaction::decode(tx_bytes).map_err(EventDecodeError::Transaction)?;
    let body = TransactionBody::decode(raw.body_bytes.as_slice())
        .map_err(EventDecodeError::Transaction)?;
    let mut invalidations = Vec::new();

    for message in body.messages {
        let message_type = message.type_url.trim_start_matches('/');
        let invalidation =
            match message_type {
                BEARER_POLICY_COMMAND => {
                    let command = BearerPolicyCommand::decode(message.value.as_slice()).map_err(
                        |source| EventDecodeError::AcpMessage {
                            message_type: message.type_url.clone(),
                            source,
                        },
                    )?;
                    command_invalidation(command.policy_id, command.command)
                }
                DIRECT_POLICY_COMMAND => {
                    let command = DirectPolicyCommand::decode(message.value.as_slice()).map_err(
                        |source| EventDecodeError::AcpMessage {
                            message_type: message.type_url.clone(),
                            source,
                        },
                    )?;
                    command_invalidation(command.policy_id, command.command)
                }
                EDIT_POLICY => {
                    let edit =
                        EditPolicyCommand::decode(message.value.as_slice()).map_err(|source| {
                            EventDecodeError::AcpMessage {
                                message_type: message.type_url.clone(),
                                source,
                            }
                        })?;
                    if edit.policy_id.is_empty() {
                        CacheInvalidation::All
                    } else {
                        CacheInvalidation::Policy(edit.policy_id)
                    }
                }
                SIGNED_POLICY_COMMAND => CacheInvalidation::All,
                CREATE_POLICY | CHECK_ACCESS => continue,
                message_type if message_type.starts_with("vera.acp.") => CacheInvalidation::All,
                _ => continue,
            };

        if invalidation == CacheInvalidation::All {
            return Ok(vec![CacheInvalidation::All]);
        }
        if !invalidations.contains(&invalidation) {
            invalidations.push(invalidation);
        }
    }

    Ok(invalidations)
}

fn command_invalidation(policy_id: String, command: Option<PolicyCommand>) -> CacheInvalidation {
    if policy_id.is_empty() {
        return CacheInvalidation::All;
    }

    let Some(command) = command else {
        return CacheInvalidation::Policy(policy_id);
    };
    if command.set_relationship.is_some() || command.delete_relationship.is_some() {
        return CacheInvalidation::Policy(policy_id);
    }
    let Some(object) = command.object() else {
        return CacheInvalidation::Policy(policy_id);
    };
    if object.resource.is_empty() || object.id.is_empty() {
        CacheInvalidation::Policy(policy_id)
    } else {
        CacheInvalidation::Object {
            policy_id,
            resource: object.resource,
            object_id: object.id,
        }
    }
}

#[derive(Clone, PartialEq, Message)]
struct RawTransaction {
    #[prost(bytes = "vec", tag = "1")]
    body_bytes: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
struct TransactionBody {
    #[prost(message, repeated, tag = "1")]
    messages: Vec<ProtoAny>,
}

#[derive(Clone, PartialEq, Message)]
struct ProtoAny {
    #[prost(string, tag = "1")]
    type_url: String,
    #[prost(bytes = "vec", tag = "2")]
    value: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
struct BearerPolicyCommand {
    #[prost(string, tag = "3")]
    policy_id: String,
    #[prost(message, optional, tag = "4")]
    command: Option<PolicyCommand>,
}

#[derive(Clone, PartialEq, Message)]
struct DirectPolicyCommand {
    #[prost(string, tag = "2")]
    policy_id: String,
    #[prost(message, optional, tag = "3")]
    command: Option<PolicyCommand>,
}

#[derive(Clone, PartialEq, Message)]
struct EditPolicyCommand {
    #[prost(string, tag = "2")]
    policy_id: String,
}

#[derive(Clone, PartialEq, Message)]
struct PolicyCommand {
    #[prost(message, optional, tag = "1")]
    set_relationship: Option<RelationshipCommand>,
    #[prost(message, optional, tag = "2")]
    delete_relationship: Option<RelationshipCommand>,
    #[prost(message, optional, tag = "3")]
    register_object: Option<ObjectCommand>,
    #[prost(message, optional, tag = "4")]
    archive_object: Option<ObjectCommand>,
    #[prost(message, optional, tag = "8")]
    unarchive_object: Option<ObjectCommand>,
}

impl PolicyCommand {
    fn object(self) -> Option<ObjectRef> {
        self.register_object
            .and_then(|command| command.object)
            .or_else(|| self.archive_object.and_then(|command| command.object))
            .or_else(|| self.unarchive_object.and_then(|command| command.object))
    }
}

#[derive(Clone, PartialEq, Message)]
struct RelationshipCommand {
    #[prost(message, optional, tag = "1")]
    relationship: Option<Relationship>,
}

#[derive(Clone, PartialEq, Message)]
struct Relationship {
    #[prost(message, optional, tag = "1")]
    object: Option<ObjectRef>,
}

#[derive(Clone, PartialEq, Message)]
struct ObjectCommand {
    #[prost(message, optional, tag = "1")]
    object: Option<ObjectRef>,
}

#[derive(Clone, PartialEq, Message)]
struct ObjectRef {
    #[prost(string, tag = "1")]
    resource: String,
    #[prost(string, tag = "2")]
    id: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn transaction_event(messages: Vec<ProtoAny>, code: u64) -> String {
        let body = TransactionBody { messages }.encode_to_vec();
        let tx = RawTransaction { body_bytes: body }.encode_to_vec();
        serde_json::json!({
            "result": {
                "data": {
                    "type": "tendermint/event/Tx",
                    "value": {
                        "TxResult": {
                            "tx": base64::engine::general_purpose::STANDARD.encode(tx),
                            "result": { "code": code }
                        }
                    }
                }
            }
        })
        .to_string()
    }

    fn object(resource: &str, id: &str) -> ObjectRef {
        ObjectRef {
            resource: resource.into(),
            id: id.into(),
        }
    }

    #[test]
    fn relationship_mutation_invalidates_the_whole_policy() {
        let command = BearerPolicyCommand {
            policy_id: "policy-1".into(),
            command: Some(PolicyCommand {
                set_relationship: Some(RelationshipCommand {
                    relationship: Some(Relationship {
                        object: Some(object("users", "doc-1")),
                    }),
                }),
                ..Default::default()
            }),
        };
        let event = transaction_event(
            vec![ProtoAny {
                type_url: format!("/{BEARER_POLICY_COMMAND}"),
                value: command.encode_to_vec(),
            }],
            0,
        );

        assert_eq!(
            decode_event(&event).unwrap(),
            vec![CacheInvalidation::Policy("policy-1".into())]
        );
    }

    #[test]
    fn decodes_direct_archive_object() {
        let command = DirectPolicyCommand {
            policy_id: "policy-2".into(),
            command: Some(PolicyCommand {
                archive_object: Some(ObjectCommand {
                    object: Some(object("books", "doc-2")),
                }),
                ..Default::default()
            }),
        };
        let event = transaction_event(
            vec![ProtoAny {
                type_url: format!("/{DIRECT_POLICY_COMMAND}"),
                value: command.encode_to_vec(),
            }],
            0,
        );

        assert_eq!(
            decode_event(&event).unwrap(),
            vec![CacheInvalidation::Object {
                policy_id: "policy-2".into(),
                resource: "books".into(),
                object_id: "doc-2".into(),
            }]
        );
    }

    #[test]
    fn edit_policy_invalidates_the_whole_policy() {
        let edit = EditPolicyCommand {
            policy_id: "policy-1".into(),
        };
        let event = transaction_event(
            vec![ProtoAny {
                type_url: format!("/{EDIT_POLICY}"),
                value: edit.encode_to_vec(),
            }],
            0,
        );

        assert_eq!(
            decode_event(&event).unwrap(),
            vec![CacheInvalidation::Policy("policy-1".into())]
        );
    }

    #[test]
    fn opaque_signed_command_fails_safe() {
        let event = transaction_event(
            vec![ProtoAny {
                type_url: format!("/{SIGNED_POLICY_COMMAND}"),
                value: Vec::new(),
            }],
            0,
        );

        assert_eq!(decode_event(&event).unwrap(), vec![CacheInvalidation::All]);
    }

    #[test]
    fn unknown_acp_message_fails_safe() {
        let event = transaction_event(
            vec![ProtoAny {
                type_url: "/vera.acp.MsgFuturePolicyMutation".into(),
                value: Vec::new(),
            }],
            0,
        );

        assert_eq!(decode_event(&event).unwrap(), vec![CacheInvalidation::All]);
    }

    #[test]
    fn create_policy_does_not_invalidate() {
        let event = transaction_event(
            vec![ProtoAny {
                type_url: format!("/{CREATE_POLICY}"),
                value: Vec::new(),
            }],
            0,
        );

        assert!(decode_event(&event).unwrap().is_empty());
    }

    #[test]
    fn failed_transactions_do_not_invalidate() {
        let event = transaction_event(
            vec![ProtoAny {
                type_url: format!("/{SIGNED_POLICY_COMMAND}"),
                value: Vec::new(),
            }],
            7,
        );

        assert!(decode_event(&event).unwrap().is_empty());
    }

    #[test]
    fn subscription_ack_is_ignored() {
        assert!(decode_event(r#"{"jsonrpc":"2.0","id":1,"result":{}}"#)
            .unwrap()
            .is_empty());
    }
}
