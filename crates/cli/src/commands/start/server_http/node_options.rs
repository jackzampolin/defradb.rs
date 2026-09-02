use serde_json::{json, Map, Value};

use crate::config::{AcpDocumentType, Config, DatastoreType};

fn seconds_as_nanos(seconds: u64) -> u64 {
    seconds.saturating_mul(1_000_000_000)
}

pub(super) fn sanitized_node_options(
    config: &Config,
    user_identity_present: bool,
    signing_enabled: bool,
) -> Map<String, Value> {
    let redacted = |present: bool| present.then_some("<redacted>");
    let redacted_text = |present| if present { "<redacted>" } else { "" };
    let redacted_list = |values: &[String]| vec!["<redacted>"; values.len()];
    let persistent = config.datastore.store == DatastoreType::Regolith;

    #[cfg(feature = "sourcehub")]
    let (sourcehub_chain_id, sourcehub_grpc_address, sourcehub_comet_address) = (
        redacted_text(!config.acp.sourcehub_chain_id.is_empty()),
        redacted_text(!config.acp.sourcehub_address.is_empty()),
        redacted_text(!config.acp.sourcehub_comet_address.is_empty()),
    );
    #[cfg(not(feature = "sourcehub"))]
    let (sourcehub_chain_id, sourcehub_grpc_address, sourcehub_comet_address) = ("", "", "");
    #[cfg(feature = "sourcehub")]
    let document_signer_present = user_identity_present
        && matches!(
            config.acp.document_type,
            AcpDocumentType::SourceHub | AcpDocumentType::HubRs
        );
    #[cfg(not(feature = "sourcehub"))]
    let document_signer_present = false;

    let value = json!({
        "DisableP2P": config.net.p2p_disabled,
        "DisableAPI": false,
        "EnableDevelopment": config.development,
        "KMSType": Value::Null,
        "Store": {
            "Store": config.datastore.store.to_string(),
            "Path": redacted_text(!config.datastore.path.is_empty()),
            "BadgerFileSize": config.datastore.valuelogfilesize,
            "BadgerEncryptionKey": redacted(config.datastore.at_rest_encryption),
            "BadgerInMemory": config.datastore.store == DatastoreType::Memory,
        },
        "DocumentACP": {
            "DocumentACPType": config.acp.document_type.to_string(),
            "Path": redacted_text(config.acp.document_type != AcpDocumentType::None && persistent),
            "Signer": redacted(document_signer_present),
            "SourceHubChainID": sourcehub_chain_id,
            "SourceHubGRPCAddress": sourcehub_grpc_address,
            "SourceHubCometRPCAddress": sourcehub_comet_address,
        },
        "NodeACP": {
            "IsEnabled": config.acp.node_enable,
            "Path": redacted_text(config.acp.node_enable && persistent),
        },
        "DB": {
            "MaxTxnRetries": config.datastore.max_txn_retries,
            "Identity": redacted(user_identity_present),
            "EnableSigning": signing_enabled,
            "SearchableEncryptionKey": redacted(
                !config.datastore.no_searchable_encryption && !config.keyring.disabled
            ),
            "RetryIntervals": config
                .replicator_retry_intervals
                .iter()
                .copied()
                .map(u64::from)
                .map(seconds_as_nanos)
                .collect::<Vec<_>>(),
            "P2PBlockSyncTimeout": seconds_as_nanos(config.net.stream_timeout),
            "LensRuntime": "wasmtime",
            "LensPoolSize": 0,
            "ChunkSize": Value::Null,
        },
        "P2P": {
            "ListenAddresses": redacted_list(&config.net.p2p_addresses),
            "BootstrapPeers": redacted_list(&config.net.peers),
            "EnablePubSub": config.net.pubsub_enabled,
            "EnableRelay": config.net.relay_enabled,
            "EnableClearBackoffOnRetry": false,
            "PrivateKey": redacted(!config.net.p2p_disabled),
        },
        "HTTP": {
            "Address": redacted_text(!config.api.address.is_empty()),
            "AllowedOrigins": redacted_list(&config.api.allowed_origins),
            "TLSCertPath": redacted_text(!config.api.pubkey_path.is_empty()),
            "TLSKeyPath": redacted_text(!config.api.privkey_path.is_empty()),
            "ReadTimeout": seconds_as_nanos(config.api.request_timeout),
            "WriteTimeout": seconds_as_nanos(config.api.request_timeout),
            "IdleTimeout": 0,
            "TxnTTL": seconds_as_nanos(config.api.transaction_idle_timeout),
            "TxnTTLTick": seconds_as_nanos(config.api.transaction_cleanup_interval),
            "TxnTTLBuckets": config
                .api
                .transaction_idle_timeout
                .checked_div(config.api.transaction_cleanup_interval)
                .unwrap_or_default(),
        },
    });

    value
        .as_object()
        .expect("node options must serialize as an object")
        .clone()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_go_shape_without_disclosing_configuration() {
        let mut config = Config::default();
        config.datastore.store = DatastoreType::Memory;
        config.datastore.path = "/private/data".into();
        config.datastore.valuelogfilesize = Some(1_048_576);
        config.net.p2p_addresses = vec!["/ip4/127.0.0.1/tcp/9171".into()];
        config.net.peers = vec!["/ip4/10.0.0.2/tcp/9171/p2p/example".into()];
        config.api.address = "127.0.0.1:9181".into();
        config.api.allowed_origins = vec!["https://private.example".into()];
        config.api.privkey_path = "/private/tls/key.pem".into();

        let options = sanitized_node_options(&config, true, true);

        assert_eq!(options["DisableP2P"], false);
        assert_eq!(options["Store"]["Store"], "memory");
        assert_eq!(options["Store"]["Path"], "<redacted>");
        assert_eq!(options["Store"]["BadgerFileSize"], 1_048_576);
        assert_eq!(options["DB"]["Identity"], "<redacted>");
        assert_eq!(options["DB"]["EnableSigning"], true);
        assert_eq!(options["P2P"]["ListenAddresses"][0], "<redacted>");
        assert_eq!(options["HTTP"]["Address"], "<redacted>");
        assert_eq!(options["HTTP"]["AllowedOrigins"][0], "<redacted>");
        assert_eq!(options["HTTP"]["TLSKeyPath"], "<redacted>");

        let encoded = serde_json::to_string(&options).unwrap();
        for sensitive in [
            "/private/data",
            "/ip4/127.0.0.1/tcp/9171",
            "/ip4/10.0.0.2/tcp/9171/p2p/example",
            "127.0.0.1:9181",
            "https://private.example",
            "/private/tls/key.pem",
        ] {
            assert!(!encoded.contains(sensitive));
        }
    }

    #[test]
    fn seconds_to_nanoseconds_saturates() {
        assert_eq!(seconds_as_nanos(5), 5_000_000_000);
        assert_eq!(seconds_as_nanos(u64::MAX), u64::MAX);
    }
}
