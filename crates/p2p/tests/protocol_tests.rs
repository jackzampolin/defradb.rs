use p2p::{BASE_PROTOCOL_ID, CODE, MESSAGE_VERSION, NAME, VERSION};

#[test]
fn base_protocol_id_uses_name_and_version() {
    assert_eq!(BASE_PROTOCOL_ID, format!("/{NAME}/{VERSION}"));
}

#[test]
fn multicodec_code_matches_go() {
    assert_eq!(CODE, 961);
}

#[cfg(feature = "libp2p-transport")]
#[test]
fn replication_protocols_match_go() {
    assert_eq!(
        p2p::protocol::rep_request_protocol().as_ref(),
        "/defradb/rep_req/0.0.1"
    );
    assert_eq!(
        p2p::protocol::rep_response_protocol().as_ref(),
        "/defradb/rep_resp/0.0.1"
    );
}

#[cfg(feature = "libp2p-transport")]
#[test]
fn searchable_encryption_query_protocols_match_go() {
    assert_eq!(
        p2p::protocol::se_query_request_protocol().as_ref(),
        "/defradb/se_query_req/0.0.1"
    );
    assert_eq!(
        p2p::protocol::se_query_response_protocol().as_ref(),
        "/defradb/se_query_resp/0.0.1"
    );
}

#[test]
fn message_version_matches_go() {
    assert_eq!(MESSAGE_VERSION, "/defradb/0.0.1");
}
