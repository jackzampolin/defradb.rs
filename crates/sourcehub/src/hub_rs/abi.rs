use alloy_primitives::Address;
use alloy_sol_types::sol;

/// EVM precompile address for IAcp on hub.rs: 0x0810
pub const ACP_ADDRESS: Address = Address::new([
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x08, 0x10,
]);

sol! {
    #[allow(missing_docs)]
    interface IAcp {
        function createPolicy(bytes calldata policy, uint8 marshalType) external returns (bytes);
        function registerObject(bytes32 policyId, string objectId, string resource) external returns (bytes record);
        function archiveObject(bytes32 policyId, string objectId, string resource) external returns (bool found, uint64 relationshipsRemoved);
        function bearerPolicyCmd(string bearerToken, bytes32 policyId, bytes cmd) external returns (bytes);

        function setRelationshipSubject(
            bytes32 policyId, string resource, string objectId, string relation,
            uint8  subjectKind,
            string subjectResource,
            string subjectObjectId,
            string subjectRelation
        ) external returns (bool recordExisted, bytes record);
        function deleteRelationshipSubject(
            bytes32 policyId, string resource, string objectId, string relation,
            uint8  subjectKind,
            string subjectResource,
            string subjectObjectId,
            string subjectRelation
        ) external returns (bool recordFound);

        function checkAccess(bytes32 policyId, string[] resources, string[] objectIds, string[] permissions, string actor) external returns (bytes);
        function verifyAccessRequest(bytes32 policyId, string[] resources, string[] objectIds, string[] permissions, string actor) external view returns (bool);
        function getObjectOwner(bytes32 policyId, string resource, string objectId) external view returns (bool registered, bytes record);
        function getPolicy(bytes32 policyId) external view returns (bytes);
        function getPolicyIds() external view returns (string[]);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::FixedBytes;
    use alloy_sol_types::SolCall;

    fn pid() -> FixedBytes<32> {
        FixedBytes::from([7u8; 32])
    }

    #[test]
    fn set_relationship_subject_object_edge_round_trips() {
        let call = IAcp::setRelationshipSubjectCall {
            policyId: pid(),
            resource: "users".to_string(),
            objectId: "doc-1".to_string(),
            relation: "reader".to_string(),
            subjectKind: 2,
            subjectResource: "directory".to_string(),
            subjectObjectId: "d1".to_string(),
            subjectRelation: String::new(),
        };
        let encoded = call.abi_encode();
        assert_eq!(
            &encoded[..4],
            IAcp::setRelationshipSubjectCall::SELECTOR.as_slice()
        );

        let decoded = IAcp::setRelationshipSubjectCall::abi_decode(&encoded)
            .expect("object-edge call should decode");
        assert_eq!(decoded.policyId, pid());
        assert_eq!(decoded.resource, "users");
        assert_eq!(decoded.objectId, "doc-1");
        assert_eq!(decoded.relation, "reader");
        assert_eq!(decoded.subjectKind, 2);
        assert_eq!(decoded.subjectResource, "directory");
        assert_eq!(decoded.subjectObjectId, "d1");
        assert_eq!(decoded.subjectRelation, "");
    }

    #[test]
    fn delete_relationship_subject_userset_round_trips() {
        let call = IAcp::deleteRelationshipSubjectCall {
            policyId: pid(),
            resource: "users".to_string(),
            objectId: "doc-1".to_string(),
            relation: "reader".to_string(),
            subjectKind: 3,
            subjectResource: "directory".to_string(),
            subjectObjectId: "d1".to_string(),
            subjectRelation: "member".to_string(),
        };
        let encoded = call.abi_encode();
        assert_eq!(
            &encoded[..4],
            IAcp::deleteRelationshipSubjectCall::SELECTOR.as_slice()
        );

        let decoded = IAcp::deleteRelationshipSubjectCall::abi_decode(&encoded)
            .expect("userset call should decode");
        assert_eq!(decoded.subjectKind, 3);
        assert_eq!(decoded.subjectResource, "directory");
        assert_eq!(decoded.subjectObjectId, "d1");
        assert_eq!(decoded.subjectRelation, "member");
    }
}
