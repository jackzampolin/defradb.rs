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

        function verifyAccessRequest(bytes32 policyId, string[] resources, string[] objectIds, string[] permissions, string actor) external view returns (bool);
        function getObjectOwner(bytes32 policyId, string resource, string objectId) external view returns (bool registered, bytes record);
        function getPolicy(bytes32 policyId) external view returns (bytes);
    }
}
