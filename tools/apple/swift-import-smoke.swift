import DefraFFI

let defraSwiftSmokeCallback: DefraRemoteSignCallback = { _, _, _, _, _, _, _, _ in
    return 0
}

func defraSwiftSmoke(
    did: UnsafePointer<CChar>,
    publicKeyHex: UnsafePointer<CChar>,
    publicKeyBytes: UnsafePointer<UInt8>,
    keyType: UnsafePointer<CChar>
) {
    let _ = register_remote_identity(did, publicKeyHex, keyType, 1, defraSwiftSmokeCallback)
    let _ = register_remote_identity_bytes(
        did,
        publicKeyBytes,
        65,
        keyType,
        1,
        defraSwiftSmokeCallback
    )
    let _ = bind_identity_bearer_token(did, publicKeyHex)
    let _ = node_set_default_identity(0, did)
}
