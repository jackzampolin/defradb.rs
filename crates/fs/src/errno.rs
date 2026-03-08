/// Map a db::Error to a POSIX errno value.
///
/// Ensures FUSE never returns generic EIO when a more specific errno applies:
/// - Validation/format errors → EINVAL
/// - Not found → ENOENT
/// - ACP/permission denial → EACCES
/// - Already exists → EEXIST
/// - Storage/internal → EIO
pub fn db_err_to_errno(err: &db::Error) -> i32 {
    match err {
        db::Error::DocumentNotFound(_) | db::Error::CollectionNotFound(_) => libc::ENOENT,

        db::Error::InvalidDocument(_)
        | db::Error::InvalidCollectionName(_)
        | db::Error::InvalidPatch(_)
        | db::Error::Serialization(_)
        | db::Error::CollectionVersionIDEmpty
        | db::Error::CollectionVersionNotFound(_) => libc::EINVAL,

        db::Error::Acp(_) | db::Error::UnsafePolicyTransition(_) => libc::EACCES,

        db::Error::CollectionAlreadyExists(_) => libc::EEXIST,

        db::Error::Document(doc_err) => doc_err_to_errno(doc_err),

        db::Error::Schema(_)
        | db::Error::Storage(_)
        | db::Error::Datastore(_)
        | db::Error::Query(_)
        | db::Error::DatabaseClosed
        | db::Error::TxnNotActive
        | db::Error::ExplicitTxnMustUseForce
        | db::Error::UnsupportedTxnType
        | db::Error::TransactionNotFound(_)
        | db::Error::LockPoisoned(_)
        | db::Error::CacheUpdateFailedAfterCommit(_)
        | db::Error::Lens(_)
        | db::Error::JsonPatch(_)
        | db::Error::Other(_) => libc::EIO,
    }
}

/// Map a document::Error to a POSIX errno value.
fn doc_err_to_errno(err: &document::Error) -> i32 {
    match err {
        document::Error::MalformedDocID
        | document::Error::InvalidDocIDVersion(_)
        | document::Error::EmptyFieldName
        | document::Error::TypeMismatch { .. }
        | document::Error::InvalidFieldValue { .. }
        | document::Error::MissingRequiredField(_)
        | document::Error::JsonParse(_)
        | document::Error::JsonNumberOutOfRange(_)
        | document::Error::NonFiniteFloat(_)
        | document::Error::IncompatibleCrdtType { .. }
        | document::Error::UuidParse(_)
        | document::Error::MultibaseDecode(_) => libc::EINVAL,

        document::Error::FieldNotFound(_) => libc::ENOENT,

        document::Error::CborEncode(_)
        | document::Error::CborDecode(_)
        | document::Error::Cid(_)
        | document::Error::Schema(_) => libc::EIO,
    }
}
