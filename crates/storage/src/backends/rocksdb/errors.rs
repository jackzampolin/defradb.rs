use crate::corekv::Error;

impl From<rocksdb::Error> for Error {
    fn from(err: rocksdb::Error) -> Self {
        let msg = err.into_string();
        if msg.contains("lock") {
            tracing::warn!("Database is locked by another process");
            Error::Backend(
                "database is locked by another process. \
                 Check for other running processes or stale lock files"
                    .into(),
            )
        } else if msg.contains("Corruption") || msg.contains("corruption") {
            tracing::error!("Database corruption detected");
            Error::Backend(format!("database corruption: {}", msg))
        } else if msg.contains("IO error") {
            Error::Io(msg)
        } else {
            Error::Backend(format!("rocksdb error: {}", msg))
        }
    }
}
