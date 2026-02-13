use crate::corekv::Error;

impl From<fjall::Error> for Error {
    fn from(err: fjall::Error) -> Self {
        match err {
            fjall::Error::Io(io_err) => Error::Io(io_err.to_string()),
            fjall::Error::Locked => {
                tracing::warn!("Database is locked by another process");
                Error::Backend(
                    "database is locked by another process. \
                     Check for other running processes or stale lock files"
                        .into(),
                )
            }
            fjall::Error::Poisoned => {
                tracing::error!("Previous fsync failed - database is poisoned");
                Error::Backend(
                    "previous fsync failed - database is in an unrecoverable state. \
                     Restart the application and recover from backup"
                        .into(),
                )
            }
            fjall::Error::KeyspaceDeleted => Error::Backend("keyspace has been deleted".into()),
            fjall::Error::Unrecoverable => {
                tracing::error!("Database is unrecoverable");
                Error::Backend("database is unrecoverable - restore from backup".into())
            }
            other => Error::Backend(format!("fjall error: {:?}", other)),
        }
    }
}
