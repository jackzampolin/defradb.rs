use std::sync::Arc;

use clap::Args;

use crate::config::{Config, DatastoreType};
use crate::error::{Error, Result};

/// Dump server-side data directly from the database (no running server required)
#[derive(Args, Debug)]
pub struct ServerDumpArgs {}

impl ServerDumpArgs {
    pub async fn execute(&self, config: &Config) -> Result<()> {
        let lines = match config.datastore.store {
            DatastoreType::Memory => {
                return Err(Error::InvalidDatastore(
                    "server-dump is not supported for in-memory datastore".into(),
                ));
            }
            DatastoreType::Redb => {
                let opts = storage::backends::RedbStoreOptions::new()
                    .with_durability(config.datastore.durability);
                let store = Arc::new(storage::RedbStore::open_with_options(
                    config.data_path(),
                    opts,
                )?);
                let database = db::DB::open_from_arc(store)
                    .await
                    .map_err(|e| Error::Server(e.to_string()))?;
                database.print_dump().await.map_err(Error::Server)?
            }
            #[cfg(feature = "fjall")]
            DatastoreType::Fjall => {
                let opts = storage::backends::FjallStoreOptions::new()
                    .with_durability(config.datastore.durability);
                let store = Arc::new(storage::FjallStore::open_with_options(
                    config.data_path(),
                    opts,
                )?);
                let database = db::DB::open_from_arc(store)
                    .await
                    .map_err(|e| Error::Server(e.to_string()))?;
                database.print_dump().await.map_err(Error::Server)?
            }
            #[cfg(not(feature = "fjall"))]
            DatastoreType::Fjall => {
                return Err(Error::InvalidDatastore(
                    "fjall backend not enabled. Rebuild with --features fjall".into(),
                ));
            }
            #[cfg(feature = "rocksdb")]
            DatastoreType::RocksDb => {
                let opts = storage::backends::RocksDbStoreOptions::new()
                    .with_durability(config.datastore.durability);
                let store = Arc::new(storage::RocksDbStore::open_with_options(
                    config.data_path(),
                    opts,
                )?);
                let database = db::DB::open_from_arc(store)
                    .await
                    .map_err(|e| Error::Server(e.to_string()))?;
                database.print_dump().await.map_err(Error::Server)?
            }
            #[cfg(not(feature = "rocksdb"))]
            DatastoreType::RocksDb => {
                return Err(Error::InvalidDatastore(
                    "rocksdb backend not enabled. Rebuild with --features rocksdb".into(),
                ));
            }
        };

        for line in &lines {
            println!("{}", line);
        }
        Ok(())
    }
}
