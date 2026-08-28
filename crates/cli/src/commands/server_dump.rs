use std::sync::Arc;

use clap::Args;

use crate::commands::start::Node;
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
                    "server-dump needs a datastore on disk; an in-memory one does not \
                     outlive the process that wrote it"
                        .into(),
                ));
            }
            DatastoreType::Regolith => {
                let opts = storage::RegolithStoreOptions::default()
                    .with_durability(config.datastore.durability);
                let backend = storage::RegolithStore::open_with_options(config.data_path(), opts)?;
                let store = Arc::new(Node::wrap_store(config, backend)?);
                let database = db::DB::open_from_arc(store)
                    .await
                    .map_err(|e| Error::Server(e.to_string()))?;
                database.print_dump().await.map_err(Error::Server)?
            }
        };

        for line in &lines {
            println!("{}", line);
        }
        Ok(())
    }
}
