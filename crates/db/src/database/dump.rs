use storage::corekv::{IterOptions, Reader, Store};
use storage::namespace::Namespace;

use crate::database::DB;

impl<S: Store + 'static> DB<S> {
    /// Dump all database key/value pairs as human-readable strings.
    ///
    /// Iterates over all namespaced keys in the underlying store and returns
    /// each as a formatted string showing the namespace, key, and value size.
    pub async fn print_dump(&self) -> Result<Vec<String>, String> {
        let store = self.store();
        let txn = store
            .new_txn(true)
            .await
            .map_err(|e| format!("failed to create read txn: {}", e))?;

        let mut lines = Vec::new();

        let namespaces = [
            Namespace::Datastore,
            Namespace::Blockstore,
            Namespace::Headstore,
            Namespace::Systemstore,
            Namespace::Peerstore,
            Namespace::Encstore,
            Namespace::Acpstore,
        ];

        for ns in &namespaces {
            let prefix = vec![ns.prefix()];
            let opts = IterOptions::new().with_prefix(prefix);

            let mut iter = txn
                .iterator(opts)
                .await
                .map_err(|e| format!("iterator error: {}", e))?;

            while let Some(kv) = iter
                .next()
                .await
                .map_err(|e| format!("iteration error: {}", e))?
            {
                let key_display = String::from_utf8_lossy(&kv.key);
                lines.push(format!(
                    "[{:?}] {} ({} bytes)",
                    ns,
                    key_display,
                    kv.value.len()
                ));
            }

            iter.close()
                .await
                .map_err(|e| format!("close error: {}", e))?;
        }

        Ok(lines)
    }
}
