mod export;
mod import;

pub use export::basic_export;
pub use import::basic_import;

use serde::{Deserialize, Deserializer};

/// Deserialize null or missing JSON values as an empty Vec.
/// Go sends `"collections": null` when the slice is nil.
fn null_to_empty_vec<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: Deserializer<'de>,
{
    Option::<Vec<String>>::deserialize(deserializer).map(|opt| opt.unwrap_or_default())
}

/// BackupConfig matches Go's client.BackupConfig.
#[derive(Deserialize)]
pub(crate) struct BackupConfig {
    pub filepath: String,
    #[serde(default)]
    pub pretty: bool,
    #[serde(default, deserialize_with = "null_to_empty_vec")]
    pub collections: Vec<String>,
}
