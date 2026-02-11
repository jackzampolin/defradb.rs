mod create;
mod manage;

pub use create::create_index;
pub use manage::{drop_index, get_all_indexes, get_indexes};

/// Input structure for creating an index via FFI.
#[derive(serde::Deserialize)]
pub(crate) struct IndexCreateInput {
    #[serde(rename = "Name")]
    pub name: String,
    #[serde(rename = "Fields", default)]
    pub fields: Vec<IndexFieldInput>,
    #[serde(rename = "Unique", default)]
    pub unique: bool,
}

/// Input structure for an indexed field.
#[derive(serde::Deserialize)]
pub(crate) struct IndexFieldInput {
    #[serde(rename = "Name")]
    pub name: String,
    #[serde(rename = "Descending", default)]
    pub descending: bool,
}
