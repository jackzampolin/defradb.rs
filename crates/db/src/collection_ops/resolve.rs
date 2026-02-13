use super::*;

impl<S: Store> crate::database::DB<S> {
    /// Get a collection by name, returning an error if not found.
    pub fn require_collection(&self, name: &str) -> Result<Collection> {
        self.get_collection(name)?
            .ok_or_else(|| Error::CollectionNotFound(name.to_string()))
    }

    /// Resolve collection names to `(name, collection_id)` pairs.
    pub fn resolve_collection_ids(&self, names: &[String]) -> Result<Vec<(String, String)>> {
        names
            .iter()
            .map(|name| {
                let col = self.require_collection(name)?;
                Ok((name.clone(), col.collection_id().to_string()))
            })
            .collect()
    }
}
