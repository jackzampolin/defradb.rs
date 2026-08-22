//! SE Coordinator for managing searchable encryption artifacts.
//!
//! The coordinator handles:
//! - Generating artifacts when documents are created/updated
//! - Pushing artifacts to replicator nodes
//! - Querying replicators for matching documents
//!
//! Matches Go's internal/se/coordinator.go

use crypto::se::Artifact;
use document::NormalValue;
use schema::EncryptedIndexDescription;
use storage::corekv::Result;
use zeroize::Zeroizing;

use super::artifact_gen::generate_field_artifact;
use super::storage::FieldQuery;

/// Query for a field value in SE searches.
#[derive(Debug, Clone)]
pub struct FieldValueQuery {
    /// Name of the field being queried.
    pub field_name: String,
    /// Encrypted index description.
    pub index_desc: EncryptedIndexDescription,
    /// The value to search for.
    pub value: NormalValue,
}

impl FieldValueQuery {
    /// Create a new field value query.
    pub fn new(
        field_name: impl Into<String>,
        index_desc: EncryptedIndexDescription,
        value: NormalValue,
    ) -> Self {
        Self {
            field_name: field_name.into(),
            index_desc,
            value,
        }
    }

    /// Create a simple equality query for a field.
    pub fn equality(field_name: impl Into<String>, value: NormalValue) -> Self {
        let field_name = field_name.into();
        Self {
            index_desc: EncryptedIndexDescription::new(&field_name),
            field_name,
            value,
        }
    }
}

/// Configuration for the SE coordinator.
#[derive(Debug, Clone)]
pub struct SECoordinatorConfig {
    /// SE encryption key (32 bytes). Zeroized on drop.
    pub enc_key: Zeroizing<Vec<u8>>,
    /// Identity's public key for tag isolation.
    pub identity_pubkey: Option<Vec<u8>>,
}

impl Default for SECoordinatorConfig {
    fn default() -> Self {
        Self {
            enc_key: Zeroizing::new(vec![0u8; 32]),
            identity_pubkey: None,
        }
    }
}

/// SE Coordinator for managing searchable encryption operations.
///
/// The coordinator is responsible for:
/// 1. Generating search artifacts from document field values
/// 2. Converting field value queries to search queries (with tags)
/// 3. Coordinating with P2P layer for artifact replication
pub struct SECoordinator {
    config: SECoordinatorConfig,
}

impl SECoordinator {
    /// Create a new SE coordinator with the given configuration.
    pub fn new(config: SECoordinatorConfig) -> Self {
        Self { config }
    }

    /// Create a coordinator with just an encryption key.
    pub fn with_key(enc_key: Vec<u8>) -> Self {
        Self::new(SECoordinatorConfig {
            enc_key: Zeroizing::new(enc_key),
            ..Default::default()
        })
    }

    /// Create a coordinator with an encryption key and identity pubkey.
    pub fn with_key_and_identity(enc_key: Vec<u8>, identity_pubkey: Vec<u8>) -> Self {
        Self::new(SECoordinatorConfig {
            enc_key: Zeroizing::new(enc_key),
            identity_pubkey: Some(identity_pubkey),
        })
    }

    /// Get the encryption key.
    pub fn enc_key(&self) -> &[u8] {
        &self.config.enc_key
    }

    /// Get the identity public key.
    pub fn identity_pubkey(&self) -> Option<&[u8]> {
        self.config.identity_pubkey.as_deref()
    }

    /// Generate artifacts for a document's encrypted fields.
    ///
    /// # Arguments
    ///
    /// * `collection_id` - Collection version ID
    /// * `doc_id` - Document ID
    /// * `encrypted_indexes` - Encrypted indexes for the collection
    /// * `field_names` - Fields to generate artifacts for (empty = all)
    /// * `field_values` - Map of field name to value
    pub fn generate_artifacts(
        &self,
        collection_id: &str,
        doc_id: &str,
        encrypted_indexes: &[EncryptedIndexDescription],
        field_names: &[String],
        field_values: &std::collections::HashMap<String, NormalValue>,
    ) -> Result<Vec<Artifact>> {
        super::artifact_gen::generate_doc_artifacts(
            collection_id,
            doc_id,
            encrypted_indexes,
            field_names,
            field_values,
            self.config.identity_pubkey.as_deref(),
            &self.config.enc_key,
        )
    }

    /// Convert field value queries to field queries with search tags.
    ///
    /// This is used at query time to generate the search tags that will
    /// be sent to replicators.
    ///
    /// # Arguments
    ///
    /// * `collection_id` - Collection version ID
    /// * `queries` - Field value queries from the query planner
    pub fn to_field_queries(
        &self,
        collection_id: &str,
        queries: &[FieldValueQuery],
    ) -> Result<Vec<FieldQuery>> {
        let mut field_queries = Vec::with_capacity(queries.len());

        for q in queries {
            let artifact = generate_field_artifact(
                collection_id,
                "", // doc_id not needed for tag generation
                &q.index_desc,
                &q.value,
                self.config.identity_pubkey.as_deref(),
                &self.config.enc_key,
            )?;

            field_queries.push(FieldQuery::new(
                &q.field_name,
                &q.field_name, // IndexID is field name
                artifact.search_tag,
            ));
        }

        Ok(field_queries)
    }
}
