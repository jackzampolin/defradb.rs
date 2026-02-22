//! Lens configuration types.
//!
//! Matches Go's client/lens.go types.

use serde::{Deserialize, Serialize};

/// Configuration for a Lens migration.
///
/// Matches Go's client.LensConfig, which embeds model.Lens (flattened).
/// JSON format: {"SourceCollectionVersionID":"...","DestinationCollectionVersionID":"...","Lenses":[...]}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LensConfig {
    /// ID of the collection version to migrate from.
    ///
    /// The source and destination versions must be adjacent in the version history.
    #[serde(rename = "SourceCollectionVersionID", alias = "SourceSchemaVersionID")]
    pub source_schema_version_id: String,

    /// ID of the collection version to migrate to.
    ///
    /// The source and destination versions must be adjacent in the version history.
    #[serde(
        rename = "DestinationCollectionVersionID",
        alias = "DestinationSchemaVersionID"
    )]
    pub destination_schema_version_id: String,

    /// The Lens modules to apply, in execution order.
    ///
    /// Go's model.Lens embeds this as a flat `Lenses` array.
    #[serde(rename = "Lenses", alias = "Lens", default)]
    pub lenses: Vec<LensModule>,
}

impl LensConfig {
    /// Create a new lens configuration with a single module.
    pub fn new(
        source_schema_version_id: impl Into<String>,
        destination_schema_version_id: impl Into<String>,
        lens: LensModule,
    ) -> Self {
        Self {
            source_schema_version_id: source_schema_version_id.into(),
            destination_schema_version_id: destination_schema_version_id.into(),
            lenses: vec![lens],
        }
    }

    /// Get the first lens module (convenience accessor).
    pub fn lens(&self) -> Option<&LensModule> {
        self.lenses.first()
    }

    /// Validate that this config is safe to use from an HTTP request.
    ///
    /// Rejects any lens module that uses file paths (prevents path traversal).
    pub fn validate_for_http(&self) -> Result<(), crate::Error> {
        for lens in &self.lenses {
            lens.validate_for_http()?;
        }
        Ok(())
    }
}

/// Configuration for a Lens WASM module.
///
/// Matches Go's model.LensModule from lens/host-go/config/model.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct LensModule {
    /// Path to the WASM module file.
    ///
    /// The WASM module must remain at this location as long as the migration is active.
    #[serde(rename = "Path", default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,

    /// Whether to inverse the module transform.
    #[serde(rename = "Inverse", default)]
    pub inverse: bool,

    /// Raw WASM module bytes (alternative to path).
    #[serde(
        rename = "Module",
        default,
        skip_serializing_if = "Option::is_none",
        with = "serde_bytes_opt"
    )]
    pub module: Option<Vec<u8>>,

    /// Arguments passed to the WASM module.
    #[serde(rename = "Arguments", default, skip_serializing_if = "Option::is_none")]
    pub arguments: Option<serde_json::Value>,
}

impl LensModule {
    /// Create a lens module from a file path.
    pub fn from_path(path: impl Into<String>) -> Self {
        Self {
            path: Some(path.into()),
            inverse: false,
            module: None,
            arguments: None,
        }
    }

    /// Create a lens module from raw WASM bytes.
    pub fn from_bytes(module: Vec<u8>) -> Self {
        Self {
            path: None,
            inverse: false,
            module: Some(module),
            arguments: None,
        }
    }

    /// Set arguments for the module.
    pub fn with_arguments(mut self, arguments: serde_json::Value) -> Self {
        self.arguments = Some(arguments);
        self
    }

    /// Validate that this module is safe to load from an HTTP request.
    ///
    /// HTTP requests must not use file paths to load WASM modules (prevents
    /// path traversal attacks). Only inline module bytes are allowed via HTTP.
    /// File path loading is permitted only via CLI or when dev_mode is enabled.
    pub fn validate_for_http(&self) -> Result<(), crate::Error> {
        if self.path.is_some() {
            return Err(crate::Error::PathNotAllowed(
                "file path WASM loading is not allowed via HTTP API; \
                 use inline module bytes instead"
                    .to_string(),
            ));
        }
        Ok(())
    }
}

mod serde_bytes_opt {
    use base64::{engine::general_purpose::STANDARD, Engine};
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S>(data: &Option<Vec<u8>>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match data {
            Some(bytes) => {
                let encoded = STANDARD.encode(bytes);
                encoded.serialize(serializer)
            }
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<Vec<u8>>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let opt: Option<String> = Option::deserialize(deserializer)?;
        match opt {
            Some(s) => STANDARD
                .decode(&s)
                .map(Some)
                .map_err(serde::de::Error::custom),
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lens_config_serialization() {
        let config = LensConfig::new(
            "bafkrei_v1",
            "bafkrei_v2",
            LensModule::from_path("/path/to/transform.wasm"),
        );

        let json = serde_json::to_string(&config).unwrap();
        assert!(json.contains("\"SourceCollectionVersionID\""));
        assert!(json.contains("\"DestinationCollectionVersionID\""));
        assert!(json.contains("\"Lenses\""));
        assert!(json.contains("\"Path\""));

        let parsed: LensConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(config, parsed);
    }

    #[test]
    fn test_lens_config_go_format() {
        // Go sends this format (embedded model.Lens with Lenses array)
        let json = r#"{
            "SourceCollectionVersionID": "v1",
            "DestinationCollectionVersionID": "v2",
            "Lenses": [{"Path": "/path/to/transform.wasm", "Inverse": false}]
        }"#;
        let parsed: LensConfig = serde_json::from_str(json).unwrap();
        assert_eq!(parsed.source_schema_version_id, "v1");
        assert_eq!(parsed.destination_schema_version_id, "v2");
        assert_eq!(parsed.lenses.len(), 1);
        assert_eq!(
            parsed.lenses[0].path,
            Some("/path/to/transform.wasm".to_string())
        );
    }

    #[test]
    fn test_lens_module_from_path() {
        let module = LensModule::from_path("/path/to/transform.wasm");
        assert_eq!(module.path, Some("/path/to/transform.wasm".to_string()));
        assert!(module.module.is_none());
    }

    #[test]
    fn test_lens_module_from_bytes() {
        let wasm_bytes = vec![0x00, 0x61, 0x73, 0x6d]; // WASM magic header
        let module = LensModule::from_bytes(wasm_bytes.clone());
        assert!(module.path.is_none());
        assert_eq!(module.module, Some(wasm_bytes));
    }

    #[test]
    fn test_lens_module_with_arguments() {
        let args = serde_json::json!({
            "mapping": {"old_field": "new_field"}
        });
        let module = LensModule::from_path("/path/to/transform.wasm").with_arguments(args.clone());
        assert_eq!(module.arguments, Some(args));
    }

    #[test]
    fn test_validate_for_http_rejects_file_path() {
        let module = LensModule::from_path("/path/to/transform.wasm");
        assert!(module.validate_for_http().is_err());
    }

    #[test]
    fn test_validate_for_http_accepts_bytes() {
        let module = LensModule::from_bytes(vec![0x00, 0x61, 0x73, 0x6d]);
        assert!(module.validate_for_http().is_ok());
    }

    #[test]
    fn test_config_validate_for_http_rejects_file_path() {
        let config = LensConfig::new("v1", "v2", LensModule::from_path("/path/to/transform.wasm"));
        assert!(config.validate_for_http().is_err());
    }

    #[test]
    fn test_config_validate_for_http_accepts_bytes() {
        let config = LensConfig::new(
            "v1",
            "v2",
            LensModule::from_bytes(vec![0x00, 0x61, 0x73, 0x6d]),
        );
        assert!(config.validate_for_http().is_ok());
    }
}
