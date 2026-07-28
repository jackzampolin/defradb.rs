//! WASM transform execution using wasmtime.
//!
//! Provides the runtime for executing Lens WASM modules.
//! Resource limits (memory, CPU fuel, epoch interruption) are opt-in via
//! `WasmSandboxConfig` -- by default modules run without restrictions.

use async_trait::async_trait;
use futures::StreamExt;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::path::Path;
use wasmtime::{Config, Engine, Module, StoreLimits, StoreLimitsBuilder};

use tracing::{info, warn};

use crate::store::{LensDocResultStream, LensDocStream, TransformId, TransformStore};
use crate::wasm_runtime::execute_batch_transform;
use crate::{Error, LensConfig, LensDoc, LensModule, Result};

/// Opt-in resource limits for WASM execution.
///
/// All fields are optional. When `None`, the corresponding limit is not applied,
/// giving the WASM module full access to that resource.
#[derive(Debug, Clone)]
pub struct WasmSandboxConfig {
    /// Maximum memory a single WASM instance may allocate (bytes).
    pub max_memory_bytes: Option<usize>,

    /// Fuel budget per transform execution (roughly maps to instruction count).
    pub fuel_budget: Option<u64>,

    /// Epoch deadline ticks before interruption.
    pub epoch_deadline_ticks: Option<u64>,
}

impl WasmSandboxConfig {
    /// Recommended defaults for high-security environments.
    pub fn restrictive() -> Self {
        Self {
            max_memory_bytes: Some(64 * 1024 * 1024), // 64 MiB
            fuel_budget: Some(1_000_000),
            epoch_deadline_ticks: Some(2),
        }
    }
}

/// WASM-based transform store.
///
/// Manages WASM module instances and executes transforms.
pub struct WasmTransformStore {
    engine: Engine,
    modules: RwLock<HashMap<TransformId, Vec<CompiledModule>>>,
    configs: RwLock<HashMap<TransformId, LensConfig>>,
    sandbox: Option<WasmSandboxConfig>,
}

#[derive(Clone)]
struct CompiledModule {
    module: Module,
    arguments: Option<serde_json::Value>,
    inverse: bool,
}

impl WasmTransformStore {
    /// Create a new WASM transform store.
    ///
    /// By default, no resource limits are applied. Pass a `WasmSandboxConfig`
    /// via `with_sandbox` to enable opt-in resource restrictions.
    pub fn new() -> Result<Self> {
        Self::with_sandbox(None)
    }

    /// Create a WASM transform store with optional sandboxing.
    pub fn with_sandbox(sandbox: Option<WasmSandboxConfig>) -> Result<Self> {
        let mut config = Config::new();
        #[cfg(target_os = "macos")]
        {
            // Fork-capable embedders can crash when Wasmtime's Mach-port
            // exception handler is initialized before spawning child processes.
            config.macos_use_mach_ports(false);
        }
        if let Some(ref sb) = sandbox {
            if sb.fuel_budget.is_some() {
                config.consume_fuel(true);
            }
            if sb.epoch_deadline_ticks.is_some() {
                config.epoch_interruption(true);
            }
        }
        let engine = Engine::new(&config)
            .map_err(|e| Error::WasmLoad(format!("failed to create WASM engine: {}", e)))?;

        Ok(Self {
            engine,
            modules: RwLock::new(HashMap::new()),
            configs: RwLock::new(HashMap::new()),
            sandbox,
        })
    }

    /// Load a WASM module from the given lens configuration.
    ///
    /// When loading from a file path, validates the path to prevent traversal
    /// attacks. The path must be absolute, must not contain `..` segments,
    /// and must have a `.wasm` extension.
    fn load_module(&self, lens: &LensModule) -> Result<Module> {
        if let Some(ref path_str) = lens.path {
            let clean_path = path_str.strip_prefix("file://").unwrap_or(path_str);
            Self::validate_wasm_path(clean_path)?;
            let path = Path::new(clean_path);
            Module::from_file(&self.engine, path).map_err(|e| {
                Error::WasmLoad(format!(
                    "failed to load WASM from {}: {}",
                    path.display(),
                    e
                ))
            })
        } else if let Some(ref bytes) = lens.module {
            Module::new(&self.engine, bytes)
                .map_err(|e| Error::WasmLoad(format!("failed to load WASM from bytes: {}", e)))
        } else {
            Err(Error::InvalidConfig(
                "lens module must have either path or module bytes".to_string(),
            ))
        }
    }

    fn compile_modules(&self, lenses: &[LensModule]) -> Result<Vec<CompiledModule>> {
        lenses
            .iter()
            .map(|lens| {
                Ok(CompiledModule {
                    module: self.load_module(lens)?,
                    arguments: lens.arguments.clone(),
                    inverse: lens.inverse,
                })
            })
            .collect()
    }

    /// Transform JSON values using a registered lens pipeline.
    ///
    /// This mirrors the standalone host contract where top-level inputs and
    /// outputs are JSON arrays, including `null` items.
    pub fn transform_json(
        &self,
        id: &TransformId,
        values: Vec<serde_json::Value>,
    ) -> Result<Vec<serde_json::Value>> {
        let modules = self.modules.read();
        let compiled_modules = modules
            .get(id)
            .cloned()
            .ok_or_else(|| Error::TransformNotFound(id.to_string()))?;
        drop(modules);

        execute_pipeline_values(
            &self.engine,
            compiled_modules,
            values,
            self.sandbox.clone(),
            false,
        )
    }

    /// Inverse transform JSON values using a registered lens pipeline.
    pub fn inverse_json(
        &self,
        id: &TransformId,
        values: Vec<serde_json::Value>,
    ) -> Result<Vec<serde_json::Value>> {
        let modules = self.modules.read();
        let compiled_modules = modules
            .get(id)
            .cloned()
            .ok_or_else(|| Error::TransformNotFound(id.to_string()))?;
        drop(modules);

        execute_pipeline_values(
            &self.engine,
            compiled_modules,
            values,
            self.sandbox.clone(),
            true,
        )
    }

    /// Validate a WASM module file path to prevent path traversal.
    fn validate_wasm_path(path_str: &str) -> Result<()> {
        let path = Path::new(path_str);

        if !path.is_absolute() {
            return Err(Error::PathNotAllowed(
                "WASM module path must be absolute".to_string(),
            ));
        }

        for component in path.components() {
            if let std::path::Component::ParentDir = component {
                return Err(Error::PathNotAllowed(
                    "WASM module path must not contain '..' segments".to_string(),
                ));
            }
        }

        match path.extension().and_then(|e| e.to_str()) {
            Some("wasm") => {}
            _ => {
                return Err(Error::PathNotAllowed(
                    "WASM module path must have .wasm extension".to_string(),
                ));
            }
        }

        Ok(())
    }
}

#[async_trait]
impl TransformStore for WasmTransformStore {
    async fn add(&self, config: LensConfig) -> Result<TransformId> {
        use sha2::{Digest, Sha256};

        info!(
            source_version = %config.source_schema_version_id,
            dest_version = %config.destination_schema_version_id,
            lenses_count = config.lenses.len(),
            "Adding transform to WASM store"
        );

        let lenses_json = serde_json::to_vec(&config.lenses)
            .map_err(|e| Error::Pipeline(format!("failed to serialize lenses: {}", e)))?;
        let mut hasher = Sha256::new();
        hasher.update(&lenses_json);
        let hash = hasher.finalize();
        let id = TransformId::new(format!("baf{}", hex::encode(&hash[..16])));

        info!(transform_id = %id, "Computed transform ID");

        {
            let modules = self.modules.read();
            if modules.contains_key(&id) {
                info!(transform_id = %id, "Transform already exists, returning existing ID");
                return Ok(id);
            }
        }

        info!("Loading WASM modules");
        let compiled_modules = self.compile_modules(&config.lenses)?;
        info!(transform_id = %id, "WASM module compiled successfully");

        self.modules.write().insert(id.clone(), compiled_modules);
        self.configs.write().insert(id.clone(), config);

        info!(transform_id = %id, "Transform added to store");

        Ok(id)
    }

    async fn add_with_id(&self, id: TransformId, config: LensConfig) -> Result<()> {
        info!(transform_id = %id, "Adding transform to WASM store with explicit ID");

        {
            let modules = self.modules.read();
            if modules.contains_key(&id) {
                info!(transform_id = %id, "Transform already exists");
                return Ok(());
            }
        }

        let compiled_modules = self.compile_modules(&config.lenses)?;

        self.modules.write().insert(id.clone(), compiled_modules);
        self.configs.write().insert(id.clone(), config);

        info!(transform_id = %id, "Transform added with explicit ID");
        Ok(())
    }

    async fn list(&self) -> Result<std::collections::HashMap<String, crate::LensModule>> {
        let configs = self.configs.read();
        let result = configs
            .iter()
            .filter_map(|(id, config)| config.lens().cloned().map(|l| (id.to_string(), l)))
            .collect();
        Ok(result)
    }

    fn transform(&self, id: &TransformId, docs: LensDocStream) -> Result<LensDocResultStream> {
        info!(transform_id = %id, "WasmTransformStore::transform called");

        let modules = self.modules.read();
        let compiled_modules = modules.get(id).cloned().ok_or_else(|| {
            warn!(
                transform_id = %id,
                stored_ids = ?modules.keys().map(|k| k.to_string()).collect::<Vec<_>>(),
                "Transform not found in WASM store"
            );
            Error::TransformNotFound(id.to_string())
        })?;
        drop(modules);

        info!(
            transform_id = %id,
            modules_count = compiled_modules.len(),
            "Transform found, creating execution stream"
        );

        info!(transform_id = %id, "Executing forward WASM transform (batch mode)");
        Ok(execute_pipeline_stream(
            self.engine.clone(),
            compiled_modules,
            docs,
            self.sandbox.clone(),
            false,
        ))
    }

    fn inverse(&self, id: &TransformId, docs: LensDocStream) -> Result<LensDocResultStream> {
        let modules = self.modules.read();
        let compiled_modules = modules
            .get(id)
            .cloned()
            .ok_or_else(|| Error::TransformNotFound(id.to_string()))?;
        drop(modules);

        Ok(execute_pipeline_stream(
            self.engine.clone(),
            compiled_modules,
            docs,
            self.sandbox.clone(),
            true,
        ))
    }

    fn has_transform(&self, id: &TransformId) -> bool {
        self.modules.read().contains_key(id)
    }

    async fn remove(&self, id: &TransformId) -> Result<()> {
        if self.modules.write().remove(id).is_none() {
            return Err(Error::TransformNotFound(id.to_string()));
        }
        self.configs.write().remove(id);
        Ok(())
    }
}

fn execute_pipeline_stream(
    engine: Engine,
    mut modules: Vec<CompiledModule>,
    docs: LensDocStream,
    sandbox: Option<WasmSandboxConfig>,
    inverse: bool,
) -> LensDocResultStream {
    enum Phase {
        Collecting {
            engine: Engine,
            modules: Vec<CompiledModule>,
            docs: LensDocStream,
            sandbox: Option<WasmSandboxConfig>,
        },
        Yielding {
            results: Vec<LensDoc>,
            index: usize,
        },
    }

    if inverse {
        modules.reverse();
        for module in &mut modules {
            module.inverse = !module.inverse;
        }
    }

    let initial = Phase::Collecting {
        engine,
        modules,
        docs,
        sandbox,
    };

    Box::pin(futures::stream::unfold(initial, |phase| async {
        match phase {
            Phase::Collecting {
                engine,
                modules,
                docs,
                sandbox,
            } => {
                let current_docs: Vec<serde_json::Value> = docs
                    .collect::<Vec<_>>()
                    .await
                    .into_iter()
                    .map(serde_json::Value::Object)
                    .collect();

                let current_docs =
                    match execute_pipeline_values(&engine, modules, current_docs, sandbox, false) {
                        Ok(outputs) => outputs,
                        Err(e) => {
                            return Some((
                                Err(e),
                                Phase::Yielding {
                                    results: Vec::new(),
                                    index: 0,
                                },
                            ));
                        }
                    };

                let mut results = Vec::with_capacity(current_docs.len());
                for value in current_docs {
                    match value {
                        serde_json::Value::Object(doc) => results.push(doc),
                        other => {
                            return Some((
                                Err(Error::WasmExecution(format!(
                                    "expected JSON object output, got {}",
                                    json_value_type(&other)
                                ))),
                                Phase::Yielding {
                                    results: Vec::new(),
                                    index: 0,
                                },
                            ));
                        }
                    }
                }

                if results.is_empty() {
                    None
                } else {
                    let doc = results[0].clone();
                    Some((Ok(doc), Phase::Yielding { results, index: 1 }))
                }
            }
            Phase::Yielding { results, index } => {
                if index < results.len() {
                    let doc = results[index].clone();
                    Some((
                        Ok(doc),
                        Phase::Yielding {
                            results,
                            index: index + 1,
                        },
                    ))
                } else {
                    None
                }
            }
        }
    }))
}

fn execute_pipeline_values(
    engine: &Engine,
    mut modules: Vec<CompiledModule>,
    mut current_docs: Vec<serde_json::Value>,
    sandbox: Option<WasmSandboxConfig>,
    inverse: bool,
) -> Result<Vec<serde_json::Value>> {
    if inverse {
        modules.reverse();
        for module in &mut modules {
            module.inverse = !module.inverse;
        }
    }

    for module in modules {
        current_docs = execute_batch_transform(
            engine,
            &module.module,
            current_docs,
            module.arguments,
            module.inverse,
            store_limits(&sandbox),
            &sandbox,
        )?;
    }

    Ok(current_docs)
}

fn store_limits(sandbox: &Option<WasmSandboxConfig>) -> Option<StoreLimits> {
    sandbox.as_ref().and_then(|sb| {
        sb.max_memory_bytes
            .map(|max| StoreLimitsBuilder::new().memory_size(max).build())
    })
}

fn json_value_type(value: &serde_json::Value) -> &'static str {
    match value {
        serde_json::Value::Null => "null",
        serde_json::Value::Bool(_) => "boolean",
        serde_json::Value::Number(_) => "number",
        serde_json::Value::String(_) => "string",
        serde_json::Value::Array(_) => "array",
        serde_json::Value::Object(_) => "object",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wasm_store_creation() {
        let store = WasmTransformStore::new();
        assert!(store.is_ok());
    }

    #[test]
    fn test_wasm_store_with_sandbox() {
        let store = WasmTransformStore::with_sandbox(Some(WasmSandboxConfig::restrictive()));
        assert!(store.is_ok());
    }

    #[test]
    fn test_invalid_lens_config() {
        let store = WasmTransformStore::new().unwrap();
        let config = LensConfig::new("v1", "v2", LensModule::default());

        let first_lens = config.lens().cloned().unwrap_or_default();
        let result = store.load_module(&first_lens);
        assert!(matches!(result, Err(Error::InvalidConfig(_))));
    }

    #[test]
    fn test_validate_wasm_path_rejects_relative() {
        let result = WasmTransformStore::validate_wasm_path("relative/path.wasm");
        assert!(matches!(result, Err(Error::PathNotAllowed(_))));
    }

    #[test]
    fn test_validate_wasm_path_rejects_traversal() {
        let result = WasmTransformStore::validate_wasm_path("/safe/../../etc/passwd.wasm");
        assert!(matches!(result, Err(Error::PathNotAllowed(_))));
    }

    #[test]
    fn test_validate_wasm_path_rejects_non_wasm_extension() {
        let result = WasmTransformStore::validate_wasm_path("/path/to/module.so");
        assert!(matches!(result, Err(Error::PathNotAllowed(_))));
    }

    #[test]
    fn test_validate_wasm_path_accepts_valid() {
        let result = WasmTransformStore::validate_wasm_path("/path/to/transform.wasm");
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_wasm_path_rejects_no_extension() {
        let result = WasmTransformStore::validate_wasm_path("/path/to/module");
        assert!(matches!(result, Err(Error::PathNotAllowed(_))));
    }
}
