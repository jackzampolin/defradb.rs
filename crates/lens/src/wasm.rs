//! WASM transform execution using wasmtime.
//!
//! Provides the runtime for executing Lens WASM modules.
//! Resource limits (memory, CPU fuel, epoch interruption) are opt-in via
//! `WasmSandboxConfig` — by default modules run without restrictions.

use async_trait::async_trait;
use futures::StreamExt;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::path::Path;
use wasmtime::{
    AsContext, AsContextMut, Config, Engine, Linker, Module, Store as WasmStore, StoreLimits,
    StoreLimitsBuilder, TypedFunc,
};

use tracing::{info, warn};

use crate::store::{LensDocResultStream, LensDocStream, TransformId, TransformStore};
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
    modules: RwLock<HashMap<TransformId, CompiledModule>>,
    configs: RwLock<HashMap<TransformId, LensConfig>>,
    sandbox: Option<WasmSandboxConfig>,
}

struct CompiledModule {
    module: Module,
    arguments: Option<serde_json::Value>,
}

/// Host state for batch transforms that feed multiple docs via lens::next.
struct BatchHostState {
    input_docs: Vec<LensDoc>,
    current_index: usize,
    limits: Option<StoreLimits>,
}

impl BatchHostState {
    fn new(docs: Vec<LensDoc>, limits: Option<StoreLimits>) -> Self {
        Self {
            input_docs: docs,
            current_index: 0,
            limits,
        }
    }

    fn next_input(&mut self) -> Option<LensDoc> {
        if self.current_index < self.input_docs.len() {
            let doc = self.input_docs[self.current_index].clone();
            self.current_index += 1;
            Some(doc)
        } else {
            None
        }
    }
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

    /// Build store limits when memory sandboxing is configured.
    fn store_limits(&self) -> Option<StoreLimits> {
        self.sandbox.as_ref().and_then(|sb| {
            sb.max_memory_bytes
                .map(|max| StoreLimitsBuilder::new().memory_size(max).build())
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

impl Default for WasmTransformStore {
    fn default() -> Self {
        Self::new().expect("failed to create WASM engine")
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

        // Compute content-based ID for deduplication (matches Go's IPLD CID approach)
        // Hash only the lens modules, not the version IDs, so identical lens content
        // produces the same ID regardless of which versions it's associated with.
        let lenses_json = serde_json::to_vec(&config.lenses)
            .map_err(|e| Error::Pipeline(format!("failed to serialize lenses: {}", e)))?;
        let mut hasher = Sha256::new();
        hasher.update(&lenses_json);
        let hash = hasher.finalize();
        // Use "baf" prefix to mimic CID format, then 16 bytes of hash for uniqueness
        let id = TransformId::new(format!("baf{}", hex::encode(&hash[..16])));

        info!(
            transform_id = %id,
            "Computed transform ID"
        );

        // Check if this transform already exists (deduplication)
        {
            let modules = self.modules.read();
            if modules.contains_key(&id) {
                info!(
                    transform_id = %id,
                    "Transform already exists, returning existing ID"
                );
                return Ok(id);
            }
        }

        // Load and compile the WASM module
        let first_lens = config.lens().cloned().unwrap_or_default();
        info!(
            path = ?first_lens.path,
            has_module_bytes = first_lens.module.is_some(),
            has_arguments = first_lens.arguments.is_some(),
            "Loading WASM module"
        );

        let module = self.load_module(&first_lens)?;
        info!(
            transform_id = %id,
            "WASM module compiled successfully"
        );

        let compiled = CompiledModule {
            module,
            arguments: first_lens.arguments.clone(),
        };

        self.modules.write().insert(id.clone(), compiled);
        self.configs.write().insert(id.clone(), config);

        info!(
            transform_id = %id,
            "Transform added to store"
        );

        Ok(id)
    }

    async fn add_with_id(&self, id: TransformId, config: LensConfig) -> Result<()> {
        info!(
            transform_id = %id,
            "Adding transform to WASM store with explicit ID"
        );

        // Check if this transform already exists
        {
            let modules = self.modules.read();
            if modules.contains_key(&id) {
                info!(transform_id = %id, "Transform already exists");
                return Ok(());
            }
        }

        let first_lens = config.lens().cloned().unwrap_or_default();
        let module = self.load_module(&first_lens)?;

        let compiled = CompiledModule {
            module,
            arguments: first_lens.arguments.clone(),
        };

        self.modules.write().insert(id.clone(), compiled);
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
        info!(
            transform_id = %id,
            "WasmTransformStore::transform called"
        );

        let modules = self.modules.read();
        let compiled = modules.get(id).ok_or_else(|| {
            warn!(
                transform_id = %id,
                stored_ids = ?modules.keys().map(|k| k.to_string()).collect::<Vec<_>>(),
                "Transform not found in WASM store"
            );
            Error::TransformNotFound(id.to_string())
        })?;
        let module = compiled.module.clone();
        let arguments = compiled.arguments.clone();
        drop(modules);

        info!(
            transform_id = %id,
            has_arguments = arguments.is_some(),
            "Transform found, creating execution stream"
        );

        let engine = self.engine.clone();
        let limits = self.store_limits();
        let sandbox = self.sandbox.clone();
        let transform_id_str = id.to_string();

        info!(
            transform_id = %transform_id_str,
            "Executing forward WASM transform (batch mode)"
        );

        // Batch mode: collect all inputs, run in a single WASM instance, yield all outputs.
        // This supports transforms that aggregate (N→1) or multiply (1→N) documents.
        enum Phase {
            Collecting {
                engine: Engine,
                module: Module,
                arguments: Option<serde_json::Value>,
                docs: LensDocStream,
                limits: Option<StoreLimits>,
                sandbox: Option<WasmSandboxConfig>,
            },
            Yielding {
                results: Vec<LensDoc>,
                index: usize,
            },
        }

        let initial = Phase::Collecting {
            engine,
            module,
            arguments,
            docs,
            limits,
            sandbox,
        };

        Ok(Box::pin(futures::stream::unfold(initial, |phase| async {
            match phase {
                Phase::Collecting {
                    engine,
                    module,
                    arguments,
                    docs,
                    limits,
                    sandbox,
                } => {
                    let input_docs: Vec<LensDoc> = docs.collect().await;
                    match execute_batch_transform(
                        &engine, &module, input_docs, arguments, false, limits, &sandbox,
                    ) {
                        Ok(outputs) => {
                            if outputs.is_empty() {
                                None
                            } else {
                                let doc = outputs[0].clone();
                                Some((
                                    Ok(doc),
                                    Phase::Yielding {
                                        results: outputs,
                                        index: 1,
                                    },
                                ))
                            }
                        }
                        Err(e) => Some((
                            Err(e),
                            Phase::Yielding {
                                results: Vec::new(),
                                index: 0,
                            },
                        )),
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
        })))
    }

    fn inverse(&self, id: &TransformId, docs: LensDocStream) -> Result<LensDocResultStream> {
        let modules = self.modules.read();
        let compiled = modules
            .get(id)
            .ok_or_else(|| Error::TransformNotFound(id.to_string()))?;
        let module = compiled.module.clone();
        let arguments = compiled.arguments.clone();
        drop(modules);

        let engine = self.engine.clone();
        let limits = self.store_limits();
        let sandbox = self.sandbox.clone();

        // Batch mode for inverse transforms (same as forward)
        enum Phase {
            Collecting {
                engine: Engine,
                module: Module,
                arguments: Option<serde_json::Value>,
                docs: LensDocStream,
                limits: Option<StoreLimits>,
                sandbox: Option<WasmSandboxConfig>,
            },
            Yielding {
                results: Vec<LensDoc>,
                index: usize,
            },
        }

        let initial = Phase::Collecting {
            engine,
            module,
            arguments,
            docs,
            limits,
            sandbox,
        };

        Ok(Box::pin(futures::stream::unfold(initial, |phase| async {
            match phase {
                Phase::Collecting {
                    engine,
                    module,
                    arguments,
                    docs,
                    limits,
                    sandbox,
                } => {
                    let input_docs: Vec<LensDoc> = docs.collect().await;
                    match execute_batch_transform(
                        &engine, &module, input_docs, arguments, true, limits, &sandbox,
                    ) {
                        Ok(outputs) => {
                            if outputs.is_empty() {
                                None
                            } else {
                                let doc = outputs[0].clone();
                                Some((
                                    Ok(doc),
                                    Phase::Yielding {
                                        results: outputs,
                                        index: 1,
                                    },
                                ))
                            }
                        }
                        Err(e) => Some((
                            Err(e),
                            Phase::Yielding {
                                results: Vec::new(),
                                index: 0,
                            },
                        )),
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
        })))
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

/// Execute a batch transform: all input docs are fed to a single WASM instance,
/// and all outputs are collected by calling transform/inverse repeatedly until EOS.
///
/// This supports transforms that change document counts (aggregate N→1, multiply 1→N).
///
/// When sandbox config is provided, resource limits (memory, fuel, epoch) are applied.
fn execute_batch_transform(
    engine: &Engine,
    module: &Module,
    input_docs: Vec<LensDoc>,
    arguments: Option<serde_json::Value>,
    inverse: bool,
    limits: Option<StoreLimits>,
    sandbox: &Option<WasmSandboxConfig>,
) -> Result<Vec<LensDoc>> {
    let mut store = WasmStore::new(engine, BatchHostState::new(input_docs, limits));

    // Apply opt-in resource limits
    if store.data().limits.is_some() {
        store.limiter(|state| state.limits.as_mut().unwrap());
    }
    if let Some(ref sb) = sandbox {
        if let Some(fuel) = sb.fuel_budget {
            store
                .set_fuel(fuel)
                .map_err(|e| Error::WasmExecution(format!("failed to set fuel: {}", e)))?;
        }
        if let Some(ticks) = sb.epoch_deadline_ticks {
            store.set_epoch_deadline(ticks);
        }
    }

    let mut linker: Linker<BatchHostState> = Linker::new(engine);

    // Add lens::next import that returns docs from the queue one at a time
    linker
        .func_wrap(
            "lens",
            "next",
            |mut caller: wasmtime::Caller<'_, BatchHostState>| -> i32 {
                let memory = match caller.get_export("memory") {
                    Some(wasmtime::Extern::Memory(mem)) => mem,
                    _ => return 0,
                };

                let alloc = match caller.get_export("alloc") {
                    Some(wasmtime::Extern::Func(f)) => f,
                    _ => return 0,
                };

                let doc = match caller.data_mut().next_input() {
                    Some(d) => d,
                    None => {
                        // No more input docs — write an EOS marker (type_id=127)
                        // to WASM memory so the module knows the stream ended.
                        // Go's writeEOS does exactly this.
                        let offset = match alloc.typed::<i32, i32>(&caller) {
                            Ok(typed_alloc) => match typed_alloc.call(&mut caller, 1) {
                                Ok(o) => o,
                                Err(_) => return 0,
                            },
                            Err(_) => return 0,
                        };
                        if memory
                            .write(&mut caller, offset as usize, &[127u8])
                            .is_err()
                        {
                            return 0;
                        }
                        return offset;
                    }
                };

                let json = match serde_json::to_vec(&doc) {
                    Ok(j) => j,
                    Err(_) => return 0,
                };

                // Format: [type_id: i8][len: u32 LE][data: bytes]
                let header_size = 5i32;
                let total_size = header_size + json.len() as i32;

                let offset = match alloc.typed::<i32, i32>(&caller) {
                    Ok(typed_alloc) => match typed_alloc.call(&mut caller, total_size) {
                        Ok(o) => o,
                        Err(_) => return 0,
                    },
                    Err(_) => return 0,
                };

                // Write type_id (1 = JSON)
                if memory.write(&mut caller, offset as usize, &[1u8]).is_err() {
                    return 0;
                }

                // Write length as u32 LE
                let len_bytes = (json.len() as u32).to_le_bytes();
                if memory
                    .write(&mut caller, (offset + 1) as usize, &len_bytes)
                    .is_err()
                {
                    return 0;
                }

                // Write JSON data
                if memory
                    .write(&mut caller, (offset + header_size) as usize, &json)
                    .is_err()
                {
                    return 0;
                }

                offset
            },
        )
        .map_err(|e| Error::WasmExecution(format!("failed to define lens::next: {}", e)))?;

    let instance = linker
        .instantiate(&mut store, module)
        .map_err(|e| Error::WasmExecution(format!("failed to instantiate: {}", e)))?;

    let memory = instance
        .get_memory(store.as_context_mut(), "memory")
        .ok_or_else(|| Error::WasmExecution("no memory export".to_string()))?;

    // Set parameters if provided
    if let Some(params) = arguments {
        let alloc_fn: Option<TypedFunc<i32, i32>> = instance
            .get_typed_func(store.as_context_mut(), "alloc")
            .ok();
        let set_param_fn: Option<TypedFunc<i32, i32>> = instance
            .get_typed_func(store.as_context_mut(), "set_param")
            .ok();

        if let (Some(alloc), Some(set_param)) = (alloc_fn, set_param_fn) {
            let param_json = serde_json::to_vec(&params)
                .map_err(|e| Error::WasmExecution(format!("failed to serialize params: {}", e)))?;

            let total_size = 5 + param_json.len() as i32;
            let offset = alloc
                .call(store.as_context_mut(), total_size)
                .map_err(|e| Error::WasmExecution(format!("alloc for params failed: {}", e)))?;

            memory
                .write(store.as_context_mut(), offset as usize, &[1u8])
                .map_err(|e| Error::WasmExecution(format!("write type_id failed: {}", e)))?;

            let len_bytes = (param_json.len() as u32).to_le_bytes();
            memory
                .write(store.as_context_mut(), (offset + 1) as usize, &len_bytes)
                .map_err(|e| Error::WasmExecution(format!("write len failed: {}", e)))?;

            memory
                .write(store.as_context_mut(), (offset + 5) as usize, &param_json)
                .map_err(|e| Error::WasmExecution(format!("write data failed: {}", e)))?;

            let _ = set_param
                .call(store.as_context_mut(), offset)
                .map_err(|e| Error::WasmExecution(format!("set_param failed: {}", e)))?;
        }
    }

    // Get the transform/inverse function
    let func_name = if inverse { "inverse" } else { "transform" };
    let transform_fn: TypedFunc<(), i32> = instance
        .get_typed_func(store.as_context_mut(), func_name)
        .map_err(|e| Error::WasmExecution(format!("{} func not found: {}", func_name, e)))?;

    // Call transform repeatedly until EOS to collect all output docs
    let mut output_docs = Vec::new();
    loop {
        let result_offset = transform_fn
            .call(store.as_context_mut(), ())
            .map_err(|e| Error::WasmExecution(format!("{} call failed: {}", func_name, e)))?;

        if result_offset == 0 {
            break;
        }

        let mut type_id_buf = [0u8; 1];
        memory
            .read(store.as_context(), result_offset as usize, &mut type_id_buf)
            .map_err(|e| Error::WasmExecution(format!("read type_id failed: {}", e)))?;

        let type_id = type_id_buf[0] as i8;

        if type_id == 127 {
            // EOS - no more output documents
            break;
        }

        if type_id < 0 {
            // Error from WASM
            let mut len_buf = [0u8; 4];
            memory
                .read(
                    store.as_context(),
                    (result_offset + 1) as usize,
                    &mut len_buf,
                )
                .map_err(|e| Error::WasmExecution(format!("read error len failed: {}", e)))?;
            let len = u32::from_le_bytes(len_buf) as usize;

            let mut error_bytes = vec![0u8; len];
            memory
                .read(
                    store.as_context(),
                    (result_offset + 5) as usize,
                    &mut error_bytes,
                )
                .map_err(|e| Error::WasmExecution(format!("read error failed: {}", e)))?;

            let error_str = String::from_utf8_lossy(&error_bytes);
            return Err(Error::WasmExecution(format!("WASM error: {}", error_str)));
        }

        if type_id != 1 {
            return Err(Error::WasmExecution(format!(
                "unexpected type_id: {}",
                type_id
            )));
        }

        // Read JSON document
        let mut len_buf = [0u8; 4];
        memory
            .read(
                store.as_context(),
                (result_offset + 1) as usize,
                &mut len_buf,
            )
            .map_err(|e| Error::WasmExecution(format!("read len failed: {}", e)))?;
        let len = u32::from_le_bytes(len_buf) as usize;

        let mut result_bytes = vec![0u8; len];
        memory
            .read(
                store.as_context(),
                (result_offset + 5) as usize,
                &mut result_bytes,
            )
            .map_err(|e| Error::WasmExecution(format!("read data failed: {}", e)))?;

        let doc: LensDoc = serde_json::from_slice(&result_bytes)
            .map_err(|e| Error::WasmExecution(e.to_string()))?;
        output_docs.push(doc);
    }

    Ok(output_docs)
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
