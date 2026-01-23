//! WASM transform execution using wasmtime.
//!
//! Provides the runtime for executing Lens WASM modules.

use async_trait::async_trait;
use futures::StreamExt;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::path::Path;
use wasmtime::{
    AsContext, AsContextMut, Engine, Instance, Linker, Module, Store as WasmStore, TypedFunc,
};

use crate::store::{LensDocResultStream, LensDocStream, TransformId, TransformStore};
use crate::{Error, LensConfig, LensDoc, LensModule, Result};

/// WASM-based transform store.
///
/// Manages WASM module instances and executes transforms.
pub struct WasmTransformStore {
    engine: Engine,
    modules: RwLock<HashMap<TransformId, CompiledModule>>,
    configs: RwLock<HashMap<TransformId, LensConfig>>,
    next_id: std::sync::atomic::AtomicU64,
}

struct CompiledModule {
    module: Module,
    #[allow(dead_code)]
    arguments: Option<serde_json::Value>,
}

impl WasmTransformStore {
    /// Create a new WASM transform store.
    pub fn new() -> Result<Self> {
        let engine = Engine::default();

        Ok(Self {
            engine,
            modules: RwLock::new(HashMap::new()),
            configs: RwLock::new(HashMap::new()),
            next_id: std::sync::atomic::AtomicU64::new(0),
        })
    }

    /// Load a WASM module from the given lens configuration.
    fn load_module(&self, lens: &LensModule) -> Result<Module> {
        if let Some(ref path) = lens.path {
            let path = Path::new(path);
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

    /// Execute a transform on a single document.
    #[allow(dead_code)]
    fn execute_transform(&self, module: &Module, doc: LensDoc, inverse: bool) -> Result<LensDoc> {
        let mut store = WasmStore::new(&self.engine, ());
        let linker = Linker::new(&self.engine);

        let instance = linker.instantiate(&mut store, module).map_err(|e| {
            Error::WasmExecution(format!("failed to instantiate WASM module: {}", e))
        })?;

        // Serialize input document
        let input_json =
            serde_json::to_string(&doc).map_err(|e| Error::WasmExecution(e.to_string()))?;

        // Get the transform function
        let func_name = if inverse { "inverse" } else { "transform" };

        // Try to get the typed function
        let result = self.call_wasm_transform(&instance, &mut store, func_name, &input_json)?;

        // Parse output document
        serde_json::from_str(&result).map_err(|e| Error::WasmExecution(e.to_string()))
    }

    #[allow(dead_code)]
    fn call_wasm_transform(
        &self,
        instance: &Instance,
        store: &mut WasmStore<()>,
        func_name: &str,
        input: &str,
    ) -> Result<String> {
        // Get memory and allocator functions
        let memory = instance
            .get_memory(store.as_context_mut(), "memory")
            .ok_or_else(|| Error::WasmExecution("WASM module has no memory export".to_string()))?;

        // Try to get alloc and dealloc functions
        let alloc: Option<TypedFunc<i32, i32>> = instance
            .get_typed_func(store.as_context_mut(), "alloc")
            .ok();
        let _dealloc: Option<TypedFunc<(i32, i32), ()>> = instance
            .get_typed_func(store.as_context_mut(), "dealloc")
            .ok();

        // Allocate input buffer
        let input_bytes = input.as_bytes();
        let input_len = input_bytes.len() as i32;

        let input_ptr = if let Some(ref alloc_fn) = alloc {
            alloc_fn
                .call(store.as_context_mut(), input_len)
                .map_err(|e| Error::WasmExecution(format!("alloc failed: {}", e)))?
        } else {
            // Simple fallback: use beginning of memory
            0
        };

        // Write input to memory
        memory
            .write(store.as_context_mut(), input_ptr as usize, input_bytes)
            .map_err(|e| Error::WasmExecution(format!("memory write failed: {}", e)))?;

        // Call the transform function
        // Expected signature: (input_ptr: i32, input_len: i32) -> i64 (packed ptr/len)
        let transform_fn: TypedFunc<(i32, i32), i64> = instance
            .get_typed_func(store.as_context_mut(), func_name)
            .map_err(|e| {
                Error::WasmExecution(format!("function '{}' not found: {}", func_name, e))
            })?;

        let result = transform_fn
            .call(store.as_context_mut(), (input_ptr, input_len))
            .map_err(|e| Error::WasmExecution(format!("transform call failed: {}", e)))?;

        // Unpack result (ptr in high 32 bits, len in low 32 bits)
        let result_ptr = (result >> 32) as i32;
        let result_len = (result & 0xFFFFFFFF) as i32;

        // Read result from memory
        let mut result_bytes = vec![0u8; result_len as usize];
        memory
            .read(store.as_context(), result_ptr as usize, &mut result_bytes)
            .map_err(|e| Error::WasmExecution(format!("memory read failed: {}", e)))?;

        String::from_utf8(result_bytes)
            .map_err(|e| Error::WasmExecution(format!("invalid UTF-8 in result: {}", e)))
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
        let module = self.load_module(&config.lens)?;
        let id = TransformId::new(format!(
            "lens_{}",
            self.next_id
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
        ));

        let compiled = CompiledModule {
            module,
            arguments: config.lens.arguments.clone(),
        };

        self.modules.write().insert(id.clone(), compiled);
        self.configs.write().insert(id.clone(), config);

        Ok(id)
    }

    fn transform(&self, id: &TransformId, docs: LensDocStream) -> Result<LensDocResultStream> {
        let modules = self.modules.read();
        let compiled = modules
            .get(id)
            .ok_or_else(|| Error::TransformNotFound(id.to_string()))?;
        let module = compiled.module.clone();
        drop(modules);

        let engine = self.engine.clone();

        Ok(Box::pin(docs.map(move |doc| {
            // Clone module for each document (wasmtime modules are Arc internally)
            let mut store = WasmStore::new(&engine, ());
            let linker = Linker::new(&engine);

            let instance = linker
                .instantiate(&mut store, &module)
                .map_err(|e| Error::WasmExecution(format!("failed to instantiate: {}", e)))?;

            execute_transform_inner(&instance, &mut store, doc, false)
        })))
    }

    fn inverse(&self, id: &TransformId, docs: LensDocStream) -> Result<LensDocResultStream> {
        let modules = self.modules.read();
        let compiled = modules
            .get(id)
            .ok_or_else(|| Error::TransformNotFound(id.to_string()))?;
        let module = compiled.module.clone();
        drop(modules);

        let engine = self.engine.clone();

        Ok(Box::pin(docs.map(move |doc| {
            let mut store = WasmStore::new(&engine, ());
            let linker = Linker::new(&engine);

            let instance = linker
                .instantiate(&mut store, &module)
                .map_err(|e| Error::WasmExecution(format!("failed to instantiate: {}", e)))?;

            execute_transform_inner(&instance, &mut store, doc, true)
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

/// Execute transform using an existing instance.
fn execute_transform_inner(
    instance: &Instance,
    store: &mut WasmStore<()>,
    doc: LensDoc,
    inverse: bool,
) -> Result<LensDoc> {
    let input_json =
        serde_json::to_string(&doc).map_err(|e| Error::WasmExecution(e.to_string()))?;

    let func_name = if inverse { "inverse" } else { "transform" };

    let memory = instance
        .get_memory(store.as_context_mut(), "memory")
        .ok_or_else(|| Error::WasmExecution("no memory export".to_string()))?;

    let alloc: Option<TypedFunc<i32, i32>> = instance
        .get_typed_func(store.as_context_mut(), "alloc")
        .ok();

    let input_bytes = input_json.as_bytes();
    let input_len = input_bytes.len() as i32;

    let input_ptr = if let Some(ref alloc_fn) = alloc {
        alloc_fn
            .call(store.as_context_mut(), input_len)
            .map_err(|e| Error::WasmExecution(format!("alloc failed: {}", e)))?
    } else {
        0
    };

    memory
        .write(store.as_context_mut(), input_ptr as usize, input_bytes)
        .map_err(|e| Error::WasmExecution(format!("write failed: {}", e)))?;

    let transform_fn: TypedFunc<(i32, i32), i64> = instance
        .get_typed_func(store.as_context_mut(), func_name)
        .map_err(|e| Error::WasmExecution(format!("func {} not found: {}", func_name, e)))?;

    let result = transform_fn
        .call(store.as_context_mut(), (input_ptr, input_len))
        .map_err(|e| Error::WasmExecution(format!("call failed: {}", e)))?;

    let result_ptr = (result >> 32) as i32;
    let result_len = (result & 0xFFFFFFFF) as i32;

    let mut result_bytes = vec![0u8; result_len as usize];
    memory
        .read(store.as_context(), result_ptr as usize, &mut result_bytes)
        .map_err(|e| Error::WasmExecution(format!("read failed: {}", e)))?;

    let result_str = String::from_utf8(result_bytes)
        .map_err(|e| Error::WasmExecution(format!("invalid UTF-8: {}", e)))?;

    serde_json::from_str(&result_str).map_err(|e| Error::WasmExecution(e.to_string()))
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
    fn test_invalid_lens_config() {
        let store = WasmTransformStore::new().unwrap();
        let config = LensConfig::new("v1", "v2", LensModule::default());

        let result = store.load_module(&config.lens);
        assert!(matches!(result, Err(Error::InvalidConfig(_))));
    }
}
