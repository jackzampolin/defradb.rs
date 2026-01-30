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
    arguments: Option<serde_json::Value>,
}

/// Host state passed to WASM store for lens callbacks.
struct HostState {
    input_doc: LensDoc,
    input_consumed: bool,
}

impl HostState {
    fn new(doc: LensDoc) -> Self {
        Self {
            input_doc: doc,
            input_consumed: false,
        }
    }
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
        if let Some(ref path_str) = lens.path {
            // Strip file:// URL scheme if present (Go sends paths as file:// URLs)
            let clean_path = path_str.strip_prefix("file://").unwrap_or(path_str);
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

    async fn list(&self) -> Result<std::collections::HashMap<String, crate::LensModule>> {
        let configs = self.configs.read();
        let result = configs
            .iter()
            .map(|(id, config)| (id.to_string(), config.lens.clone()))
            .collect();
        Ok(result)
    }

    fn transform(&self, id: &TransformId, docs: LensDocStream) -> Result<LensDocResultStream> {
        let modules = self.modules.read();
        let compiled = modules
            .get(id)
            .ok_or_else(|| Error::TransformNotFound(id.to_string()))?;
        let module = compiled.module.clone();
        let arguments = compiled.arguments.clone();
        drop(modules);

        let engine = self.engine.clone();

        Ok(Box::pin(docs.map(move |doc| {
            // Clone module for each document (wasmtime modules are Arc internally)
            let mut store = WasmStore::new(&engine, HostState::new(doc.clone()));
            let mut linker: Linker<HostState> = Linker::new(&engine);

            // Add the lens::next import - this is called by WASM to get input document offset
            linker
                .func_wrap(
                    "lens",
                    "next",
                    |mut caller: wasmtime::Caller<'_, HostState>| -> i32 {
                        // Check if input already consumed
                        if caller.data().input_consumed {
                            return 0; // No more input
                        }

                        // Get memory
                        let memory = match caller.get_export("memory") {
                            Some(wasmtime::Extern::Memory(mem)) => mem,
                            _ => return 0,
                        };

                        // Get alloc function to allocate in WASM's allocator
                        let alloc = match caller.get_export("alloc") {
                            Some(wasmtime::Extern::Func(f)) => f,
                            _ => return 0,
                        };

                        // Serialize document to JSON
                        let json = match serde_json::to_vec(&caller.data().input_doc) {
                            Ok(j) => j,
                            Err(_) => return 0,
                        };

                        // Format: [type_id: i8][len: u32 LE][data: bytes]
                        // type_id: 1 = JSON
                        let header_size = 5i32; // 1 byte type + 4 bytes len
                        let total_size = header_size + json.len() as i32;

                        // Allocate memory using WASM's allocator
                        let offset = match alloc.typed::<i32, i32>(&caller) {
                            Ok(typed_alloc) => match typed_alloc.call(&mut caller, total_size) {
                                Ok(o) => o,
                                Err(_) => return 0,
                            },
                            Err(_) => return 0,
                        };

                        // Write type ID (1 = JSON)
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

                        // Mark input as consumed
                        caller.data_mut().input_consumed = true;
                        offset
                    },
                )
                .map_err(|e| Error::WasmExecution(format!("failed to define lens::next: {}", e)))?;

            let instance = linker
                .instantiate(&mut store, &module)
                .map_err(|e| Error::WasmExecution(format!("failed to instantiate: {}", e)))?;

            execute_transform_with_host(&instance, &mut store, arguments.clone())
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

        Ok(Box::pin(docs.map(move |doc| {
            let mut store = WasmStore::new(&engine, HostState::new(doc.clone()));
            let mut linker: Linker<HostState> = Linker::new(&engine);

            // Add the lens::next import for inverse too
            linker
                .func_wrap(
                    "lens",
                    "next",
                    |mut caller: wasmtime::Caller<'_, HostState>| -> i32 {
                        if caller.data().input_consumed {
                            return 0;
                        }

                        let memory = match caller.get_export("memory") {
                            Some(wasmtime::Extern::Memory(mem)) => mem,
                            _ => return 0,
                        };

                        let alloc = match caller.get_export("alloc") {
                            Some(wasmtime::Extern::Func(f)) => f,
                            _ => return 0,
                        };

                        let json = match serde_json::to_vec(&caller.data().input_doc) {
                            Ok(j) => j,
                            Err(_) => return 0,
                        };

                        let header_size = 5i32;
                        let total_size = header_size + json.len() as i32;

                        let offset = match alloc.typed::<i32, i32>(&caller) {
                            Ok(typed_alloc) => match typed_alloc.call(&mut caller, total_size) {
                                Ok(o) => o,
                                Err(_) => return 0,
                            },
                            Err(_) => return 0,
                        };

                        if memory.write(&mut caller, offset as usize, &[1u8]).is_err() {
                            return 0;
                        }

                        let len_bytes = (json.len() as u32).to_le_bytes();
                        if memory
                            .write(&mut caller, (offset + 1) as usize, &len_bytes)
                            .is_err()
                        {
                            return 0;
                        }

                        if memory
                            .write(&mut caller, (offset + header_size) as usize, &json)
                            .is_err()
                        {
                            return 0;
                        }

                        caller.data_mut().input_consumed = true;
                        offset
                    },
                )
                .map_err(|e| Error::WasmExecution(format!("failed to define lens::next: {}", e)))?;

            let instance = linker
                .instantiate(&mut store, &module)
                .map_err(|e| Error::WasmExecution(format!("failed to instantiate: {}", e)))?;

            execute_inverse_with_host(&instance, &mut store, arguments.clone())
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

/// Execute transform with host state (supports lens::next callback).
fn execute_transform_with_host(
    instance: &Instance,
    store: &mut WasmStore<HostState>,
    arguments: Option<serde_json::Value>,
) -> Result<LensDoc> {
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

            // Allocate memory: type_id (1) + len (4) + data
            let total_size = 5 + param_json.len() as i32;
            let offset = alloc
                .call(store.as_context_mut(), total_size)
                .map_err(|e| Error::WasmExecution(format!("alloc for params failed: {}", e)))?;

            // Write type_id (1 = JSON)
            memory
                .write(store.as_context_mut(), offset as usize, &[1u8])
                .map_err(|e| Error::WasmExecution(format!("write type_id failed: {}", e)))?;

            // Write length
            let len_bytes = (param_json.len() as u32).to_le_bytes();
            memory
                .write(store.as_context_mut(), (offset + 1) as usize, &len_bytes)
                .map_err(|e| Error::WasmExecution(format!("write len failed: {}", e)))?;

            // Write data
            memory
                .write(store.as_context_mut(), (offset + 5) as usize, &param_json)
                .map_err(|e| Error::WasmExecution(format!("write data failed: {}", e)))?;

            // Call set_param
            let _ = set_param
                .call(store.as_context_mut(), offset)
                .map_err(|e| Error::WasmExecution(format!("set_param failed: {}", e)))?;
        }
    }

    // Call transform (no parameters - it calls lens::next internally)
    let transform_fn: TypedFunc<(), i32> = instance
        .get_typed_func(store.as_context_mut(), "transform")
        .map_err(|e| Error::WasmExecution(format!("transform func not found: {}", e)))?;

    let result_offset = transform_fn
        .call(store.as_context_mut(), ())
        .map_err(|e| Error::WasmExecution(format!("transform call failed: {}", e)))?;

    if result_offset == 0 {
        return Err(Error::WasmExecution("transform returned null".to_string()));
    }

    // Read result using protocol: [type_id: i8][len: u32 LE][data: bytes]
    let mut type_id_buf = [0u8; 1];
    memory
        .read(store.as_context(), result_offset as usize, &mut type_id_buf)
        .map_err(|e| Error::WasmExecution(format!("read type_id failed: {}", e)))?;

    let type_id = type_id_buf[0] as i8;
    if type_id < 0 {
        // Error type - read error message
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

    if type_id == 127 {
        // EOS - end of stream, no more documents
        return Err(Error::WasmExecution("unexpected EOS".to_string()));
    }

    if type_id != 1 {
        return Err(Error::WasmExecution(format!(
            "unexpected type_id: {}",
            type_id
        )));
    }

    // Read JSON data
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

    serde_json::from_slice(&result_bytes).map_err(|e| Error::WasmExecution(e.to_string()))
}

/// Execute inverse with host state (supports lens::next callback).
fn execute_inverse_with_host(
    instance: &Instance,
    store: &mut WasmStore<HostState>,
    arguments: Option<serde_json::Value>,
) -> Result<LensDoc> {
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

    // Call inverse function
    let inverse_fn: TypedFunc<(), i32> = instance
        .get_typed_func(store.as_context_mut(), "inverse")
        .map_err(|e| Error::WasmExecution(format!("inverse func not found: {}", e)))?;

    let result_offset = inverse_fn
        .call(store.as_context_mut(), ())
        .map_err(|e| Error::WasmExecution(format!("inverse call failed: {}", e)))?;

    if result_offset == 0 {
        return Err(Error::WasmExecution("inverse returned null".to_string()));
    }

    // Read result using protocol
    let mut type_id_buf = [0u8; 1];
    memory
        .read(store.as_context(), result_offset as usize, &mut type_id_buf)
        .map_err(|e| Error::WasmExecution(format!("read type_id failed: {}", e)))?;

    let type_id = type_id_buf[0] as i8;
    if type_id < 0 {
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

    if type_id == 127 {
        return Err(Error::WasmExecution("unexpected EOS".to_string()));
    }

    if type_id != 1 {
        return Err(Error::WasmExecution(format!(
            "unexpected type_id: {}",
            type_id
        )));
    }

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

    serde_json::from_slice(&result_bytes).map_err(|e| Error::WasmExecution(e.to_string()))
}

/// Execute transform using an existing instance (old API, not used by lens host).
#[allow(dead_code)]
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
