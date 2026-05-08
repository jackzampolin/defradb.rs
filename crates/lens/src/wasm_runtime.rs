//! WASM batch transform execution.
//!
//! Handles the actual execution of WASM modules for document transforms.

use wasmtime::{
    AsContext, AsContextMut, Engine, Linker, Module, Store as WasmStore, StoreLimits, TypedFunc,
};

use super::wasm::WasmSandboxConfig;
use crate::{Error, Result};

/// Host state for batch transforms that feed multiple docs via lens::next.
pub(crate) struct BatchHostState {
    input_docs: Vec<serde_json::Value>,
    current_index: usize,
    pub(crate) limits: Option<StoreLimits>,
}

impl BatchHostState {
    pub(crate) fn new(docs: Vec<serde_json::Value>, limits: Option<StoreLimits>) -> Self {
        Self {
            input_docs: docs,
            current_index: 0,
            limits,
        }
    }

    pub(crate) fn next_input(&mut self) -> Option<serde_json::Value> {
        if self.current_index < self.input_docs.len() {
            let doc = self.input_docs[self.current_index].clone();
            self.current_index += 1;
            Some(doc)
        } else {
            None
        }
    }
}

/// Execute a batch transform: all input docs are fed to a single WASM instance,
/// and all outputs are collected by calling transform/inverse repeatedly until EOS.
///
/// This supports transforms that change document counts (aggregate N->1, multiply 1->N).
///
/// When sandbox config is provided, resource limits (memory, fuel, epoch) are applied.
pub(crate) fn execute_batch_transform(
    engine: &Engine,
    module: &Module,
    input_docs: Vec<serde_json::Value>,
    arguments: Option<serde_json::Value>,
    inverse: bool,
    limits: Option<StoreLimits>,
    sandbox: &Option<WasmSandboxConfig>,
) -> Result<Vec<serde_json::Value>> {
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
                        // No more input docs -- write an EOS marker (type_id=127)
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
        let has_params = !matches!(&params, serde_json::Value::Object(map) if map.is_empty());

        if has_params {
            let alloc: TypedFunc<i32, i32> = instance
                .get_typed_func(store.as_context_mut(), "alloc")
                .map_err(|e| Error::WasmExecution(format!("alloc func not found: {}", e)))?;
            let set_param: TypedFunc<i32, i32> = instance
                .get_typed_func(store.as_context_mut(), "set_param")
                .map_err(|_| {
                    Error::WasmExecution("Export `set_param` does not exist".to_string())
                })?;

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

            let result_offset = set_param
                .call(store.as_context_mut(), offset)
                .map_err(|e| Error::WasmExecution(format!("set_param failed: {}", e)))?;
            let mut type_id_buf = [0u8; 1];
            memory
                .read(store.as_context(), result_offset as usize, &mut type_id_buf)
                .map_err(|e| {
                    Error::WasmExecution(format!("read set_param type_id failed: {}", e))
                })?;

            let type_id = type_id_buf[0] as i8;
            if type_id < 0 {
                let mut len_buf = [0u8; 4];
                memory
                    .read(
                        store.as_context(),
                        (result_offset + 1) as usize,
                        &mut len_buf,
                    )
                    .map_err(|e| {
                        Error::WasmExecution(format!("read set_param error len failed: {}", e))
                    })?;
                let len = u32::from_le_bytes(len_buf) as usize;

                let mut error_bytes = vec![0u8; len];
                memory
                    .read(
                        store.as_context(),
                        (result_offset + 5) as usize,
                        &mut error_bytes,
                    )
                    .map_err(|e| {
                        Error::WasmExecution(format!("read set_param error failed: {}", e))
                    })?;

                let error_str = String::from_utf8_lossy(&error_bytes);
                return Err(Error::WasmExecution(format!("WASM error: {}", error_str)));
            }
        }
    }

    // Get the transform/inverse function
    let func_name = if inverse { "inverse" } else { "transform" };
    let transform_fn: TypedFunc<(), i32> = instance
        .get_typed_func(store.as_context_mut(), func_name)
        .map_err(|e| {
            if inverse && e.to_string().contains("failed to find function export") {
                Error::WasmExecution("Export `inverse` does not exist".to_string())
            } else {
                Error::WasmExecution(format!("{} func not found: {}", func_name, e))
            }
        })?;

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

        if type_id == 0 {
            output_docs.push(serde_json::Value::Null);
            continue;
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

        let doc: serde_json::Value = serde_json::from_slice(&result_bytes)
            .map_err(|e| Error::WasmExecution(e.to_string()))?;
        output_docs.push(doc);
    }

    Ok(output_docs)
}
