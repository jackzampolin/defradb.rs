use std::ffi::c_char;
use std::fs::File;
use std::sync::{Mutex, OnceLock};

use tracing_chrome::{ChromeLayerBuilder, FlushGuard};
use tracing_subscriber::prelude::*;
use tracing_subscriber::EnvFilter;

use crate::helpers::require_c_str;
use crate::types::FfiResult;
use crate::{ffi_entry, try_ffi};

static PROFILING_GUARD: OnceLock<Mutex<Option<FlushGuard>>> = OnceLock::new();

fn guard_slot() -> &'static Mutex<Option<FlushGuard>> {
    PROFILING_GUARD.get_or_init(|| Mutex::new(None))
}

fn with_default_transport_noise_filters(filter: EnvFilter) -> EnvFilter {
    filter
        .add_directive(
            "iroh_quinn_proto::connection=error"
                .parse()
                .expect("valid tracing directive"),
        )
        .add_directive(
            "noq_proto::connection=error"
                .parse()
                .expect("valid tracing directive"),
        )
}

#[allow(clippy::not_unsafe_ptr_arg_deref)]
#[no_mangle]
pub extern "C" fn defra_profiling_start(output_path: *const c_char) -> FfiResult {
    ffi_entry! {
        let output_path = try_ffi!(unsafe { require_c_str(output_path, "output_path") });

        let mut guard = match guard_slot().lock() {
            Ok(guard) => guard,
            Err(_) => return FfiResult::error("profiling guard mutex poisoned"),
        };

        if guard.is_some() {
            return FfiResult::error("profiling is already running");
        }

        let file = match File::create(&output_path) {
            Ok(file) => file,
            Err(error) => {
                return FfiResult::error(format!(
                    "failed to create profiling trace file {}: {}",
                    output_path, error
                ))
            }
        };

        let filter = with_default_transport_noise_filters(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        );
        let (chrome_layer, flush_guard) = ChromeLayerBuilder::new()
            .writer(file)
            .include_args(true)
            .build();

        let subscriber = tracing_subscriber::registry()
            .with(filter)
            .with(chrome_layer);

        if let Err(error) = tracing::subscriber::set_global_default(subscriber) {
            return FfiResult::error(format!(
                "failed to initialize profiling subscriber: {}",
                error
            ));
        }

        *guard = Some(flush_guard);
        FfiResult::ok()
    }
}

#[no_mangle]
pub extern "C" fn defra_profiling_stop() -> FfiResult {
    ffi_entry! {
        let mut guard = match guard_slot().lock() {
            Ok(guard) => guard,
            Err(_) => return FfiResult::error("profiling guard mutex poisoned"),
        };

        let Some(flush_guard) = guard.take() else {
            return FfiResult::error("profiling is not running");
        };

        drop(flush_guard);
        FfiResult::ok()
    }
}
