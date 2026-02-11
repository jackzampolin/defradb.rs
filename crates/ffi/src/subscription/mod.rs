//! Subscription management for FFI.
//!
//! This module exposes a polling-based subscription API for FFI callers.
//! Since FFI can't easily handle async callbacks, we use a polling model:
//!
//! 1. `create_subscription` - Start listening for events, returns handle
//! 2. `poll_subscription` - Non-blocking poll for next event
//! 3. `close_subscription` - Stop listening and cleanup

mod create;
mod manage;
#[cfg(test)]
mod tests;

use std::ffi::{c_char, c_int};
use std::ptr;

use crate::types::sanitize_to_cstring;

pub(crate) use create::response_has_data;
pub use create::{create_merge_complete_subscription, create_subscription};
pub use manage::{
    close_graphql_subscription, close_subscription, poll_graphql_subscription, poll_subscription,
};

/// Result type for subscription creation.
#[repr(C)]
pub struct CreateSubscriptionResult {
    /// Status code: 0=success, 1=error
    pub status: c_int,
    /// Error message (null on success). Caller must free with `defra_free_string`.
    pub error: *mut c_char,
    /// Subscription handle (0 on error).
    pub subscription_handle: usize,
}

impl CreateSubscriptionResult {
    pub(crate) fn success(handle: usize) -> Self {
        Self {
            status: 0,
            error: ptr::null_mut(),
            subscription_handle: handle,
        }
    }

    pub(crate) fn error(message: impl Into<String>) -> Self {
        Self {
            status: 1,
            error: sanitize_to_cstring(message, "unknown error").into_raw(),
            subscription_handle: 0,
        }
    }
}

/// Result type for polling subscriptions.
///
/// Status codes:
/// - 0: Event available (value contains JSON event data)
/// - 1: Error occurred
/// - 2: No event available (subscription open but no pending events)
/// - 3: Subscription closed (no more events will arrive)
#[repr(C)]
pub struct PollSubscriptionResult {
    /// Status code (see above)
    pub status: c_int,
    /// Error message (null unless status=1). Caller must free with `defra_free_string`.
    pub error: *mut c_char,
    /// Event data as JSON (null unless status=0). Caller must free with `defra_free_string`.
    pub value: *mut c_char,
    /// Number of events dropped due to buffer overflow since last poll.
    /// When non-zero, the client should re-fetch data to ensure consistency.
    pub dropped_count: u64,
}

impl PollSubscriptionResult {
    pub(crate) fn event(json: String, dropped: u64) -> Self {
        Self {
            status: 0,
            error: ptr::null_mut(),
            value: sanitize_to_cstring(json, "{}").into_raw(),
            dropped_count: dropped,
        }
    }

    pub(crate) fn error(message: impl Into<String>) -> Self {
        Self {
            status: 1,
            error: sanitize_to_cstring(message, "unknown error").into_raw(),
            value: ptr::null_mut(),
            dropped_count: 0,
        }
    }

    pub(crate) fn no_event(dropped: u64) -> Self {
        Self {
            status: 2,
            error: ptr::null_mut(),
            value: ptr::null_mut(),
            dropped_count: dropped,
        }
    }

    pub(crate) fn closed() -> Self {
        Self {
            status: 3,
            error: ptr::null_mut(),
            value: ptr::null_mut(),
            dropped_count: 0,
        }
    }
}

/// Result type for closing subscriptions.
#[repr(C)]
pub struct CloseSubscriptionResult {
    /// Status code: 0=success, 1=error
    pub status: c_int,
    /// Error message (null on success). Caller must free with `defra_free_string`.
    pub error: *mut c_char,
}

impl CloseSubscriptionResult {
    pub(crate) fn success() -> Self {
        Self {
            status: 0,
            error: ptr::null_mut(),
        }
    }

    pub(crate) fn error(message: impl Into<String>) -> Self {
        Self {
            status: 1,
            error: sanitize_to_cstring(message, "unknown error").into_raw(),
        }
    }
}
