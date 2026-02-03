//! DAC bypass flag for NAC admin identities.
//!
//! When NAC is enabled and the requesting identity has the `dac-bypass`
//! node permission (owner or admin), this thread-local flag is set to
//! `true` before executing a query. The PermissionFilterNode checks this
//! flag and skips DAC permission checks when it is set.

use std::cell::RefCell;

thread_local! {
    static DAC_BYPASS: RefCell<bool> = const { RefCell::new(false) };
}

/// Set the DAC bypass flag for the current thread.
pub fn set_dac_bypass(bypass: bool) {
    DAC_BYPASS.with(|c| {
        *c.borrow_mut() = bypass;
    });
}

/// Get the current DAC bypass flag for this thread.
pub fn get_dac_bypass() -> bool {
    DAC_BYPASS.with(|c| *c.borrow())
}
