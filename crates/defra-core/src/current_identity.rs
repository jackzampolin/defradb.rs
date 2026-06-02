//! Ambient acting-identity DID for the current request thread.
//!
//! HTTP/FFI run the whole async operation inside `spawn_blocking` +
//! `block_on`, pinning it to one OS thread. Boundaries set the acting
//! identity here so DB-layer NAC checks (which receive no explicit
//! identity) can resolve who is acting.
//!
//! These threads are pooled and reused across requests. Use
//! [`scoped_current_identity`] at request boundaries so the identity is
//! always cleared on exit and never leaks into the next request.
//!
//! Stored as `String` because `defra-core` does not depend on the
//! `identity` crate; callers parse it back into a `Did` where needed.

use std::cell::RefCell;

thread_local! {
    static CURRENT_IDENTITY: RefCell<Option<String>> = const { RefCell::new(None) };
}

/// Set the ambient acting-identity DID for the current thread. Prefer
/// [`scoped_current_identity`] so it is always cleared.
pub fn set_current_identity(did: Option<String>) {
    CURRENT_IDENTITY.with(|c| *c.borrow_mut() = did);
}

/// Get the ambient acting-identity DID for the current thread, if set.
pub fn get_current_identity() -> Option<String> {
    CURRENT_IDENTITY.with(|c| c.borrow().clone())
}

/// RAII guard that clears the ambient identity on drop. MUST be used at
/// request boundaries to prevent identity leaking across pooled-thread reuse.
pub struct CurrentIdentityGuard;

impl Drop for CurrentIdentityGuard {
    fn drop(&mut self) {
        set_current_identity(None);
    }
}

/// Set the ambient identity and return a guard that clears it on drop.
#[must_use]
pub fn scoped_current_identity(did: Option<String>) -> CurrentIdentityGuard {
    set_current_identity(did);
    CurrentIdentityGuard
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scoped_identity_clears_on_drop() {
        assert_eq!(get_current_identity(), None);
        {
            let _guard = scoped_current_identity(Some("did:key:alice".to_string()));
            assert_eq!(get_current_identity(), Some("did:key:alice".to_string()));
        }
        assert_eq!(get_current_identity(), None);
    }

    #[test]
    fn set_and_get_roundtrip() {
        set_current_identity(Some("did:key:bob".to_string()));
        assert_eq!(get_current_identity(), Some("did:key:bob".to_string()));
        set_current_identity(None);
        assert_eq!(get_current_identity(), None);
    }
}
