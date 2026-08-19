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

/// RAII guard that restores the ambient identity to its prior value on drop.
/// Nested scopes therefore compose correctly (an inner scope does not wipe an
/// outer one). Used at request boundaries; restoring (not clearing) prevents a
/// nested per-mutation scope from erasing an outer node identity.
pub struct CurrentIdentityGuard {
    previous: Option<String>,
}

impl Drop for CurrentIdentityGuard {
    fn drop(&mut self) {
        set_current_identity(self.previous.take());
    }
}

/// Set the ambient identity for the current thread and return a guard that
/// restores the previous value on drop.
#[must_use]
pub fn scoped_current_identity(did: Option<String>) -> CurrentIdentityGuard {
    let previous = get_current_identity();
    set_current_identity(did);
    CurrentIdentityGuard { previous }
}

// Request-scoped acting identity for the multithreaded async (REST) path.
//
// REST handlers run on the multithreaded tokio runtime where `.await` may hop
// OS threads, so the `thread_local` above cannot carry the caller's identity
// through to DB-layer NAC checks. A `task_local` is bound to the request task
// and survives `.await`, mirroring Go's `identity.FromContext(ctx)`. The HTTP
// middleware scopes it around the whole request.
#[cfg(not(target_arch = "wasm32"))]
tokio::task_local! {
    static SCOPED_IDENTITY: Option<String>;
}

/// Read the request-scoped acting identity, if a scope is active.
#[cfg(not(target_arch = "wasm32"))]
pub fn try_get_scoped_identity() -> Option<String> {
    SCOPED_IDENTITY.try_with(|v| v.clone()).ok().flatten()
}

/// Resolve the current task scope when one exists, including an explicitly
/// anonymous (`None`) scope. Fall back to the pinned-thread identity only when
/// no task scope is active at all.
///
/// Unlike `try_get_scoped_identity().or_else(get_current_identity)`, this does
/// not let stale thread state override an anonymous request scope.
#[cfg(not(target_arch = "wasm32"))]
pub fn get_effective_identity() -> Option<String> {
    SCOPED_IDENTITY
        .try_with(Clone::clone)
        .unwrap_or_else(|_| get_current_identity())
}

/// On wasm there is no multithreaded request task; no scope is ever active.
#[cfg(target_arch = "wasm32")]
pub fn try_get_scoped_identity() -> Option<String> {
    None
}

#[cfg(target_arch = "wasm32")]
pub fn get_effective_identity() -> Option<String> {
    get_current_identity()
}

/// Run `fut` with the request-scoped acting identity set. Used by the HTTP
/// middleware to make the caller's DID available to DB-layer NAC checks
/// throughout the request task (survives `.await`, unlike the thread_local).
#[cfg(not(target_arch = "wasm32"))]
pub async fn with_scoped_identity<F, T>(did: Option<String>, fut: F) -> T
where
    F: std::future::Future<Output = T>,
{
    SCOPED_IDENTITY.scope(did, fut).await
}

/// On wasm, the scope is a no-op pass-through.
#[cfg(target_arch = "wasm32")]
pub async fn with_scoped_identity<F, T>(_did: Option<String>, fut: F) -> T
where
    F: std::future::Future<Output = T>,
{
    fut.await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scoped_identity_restores_previous_on_drop() {
        assert_eq!(get_current_identity(), None);
        {
            let _guard = scoped_current_identity(Some("did:key:alice".to_string()));
            assert_eq!(get_current_identity(), Some("did:key:alice".to_string()));
        }
        // No outer scope was active, so it restores to None.
        assert_eq!(get_current_identity(), None);
    }

    #[test]
    fn scoped_identity_nesting_restores_outer() {
        assert_eq!(get_current_identity(), None);
        {
            let _outer = scoped_current_identity(Some("did:key:outer".to_string()));
            assert_eq!(get_current_identity(), Some("did:key:outer".to_string()));
            {
                let _inner = scoped_current_identity(Some("did:key:inner".to_string()));
                assert_eq!(get_current_identity(), Some("did:key:inner".to_string()));
            }
            // Inner dropped: outer identity is restored, NOT cleared.
            assert_eq!(get_current_identity(), Some("did:key:outer".to_string()));
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

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn scoped_identity_visible_inside_scope_only() {
        assert_eq!(try_get_scoped_identity(), None);
        let inside = with_scoped_identity(Some("did:key:carol".to_string()), async {
            try_get_scoped_identity()
        })
        .await;
        assert_eq!(inside, Some("did:key:carol".to_string()));
        assert_eq!(try_get_scoped_identity(), None);
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn anonymous_task_scope_masks_stale_thread_identity() {
        set_current_identity(Some("did:key:stale-thread".to_string()));
        let inside = with_scoped_identity(None, async { get_effective_identity() }).await;
        assert_eq!(inside, None);
        assert_eq!(
            get_effective_identity().as_deref(),
            Some("did:key:stale-thread")
        );
        set_current_identity(None);
    }
}
