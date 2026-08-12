//! Single-threaded task-local storage for WASM futures.

use std::cell::RefCell;
use std::future::{poll_fn, Future};
use std::thread::LocalKey;

struct SwapGuard<'a, T: 'static> {
    local: &'static LocalKey<RefCell<Option<T>>>,
    value: &'a mut Option<T>,
}

impl<'a, T: 'static> SwapGuard<'a, T> {
    fn new(local: &'static LocalKey<RefCell<Option<T>>>, value: &'a mut Option<T>) -> Self {
        local.with(|local| std::mem::swap(value, &mut *local.borrow_mut()));
        Self { local, value }
    }

    fn value(&self) -> Option<&T> {
        self.value.as_ref()
    }
}

impl<T: 'static> Drop for SwapGuard<'_, T> {
    fn drop(&mut self) {
        self.local
            .with(|local| std::mem::swap(self.value, &mut *local.borrow_mut()));
    }
}

fn with_value<T: 'static, R>(
    local: &'static LocalKey<RefCell<Option<T>>>,
    value: &mut Option<T>,
    f: impl FnOnce() -> R,
) -> R {
    let reset = SwapGuard::new(local, value);
    let result = f();
    drop(reset);
    result
}

/// Poll a future with `value` installed in its task-local slot.
pub async fn scope<T: 'static, F>(
    local: &'static LocalKey<RefCell<Option<T>>>,
    value: T,
    future: F,
) -> F::Output
where
    F: Future,
{
    let mut value = Some(value);
    let mut future = Box::pin(future);
    poll_fn(move |cx| with_value(local, &mut value, || future.as_mut().poll(cx))).await
}

/// Access the current task-local value when a scope is active.
pub fn try_with<T: 'static, R>(
    local: &'static LocalKey<RefCell<Option<T>>>,
    f: impl FnOnce(&T) -> R,
) -> Option<R> {
    let mut value = None;
    let reset = SwapGuard::new(local, &mut value);
    let result = reset.value().map(f);
    drop(reset);
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::FutureExt;

    thread_local! {
        static VALUE: RefCell<Option<u8>> = const { RefCell::new(None) };
    }

    async fn yield_once() {
        let mut yielded = false;
        poll_fn(move |cx| {
            if yielded {
                std::task::Poll::Ready(())
            } else {
                yielded = true;
                cx.waker().wake_by_ref();
                std::task::Poll::Pending
            }
        })
        .await;
    }

    #[test]
    fn scopes_survive_interleaved_polls() {
        futures::executor::block_on(async {
            let first = scope(&VALUE, 1, async {
                assert_eq!(try_with(&VALUE, |value| *value), Some(1));
                yield_once().await;
                assert_eq!(try_with(&VALUE, |value| *value), Some(1));
            });
            let second = scope(&VALUE, 2, async {
                assert_eq!(try_with(&VALUE, |value| *value), Some(2));
                yield_once().await;
                assert_eq!(try_with(&VALUE, |value| *value), Some(2));
            });

            futures::join!(first, second);
        });
        assert_eq!(try_with(&VALUE, |value| *value), None);
    }

    #[test]
    fn try_with_allows_reentrant_scopes() {
        futures::executor::block_on(scope(&VALUE, 1, async {
            let nested = try_with(&VALUE, |_| {
                scope(&VALUE, 2, async { try_with(&VALUE, |value| *value) }).now_or_never()
            });

            assert_eq!(nested, Some(Some(Some(2))));
            assert_eq!(try_with(&VALUE, |value| *value), Some(1));
        }));
        assert_eq!(try_with(&VALUE, |value| *value), None);
    }
}
