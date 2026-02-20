use std::future::Future;
use std::pin::Pin;

#[cfg(not(target_arch = "wasm32"))]
mod inner {
    pub trait MaybeSend: Send {}
    impl<T: Send + ?Sized> MaybeSend for T {}

    pub trait MaybeSync: Sync {}
    impl<T: Sync + ?Sized> MaybeSync for T {}

    pub trait MaybeSendSync: Send + Sync {}
    impl<T: Send + Sync + ?Sized> MaybeSendSync for T {}
}

#[cfg(target_arch = "wasm32")]
mod inner {
    pub trait MaybeSend {}
    impl<T: ?Sized> MaybeSend for T {}

    pub trait MaybeSync {}
    impl<T: ?Sized> MaybeSync for T {}

    pub trait MaybeSendSync {}
    impl<T: ?Sized> MaybeSendSync for T {}
}

pub use inner::{MaybeSend, MaybeSendSync, MaybeSync};

#[cfg(not(target_arch = "wasm32"))]
pub type MaybeBoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

#[cfg(target_arch = "wasm32")]
pub type MaybeBoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;
