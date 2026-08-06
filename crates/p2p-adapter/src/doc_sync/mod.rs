//! Document-sync operations, extracted so their peer-dispatch step can be
//! substituted in tests. Mirrors the `manage` module's layout.

pub(crate) mod dispatch;
pub(crate) mod sync;
#[cfg(test)]
pub(crate) mod test_support;
