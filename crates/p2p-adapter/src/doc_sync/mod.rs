//! Document-sync operations, extracted so their peer-dispatch step can be
//! substituted in tests. Mirrors the `manage` module's layout.

pub(crate) mod dispatch;
#[cfg(feature = "libp2p")]
pub(crate) mod pubsub_replies;
pub(crate) mod sync;
#[cfg(test)]
pub(crate) mod test_support;
