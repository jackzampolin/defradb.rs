//! Guarantees a plan is closed on every execution path.

use tracing::warn;

use crate::error::Result;
use crate::planner::PlanNode;

/// Close `plan`, then resolve `outcome`.
///
/// `Drop` cannot await, so `close` is the only point at which a plan can
/// release what it holds or flush what it deferred - deferred lens migration
/// write-backs, for one. A bare `?` in a pull loop returns past it, so every
/// plan-driving site routes its result through here instead.
///
/// A body error wins over a close error: it is the root cause, and masking it
/// with a teardown failure loses the diagnosis. The close error is logged
/// rather than dropped silently.
pub(crate) async fn close_after<T>(plan: &mut dyn PlanNode, outcome: Result<T>) -> Result<T> {
    let closed = plan.close().await;
    match (outcome, closed) {
        (Err(e), Err(close_err)) => {
            warn!(error = %close_err, "plan close failed while unwinding a query error");
            Err(e)
        }
        (Err(e), Ok(())) => Err(e),
        (Ok(value), Ok(())) => Ok(value),
        (Ok(_), Err(close_err)) => Err(close_err),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::document::DocumentMapping;
    use crate::planner::{Doc, PlanNode};
    use async_trait::async_trait;
    use query_types::error::QueryError;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    struct StubPlan {
        closes: Arc<AtomicUsize>,
        close_fails: bool,
        mapping: DocumentMapping,
        doc: Doc,
    }

    impl StubPlan {
        fn new(closes: Arc<AtomicUsize>, close_fails: bool) -> Self {
            Self {
                closes,
                close_fails,
                mapping: DocumentMapping::default(),
                doc: Doc::default(),
            }
        }
    }

    #[cfg_attr(not(target_arch = "wasm32"), async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
    impl PlanNode for StubPlan {
        async fn init(&mut self) -> Result<()> {
            Ok(())
        }
        async fn start(&mut self) -> Result<()> {
            Ok(())
        }
        async fn next(&mut self) -> Result<bool> {
            Ok(false)
        }
        fn value(&self) -> &Doc {
            &self.doc
        }
        async fn close(&mut self) -> Result<()> {
            self.closes.fetch_add(1, Ordering::SeqCst);
            if self.close_fails {
                Err(QueryError::execution("close failed"))
            } else {
                Ok(())
            }
        }
        fn source(&self) -> Option<&dyn PlanNode> {
            None
        }
        fn document_map(&self) -> &DocumentMapping {
            &self.mapping
        }
        fn kind(&self) -> &'static str {
            "stub"
        }
    }

    /// The whole point: an error in the body must not skip the close.
    #[tokio::test]
    async fn closes_when_the_body_errored() {
        let closes = Arc::new(AtomicUsize::new(0));
        let mut plan = StubPlan::new(closes.clone(), false);

        let outcome: Result<()> = Err(QueryError::execution("body failed"));
        let result = close_after(&mut plan, outcome).await;

        assert_eq!(closes.load(Ordering::SeqCst), 1);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn closes_when_the_body_succeeded() {
        let closes = Arc::new(AtomicUsize::new(0));
        let mut plan = StubPlan::new(closes.clone(), false);

        let result = close_after(&mut plan, Ok(7)).await.unwrap();

        assert_eq!(closes.load(Ordering::SeqCst), 1);
        assert_eq!(result, 7);
    }

    /// The body error is the root cause. A teardown failure must not mask it.
    #[tokio::test]
    async fn body_error_wins_over_a_close_error() {
        let closes = Arc::new(AtomicUsize::new(0));
        let mut plan = StubPlan::new(closes.clone(), true);

        let outcome: Result<()> = Err(QueryError::execution("body failed"));
        let err = close_after(&mut plan, outcome).await.unwrap_err();

        assert_eq!(closes.load(Ordering::SeqCst), 1);
        assert!(
            err.to_string().contains("body failed"),
            "close error masked the root cause: {err}"
        );
    }

    /// With nothing else to report, a close failure is the failure.
    #[tokio::test]
    async fn close_error_surfaces_when_the_body_succeeded() {
        let closes = Arc::new(AtomicUsize::new(0));
        let mut plan = StubPlan::new(closes.clone(), true);

        let err = close_after(&mut plan, Ok(())).await.unwrap_err();

        assert_eq!(closes.load(Ordering::SeqCst), 1);
        assert!(err.to_string().contains("close failed"), "{err}");
    }
}
