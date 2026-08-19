//! One bounded scheduling policy for every transport entrypoint.
//!
//! The transport channel is the sole resident queue. The dispatcher classifies
//! events into fixed admission, recovery-serving, and completion worker sets;
//! lightweight state transitions drain inline. Excess requests are rejected
//! in place instead of waiting for a slot and hiding ownership progress behind
//! unrelated work.

use std::future::Future;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use tokio::sync::{mpsc, Semaphore};
use tokio::task::JoinSet;

use crate::transport::TransportEvent;

/// Whether a bounded request was admitted to a worker.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DispatchAdmission {
    Admitted,
    Saturated,
}

/// Fixed scheduling classes for all transport entrypoints.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DispatchClass {
    Inline,
    Admission,
    Recovery,
    Completion,
}

/// Classifies an event once at the shared transport boundary.
pub trait DispatchEvent {
    fn dispatch_class(&self) -> DispatchClass;
}

impl<ResponseToken> DispatchEvent for TransportEvent<ResponseToken> {
    fn dispatch_class(&self) -> DispatchClass {
        self.dispatch_class()
    }
}

#[cfg(feature = "libp2p-transport")]
impl DispatchEvent for crate::host::HostEvent {
    fn dispatch_class(&self) -> DispatchClass {
        self.dispatch_class()
    }
}

const MAX_ACTIVE_REQUESTS: usize = 32;
// CAR serving has a distinct reserve because receiver-owned recovery must not
// wait behind serialized PushLog admission on the provider.
const MAX_ACTIVE_RECOVERY: usize = 8;
const MAX_ACTIVE_REJECTIONS: usize = 8;
// Receiver fetch admission is bounded below this value (four by default).
// The extra slots cover terminal and reply events without allowing transport
// tasks to grow with the event stream.
const MAX_ACTIVE_COMPLETIONS: usize = 16;

/// Instance-local diagnostics for the shared transport scheduler.
#[derive(Debug, Default)]
pub struct DispatchDiagnostics {
    active_requests: AtomicUsize,
    active_requests_high_water: AtomicUsize,
    active_recovery: AtomicUsize,
    active_recovery_high_water: AtomicUsize,
    active_rejections: AtomicUsize,
    active_rejections_high_water: AtomicUsize,
    active_completions: AtomicUsize,
    active_completions_high_water: AtomicUsize,
    saturated_total: AtomicU64,
    recovery_saturated_total: AtomicU64,
    rejection_dropped_total: AtomicU64,
}

/// Stable point-in-time scheduler diagnostics exposed through sync status.
#[derive(Debug, Clone, Copy, serde::Serialize)]
pub struct DispatchSnapshot {
    pub request_capacity: usize,
    pub active_requests: usize,
    pub active_requests_high_water: usize,
    pub recovery_capacity: usize,
    pub active_recovery: usize,
    pub active_recovery_high_water: usize,
    pub rejection_capacity: usize,
    pub active_rejections: usize,
    pub active_rejections_high_water: usize,
    pub completion_capacity: usize,
    pub active_completions: usize,
    pub active_completions_high_water: usize,
    pub saturated_total: u64,
    pub recovery_saturated_total: u64,
    pub rejection_dropped_total: u64,
}

impl DispatchDiagnostics {
    pub fn snapshot(&self) -> DispatchSnapshot {
        DispatchSnapshot {
            request_capacity: MAX_ACTIVE_REQUESTS,
            active_requests: self.active_requests.load(Ordering::Relaxed),
            active_requests_high_water: self.active_requests_high_water.load(Ordering::Relaxed),
            recovery_capacity: MAX_ACTIVE_RECOVERY,
            active_recovery: self.active_recovery.load(Ordering::Relaxed),
            active_recovery_high_water: self.active_recovery_high_water.load(Ordering::Relaxed),
            rejection_capacity: MAX_ACTIVE_REJECTIONS,
            active_rejections: self.active_rejections.load(Ordering::Relaxed),
            active_rejections_high_water: self.active_rejections_high_water.load(Ordering::Relaxed),
            completion_capacity: MAX_ACTIVE_COMPLETIONS,
            active_completions: self.active_completions.load(Ordering::Relaxed),
            active_completions_high_water: self
                .active_completions_high_water
                .load(Ordering::Relaxed),
            saturated_total: self.saturated_total.load(Ordering::Relaxed),
            recovery_saturated_total: self.recovery_saturated_total.load(Ordering::Relaxed),
            rejection_dropped_total: self.rejection_dropped_total.load(Ordering::Relaxed),
        }
    }

    fn enter(self: &Arc<Self>, kind: DispatchActivityKind) -> DispatchActivity {
        let (current, high_water) = match kind {
            DispatchActivityKind::Request => {
                (&self.active_requests, &self.active_requests_high_water)
            }
            DispatchActivityKind::Recovery => {
                (&self.active_recovery, &self.active_recovery_high_water)
            }
            DispatchActivityKind::Rejection => {
                (&self.active_rejections, &self.active_rejections_high_water)
            }
            DispatchActivityKind::Completion => (
                &self.active_completions,
                &self.active_completions_high_water,
            ),
        };
        let occupancy = current.fetch_add(1, Ordering::Relaxed) + 1;
        high_water.fetch_max(occupancy, Ordering::Relaxed);
        DispatchActivity {
            diagnostics: Arc::clone(self),
            kind,
        }
    }
}

#[derive(Clone, Copy)]
enum DispatchActivityKind {
    Request,
    Recovery,
    Rejection,
    Completion,
}

struct DispatchActivity {
    diagnostics: Arc<DispatchDiagnostics>,
    kind: DispatchActivityKind,
}

impl Drop for DispatchActivity {
    fn drop(&mut self) {
        let current = match self.kind {
            DispatchActivityKind::Request => &self.diagnostics.active_requests,
            DispatchActivityKind::Recovery => &self.diagnostics.active_recovery,
            DispatchActivityKind::Rejection => &self.diagnostics.active_rejections,
            DispatchActivityKind::Completion => &self.diagnostics.active_completions,
        };
        current.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Drain transport events without ever waiting for a request-worker slot.
pub(crate) async fn run_event_dispatcher<E, Handler, HandlerFuture>(
    mut events: mpsc::Receiver<E>,
    diagnostics: Arc<DispatchDiagnostics>,
    handler: Handler,
) where
    E: DispatchEvent + Send + 'static,
    Handler: Fn(E, DispatchAdmission) -> HandlerFuture + Clone + Send + 'static,
    HandlerFuture: Future<Output = ()> + Send + 'static,
{
    let request_slots = Arc::new(Semaphore::new(MAX_ACTIVE_REQUESTS));
    let recovery_slots = Arc::new(Semaphore::new(MAX_ACTIVE_RECOVERY));
    let rejection_slots = Arc::new(Semaphore::new(MAX_ACTIVE_REJECTIONS));
    let completion_slots = Arc::new(Semaphore::new(MAX_ACTIVE_COMPLETIONS));
    let mut tasks = JoinSet::new();

    loop {
        tokio::select! {
            biased;
            result = tasks.join_next(), if !tasks.is_empty() => {
                report_join_result(result.expect("dispatcher task set was non-empty"));
            }
            event = events.recv() => {
                let Some(event) = event else {
                    break;
                };
                let class = event.dispatch_class();
                if class == DispatchClass::Completion {
                    // Completion producers are themselves bounded (receiver
                    // fetches, protocol replies). If the fixed completion set
                    // is full, wait for one owner to finish before retaining
                    // more work outside the transport channel.
                    let permit = Arc::clone(&completion_slots)
                        .acquire_owned()
                        .await
                        .expect("completion semaphore is never closed");
                    let completion_handler = handler.clone();
                    let activity = diagnostics.enter(DispatchActivityKind::Completion);
                    tasks.spawn(async move {
                        let _permit = permit;
                        let _activity = activity;
                        completion_handler(event, DispatchAdmission::Admitted).await;
                    });
                } else if class == DispatchClass::Inline {
                    handler.clone()(event, DispatchAdmission::Admitted).await;
                } else {
                    let (slots, activity_kind) = match class {
                        DispatchClass::Admission => (
                            &request_slots,
                            DispatchActivityKind::Request,
                        ),
                        DispatchClass::Recovery => (
                            &recovery_slots,
                            DispatchActivityKind::Recovery,
                        ),
                        DispatchClass::Inline | DispatchClass::Completion => unreachable!(),
                    };
                    if let Ok(permit) = Arc::clone(slots).try_acquire_owned() {
                        let task_handler = handler.clone();
                        let activity = diagnostics.enter(activity_kind);
                        tasks.spawn(async move {
                            let _permit = permit;
                            let _activity = activity;
                            task_handler(event, DispatchAdmission::Admitted).await;
                        });
                    } else {
                        diagnostics.saturated_total.fetch_add(1, Ordering::Relaxed);
                        if class == DispatchClass::Recovery {
                            diagnostics
                                .recovery_saturated_total
                                .fetch_add(1, Ordering::Relaxed);
                        }
                        // A capacity nack is best effort: the remote peer may stop
                        // reading its response stream. Keep those writes off the
                        // sole event drain. A stalled write keeps its bounded slot;
                        // subsequent excess tokens are dropped, closing the stream
                        // as an actionable transport failure for durable retry.
                        if let Ok(permit) = Arc::clone(&rejection_slots).try_acquire_owned() {
                            let rejection_handler = handler.clone();
                            let activity = diagnostics.enter(DispatchActivityKind::Rejection);
                            tasks.spawn(async move {
                                let _permit = permit;
                                let _activity = activity;
                                rejection_handler(event, DispatchAdmission::Saturated).await;
                            });
                        } else {
                            diagnostics
                                .rejection_dropped_total
                                .fetch_add(1, Ordering::Relaxed);
                            tracing::debug!(
                                "dropping transport request after rejection workers saturated"
                            );
                        }
                    }
                }
            }
        }
    }

    tasks.abort_all();
    while let Some(result) = tasks.join_next().await {
        report_join_result(result);
    }
}

fn report_join_result(result: Result<(), tokio::task::JoinError>) {
    if let Err(error) = result {
        if !error.is_cancelled() {
            tracing::error!(%error, "transport request task panicked");
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use tokio::sync::{mpsc, Mutex, Semaphore};

    use super::*;

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum TestEvent {
        Request(usize),
        Recovery(usize),
        Completion,
    }

    impl DispatchEvent for TestEvent {
        fn dispatch_class(&self) -> DispatchClass {
            match self {
                Self::Request(_) => DispatchClass::Admission,
                Self::Recovery(_) => DispatchClass::Recovery,
                Self::Completion => DispatchClass::Completion,
            }
        }
    }

    #[tokio::test]
    async fn saturation_rejects_work_and_still_drains_completion() {
        let (tx, rx) = mpsc::channel(64);
        let observed = Arc::new(Mutex::new(Vec::new()));
        let started = Arc::new(Semaphore::new(0));
        let release = Arc::new(Semaphore::new(0));

        let dispatcher = {
            let observed = Arc::clone(&observed);
            let started = Arc::clone(&started);
            let release = Arc::clone(&release);
            tokio::spawn(run_event_dispatcher(
                rx,
                Arc::new(DispatchDiagnostics::default()),
                move |event, admission| {
                    let observed = Arc::clone(&observed);
                    let started = Arc::clone(&started);
                    let release = Arc::clone(&release);
                    async move {
                        observed.lock().await.push((event, admission));
                        if matches!(event, TestEvent::Request(index) if index < MAX_ACTIVE_REQUESTS)
                            && admission == DispatchAdmission::Admitted
                        {
                            started.add_permits(1);
                            let _permit = release.acquire().await.unwrap();
                        }
                    }
                },
            ))
        };

        for index in 0..MAX_ACTIVE_REQUESTS {
            tx.send(TestEvent::Request(index)).await.unwrap();
        }
        tx.send(TestEvent::Request(MAX_ACTIVE_REQUESTS))
            .await
            .unwrap();
        tx.send(TestEvent::Completion).await.unwrap();

        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            started.acquire_many(MAX_ACTIVE_REQUESTS as u32),
        )
        .await
        .expect("the request worker bound should fill")
        .unwrap()
        .forget();

        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                let events = observed.lock().await;
                let rejected = events.contains(&(
                    TestEvent::Request(MAX_ACTIVE_REQUESTS),
                    DispatchAdmission::Saturated,
                ));
                let completed =
                    events.contains(&(TestEvent::Completion, DispatchAdmission::Admitted));
                drop(events);
                if rejected && completed {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("saturation must not hide a completion");

        release.add_permits(MAX_ACTIVE_REQUESTS);
        drop(tx);
        dispatcher.await.unwrap();
    }

    #[tokio::test]
    async fn saturated_admission_does_not_block_recovery_serving() {
        let (tx, rx) = mpsc::channel(64);
        let requests_started = Arc::new(Semaphore::new(0));
        let release_requests = Arc::new(Semaphore::new(0));
        let recovery_observed = Arc::new(Semaphore::new(0));
        let diagnostics = Arc::new(DispatchDiagnostics::default());

        let dispatcher = {
            let requests_started = Arc::clone(&requests_started);
            let release_requests = Arc::clone(&release_requests);
            let recovery_observed = Arc::clone(&recovery_observed);
            tokio::spawn(run_event_dispatcher(
                rx,
                Arc::clone(&diagnostics),
                move |event, admission| {
                    let requests_started = Arc::clone(&requests_started);
                    let release_requests = Arc::clone(&release_requests);
                    let recovery_observed = Arc::clone(&recovery_observed);
                    async move {
                        match (event, admission) {
                            (TestEvent::Request(_), DispatchAdmission::Admitted) => {
                                requests_started.add_permits(1);
                                let _permit = release_requests.acquire().await.unwrap();
                            }
                            (TestEvent::Recovery(_), DispatchAdmission::Admitted) => {
                                recovery_observed.add_permits(1);
                            }
                            _ => {}
                        }
                    }
                },
            ))
        };

        for index in 0..MAX_ACTIVE_REQUESTS {
            tx.send(TestEvent::Request(index)).await.unwrap();
        }
        requests_started
            .acquire_many(MAX_ACTIVE_REQUESTS as u32)
            .await
            .unwrap()
            .forget();
        tx.send(TestEvent::Recovery(0)).await.unwrap();

        tokio::time::timeout(Duration::from_secs(1), recovery_observed.acquire())
            .await
            .expect("ownership admission saturation must not block CAR serving")
            .unwrap()
            .forget();
        let snapshot = diagnostics.snapshot();
        assert_eq!(snapshot.active_requests, MAX_ACTIVE_REQUESTS);
        assert_eq!(snapshot.active_recovery_high_water, 1);
        assert_eq!(snapshot.recovery_saturated_total, 0);

        release_requests.add_permits(MAX_ACTIVE_REQUESTS);
        drop(tx);
        dispatcher.await.unwrap();
    }

    #[tokio::test]
    async fn recovery_serving_has_a_bounded_actionable_overflow() {
        let (tx, rx) = mpsc::channel(32);
        let started = Arc::new(Semaphore::new(0));
        let release = Arc::new(Semaphore::new(0));
        let rejected = Arc::new(Semaphore::new(0));
        let diagnostics = Arc::new(DispatchDiagnostics::default());

        let dispatcher = {
            let started = Arc::clone(&started);
            let release = Arc::clone(&release);
            let rejected = Arc::clone(&rejected);
            tokio::spawn(run_event_dispatcher(
                rx,
                Arc::clone(&diagnostics),
                move |event, admission| {
                    let started = Arc::clone(&started);
                    let release = Arc::clone(&release);
                    let rejected = Arc::clone(&rejected);
                    async move {
                        match (event, admission) {
                            (TestEvent::Recovery(_), DispatchAdmission::Admitted) => {
                                started.add_permits(1);
                                let _permit = release.acquire().await.unwrap();
                            }
                            (TestEvent::Recovery(_), DispatchAdmission::Saturated) => {
                                rejected.add_permits(1);
                            }
                            _ => {}
                        }
                    }
                },
            ))
        };

        for index in 0..MAX_ACTIVE_RECOVERY {
            tx.send(TestEvent::Recovery(index)).await.unwrap();
        }
        started
            .acquire_many(MAX_ACTIVE_RECOVERY as u32)
            .await
            .unwrap()
            .forget();
        tx.send(TestEvent::Recovery(MAX_ACTIVE_RECOVERY))
            .await
            .unwrap();

        tokio::time::timeout(Duration::from_secs(1), rejected.acquire())
            .await
            .expect("recovery overflow must produce an actionable rejection")
            .unwrap()
            .forget();
        let snapshot = diagnostics.snapshot();
        assert_eq!(snapshot.active_recovery, MAX_ACTIVE_RECOVERY);
        assert_eq!(snapshot.active_recovery_high_water, MAX_ACTIVE_RECOVERY);
        assert_eq!(snapshot.recovery_saturated_total, 1);

        release.add_permits(MAX_ACTIVE_RECOVERY);
        drop(tx);
        dispatcher.await.unwrap();
    }

    #[tokio::test]
    async fn stalled_capacity_rejection_does_not_block_completion() {
        let (tx, rx) = mpsc::channel(64);
        let started = Arc::new(Semaphore::new(0));
        let release_requests = Arc::new(Semaphore::new(0));
        let release_rejection = Arc::new(Semaphore::new(0));
        let completion = Arc::new(Semaphore::new(0));

        let diagnostics = Arc::new(DispatchDiagnostics::default());
        let dispatcher = {
            let started = Arc::clone(&started);
            let release_requests = Arc::clone(&release_requests);
            let release_rejection = Arc::clone(&release_rejection);
            let completion = Arc::clone(&completion);
            tokio::spawn(run_event_dispatcher(
                rx,
                Arc::clone(&diagnostics),
                move |event, admission| {
                    let started = Arc::clone(&started);
                    let release_requests = Arc::clone(&release_requests);
                    let release_rejection = Arc::clone(&release_rejection);
                    let completion = Arc::clone(&completion);
                    async move {
                        match (event, admission) {
                            (TestEvent::Request(index), DispatchAdmission::Admitted)
                                if index < MAX_ACTIVE_REQUESTS =>
                            {
                                started.add_permits(1);
                                let _permit = release_requests.acquire().await.unwrap();
                            }
                            (TestEvent::Request(_), DispatchAdmission::Saturated) => {
                                let _permit = release_rejection.acquire().await.unwrap();
                            }
                            (TestEvent::Completion, DispatchAdmission::Admitted) => {
                                completion.add_permits(1);
                            }
                            _ => {}
                        }
                    }
                },
            ))
        };

        for index in 0..MAX_ACTIVE_REQUESTS {
            tx.send(TestEvent::Request(index)).await.unwrap();
        }
        started
            .acquire_many(MAX_ACTIVE_REQUESTS as u32)
            .await
            .unwrap()
            .forget();
        for index in MAX_ACTIVE_REQUESTS..MAX_ACTIVE_REQUESTS + MAX_ACTIVE_REJECTIONS + 1 {
            tx.send(TestEvent::Request(index)).await.unwrap();
        }
        tx.send(TestEvent::Completion).await.unwrap();

        tokio::time::timeout(Duration::from_secs(1), completion.acquire())
            .await
            .expect("a stalled nack must not park the transport drain")
            .unwrap()
            .forget();
        let saturated = diagnostics.snapshot();
        assert_eq!(
            saturated.active_rejections, MAX_ACTIVE_REJECTIONS,
            "stalled capacity replies must consume only the fixed rejection slots"
        );
        assert_eq!(saturated.rejection_dropped_total, 1);

        release_rejection.add_permits(MAX_ACTIVE_REJECTIONS);
        release_requests.add_permits(MAX_ACTIVE_REQUESTS);
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let snapshot = diagnostics.snapshot();
                if snapshot.active_requests == 0 && snapshot.active_rejections == 0 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("completed dispatcher handles must be reaped while the event channel stays open");
        drop(tx);
        dispatcher.await.unwrap();
    }

    #[tokio::test]
    async fn stalled_completion_does_not_block_recovery_serving() {
        let (tx, rx) = mpsc::channel(8);
        let completion_started = Arc::new(Semaphore::new(0));
        let release_completion = Arc::new(Semaphore::new(0));
        let request_observed = Arc::new(Semaphore::new(0));
        let diagnostics = Arc::new(DispatchDiagnostics::default());

        let dispatcher = {
            let completion_started = Arc::clone(&completion_started);
            let release_completion = Arc::clone(&release_completion);
            let request_observed = Arc::clone(&request_observed);
            tokio::spawn(run_event_dispatcher(
                rx,
                Arc::clone(&diagnostics),
                move |event, admission| {
                    let completion_started = Arc::clone(&completion_started);
                    let release_completion = Arc::clone(&release_completion);
                    let request_observed = Arc::clone(&request_observed);
                    async move {
                        match (event, admission) {
                            (TestEvent::Completion, DispatchAdmission::Admitted) => {
                                completion_started.add_permits(1);
                                let _permit = release_completion.acquire().await.unwrap();
                            }
                            (TestEvent::Recovery(_), DispatchAdmission::Admitted) => {
                                request_observed.add_permits(1);
                            }
                            _ => {}
                        }
                    }
                },
            ))
        };

        tx.send(TestEvent::Completion).await.unwrap();
        completion_started.acquire().await.unwrap().forget();
        tx.send(TestEvent::Recovery(0)).await.unwrap();

        tokio::time::timeout(Duration::from_secs(1), request_observed.acquire())
            .await
            .expect("durable completion work must not head-of-line block CAR serving")
            .unwrap()
            .forget();
        assert_eq!(diagnostics.snapshot().active_completions, 1);

        release_completion.add_permits(1);
        drop(tx);
        dispatcher.await.unwrap();
        assert_eq!(diagnostics.snapshot().active_completions, 0);
    }
}
