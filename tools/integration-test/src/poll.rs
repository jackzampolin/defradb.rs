use std::time::{Duration, Instant};

/// Poll until a condition is met or timeout expires.
///
/// Calls `f` repeatedly at `interval` intervals until it returns `true`,
/// or panics with `label` if the deadline is exceeded.
pub async fn poll_until<F>(mut f: F, timeout: Duration, interval: Duration, label: &str)
where
    F: FnMut() -> bool,
{
    let deadline = Instant::now() + timeout;
    loop {
        if f() {
            return;
        }
        assert!(Instant::now() < deadline, "{} within {:?}", label, timeout);
        tokio::time::sleep(interval).await;
    }
}
