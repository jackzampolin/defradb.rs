use std::time::Duration;

pub fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    payload
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("<non-string panic>")
        .to_owned()
}

pub fn otel_timeout() -> Duration {
    const DEFAULT: Duration = Duration::from_secs(10);
    std::env::var("OTEL_EXPORTER_OTLP_TIMEOUT")
        .ok()
        .and_then(|ms| ms.parse::<u64>().ok())
        .map(Duration::from_millis)
        .unwrap_or(DEFAULT)
}
