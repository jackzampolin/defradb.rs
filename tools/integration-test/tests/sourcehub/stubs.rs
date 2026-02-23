use integration_test::TestCluster;

/// Audit gap: Circuit breaker trips after repeated SourceHub failures
/// and recovers after the backoff window. Requires SourceHub infrastructure.
async fn circuit_breaker_trip_recovery(_cluster: TestCluster) {
    todo!("implement: circuit breaker trip/recovery test")
}

#[tokio::test]
#[ignore]
async fn rust_circuit_breaker_trip_recovery() {
    let cluster = TestCluster::builder().rust_nodes(1).build().await.unwrap();
    circuit_breaker_trip_recovery(cluster).await;
}

#[tokio::test]
#[ignore]
async fn go_circuit_breaker_trip_recovery() {
    let cluster = TestCluster::builder().go_nodes(1).build().await.unwrap();
    circuit_breaker_trip_recovery(cluster).await;
}

/// Audit gap: Cached policy entries expire after TTL and are re-fetched
/// from SourceHub on next access. Requires SourceHub infrastructure.
async fn policy_cache_ttl_expiry(_cluster: TestCluster) {
    todo!("implement: policy cache TTL expiry test")
}

#[tokio::test]
#[ignore]
async fn rust_policy_cache_ttl_expiry() {
    let cluster = TestCluster::builder().rust_nodes(1).build().await.unwrap();
    policy_cache_ttl_expiry(cluster).await;
}

#[tokio::test]
#[ignore]
async fn go_policy_cache_ttl_expiry() {
    let cluster = TestCluster::builder().go_nodes(1).build().await.unwrap();
    policy_cache_ttl_expiry(cluster).await;
}
