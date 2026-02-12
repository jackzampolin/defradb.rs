use std::time::Duration;

use anyhow::Result;
use reqwest::Client;

/// Poll a node's health endpoint until it returns 200.
pub async fn health_check(client: &Client, url: &str, timeout: Duration) -> Result<()> {
    let health_url = format!("{}/health-check", url);
    let deadline = tokio::time::Instant::now() + timeout;

    loop {
        match client.get(&health_url).send().await {
            Ok(resp) if resp.status().is_success() => return Ok(()),
            _ => {}
        }

        if tokio::time::Instant::now() >= deadline {
            return Err(anyhow::anyhow!("health check timed out for {}", health_url));
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Run health checks on all URLs concurrently.
pub async fn health_check_all(client: &Client, urls: &[String], timeout: Duration) -> Result<()> {
    let checks: Vec<_> = urls
        .iter()
        .map(|url| health_check(client, url, timeout))
        .collect();

    let results = futures::future::join_all(checks).await;
    for result in results {
        result?;
    }
    Ok(())
}
