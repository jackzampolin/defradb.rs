use clap::Parser;

use crate::config;
use crate::process;

#[derive(Parser)]
pub struct QueryArgs {
    /// GraphQL query string
    pub query: String,
}

pub async fn query(args: QueryArgs) -> anyhow::Result<()> {
    let ports = process::load_ports(&config::ports_file());
    let api_port: u16 = ports
        .get("API_PORT")
        .and_then(|p| p.parse().ok())
        .ok_or_else(|| anyhow::anyhow!("No API port found. Is defra running in HTTP mode?"))?;

    let url = format!("http://127.0.0.1:{}/api/v0/graphql", api_port);
    let body = serde_json::json!({ "query": args.query });

    let client = reqwest::Client::new();
    let resp = client.post(&url).json(&body).send().await?;

    let status = resp.status();
    let text = resp.text().await?;

    if status.is_success() {
        // Pretty-print JSON
        if let Ok(json) = serde_json::from_str::<serde_json::Value>(&text) {
            println!("{}", serde_json::to_string_pretty(&json)?);
        } else {
            println!("{}", text);
        }
    } else {
        eprintln!("HTTP {}: {}", status, text);
    }

    Ok(())
}
