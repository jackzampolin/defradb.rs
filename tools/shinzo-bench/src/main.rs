mod commands;
mod config;
mod metrics;
mod process;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(
    name = "shinzo-bench",
    about = "Shinzo indexer benchmark and test harness"
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Build release, start defra + indexer, start watchdog
    Start(commands::StartArgs),
    /// Graceful stop of all processes
    Stop,
    /// Stop + wipe /tmp/shinzo-test/
    Clean,
    /// PIDs, ports, latest block height, uptime
    Status,
    /// Tail logs with color
    Logs(commands::LogsArgs),
    /// Live dashboard: RSS, CPU, disk, blocks/min, errors
    Monitor,
    /// Execute GraphQL query
    Query(commands::QueryArgs),
    /// Dump current metrics as JSON
    Metrics,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Start(args) => commands::start(args).await,
        Commands::Stop => commands::stop().await,
        Commands::Clean => commands::clean().await,
        Commands::Status => commands::status().await,
        Commands::Logs(args) => commands::logs(args).await,
        Commands::Monitor => commands::monitor().await,
        Commands::Query(args) => commands::query(args).await,
        Commands::Metrics => commands::metrics().await,
    }
}
