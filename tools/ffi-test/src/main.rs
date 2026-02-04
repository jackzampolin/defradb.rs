use clap::{Parser, Subcommand};

mod builder;
mod commands;
mod config;
mod error;
mod report;
mod runner;
mod worktree;

#[derive(Parser)]
#[command(name = "ffi-test")]
#[command(about = "FFI compatibility testing tool for defradb.rs")]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Run FFI tests for a package
    Run {
        /// Test package path (e.g., "query/simple", "mutation/create")
        package: String,

        /// Filter tests by name pattern
        #[arg(short = 't', long)]
        test: Option<String>,

        /// Show verbose output
        #[arg(short, long)]
        verbose: bool,

        /// Skip FFI build step
        #[arg(long)]
        skip_build: bool,
    },

    /// Show status of FFI tests
    Status {
        /// Show status for all worktrees
        #[arg(long)]
        all: bool,
    },

    /// Show diff between test runs
    Diff {
        /// Test package path to compare
        package: String,
    },

    /// Manage paired worktrees
    Worktree {
        #[command(subcommand)]
        command: WorktreeCommands,
    },
}

#[derive(Subcommand)]
enum WorktreeCommands {
    /// List all paired worktrees
    List,

    /// Create a new paired worktree
    Create {
        /// Worktree suffix (e.g., "index" creates defradb.rs-index and defradb-index)
        suffix: String,
    },

    /// Remove a paired worktree
    Remove {
        /// Worktree suffix to remove
        suffix: String,

        /// Force removal even with uncommitted changes
        #[arg(long)]
        force: bool,

        /// Also delete the branch
        #[arg(long)]
        delete_branch: bool,
    },
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    let result = match cli.command {
        Commands::Run {
            package,
            test,
            verbose,
            skip_build,
        } => commands::run::execute(&package, test.as_deref(), verbose, skip_build).await,

        Commands::Status { all } => commands::status::execute(all).await,

        Commands::Diff { package } => commands::diff::execute(&package).await,

        Commands::Worktree { command } => match command {
            WorktreeCommands::List => commands::worktree::list().await,
            WorktreeCommands::Create { suffix } => commands::worktree::create(&suffix).await,
            WorktreeCommands::Remove {
                suffix,
                force,
                delete_branch,
            } => commands::worktree::remove(&suffix, force, delete_branch).await,
        },
    };

    if let Err(e) = result {
        eprintln!("\x1b[31mError:\x1b[0m {}", e);
        std::process::exit(1);
    }
}
