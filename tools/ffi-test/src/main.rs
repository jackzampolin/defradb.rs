use std::path::PathBuf;

use clap::{Parser, Subcommand};

mod builder;
mod commands;
mod config;
mod embedding_fixture;
mod error;
mod report;
mod runner;
mod worktree;

#[derive(Parser)]
#[command(name = "ffi-test")]
#[command(about = "FFI compatibility testing tool for defradb.rs")]
#[command(version)]
struct Cli {
    /// Path to the Go DefraDB checkout (overrides DEFRADB_GO_REPO and worktree pairing)
    #[arg(long, global = true, value_name = "PATH")]
    go_path: Option<PathBuf>,

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
        /// Filter to a specific package prefix (e.g., "net", "query")
        package: Option<String>,

        /// Show status for all worktrees
        #[arg(long)]
        all: bool,

        /// Package path depth to display (default: 1 = top-level only)
        #[arg(short, long, default_value = "1")]
        depth: usize,
    },

    /// Show diff between test runs
    Diff {
        /// Test package path to compare
        package: String,
    },

    /// Show test output/logs from last run
    Logs {
        /// Test package path (e.g., "query/simple")
        package: String,

        /// Filter to specific test by name pattern
        #[arg(short = 't', long)]
        test: Option<String>,

        /// Show only failed tests
        #[arg(long)]
        failed: bool,

        /// Show output for all tests (not just failures)
        #[arg(short, long)]
        all: bool,
    },

    /// List available test packages from the Go test directory
    Packages {
        /// Filter to packages under a prefix (e.g., "net", "query")
        package: Option<String>,
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
    let Cli { go_path, command } = Cli::parse();

    let result = match command {
        Commands::Run {
            package,
            test,
            verbose,
            skip_build,
        } => commands::run::execute(&package, test.as_deref(), verbose, skip_build, go_path).await,

        Commands::Status {
            package,
            all,
            depth,
        } => commands::status::execute(all, depth, package.as_deref(), go_path).await,

        Commands::Diff { package } => commands::diff::execute(&package, go_path).await,

        Commands::Logs {
            package,
            test,
            failed,
            all,
        } => commands::logs::execute(&package, test.as_deref(), failed, all, go_path).await,

        Commands::Packages { package } => {
            commands::packages::execute(package.as_deref(), go_path).await
        }

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

#[cfg(test)]
mod tests {
    use super::*;

    fn go_path_of(args: &[&str]) -> Option<PathBuf> {
        Cli::try_parse_from(args).map(|cli| cli.go_path).unwrap()
    }

    #[test]
    fn go_path_is_accepted_before_and_after_the_subcommand() {
        let expected = Some(PathBuf::from("/elsewhere/defradb"));

        assert_eq!(
            go_path_of(&["ffi-test", "--go-path", "/elsewhere/defradb", "packages"]),
            expected
        );
        assert_eq!(
            go_path_of(&["ffi-test", "packages", "--go-path", "/elsewhere/defradb"]),
            expected
        );
    }

    #[test]
    fn go_path_is_absent_when_not_passed() {
        assert_eq!(go_path_of(&["ffi-test", "packages"]), None);
    }
}
