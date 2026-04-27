#[cfg(not(feature = "rocksdb"))]
fn main() {
    eprintln!(
        "coding-session-bench requires the `rocksdb` feature.\n\
         Run: cargo run -p defra-node --features rocksdb --bin coding-session-bench -- --help"
    );
    std::process::exit(1);
}

#[cfg(feature = "rocksdb")]
mod rocksdb_runner {
    use std::collections::HashSet;
    use std::path::PathBuf;
    use std::process::ExitCode;
    use std::time::{SystemTime, UNIX_EPOCH};

    use anyhow::{bail, Context, Result};
    use defra_node::benchmark_support::{
        benchmark_case, seed_coding_session_fixture, CodingSessionFixtureConfig,
    };
    use defra_node::{EmbeddedNode, StorageBackend};

    #[derive(Debug)]
    struct BenchOptions {
        data_dir: Option<PathBuf>,
        keep_data: bool,
        reuse: bool,
        explain: bool,
        warmup: usize,
        iterations: usize,
        limit: usize,
        case_filters: Vec<String>,
        fixture: CodingSessionFixtureConfig,
    }

    impl Default for BenchOptions {
        fn default() -> Self {
            Self {
                data_dir: None,
                keep_data: false,
                reuse: false,
                explain: false,
                warmup: 2,
                iterations: 5,
                limit: 10,
                case_filters: Vec::new(),
                fixture: CodingSessionFixtureConfig::default(),
            }
        }
    }

    pub async fn run() -> ExitCode {
        match try_run().await {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => {
                eprintln!("{error:?}");
                ExitCode::FAILURE
            }
        }
    }

    async fn try_run() -> Result<()> {
        init_tracing();
        let options = parse_args()?;
        let owned_temp_dir = match (&options.data_dir, options.reuse) {
            (Some(path), false) => {
                if path.exists() {
                    bail!(
                        "data dir already exists: {} (use --reuse or remove it first)",
                        path.display()
                    );
                }
                None
            }
            (Some(_), true) => None,
            (None, true) => bail!("--reuse requires --data-dir"),
            (None, false) => Some(make_temp_data_dir()),
        };

        let data_dir = owned_temp_dir
            .clone()
            .or_else(|| options.data_dir.clone())
            .context("failed to resolve data dir")?;

        let node = EmbeddedNode::builder()
            .data_path(&data_dir)
            .with_storage_backend(StorageBackend::RocksDb)
            .build()
            .await?;

        let fixture = if options.reuse {
            options.fixture.layout()
        } else {
            seed_coding_session_fixture(&node, &options.fixture).await?
        };
        let stats = options.fixture.estimated_stats();

        println!("backend=rocksdb data_dir={}", data_dir.display());
        println!(
            "fixture: sessions={} messages={} actions={} estimated_payload={}B ({:.1}MiB)",
            stats.sessions,
            stats.messages,
            stats.actions,
            stats.estimated_payload_bytes,
            stats.estimated_payload_mib(),
        );
        println!(
            "shape: hot={} messages/{} actions, medium={} messages/{} actions, background={}x{} messages/{} actions",
            fixture.hot_session.message_count,
            fixture.hot_session.action_count,
            fixture.medium_session.message_count,
            fixture.medium_session.action_count,
            fixture.background_sessions.len(),
            options.fixture.background_session_messages,
            options.fixture.background_session_actions,
        );
        println!(
            "payloads: user={}B assistant={}B action={}B",
            options.fixture.user_message_bytes,
            options.fixture.assistant_message_bytes,
            options.fixture.action_command_bytes,
        );

        let mut cases = fixture.default_cases();
        for case in &mut cases {
            case.limit = options.limit;
        }
        filter_cases(&mut cases, &options.case_filters)?;

        println!(
            "cases={} warmup={} iterations={} explain={}",
            cases.len(),
            options.warmup,
            options.iterations,
            options.explain
        );

        for case in &cases {
            if options.explain {
                let explain_query = case.render_query(true);
                let explain_response = node.execute(&explain_query).await;
                if explain_response.has_errors() {
                    let errors = explain_response
                        .errors
                        .into_iter()
                        .map(|error| error.message)
                        .collect::<Vec<_>>()
                        .join("; ");
                    bail!("{} explain failed: {errors}", case.name);
                }
                let explain_json = explain_response
                    .data
                    .context("missing explain response data")?;
                println!("explain[{}]=", case.name);
                println!("{}", serde_json::to_string_pretty(&explain_json)?);
            }

            let summary = benchmark_case(&node, case, options.warmup, options.iterations).await?;
            println!("{}", summary.render());
        }

        if let Some(path) = owned_temp_dir {
            if options.keep_data {
                println!("kept temp data dir: {}", path.display());
            } else {
                std::fs::remove_dir_all(&path).with_context(|| {
                    format!("failed to remove temp data dir {}", path.display())
                })?;
            }
        }

        Ok(())
    }

    fn init_tracing() {
        if std::env::var_os("RUST_LOG").is_none() {
            return;
        }

        let filter = tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"))
            .add_directive(
                "iroh_quinn_proto::connection=error"
                    .parse()
                    .expect("valid tracing directive"),
            )
            .add_directive(
                "noq_proto::connection=error"
                    .parse()
                    .expect("valid tracing directive"),
            );
        let _ = tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_target(true)
            .with_ansi(true)
            .try_init();
    }

    fn filter_cases(
        cases: &mut Vec<defra_node::benchmark_support::SearchQueryCase>,
        requested: &[String],
    ) -> Result<()> {
        if requested.is_empty() {
            return Ok(());
        }

        let requested = requested.iter().cloned().collect::<HashSet<_>>();
        cases.retain(|case| requested.contains(&case.name));

        if cases.is_empty() {
            bail!(
                "no matching cases. available cases: hot_messages_cargo, hot_messages_wand, hot_actions_cargo, hot_actions_rg, medium_messages_candidate, medium_actions_bench"
            );
        }

        Ok(())
    }

    fn parse_args() -> Result<BenchOptions> {
        let mut options = BenchOptions::default();
        let mut args = std::env::args().skip(1);

        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--help" | "-h" => {
                    print_usage();
                    std::process::exit(0);
                }
                "--profile" => {
                    options.fixture = parse_profile(next_value(&mut args, "--profile")?)?;
                }
                "--data-dir" => {
                    options.data_dir = Some(PathBuf::from(next_value(&mut args, "--data-dir")?));
                }
                "--keep-data" => options.keep_data = true,
                "--reuse" => options.reuse = true,
                "--explain" => options.explain = true,
                "--warmup" => {
                    options.warmup = parse_usize(next_value(&mut args, "--warmup")?, "--warmup")?;
                }
                "--iterations" => {
                    options.iterations =
                        parse_usize(next_value(&mut args, "--iterations")?, "--iterations")?;
                }
                "--limit" => {
                    options.limit = parse_usize(next_value(&mut args, "--limit")?, "--limit")?;
                }
                "--case" => {
                    options
                        .case_filters
                        .push(next_value(&mut args, "--case")?.to_string());
                }
                "--hot-messages" => {
                    options.fixture.hot_session_messages =
                        parse_usize(next_value(&mut args, "--hot-messages")?, "--hot-messages")?;
                }
                "--hot-actions" => {
                    options.fixture.hot_session_actions =
                        parse_usize(next_value(&mut args, "--hot-actions")?, "--hot-actions")?;
                }
                "--medium-messages" => {
                    options.fixture.medium_session_messages = parse_usize(
                        next_value(&mut args, "--medium-messages")?,
                        "--medium-messages",
                    )?;
                }
                "--medium-actions" => {
                    options.fixture.medium_session_actions = parse_usize(
                        next_value(&mut args, "--medium-actions")?,
                        "--medium-actions",
                    )?;
                }
                "--background-sessions" => {
                    options.fixture.background_sessions = parse_usize(
                        next_value(&mut args, "--background-sessions")?,
                        "--background-sessions",
                    )?;
                }
                "--background-messages" => {
                    options.fixture.background_session_messages = parse_usize(
                        next_value(&mut args, "--background-messages")?,
                        "--background-messages",
                    )?;
                }
                "--background-actions" => {
                    options.fixture.background_session_actions = parse_usize(
                        next_value(&mut args, "--background-actions")?,
                        "--background-actions",
                    )?;
                }
                "--user-bytes" => {
                    options.fixture.user_message_bytes =
                        parse_usize(next_value(&mut args, "--user-bytes")?, "--user-bytes")?;
                }
                "--assistant-bytes" => {
                    options.fixture.assistant_message_bytes = parse_usize(
                        next_value(&mut args, "--assistant-bytes")?,
                        "--assistant-bytes",
                    )?;
                }
                "--action-bytes" => {
                    options.fixture.action_command_bytes =
                        parse_usize(next_value(&mut args, "--action-bytes")?, "--action-bytes")?;
                }
                other => bail!("unknown argument: {other}"),
            }
        }

        if options.iterations == 0 {
            bail!("--iterations must be greater than zero");
        }
        if options.limit == 0 {
            bail!("--limit must be greater than zero");
        }

        Ok(options)
    }

    fn next_value(args: &mut impl Iterator<Item = String>, flag: &str) -> Result<String> {
        args.next()
            .with_context(|| format!("missing value for {flag}"))
    }

    fn parse_usize(value: String, flag: &str) -> Result<usize> {
        value
            .parse::<usize>()
            .with_context(|| format!("invalid value for {flag}: {value}"))
    }

    fn parse_profile(value: String) -> Result<CodingSessionFixtureConfig> {
        match value.as_str() {
            "default" | "dev" => Ok(CodingSessionFixtureConfig::default()),
            "smoke" => Ok(CodingSessionFixtureConfig::smoke_test()),
            "large" => Ok(CodingSessionFixtureConfig::large()),
            other => bail!("unknown profile: {other} (expected smoke, default, or large)"),
        }
    }

    fn make_temp_data_dir() -> PathBuf {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();
        std::env::temp_dir().join(format!(
            "defra-coding-session-bench-{}-{}",
            std::process::id(),
            suffix
        ))
    }

    fn print_usage() {
        println!(
            "coding-session-bench\n\
             \n\
             Runs a reduced coding-session-style nested BM25 benchmark against a local RocksDB store.\n\
             \n\
             Usage:\n\
               cargo run -p defra-node --features rocksdb --bin coding-session-bench -- [options]\n\
             \n\
             Options:\n\
               --profile NAME            Fixture profile: smoke, default, or large\n\
               --data-dir PATH            Persist the RocksDB store at PATH\n\
               --reuse                    Reuse an existing fixture at --data-dir\n\
               --keep-data                Keep the auto-created temp RocksDB dir after the run\n\
               --warmup N                 Warmup iterations per case (default: 2)\n\
               --iterations N             Timed iterations per case (default: 5)\n\
               --limit N                  GraphQL limit per query (default: 10)\n\
               --case NAME                Run only a named case (repeatable)\n\
               --explain                  Print @explain(type: execute) output for each case\n\
               --hot-messages N           Hot session message count (default: 1500)\n\
               --hot-actions N            Hot session action count (default: 900)\n\
               --medium-messages N        Medium session message count (default: 500)\n\
               --medium-actions N         Medium session action count (default: 250)\n\
               --background-sessions N    Background session count (default: 12)\n\
               --background-messages N    Background session message count (default: 120)\n\
               --background-actions N     Background session action count (default: 60)\n\
               --user-bytes N             Target bytes for user messages before session scaling\n\
               --assistant-bytes N        Target bytes for assistant messages before session scaling\n\
               --action-bytes N           Target bytes for action commands before session scaling\n\
             \n\
             Cases:\n\
               hot_messages_cargo\n\
               hot_messages_wand\n\
               hot_actions_cargo\n\
               hot_actions_rg\n\
               medium_messages_candidate\n\
               medium_actions_bench"
        );
    }
}

#[cfg(feature = "rocksdb")]
#[tokio::main]
async fn main() -> std::process::ExitCode {
    rocksdb_runner::run().await
}
