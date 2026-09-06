use clap::Parser;
use cli::cli::{Cli, Command};
use cli::config::Config;
use storage::stores::RetryInfo;

#[test]
fn omitted_retry_intervals_keep_the_default_ladder() {
    let mut value = serde_json::to_value(Config::default()).unwrap();
    value
        .as_object_mut()
        .unwrap()
        .remove("replicator_retry_intervals");
    let config: Config = serde_json::from_value(value).unwrap();
    config.validate().unwrap();
    assert_eq!(
        config.replicator_retry_intervals,
        Config::default().replicator_retry_intervals
    );
}

#[test]
fn config_rejects_empty_or_zero_retry_intervals() {
    for intervals in [vec![], vec![0], vec![1, 0, 3]] {
        let config = Config {
            replicator_retry_intervals: intervals,
            ..Config::default()
        };
        assert!(config
            .validate()
            .unwrap_err()
            .to_string()
            .contains("replicator retry intervals"));
    }
}

#[test]
fn retry_flag_overrides_config_and_changes_the_runtime_schedule() {
    let cli =
        Cli::try_parse_from(["defra", "start", "--replicator-retry-intervals=1,600"]).unwrap();
    let Command::Start(args) = cli.command else {
        panic!("expected start command")
    };
    let mut config = Config::default();
    args.apply_to_config(&mut config).unwrap();
    assert_eq!(config.replicator_retry_intervals, [1, 600]);
    let mut info = RetryInfo::new_initial();
    let before = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    info.bump_with_schedule("peer", &config.retry_schedule().unwrap());
    let after = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    assert!(info.next_retry_unix > before && info.next_retry_unix <= after + 1);
}

#[test]
fn zero_retry_flag_is_rejected_before_startup() {
    let cli = Cli::try_parse_from(["defra", "start", "--replicator-retry-intervals=1,0"]).unwrap();
    let Command::Start(args) = cli.command else {
        panic!("expected start command")
    };
    assert!(args.apply_to_config(&mut Config::default()).is_err());
}

#[test]
fn malformed_retry_flags_are_rejected() {
    for value in ["-1", "1,nope", "4294967296"] {
        assert!(Cli::try_parse_from([
            "defra",
            "start",
            &format!("--replicator-retry-intervals={value}")
        ])
        .is_err());
    }
}
