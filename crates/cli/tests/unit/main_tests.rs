use super::*;

#[test]
fn profile_flag_controls_startup_profiling() {
    for (args, expected) in [
        (vec!["defra", "start"], false),
        (vec!["defra", "start", "--profile"], true),
        (vec!["defra", "version"], false),
    ] {
        let cli = Cli::try_parse_from(args).unwrap();
        assert_eq!(should_profile(&cli), expected);
    }
}
